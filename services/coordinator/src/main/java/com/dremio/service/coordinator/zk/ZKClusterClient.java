/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.service.coordinator.zk;

import com.dremio.common.AutoCloseables;
import com.dremio.common.concurrent.CloseableSchedulerThreadPool;
import com.dremio.common.concurrent.NamedThreadFactory;
import com.dremio.configfeature.ConfigFeatureProvider;
import com.dremio.configfeature.Features;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.service.coordinator.CoordinatorLostHandle;
import com.dremio.service.coordinator.DistributedSemaphore;
import com.dremio.service.coordinator.ElectionListener;
import com.dremio.service.coordinator.ElectionRegistrationHandle;
import com.dremio.service.coordinator.LinearizableHierarchicalStore;
import com.dremio.service.coordinator.ObservableConnectionLostHandler;
import com.dremio.telemetry.api.metrics.Counter;
import com.dremio.telemetry.api.metrics.Metrics;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Provider;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatch.CloseMode;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.utils.ZookeeperFactory;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class ZKClusterClient implements com.dremio.service.Service {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(ZKClusterClient.class);
  private static final Pattern ZK_COMPLEX_STRING = Pattern.compile("(^[^/]*?)/(?:(.*)/)?([^/]*)$");
  public static final String ZK_LOST_HANDLER_MODULE_CLASS =
      "dremio.coordinator_lost_handle.module.class";

  private static final Counter ZK_SUPERVISOR_FAILED_COUNTER =
      Metrics.newCounter(
          Metrics.join("ZKClusterClient", "supervisorProbeFailed"), Metrics.ResetType.NEVER);
  private static final Counter ZK_SUPERVISOR_EXIT_APP_COUNTER =
      Metrics.newCounter(
          Metrics.join("ZKClusterClient", "supervisorExitApplication"), Metrics.ResetType.NEVER);
  private static final Counter ZK_SESSION_LOST_COUNTER =
      Metrics.newCounter(Metrics.join("ZKClusterClient", "sessionLost"), Metrics.ResetType.NEVER);
  private static final Counter ZK_SESSION_SUSPENDED_COUNTER =
      Metrics.newCounter(
          Metrics.join("ZKClusterClient", "sessionSuspended"), Metrics.ResetType.NEVER);
  private static final Counter ZK_RECONNECTED_COUNTER =
      Metrics.newCounter(
          Metrics.join("ZKClusterClient", "sessionRecovered"), Metrics.ResetType.NEVER);

  private final String clusterId;
  private final CountDownLatch initialConnection = new CountDownLatch(1);
  private final ListeningExecutorService executorService =
      MoreExecutors.listeningDecorator(
          Executors.newCachedThreadPool(new NamedThreadFactory("zk-curator-")));
  private CuratorFramework curator;
  private ServiceDiscovery<NodeEndpoint> discovery;
  private ZKClusterConfig config;
  private final ZookeeperFactory zkFactory;
  private final String connect;
  private String connectionString;
  private Provider<Integer> localPortProvider;
  private volatile boolean closed = false;
  private final CoordinatorLostHandle connectionLostHandler;
  private Boolean isConnected;
  private final String executorZkSupervisor = "coordinator-zk-supervisor-";
  private final ScheduledExecutorService scheduleExecutorService =
      Executors.newScheduledThreadPool(1, new NamedThreadFactory(executorZkSupervisor));
  private final ExecutorService executorServiceSupervisor = Executors.newSingleThreadExecutor();

  private int currentNumberOfSupervisorProbeFailures = 0;
  private final String clusterIdPath;

  private final ConfigFeatureProvider configFeatureProvider;

  public ZKClusterClient(ZKClusterConfig config, String connect) throws IOException {
    this(config, connect, null, new ZKClientFactory());
  }

  public ZKClusterClient(ZKClusterConfig config, String connect, ZookeeperFactory zkFactory)
      throws IOException {
    this(config, connect, null, zkFactory);
  }

  public ZKClusterClient(ZKClusterConfig config, Provider<Integer> localPort) throws IOException {
    this(config, null, localPort, new ZKClientFactory());
  }

  private ZKClusterClient(
      ZKClusterConfig config,
      String connect,
      Provider<Integer> localPort,
      ZookeeperFactory zkFactory)
      throws IOException {
    this.configFeatureProvider = config.getConfigFeatureProvider();
    this.localPortProvider = localPort;
    this.connect = connect;
    this.config = config;
    this.zkFactory = zkFactory;
    String clusterId = config.getClusterId();
    if (connect != null) {
      final Matcher m = ZK_COMPLEX_STRING.matcher(connect);
      if (m.matches()) {
        clusterId = m.group(3);
      }
    }
    this.clusterId = clusterId;
    this.connectionLostHandler =
        config.isConnectionHandleEnabled()
            ? config.getConnectionLostHandler()
            : ObservableConnectionLostHandler.OBSERVABLE_LOST_HANDLER.get();
    this.clusterIdPath = "/" + this.clusterId;
  }

  @Override
  public void start() throws Exception {
    if (localPortProvider != null) {
      connectionString = "localhost:" + localPortProvider.get();
    } else if (this.connect == null || this.connect.isEmpty()) {
      connectionString = config.getConnection();
    } else {
      connectionString = this.connect;
    }

    String zkRoot = config.getRoot();

    // check if this is a complex zk string.  If so, parse into components.
    if (connectionString != null) {
      Matcher m = ZK_COMPLEX_STRING.matcher(connectionString);
      if (m.matches()) {
        connectionString = m.group(1);
        zkRoot = m.group(2);
      }
    }

    logger.info("Connect: {}, zkRoot: {}, clusterId: {}", connectionString, zkRoot, clusterId);

    RetryPolicy rp =
        new BoundedExponentialDelay(
            config.getRetryBaseDelayMilliSecs(),
            config.getRetryMaxDelayMilliSecs(),
            config.isRetryUnlimited(),
            config.getRetryLimit());

    curator =
        CuratorFrameworkFactory.builder()
            .namespace(zkRoot)
            .connectionTimeoutMs(config.getConnectionTimeoutMilliSecs())
            .sessionTimeoutMs(config.getSessionTimeoutMilliSecs())
            .maxCloseWaitMs(config.getRetryMaxDelayMilliSecs())
            .retryPolicy(rp)
            .connectString(connectionString)
            .zookeeperFactory(zkFactory)
            .build();
    curator.getConnectionStateListenable().addListener(new InitialConnectionListener());
    curator.getConnectionStateListenable().addListener(new ConnectionListener());
    curator.start();
    discovery = newDiscovery(clusterId);

    logger.info(
        "Starting ZKClusterClient, ZK_TIMEOUT:{}, ZK_SESSION_TIMEOUT:{}, ZK_RETRY_MAX_DELAY:{}, "
            + "ZK_RETRY_UNLIMITED:{}, ZK_RETRY_LIMIT:{}, CONNECTION_HANDLE_ENABLED:{}, SUPERVISOR_INTERVAL:{}, "
            + "SUPERVISOR_READ_TIMEOUT:{}, SUPERVISOR_MAX_FAILURES:{}",
        config.getConnectionTimeoutMilliSecs(),
        config.getSessionTimeoutMilliSecs(),
        config.getRetryMaxDelayMilliSecs(),
        config.isRetryUnlimited(),
        config.getRetryLimit(),
        config.isConnectionHandleEnabled(),
        config.getZkSupervisorIntervalMilliSec(),
        config.getZkSupervisorReadTimeoutMilliSec(),
        config.getZkSupervisorMaxFailures());

    discovery.start();

    this.scheduleExecutorService.scheduleAtFixedRate(
        () -> {
          try {
            runSupervisorCheck();
          } catch (Exception e) {
            logger.error("Error in : " + e.getMessage(), e);
          }
        },
        config.getZkSupervisorIntervalMilliSec(),
        config.getZkSupervisorIntervalMilliSec(),
        TimeUnit.MILLISECONDS);

    if (!config.isRetryUnlimited()
        && !this.initialConnection.await(
            config.getInitialTimeoutMilliSecs(), TimeUnit.MILLISECONDS)) {
      logger.info("Failed to get initial connection to ZK");
      connectionLostHandler.handleConnectionState(ConnectionState.LOST);
    } else {
      this.initialConnection.await();
    }
  }

  private void runSupervisorCheck() {
    if (configFeatureProvider != null
        && configFeatureProvider.isFeatureEnabled(
            Features.COORDINATOR_ZK_SUPERVISOR.getFeatureName())) {
      boolean isProbeSucceeded = false;
      if (isConnected == null || !isConnected) {
        logger.error("ZKClusterClient: Not connected to ZK.");
      } else {
        try {
          RunnableFuture<Boolean> checkGetServiceNames =
              new FutureTask<>(() -> (getServiceNames() != null));
          executorServiceSupervisor.execute(checkGetServiceNames);
          if (!checkGetServiceNames.get(
              config.getZkSupervisorReadTimeoutMilliSec(), TimeUnit.MILLISECONDS)) {
            logger.error("ZKClusterClient: cluster id path {} not found", this.clusterIdPath);
          } else {
            isProbeSucceeded = true;
            currentNumberOfSupervisorProbeFailures = 0;
            logger.debug("ZKClusterClient: probe ok to cluster id path {}", this.clusterIdPath);
          }
        } catch (TimeoutException e) {
          logger.error(
              "ZKClusterClient: Timeout while trying to check for zk cluster id path {} ",
              this.clusterIdPath,
              e);
        } catch (Exception e) {
          logger.error(
              "ZKClusterClient: Exception while trying to check for zk cluster id path {} ",
              this.clusterIdPath,
              e);
        }
      }
      if (!isProbeSucceeded) {
        ZK_SUPERVISOR_FAILED_COUNTER.increment();
        currentNumberOfSupervisorProbeFailures++;
        if (currentNumberOfSupervisorProbeFailures >= config.getZkSupervisorMaxFailures()) {
          ZK_SUPERVISOR_EXIT_APP_COUNTER.increment();
          logger.error(
              "ZKClusterClient: max number of failures has reached [{}/{}]. Calling probe action for out of service",
              currentNumberOfSupervisorProbeFailures,
              config.getZkSupervisorMaxFailures());
          Runtime.getRuntime().halt(1);
        } else {
          logger.warn(
              "ZKClusterClient: probe failed. Attempt[{}] of max[{}]. Next in {} msec",
              currentNumberOfSupervisorProbeFailures,
              config.getZkSupervisorMaxFailures(),
              config.getZkSupervisorIntervalMilliSec());
        }
      }
    }
  }

  /**
   * This custom factory is used to ensure that Zookeeper clients in Curator are always created
   * using the hostname, so it can be resolved to a new IP in the case of a Zookeeper instance
   * restart.
   */
  @ThreadSafe
  static class ZKClientFactory implements ZookeeperFactory {
    private ZooKeeper client;
    private String connectString;
    private int sessionTimeout;
    private boolean canBeReadOnly;

    @Override
    public ZooKeeper newZooKeeper(
        String connectString, int sessionTimeout, Watcher watcher, boolean canBeReadOnly)
        throws Exception {
      Preconditions.checkNotNull(connectString);
      Preconditions.checkArgument(
          sessionTimeout > 0, "sessionTimeout should be a positive integer");
      if (client == null) {
        this.connectString = connectString;
        this.sessionTimeout = sessionTimeout;
        this.canBeReadOnly = canBeReadOnly;
      }
      logger.info(
          "Creating new Zookeeper client with arguments: {}, {}, {}.",
          this.connectString,
          this.sessionTimeout,
          this.canBeReadOnly);
      this.client =
          new ZooKeeper(this.connectString, this.sessionTimeout, watcher, this.canBeReadOnly);
      return this.client;
    }
  }

  @VisibleForTesting
  ZooKeeper getZooKeeperClient() throws Exception {
    return curator.getZookeeperClient().getZooKeeper();
  }

  @VisibleForTesting
  String getConnectionString() {
    return connectionString;
  }

  @VisibleForTesting
  public void setPortProvider(Provider<Integer> portProvider) {
    this.localPortProvider = portProvider;
  }

  @Override
  public void close() throws Exception {
    if (!closed) {
      closed = true;
      logger.info("Stopping ZKClusterClient");
      initialConnection.countDown();
      // discovery attempts to close its caches(ie serviceCache) already. however, being good
      // citizens we make sure to
      // explicitly close serviceCache. Not only that we make sure to close serviceCache before
      // discovery to prevent
      // double releasing and disallowing jvm to spit bothering warnings. simply put, we are great!
      AutoCloseables.close(
          discovery, curator, CloseableSchedulerThreadPool.of(executorService, logger));

      CloseableSchedulerThreadPool.close(this.scheduleExecutorService, logger);
      CloseableSchedulerThreadPool.close(this.executorServiceSupervisor, logger);

      logger.info("Stopped ZKClusterClient");
    }
  }

  public DistributedSemaphore getSemaphore(String name, int maximumLeases) {
    try {
      return new ZkDistributedSemaphore(
          curator, clusterIdPath + "/semaphore/" + name, maximumLeases);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public LinearizableHierarchicalStore getHierarchicalStore() {
    final ZKLinearizableStore store = new ZKLinearizableStore(curator, getRootLatchPath());
    if (connectionLostHandler instanceof ObservableConnectionLostHandler) {
      logger.info("Attaching connection lost observer");
      ((ObservableConnectionLostHandler) connectionLostHandler).attachObserver(store);
    } else {
      logger.info("Connection handle is {}", connectionLostHandler.getClass().getSimpleName());
    }
    return store;
  }

  public Iterable<String> getServiceNames() throws Exception {
    return curator.getChildren().forPath(clusterIdPath);
  }

  public ElectionRegistrationHandle joinElection(
      final String name, final ElectionListener listener) {
    final String id = UUID.randomUUID().toString();
    // In case of multicluster Dremio env. that use the same zookeeper
    // we need a root per Dremio clusterId
    final String latchPath = getRootLatchPath() + name;
    final LeaderLatch leaderLatch = new LeaderLatch(curator, latchPath, id, CloseMode.SILENT);

    logger.info("joinElection called {} - {}.", id, name);

    final AtomicReference<ListenableFuture<?>> newLeaderRef = new AtomicReference<>();

    // incremented every time this node is elected as leader.
    final AtomicLong leaderElectedGeneration = new AtomicLong(0);

    leaderLatch.addListener(
        new LeaderLatchListener() {
          private final long electionTimeoutMs = config.getElectionTimeoutMilliSecs();
          private final long electionPollingMs = config.getElectionPollingMilliSecs();
          private final long delayForLeaderCallbackMs =
              config.getElectionDelayForLeaderCallbackMilliSecs();

          @Override
          public void notLeader() {
            logger.info("Lost latch {} for election {}.", id, name);

            // If latch is closed, notify right away
            if (leaderLatch.getState() == LeaderLatch.State.CLOSED) {
              listener.onCancelled();
              return;
            }

            // For testing purpose
            if (listener instanceof ZKElectionListener) {
              ((ZKElectionListener) listener).onConnectionLoss();
            }

            final long savedLeaderElectedGeneration = leaderElectedGeneration.get();
            // submit a task to get notified about a new leader being elected
            final Future<Void> newLeader =
                executorService.submit(
                    new Callable<Void>() {
                      @Override
                      public Void call() throws Exception {
                        // loop until election happened, or timeout
                        do {
                          // No polling required to check its own state
                          if (leaderLatch.hasLeadership()) {
                            // For testing purpose
                            // (Connection silently restored)
                            if (listener instanceof ZKElectionListener) {
                              ((ZKElectionListener) listener).onReconnection();
                            }

                            // Add a small delay to make sure that curator has made the isLeader()
                            // callback.
                            TimeUnit.MILLISECONDS.sleep(delayForLeaderCallbackMs);
                            return null;
                          }

                          // The next call will block until connection is back
                          Participant participant = leaderLatch.getLeader();

                          // For testing purpose
                          if (listener instanceof ZKElectionListener) {
                            ((ZKElectionListener) listener).onReconnection();
                          }

                          // A dummy participant can be returned if election hasn't happen yet,
                          // but it would not be leader...
                          if (participant.isLeader()) {
                            // Add a small delay to make sure that curator has made the isLeader()
                            // callback.
                            TimeUnit.MILLISECONDS.sleep(delayForLeaderCallbackMs);
                            return null;
                          }

                          TimeUnit.MILLISECONDS.sleep(electionPollingMs);
                        } while (true);
                      }
                    });

            // Wrap the previous task to detect timeout
            final ListenableFuture<Void> newLeaderWithTimeout =
                executorService.submit(
                    new Callable<Void>() {
                      @Override
                      public Void call() throws Exception {
                        try {
                          return newLeader.get(electionTimeoutMs, TimeUnit.MILLISECONDS);
                        } catch (TimeoutException e) {
                          logger.info(
                              "Not able to get election status in {}ms for {}. Cancelling election...",
                              electionTimeoutMs,
                              name);
                          newLeader.cancel(true);
                          throw e;
                        }
                      }
                    });

            // Add callback to notify the user if needed
            Futures.addCallback(
                newLeaderWithTimeout,
                new FutureCallback<Void>() {
                  @Override
                  public void onSuccess(Void v) {
                    checkAndNotifyCancelled(savedLeaderElectedGeneration);
                  }

                  @Override
                  public void onFailure(Throwable t) {
                    if (t instanceof CancellationException) {
                      // ignore
                      return;
                    }
                    checkAndNotifyCancelled(savedLeaderElectedGeneration);
                  }
                },
                MoreExecutors.directExecutor());

            newLeaderRef.set(newLeaderWithTimeout);
          }

          @Override
          public void isLeader() {
            // For testing purpose
            if (listener instanceof ZKElectionListener) {
              ((ZKElectionListener) listener).onBeginIsLeader();
            }

            logger.info("Acquired latch {} for election {}.", id, name);
            // Cancel possible watcher task
            ListenableFuture<?> newLeader = newLeaderRef.getAndSet(null);
            if (newLeader != null) {
              newLeader.cancel(false);
            }

            synchronized (this) {
              leaderElectedGeneration.getAndIncrement();
              listener.onElected();
            }
          }

          // unless this node has become leader again, notify cancel.
          private synchronized void checkAndNotifyCancelled(long svdLeaderGeneration) {
            if (leaderElectedGeneration.get() == svdLeaderGeneration) {
              logger.info("New leader elected. Invoke cancel on listener");
              listener.onCancelled();
            }
          }
        });

    // Time to start the latch
    try {
      leaderLatch.start();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }

    return new ElectionRegistrationHandle() {

      @Override
      public void close() {
        try {
          leaderLatch.close();
          deleteServiceLeaderElectionPath();
        } catch (IOException e) {
          logger.error("Error when closing registration handle for election {}", name, e);
        }
      }

      @Override
      public Object synchronizer() {
        return leaderLatch;
      }

      @Override
      public int instanceCount() {
        try {
          return leaderLatch.getParticipants().size();
        } catch (Exception e) {
          logger.error("Unable to get leader latch participants count for {}", name, e);
        }
        return 0;
      }

      private void deleteServiceLeaderElectionPath() {
        try {
          boolean isZkConnected = isConnected != null && isConnected.equals(true);
          if (isZkConnected && curator.checkExists().forPath(latchPath) != null) {
            List<String> allChildren = curator.getChildren().forPath(latchPath);
            // every element in the election has a child in the election path. When there is no more
            // child elements in
            // the election path (latchPath), we can clear the election path otherwise it will stay
            // in zk, since it is
            // a persistent path in zk.
            if (allChildren.isEmpty()) {
              curator.delete().guaranteed().forPath(latchPath);
              logger.info("Closed leader latch. Deleted latch path {}", latchPath);
            } else {
              logger.info(
                  "Closed leader latch. Nothing to do about latch path {}. It has children: {}",
                  latchPath,
                  allChildren.size());
            }
          } else if (!isZkConnected) {
            logger.warn(
                "Closed leader latch. Nothing to do about latch path {}. Not connected to ZK",
                latchPath);
          }
        } catch (Exception e) {
          logger.warn("Could not delete latch path {}", latchPath, e);
        }
      }
    };
  }

  private String getRootLatchPath() {
    return clusterIdPath + "/leader-latch/";
  }

  public ZKServiceSet newServiceSet(String name) {
    return new ZKServiceSet(name, discovery);
  }

  public void deleteServiceSetZkNode(String name) {
    String zkNodePath = clusterIdPath + "/" + name;
    try {
      boolean isZkConnected = isConnected != null && isConnected.equals(true);
      if (isZkConnected && curator.checkExists().forPath(zkNodePath) != null) {
        List<String> allChildren = curator.getChildren().forPath(zkNodePath);
        if (allChildren.isEmpty()) {
          curator.delete().guaranteed().forPath(zkNodePath);
          logger.info("Deleted ZKServiceSet zk node path {}", zkNodePath);
        } else {
          logger.info(
              "Deleted ZKServiceSet. Nothing to do about zk node path {}. It has children: {}",
              zkNodePath,
              allChildren.size());
        }
      } else if (!isZkConnected) {
        logger.warn(
            "Deleted ZKServiceSet. Nothing to do about zk node path {}. Not connected to ZK",
            zkNodePath);
      }
    } catch (Exception e) {
      logger.warn("Deleted ZKServiceSet - Could not delete zk node path {}", zkNodePath, e);
    }
  }

  private ServiceDiscovery<NodeEndpoint> newDiscovery(String clusterId) {
    return ServiceDiscoveryBuilder.builder(NodeEndpoint.class)
        .basePath(clusterId)
        .client(curator)
        .serializer(ServiceInstanceHelper.SERIALIZER)
        .build();
  }

  private final class InitialConnectionListener implements ConnectionStateListener {

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState) {
      if (newState == ConnectionState.CONNECTED) {
        ZKClusterClient.this.initialConnection.countDown();
        client.getConnectionStateListenable().removeListener(this);
      }
    }
  }

  private final class ConnectionListener implements ConnectionStateListener {

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState) {
      isConnected = newState.isConnected();
      logger.info(
          "ZKClusterClient: new state received[{}] - isConnected: {}", newState, isConnected);
      // adjust metrics
      if (isConnected) {
        ZK_RECONNECTED_COUNTER.increment();
      } else {
        if (ConnectionState.LOST.equals(newState)) {
          ZK_SESSION_LOST_COUNTER.increment();
        } else {
          ZK_SESSION_SUSPENDED_COUNTER.increment();
        }
      }
      connectionLostHandler.handleConnectionState(newState);
    }
  }
}
