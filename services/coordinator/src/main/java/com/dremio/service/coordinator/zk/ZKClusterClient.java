/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import static com.dremio.service.coordinator.ClusterCoordinator.Options.CLUSTER_ID;
import static com.dremio.service.coordinator.ClusterCoordinator.Options.ZK_CONNECTION;
import static com.dremio.service.coordinator.ClusterCoordinator.Options.ZK_ELECTION_POLLING;
import static com.dremio.service.coordinator.ClusterCoordinator.Options.ZK_ELECTION_TIMEOUT;
import static com.dremio.service.coordinator.ClusterCoordinator.Options.ZK_RETRY_BASE_DELAY;
import static com.dremio.service.coordinator.ClusterCoordinator.Options.ZK_RETRY_MAX_DELAY;
import static com.dremio.service.coordinator.ClusterCoordinator.Options.ZK_ROOT;
import static com.dremio.service.coordinator.ClusterCoordinator.Options.ZK_SESSION_TIMEOUT;
import static com.dremio.service.coordinator.ClusterCoordinator.Options.ZK_TIMEOUT;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.zookeeper.ZooKeeper;

import com.dremio.common.AutoCloseables;
import com.dremio.common.concurrent.CloseableSchedulerThreadPool;
import com.dremio.common.concurrent.NamedThreadFactory;
import com.dremio.common.config.SabotConfig;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.service.coordinator.DistributedSemaphore;
import com.dremio.service.coordinator.ElectionListener;
import com.dremio.service.coordinator.ServiceSet.RegistrationHandle;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

class ZKClusterClient implements com.dremio.service.Service {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ZKClusterClient.class);
  private static final Pattern ZK_COMPLEX_STRING = Pattern.compile("(^[^/]*?)/(?:(.*)/)?([^/]*)$");

  private final String clusterId;
  private final CountDownLatch initialConnection = new CountDownLatch(1);
  private final ListeningExecutorService executorService = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool(new NamedThreadFactory("zk-curator-")));
  private CuratorFramework curator;
  private ServiceDiscovery<NodeEndpoint> discovery;
  private SabotConfig config;
  private final String connect;
  private String connectionString;
  private final Provider<Integer> localPortProvider;
  private volatile boolean closed = false;


  public ZKClusterClient(SabotConfig config, String connect) throws IOException {
    this(config, connect, null);
  }

  public ZKClusterClient(SabotConfig config, Provider<Integer> localPort) throws IOException {
    this(config, null, localPort);
  }

  private ZKClusterClient(SabotConfig config, String connect, Provider<Integer> localPort) throws IOException {
    this.localPortProvider = localPort;
    this.connect = connect;
    this.config = config;
    String clusterId = config.getString(CLUSTER_ID);
    if(connect != null){
      final Matcher m = ZK_COMPLEX_STRING.matcher(connect);
      if(m.matches()) {
        clusterId = m.group(3);
      }
    }
    this.clusterId = clusterId;
  }

  @Override
  public void start() throws Exception {
    if(localPortProvider != null){
      connectionString = "localhost:" + localPortProvider.get();
    } else if(this.connect == null || this.connect.isEmpty()){
      connectionString = config.getString(ZK_CONNECTION);
    } else {
      connectionString = this.connect;
    }

    String zkRoot = config.getString(ZK_ROOT);


    // check if this is a complex zk string.  If so, parse into components.
    if(connectionString != null){
      Matcher m = ZK_COMPLEX_STRING.matcher(connectionString);
      if(m.matches()) {
        connectionString = m.group(1);
        zkRoot = m.group(2);
      }
    }

    logger.debug("Connect: {}, zkRoot: {}, clusterId: {}", connectionString, zkRoot, clusterId);

    RetryPolicy rp = new BoundedExponentialDelayWithUnlimitedRetry(
      config.getMilliseconds(ZK_RETRY_BASE_DELAY).intValue(),
      config.getMilliseconds(ZK_RETRY_MAX_DELAY).intValue());
    curator = CuratorFrameworkFactory.builder()
      .namespace(zkRoot)
      .connectionTimeoutMs(config.getInt(ZK_TIMEOUT))
      .sessionTimeoutMs(config.getInt(ZK_SESSION_TIMEOUT))
      .maxCloseWaitMs(config.getMilliseconds(ZK_RETRY_MAX_DELAY).intValue())
      .retryPolicy(rp)
      .connectString(connectionString)
      .build();
    curator.getConnectionStateListenable().addListener(new InitialConnectionListener());
    curator.getConnectionStateListenable().addListener(new ConnectionLogger());
    curator.start();
    discovery = newDiscovery(clusterId);

    logger.info("Starting ZKClusterClient");
    discovery.start();

    this.initialConnection.await();
  }

  @VisibleForTesting
  ZooKeeper getZooKeeperClient() throws Exception {
    return curator.getZookeeperClient().getZooKeeper();
  }

  @VisibleForTesting
  String getConnectionString() {
    return connectionString;
  }

  @Override
  public void close() throws Exception {
    if (!closed) {
      closed = true;
      logger.info("Stopping ZKClusterClient");
      initialConnection.countDown();
      // discovery attempts to close its caches(ie serviceCache) already. however, being good citizens we make sure to
      // explicitly close serviceCache. Not only that we make sure to close serviceCache before discovery to prevent
      // double releasing and disallowing jvm to spit bothering warnings. simply put, we are great!
      AutoCloseables.close(discovery, curator, CloseableSchedulerThreadPool.of(executorService, logger));
      logger.info("Stopped ZKClusterClient");
    }
  }

  public DistributedSemaphore getSemaphore(String name, int maximumLeases) {
    return new ZkDistributedSemaphore(curator, "/semaphore/" + name, maximumLeases);
  }

  public RegistrationHandle joinElection(final String name, final ElectionListener listener) {
    final String id = UUID.randomUUID().toString();
    final LeaderLatch leaderLatch = new LeaderLatch(curator, "/leader-latch/" + name, id, CloseMode.SILENT);

    final AtomicReference<ListenableFuture<?>> newLeaderRef = new AtomicReference<>();

    leaderLatch.addListener(new LeaderLatchListener() {
      private final long electionTimeoutMs = config.getMilliseconds(ZK_ELECTION_TIMEOUT);
      private final long electionPollingMs = config.getMilliseconds(ZK_ELECTION_POLLING);

      @Override
      public void notLeader() {
        logger.debug("Lost latch for election {}.", name);

        // If latch is closed, notify right away
        if (leaderLatch.getState() == LeaderLatch.State.CLOSED) {
          listener.onCancelled();
          return;
        }

        // For testing purpose
        if (listener instanceof ZKElectionListener) {
          ((ZKElectionListener) listener).onConnectionLoss();
        }

        // submit a task to get notified about a new leader being elected
        final Future<String> newLeader = executorService.submit(new Callable<String>() {
          @Override
          public String call() throws Exception {
            // loop until election happened, or timeout
            do {
              // No polling required to check its own state
              if (leaderLatch.hasLeadership()) {
                // For testing purpose
                // (Connection silently restored)
                if (listener instanceof ZKElectionListener) {
                  ((ZKElectionListener) listener).onReconnection();
                }
                return id;
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
                return participant.getId();
              }

              TimeUnit.MILLISECONDS.sleep(electionPollingMs);
            } while(true);
          }
        });

        // Wrap the previous task to detect timeout
        final ListenableFuture<String> newLeaderWithTimeout = executorService.submit(new Callable<String>() {
          @Override
          public String call() throws Exception {
            try {
              return newLeader.get(electionTimeoutMs, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
              logger.info("Not able to get election status in {}ms. Cancelling election...", electionTimeoutMs);
              newLeader.cancel(true);
              throw e;
            }
          }
        });

        // Add callback to notify the user if needed
        Futures.addCallback(newLeaderWithTimeout, new FutureCallback<String>() {
          @Override
          public void onSuccess(String result) {
            if (!id.equals(result)) {
              logger.info("New leader elected. Cancelling election...", electionTimeoutMs);
              listener.onCancelled();
            }
          }

          @Override
          public void onFailure(Throwable t) {
            if (t instanceof CancellationException) {
              // ignore
              return;
            }
            listener.onCancelled();
          }
        });

        newLeaderRef.set(newLeaderWithTimeout);
      }

      @Override
      public void isLeader() {
        logger.debug("Acquired latch for election {}.", name);
        // Cancel possible watcher task
        ListenableFuture<?> newLeader = newLeaderRef.getAndSet(null);
        if (newLeader != null) {
          newLeader.cancel(false);
        }
        listener.onElected();
      }
    });


    // Time to start the latch
    try {
      leaderLatch.start();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }

    return new RegistrationHandle() {

      @Override
      public void close() {
        try {
          leaderLatch.close();
        } catch (IOException e) {
          logger.error("Error when closing registration handle for election {}", name, e);
        }
      }
    };
  }

  public ZKServiceSet newServiceSet(String name) throws Exception {
    return new ZKServiceSet(name, discovery);
  }

  private ServiceDiscovery<NodeEndpoint> newDiscovery(String clusterId) {
    return ServiceDiscoveryBuilder
      .builder(NodeEndpoint.class)
      .basePath(clusterId)
      .client(curator)
      .serializer(ServiceInstanceHelper.SERIALIZER)
      .build();
  }

  private class InitialConnectionListener implements ConnectionStateListener {

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState) {
      if(newState == ConnectionState.CONNECTED) {
        ZKClusterClient.this.initialConnection.countDown();
        client.getConnectionStateListenable().removeListener(this);
      }
    }
  }

  private class ConnectionLogger implements ConnectionStateListener {

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState) {
      logger.info("ZK connection state changed to {}", newState);
    }
  }


}
