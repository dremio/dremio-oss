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
package com.dremio.service.coordinator;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import javax.inject.Provider;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.proto.CoordinationProtos;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Task Leader election service - allows to elect leader among
 * nodes that handle a particular service
 */
public class TaskLeaderElection implements AutoCloseable {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TaskLeaderElection.class);

  private static final long LEADER_UNAVAILABLE_DURATION_SECS = 600; // 10 minutes

  private final Provider<ClusterServiceSetManager> clusterServiceSetManagerProvider;
  private final TaskLeaderStatusListener taskLeaderStatusListener;
  private final String serviceName;
  private final AtomicReference<Long> leaseExpirationTime = new AtomicReference<>(null);
  private final ScheduledExecutorService executorService;
  private final Provider<CoordinationProtos.NodeEndpoint> currentEndPoint;
  private final long failSafeLeaderUnavailableDuration;

  private ElectionRegistrationHandle electionHandle;
  private final AtomicBoolean isTaskLeader = new AtomicBoolean(false);
  private ServiceSet serviceSet;
  private volatile RegistrationHandle nodeEndpointRegistrationHandle;
  private Future leadershipReleaseFuture;
  private ConcurrentMap<TaskLeaderChangeListener, TaskLeaderChangeListener> listeners = new ConcurrentHashMap<>();
  private volatile boolean electionHandleClosed = false;
  private final Object reElectionLock = new Object();
  private final Function<ElectionListener, ElectionListener> electionListenerProvider;
  private FailSafeReElectionTask failSafeReElectionTask;
  private ScheduledThreadPoolExecutor reElectionExecutor;

  /**
   * If we don't use relinquishing leadership - don't need executor
   * @param serviceName
   * @param clusterServiceSetManagerProvider
   * @param currentEndPoint
   */
  public TaskLeaderElection(String serviceName,
                            Provider<ClusterServiceSetManager> clusterServiceSetManagerProvider,
                            Provider<CoordinationProtos.NodeEndpoint> currentEndPoint) {
    this(serviceName, clusterServiceSetManagerProvider, null, currentEndPoint, null);
  }

  public TaskLeaderElection(String serviceName,
                            Provider<ClusterServiceSetManager> clusterServiceSetManagerProvider,
                            Long leaseExpirationTime,
                            Provider<CoordinationProtos.NodeEndpoint> currentEndPoint,
                            ScheduledExecutorService executorService) {
    this(serviceName, clusterServiceSetManagerProvider, leaseExpirationTime, currentEndPoint,
      executorService, LEADER_UNAVAILABLE_DURATION_SECS, Function.identity());

  }

  public TaskLeaderElection(String serviceName,
                            Provider<ClusterServiceSetManager> clusterServiceSetManagerProvider,
                            Long leaseExpirationTime,
                            Provider<CoordinationProtos.NodeEndpoint> currentEndPoint,
                            ScheduledExecutorService executorService,
                            Long failSafeLeaderUnavailableDuration,
                            Function<ElectionListener, ElectionListener> electionListenerProvider) {
    this.serviceName = serviceName;
    this.clusterServiceSetManagerProvider = clusterServiceSetManagerProvider;
    this.leaseExpirationTime.set(leaseExpirationTime);
    this.currentEndPoint = currentEndPoint;
    this.taskLeaderStatusListener = new TaskLeaderStatusListener(serviceName, clusterServiceSetManagerProvider);
    this.failSafeLeaderUnavailableDuration = failSafeLeaderUnavailableDuration;
    this.electionListenerProvider = electionListenerProvider;

    if (executorService == null) {
      reElectionExecutor = new ScheduledThreadPoolExecutor(1, new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat("TaskLeaderElection-serviceName")
        .build());
      reElectionExecutor.setRemoveOnCancelPolicy(true);
      this.executorService = reElectionExecutor;
    } else {
      this.executorService = executorService;
    }
  }

  public void start() throws Exception {
    serviceSet = clusterServiceSetManagerProvider.get().getOrCreateServiceSet(serviceName);
    taskLeaderStatusListener.start();
    enterElections();
  }

  public void addListener(TaskLeaderChangeListener listener) {
    listeners.put(listener, listener);
  }

  public void removeListener(TaskLeaderChangeListener listener) {
    listeners.remove(listener);
  }

  public void updateLeaseExpirationTime(Long newLeaseExpirationTime) {
    leaseExpirationTime.updateAndGet(operand -> newLeaseExpirationTime);
  }

  @VisibleForTesting
  public Collection<TaskLeaderChangeListener> getTaskLeaderChangeListeners() {
    return listeners.values();
  }

  private void enterElections() {
    logger.info("Starting TaskLeader Election Service for {}", serviceName);

    electionHandle = clusterServiceSetManagerProvider.get()
      .joinElection(serviceName, electionListenerProvider.apply(new ElectionListener() {
      @Override
      public void onElected() {
        synchronized (reElectionLock) {
          if (electionHandleClosed) {
            return;
          }

          // in case ZK connection is lost but reestablished later
          // it may get to the situation when 'onElected' is called
          // multiple times - this can create an issue with registering
          // currentEndPoint as master again and again
          // therefore checking if we were a leader before registering
          // and doing other operations
          if (isTaskLeader.compareAndSet(false, true)) {
            logger.info("Electing Leader for {}", serviceName);
            // registering node with service
            nodeEndpointRegistrationHandle = serviceSet.register(currentEndPoint.get());
            listeners.keySet().forEach(TaskLeaderChangeListener::onLeadershipGained);

            // start thread only if relinquishing leadership time was set
            if (leaseExpirationTime.get() != null) {
              leadershipReleaseFuture = executorService.schedule(
                new LeadershipReset(),
                leaseExpirationTime.get(),
                TimeUnit.MILLISECONDS
              );
            }
          }
        }
      }

      @Override
      public void onCancelled() {
        if (isTaskLeader.compareAndSet(true, false)) {
          logger.info("Rejecting Leader for {}", serviceName);
          if (leadershipReleaseFuture != null) {
            leadershipReleaseFuture.cancel(false);
          }
          // unregistering node from service
          nodeEndpointRegistrationHandle.close();
          listeners.keySet().forEach(TaskLeaderChangeListener::onLeadershipLost);

        }
      }
    }));
    // no need to do anything if it is a follower

    electionHandleClosed = false;
    failSafeReElectionTask = new FailSafeReElectionTask();
  }

  public CoordinationProtos.NodeEndpoint getTaskLeader() {
    try {
      taskLeaderStatusListener.waitForTaskLeader();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    return taskLeaderStatusListener.getTaskLeaderNode();
  }

  public boolean isTaskLeader() {
    return isTaskLeader.get();
  }

  public Long getLeaseExpirationTime() {
    return leaseExpirationTime.get();
  }

  @VisibleForTesting
  public CoordinationProtos.NodeEndpoint getCurrentEndPoint() {
    return currentEndPoint.get();
  }

  /**
   * To abandon leadership after some time
   * and enter elections again
   */
  private class LeadershipReset implements Runnable {
    @Override
    public void run() {
      // do not abandon elections if there is no more participants in the elections
      if (isTaskLeader.compareAndSet(true, false) && electionHandle.instanceCount() > 1) {
        reset();
      } else {
        logger.info("Do not relinquish leadership as it is {} and number of election participants is {}",
          (isTaskLeader.get()) ? "task leader" : "task follower", electionHandle.instanceCount());
      }
    }

    void reset() {
      try {
        logger.info("Trying to relinquish leadership for {}, as number of participants is {}", serviceName, electionHandle.instanceCount());
        listeners.keySet().forEach(TaskLeaderChangeListener::onLeadershipRelinquished);
        // abandon leadership
        // and reenter elections
        // unregistering node from service
        closeHandles();
        if (leadershipReleaseFuture != null) {
          leadershipReleaseFuture.cancel(false);
        }
      } catch(InterruptedException ie) {
        logger.error("Current thread is interrupted. stopping elections for {} before leader reelections for {}",
          serviceName, ie);
        Thread.currentThread().interrupt();
      } catch (Exception e) {
        logger.error("Error while trying to close elections before leader reelections for {}", serviceName);
      }
      enterElections();
    }
  }

  // In general, when ZK restarts, the current leader will loose leadership, and when the clients gets reconnected one of
  // the election participant would become a leader. However, in some situations the curator client looses the leader
  // election notification from ZK, because of which there arises a situation where no leader is elected across the participants.
  // FailSafeReElectionTask resolves this situation. FailSafeReElectionTask, periodically (every 5 minutes) checks if there is
  // a leader. If there is no leader for 10 minutes consecutively, every participant will end the current election
  // and re-enters the election.
  private class FailSafeReElectionTask {
    private long leaderIsNotAvailableFrom = Long.MAX_VALUE;
    private boolean leaderIsNotAvailable = false;
    private final Future failSafeReElectionFuture;

    FailSafeReElectionTask() {
      failSafeReElectionFuture = executorService.scheduleAtFixedRate(
        this::checkAndReElect,
        0,
        failSafeLeaderUnavailableDuration/2,
        TimeUnit.SECONDS
      );
    }

    private void checkAndReElect() {
      if (!leaderIsNotAvailable && taskLeaderStatusListener.getTaskLeaderNode() == null) {
        leaderIsNotAvailable = true;
        leaderIsNotAvailableFrom = System.currentTimeMillis();
      }

      if (leaderIsNotAvailable) {
        if (taskLeaderStatusListener.getTaskLeaderNode() != null) {
          leaderIsNotAvailableFrom = Long.MAX_VALUE;
          leaderIsNotAvailable = false;
        } else {
          long leaderUnavailableDuration = (System.currentTimeMillis() - leaderIsNotAvailableFrom)/1000;
          if (leaderUnavailableDuration >= failSafeLeaderUnavailableDuration) {
            synchronized (reElectionLock) {
              electionHandleClosed = true;
              logger.info("Closing current election handle and reentering elections for {} as there is no leader for {} secs",
                serviceName, leaderUnavailableDuration);
              if (isTaskLeader.compareAndSet(true, false)) {
                LeadershipReset leadershipReset = new LeadershipReset();
                leadershipReset.reset();
                this.cancel(false);
              } else {
                try {
                  AutoCloseables.close(electionHandle);
                  enterElections();
                  this.cancel(false);
                } catch (Exception e) {
                  logger.error("Failed to end current election handle");
                }
              }
            }
          }
        }
      }
    }

    void cancel(boolean mayInterruptRunning) {
      if (failSafeReElectionFuture != null) {
        failSafeReElectionFuture.cancel(mayInterruptRunning);
      }
    }
  }

  @Override
  public void close() throws Exception {
    if (isTaskLeader.compareAndSet(true, false)) {
      listeners.keySet().forEach(TaskLeaderChangeListener::onLeadershipLost);
    }
    listeners.clear();
    if (leadershipReleaseFuture != null) {
      leadershipReleaseFuture.cancel(true);
    }

    if (failSafeReElectionTask != null) {
      failSafeReElectionTask.cancel(true);
    }

    if (reElectionExecutor != null) {
      AutoCloseables.close(reElectionExecutor::shutdown);
    }

    closeHandles();
    AutoCloseables.close(taskLeaderStatusListener);
  }

  private synchronized void closeHandles() throws Exception {
    AutoCloseables.close(nodeEndpointRegistrationHandle, electionHandle);
    nodeEndpointRegistrationHandle = null;
    electionHandle = null;
  }
}
