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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.inject.Provider;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.proto.CoordinationProtos;
import com.google.common.annotations.VisibleForTesting;

/**
 * Task Leader election service - allows to elect leader among
 * nodes that handle a particular service
 */
public class TaskLeaderElection implements AutoCloseable {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TaskLeaderElection.class);

  private final Provider<ClusterServiceSetManager> clusterServiceSetManagerProvider;
  private final TaskLeaderStatusListener taskLeaderStatusListener;
  private final String serviceName;
  private final AtomicReference<Long> leaseExpirationTime = new AtomicReference<>(null);
  private final ScheduledExecutorService executorService;
  private final Provider<CoordinationProtos.NodeEndpoint> currentEndPoint;

  private ElectionRegistrationHandle electionHandle;
  private final AtomicBoolean isTaskLeader = new AtomicBoolean(false);
  private volatile boolean isLatchClosed = false;
  private ServiceSet serviceSet;
  private volatile RegistrationHandle nodeEndpointRegistrationHandle;
  private Future leadershipReleaseFuture;
  private ConcurrentMap<TaskLeaderChangeListener, TaskLeaderChangeListener> listeners = new ConcurrentHashMap<>();

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
    this.serviceName = serviceName;
    this.clusterServiceSetManagerProvider = clusterServiceSetManagerProvider;
    this.leaseExpirationTime.set(leaseExpirationTime);
    this.executorService = executorService;
    this.currentEndPoint = currentEndPoint;
    this.taskLeaderStatusListener = new TaskLeaderStatusListener(serviceName, clusterServiceSetManagerProvider);
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
      .joinElection(serviceName, new ElectionListener() {
      @Override
      public void onElected() {
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
    });
    isLatchClosed = false;
    // no need to do anything if it is a follower
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
        try {
          logger.info("Trying to relinquish leadership for {}, as number of participants is {}", serviceName, electionHandle.instanceCount());
          listeners.keySet().forEach(TaskLeaderChangeListener::onLeadershipRelinquished);
          // abandon leadership
          // and reenter elections
          // unregistering node from service
          AutoCloseables.close(nodeEndpointRegistrationHandle, electionHandle);
          isLatchClosed = true;
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
      } else {
        logger.info("Do not relinquish leadership as it is {} and number of election participants is {}",
          (isTaskLeader.get()) ? "task leader" : "task follower", electionHandle.instanceCount());
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
    if (isLatchClosed) {
      AutoCloseables.close(taskLeaderStatusListener);
    } else {
      // order is important
      AutoCloseables.close(nodeEndpointRegistrationHandle, electionHandle, taskLeaderStatusListener);
    }
  }
}
