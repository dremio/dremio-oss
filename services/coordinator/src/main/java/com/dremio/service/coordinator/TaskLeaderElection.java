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
package com.dremio.service.coordinator;

import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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

  private final Provider<ClusterCoordinator> clusterCoordinator;
  private final TaskLeaderStatusListener taskLeaderStatusListener;
  private final String serviceName;
  private final Long firstLeaseExpiration;
  private final Long leaseExpirationTime;
  private final ScheduledExecutorService executorService;
  private final Provider<CoordinationProtos.NodeEndpoint> currentEndPoint;

  private ServiceSet.RegistrationHandle registrationHandle;
  private final AtomicBoolean isTaskLeader = new AtomicBoolean(false);
  private volatile boolean isLatchClosed = false;
  private ServiceSet zkService;
  private volatile ServiceSet.RegistrationHandle nodeEndpointRegistrationHandle;
  private Future leadershipReleaseFuture;

  /**
   * If we don't use relinquishing leadership - don't need executor
   * @param serviceName
   * @param clusterCoordinator
   * @param currentEndPoint
   */
  public TaskLeaderElection(String serviceName,
                            Provider<ClusterCoordinator> clusterCoordinator,
                            Provider<CoordinationProtos.NodeEndpoint> currentEndPoint) {
    this(serviceName, clusterCoordinator, null, null, currentEndPoint, null);
  }

  public TaskLeaderElection(String serviceName,
                            Provider<ClusterCoordinator> clusterCoordinator,
                            Long firstLeaseExpiration,
                            Long leaseExpirationTime,
                            Provider<CoordinationProtos.NodeEndpoint> currentEndPoint,
                            ScheduledExecutorService executorService) {
    this.serviceName = serviceName;
    this.clusterCoordinator = clusterCoordinator;
    this.firstLeaseExpiration = firstLeaseExpiration;
    this.leaseExpirationTime = leaseExpirationTime;
    this.executorService = executorService;
    this.currentEndPoint = currentEndPoint;
    this.taskLeaderStatusListener = new TaskLeaderStatusListener(serviceName, clusterCoordinator);
  }

  public void start() throws Exception {
    zkService = clusterCoordinator.get().getOrCreateServiceSet(serviceName);
    taskLeaderStatusListener.start();
    enterElections();
    // start thread only of relinquishing leadership time was set
    if (leaseExpirationTime != null && firstLeaseExpiration != null) {
      leadershipReleaseFuture = executorService.scheduleAtFixedRate(
        new LeadershipReset(),
        firstLeaseExpiration,
        leaseExpirationTime,
        TimeUnit.MILLISECONDS
      );
    }
  }

  private void enterElections() {
    logger.info("Starting TaskLeader Election Service for {}", serviceName);

    registrationHandle = clusterCoordinator.get()
      .joinElection(serviceName, new ElectionListener() {
      @Override
      public void onElected() {
        // in case ZK connection is lost but reestablished later
        // it may get to the situation when 'onElected' is called
        // multiple times - this can create an issue with registering
        // currentEndPoint as master again and again
        // therefore checking if we were a leader before registering
        // and doing other operations
        if (!isTaskLeader.get()) {
          logger.info("Electing Leader for {}", serviceName);
          // registering node with service
          nodeEndpointRegistrationHandle = zkService.register(currentEndPoint.get());
          isTaskLeader.compareAndSet(false, true);
        }
      }

      @Override
      public void onCancelled() {
        logger.info("Rejecting Leader for {}", serviceName);
        isTaskLeader.compareAndSet(true, false);
        // unregistering node from service
        nodeEndpointRegistrationHandle.close();
      }
    });
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
    return leaseExpirationTime;
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
    // TODO (DX-13809) should not reelect if task is currently running
    @Override
    public void run() {
      // do not abandon elections if there is no more participants in the elections
      if (isTaskLeader.get() && registrationHandle.instanceCount() > 1) {
        try {
          logger.info("Trying to relinquish leadership for {}, as number of participants is {}", serviceName, registrationHandle.instanceCount());
          // abandon leadership
          // and reenter elections
          isTaskLeader.compareAndSet(true, false);
          // unregistering node from service
          AutoCloseables.close(nodeEndpointRegistrationHandle, registrationHandle);
          isLatchClosed = true;
        } catch(InterruptedException ie) {
          Thread.currentThread().interrupt();
        } catch (Exception e) {
          logger.error("Error while trying to close elections before leader reelections for {}", serviceName);
        } finally {
          enterElections();
        }
      } else {
        logger.info("Do not relinquish leadership as it is {} and number of election participants is {}",
          (isTaskLeader.get()) ? "task leader" : "task follower", registrationHandle.instanceCount());
      }
    }
  }

  @Override
  public void close() throws Exception {
    isTaskLeader.compareAndSet(true, false);
    if (leadershipReleaseFuture != null) {
      leadershipReleaseFuture.cancel(true);
    }
    if (isLatchClosed) {
      AutoCloseables.close(taskLeaderStatusListener, nodeEndpointRegistrationHandle);
    } else {
      AutoCloseables.close(taskLeaderStatusListener, registrationHandle, nodeEndpointRegistrationHandle);
    }
  }
}
