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
package com.dremio.service.coordinator.local;

import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.service.coordinator.AbstractServiceSet;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.DistributedSemaphore;
import com.dremio.service.coordinator.ElectionListener;
import com.dremio.service.coordinator.ServiceSet;
import com.dremio.service.coordinator.ServiceSet.RegistrationHandle;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * A {@code ClusterCoordinator} local implementation for testing purposes
 */
public class LocalClusterCoordinator extends ClusterCoordinator {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LocalClusterCoordinator.class);

  /*
   * Since we hand out the endpoints list in {@see #getAvailableEndpoints()}, we use a
   * {@see java.util.concurrent.ConcurrentHashMap} because those guarantee not to throw
   * ConcurrentModificationException.
   */
  private final ConcurrentMap<String, DistributedSemaphore> semaphores = Maps.newConcurrentMap();

  private final ConcurrentMap<String, Election> elections = Maps.newConcurrentMap();

  private final EnumMap<ClusterCoordinator.Role, LocalServiceSet> serviceSets = new EnumMap<>(ClusterCoordinator.Role.class);

  /**
   * Returns a new local cluster coordinator, already started
   *
   * @return a new instance, just started
   */
  @VisibleForTesting
  public static LocalClusterCoordinator newRunningCoordinator() throws Exception {
    LocalClusterCoordinator coordinator = new LocalClusterCoordinator();
    coordinator.start();

    return coordinator;
  }

  public LocalClusterCoordinator() {
    logger.info("Local Cluster Coordinator is up.");
    for(ClusterCoordinator.Role role : ClusterCoordinator.Role.values()) {
      serviceSets.put(role, new LocalServiceSet(role));
    }
  }

  @Override
  public void close() throws Exception {
    logger.info("Stopping Local Cluster Coordinator");
    AutoCloseables.close(serviceSets.values());
    logger.info("Stopped Local Cluster Coordinator");
  }

  @Override
  public void start() {
  }

  @Override
  public ServiceSet getServiceSet(Role role) {
    return serviceSets.get(role);
  }

  private final class LocalServiceSet extends AbstractServiceSet implements AutoCloseable {
    private final Map<RegistrationHandle, NodeEndpoint> endpoints = new ConcurrentHashMap<>();

    private final Role role;

    private class Handle implements RegistrationHandle {
      private final UUID id = UUID.randomUUID();

      @Override
      public int hashCode() {
        return Objects.hash(getOuterType(), id);
      }

      @Override
      public boolean equals(final Object obj) {
        if (this == obj) {
          return true;
        }
        if (obj == null) {
          return false;
        }
        if (getClass() != obj.getClass()) {
          return false;
        }
        final Handle other = (Handle) obj;
        return getOuterType().equals(other.getOuterType()) && id.equals(other.id);
      }

      private LocalServiceSet getOuterType() {
        return LocalServiceSet.this;
      }

      @Override
      public void close() {
        // when SabotNode is unregistered, clean all the listeners registered in CC.
        clearListeners();

        endpoints.remove(this);
      }
    }

    public LocalServiceSet(Role role) {
      this.role = role;
    }

    @Override
    public RegistrationHandle register(NodeEndpoint endpoint) {
      logger.debug("Endpoint registered {}. {}", role, endpoint);
      final Handle h = new Handle();
      endpoints.put(h, endpoint);
      nodesRegistered(Sets.newHashSet(endpoint));
      return h;
    }

    @Override
    public Collection<NodeEndpoint> getAvailableEndpoints() {
      return Collections.unmodifiableCollection(endpoints.values());
    }

    @Override
    public String toString() {
      return role.name();
    }

    @Override
    public void close() throws Exception {
      logger.info("Stopping Local Cluster Coordinator");
      endpoints.clear();
      logger.info("Stopped Local Cluster Coordinator");
    }
  }


  @Override
  public DistributedSemaphore getSemaphore(final String name, final int maximumLeases) {
    if (!semaphores.containsKey(name)) {
      semaphores.putIfAbsent(name, new LocalSemaphore(maximumLeases));
    }
    return semaphores.get(name);
  }

  @Override
  public RegistrationHandle joinElection(String name, ElectionListener listener) {
    if (!elections.containsKey(name)) {
      elections.putIfAbsent(name, new Election());
    }
    return elections.get(name).joinElection(listener);
  }

  private class LocalSemaphore implements DistributedSemaphore {
    private final Semaphore semaphore;
    private final LocalLease singleLease = new LocalLease(1);

    LocalSemaphore(final int size) {
      semaphore = new Semaphore(size);
    }

    @Override
    public DistributedLease acquire(int permits, long timeout, TimeUnit timeUnit) throws Exception {
      Preconditions.checkArgument(permits > 0, "permits must be a positive integer");
      if (!semaphore.tryAcquire(permits, timeout, timeUnit)) {
        return null;
      } else {
        return permits == 1 ? singleLease : new LocalLease(permits);
      }
    }

    private class LocalLease implements DistributedLease {
      private final int permits;

      LocalLease(int permits) {
        this.permits = permits;
      }

      @Override
      public void close() throws Exception {
        semaphore.release(permits);
      }
    }
  }



  private final class Candidate {
    private final ElectionListener listener;

    public Candidate(ElectionListener listener) {
      this.listener = listener;
    }
  }

  private final class Election {
    private final Queue<Candidate> waiting = new LinkedBlockingQueue<>();
    private volatile Candidate currentLeader = null;

    public RegistrationHandle joinElection(final ElectionListener listener) {
      final Candidate candidate = new Candidate(listener);
      synchronized(this) {
        if (currentLeader == null) {
          currentLeader = candidate;
          candidate.listener.onElected();
        } else {
          waiting.add(candidate);
        }
      }


      return new RegistrationHandle() {
        @Override
        public void close() {
          leaveElection(candidate);
        }
      };
    }

    private void leaveElection(final Candidate candidate) {
      synchronized(this) {
        if (currentLeader == candidate) {
          currentLeader = waiting.poll();
          if (currentLeader != null) {
            currentLeader.listener.onElected();
          }
        } else {
          waiting.remove(candidate);
        }
      }
    }
  }
}
