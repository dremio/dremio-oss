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
package com.dremio.service.coordinator.local;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.service.coordinator.AbstractServiceSet;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.DistributedSemaphore;
import com.dremio.service.coordinator.ElectionListener;
import com.dremio.service.coordinator.ElectionRegistrationHandle;
import com.dremio.service.coordinator.LinearizableHierarchicalStore;
import com.dremio.service.coordinator.RegistrationHandle;
import com.dremio.service.coordinator.ServiceSet;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/** A {@code ClusterCoordinator} local implementation for testing purposes */
public class LocalClusterCoordinator extends ClusterCoordinator {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(LocalClusterCoordinator.class);

  /*
   * Since we hand out the endpoints list in {@see #getAvailableEndpoints()}, we use a
   * {@see java.util.concurrent.ConcurrentHashMap} because those guarantee not to throw
   * ConcurrentModificationException.
   */
  private final ConcurrentMap<String, DistributedSemaphore> semaphores = new ConcurrentHashMap<>();

  private final ConcurrentMap<String, Election> elections = new ConcurrentHashMap<>();

  private final ConcurrentMap<String, LocalServiceSet> serviceSets = new ConcurrentHashMap<>();

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
    for (ClusterCoordinator.Role role : ClusterCoordinator.Role.values()) {
      serviceSets.put(role.name(), new LocalServiceSet(role.name()));
    }
  }

  @Override
  public void close() throws Exception {
    logger.info("Stopping Local Cluster Coordinator");
    AutoCloseables.close(serviceSets.values());
    logger.info("Stopped Local Cluster Coordinator");
  }

  @Override
  public void start() {}

  @Override
  public ServiceSet getServiceSet(Role role) {
    return serviceSets.get(role.name());
  }

  @Override
  public ServiceSet getOrCreateServiceSet(final String name) {
    return serviceSets.computeIfAbsent(name, LocalServiceSet::new);
  }

  @Override
  public void deleteServiceSet(String name) {
    LocalServiceSet localServiceSet = serviceSets.remove(name);
    if (localServiceSet != null) {
      try {
        localServiceSet.close();
      } catch (Exception e) {
        logger.error("Unable to close LocalServiceSet {}", name, e);
      }
    }
  }

  @Override
  public Iterable<String> getServiceNames() {
    return serviceSets.keySet();
  }

  @VisibleForTesting
  public LocalServiceSet getServiceSet(String name) {
    return serviceSets.get(name);
  }

  private static final class LocalServiceSet extends AbstractServiceSet implements AutoCloseable {
    private final ConcurrentMap<Handle, NodeEndpoint> endpoints = new ConcurrentHashMap<>();

    private final String serviceName;

    private final class Handle implements RegistrationHandle {
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
        // do not clear listeners, as they are global
        endpoints.remove(this);
      }
    }

    public LocalServiceSet(String serviceName) {
      this.serviceName = serviceName;
    }

    @Override
    public RegistrationHandle register(NodeEndpoint endpoint) {
      logger.debug("Endpoint registered {}. {}", serviceName, endpoint);
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
      return serviceName;
    }

    @Override
    public void close() throws Exception {
      logger.info("Stopping LocalServiceSet");
      clearListeners();
      endpoints.clear();
      logger.info("Stopped LocalServiceSet");
    }
  }

  @Override
  public DistributedSemaphore getSemaphore(final String name, final int maximumLeases) {
    return semaphores.computeIfAbsent(name, key -> new LocalSemaphore(maximumLeases));
  }

  @Override
  public ElectionRegistrationHandle joinElection(String name, ElectionListener listener) {
    return elections.computeIfAbsent(name, key -> new Election()).joinElection(listener);
  }

  @Override
  public LinearizableHierarchicalStore getHierarchicalStore() {
    throw new UnsupportedOperationException("Hierarchical Store is not supported in Local");
  }

  private static final class LocalSemaphore implements DistributedSemaphore {
    private final Semaphore semaphore;
    private final int size;
    private final LocalLease singleLease = new LocalLease(1);
    private final Set<UpdateListener> listeners =
        Collections.newSetFromMap(Collections.synchronizedMap(new WeakHashMap<>()));

    LocalSemaphore(final int size) {
      this.semaphore = new Semaphore(size);
      this.size = size;
    }

    @Override
    public boolean hasOutstandingPermits() {
      return semaphore.availablePermits() < size;
    }

    @Override
    public DistributedLease acquire(int numPermits, long timeout, TimeUnit timeUnit)
        throws Exception {
      Preconditions.checkArgument(numPermits > 0, "numPermits must be a positive integer");
      notifyUpdateListeners();
      if (!semaphore.tryAcquire(numPermits, timeout, timeUnit)) {
        return null;
      } else {
        return numPermits == 1 ? singleLease : new LocalLease(numPermits);
      }
    }

    @Override
    public boolean registerUpdateListener(UpdateListener listener) {
      return listeners.add(listener);
    }

    private void notifyUpdateListeners() {
      List<UpdateListener> copy = new ArrayList<>(listeners);
      for (UpdateListener listener : copy) {
        try {
          listener.updated();
        } catch (Exception e) {
          logger.warn("Exception occurred while notifying listener: " + listener, e);
        }
      }
    }

    private class LocalLease implements DistributedLease {
      private final int numPermits;

      LocalLease(int numPermits) {
        this.numPermits = numPermits;
      }

      @Override
      public void close() throws Exception {
        semaphore.release(numPermits);
        notifyUpdateListeners();
      }
    }
  }

  private static final class Candidate {
    private final ElectionListener listener;

    public Candidate(ElectionListener listener) {
      this.listener = listener;
    }
  }

  private static final class Election {
    private final Queue<Candidate> waiting = new LinkedBlockingQueue<>();
    private volatile Candidate currentLeader = null;

    public ElectionRegistrationHandle joinElection(final ElectionListener listener) {
      final Candidate candidate = new Candidate(listener);
      synchronized (this) {
        if (currentLeader == null) {
          currentLeader = candidate;
          candidate.listener.onElected();
        } else {
          waiting.add(candidate);
        }
      }

      return new ElectionRegistrationHandle() {
        @Override
        public void close() {
          leaveElection(candidate);
        }

        @Override
        public Object synchronizer() {
          return this;
        }

        @Override
        public int instanceCount() {
          return waiting.size();
        }
      };
    }

    private void leaveElection(final Candidate candidate) {
      synchronized (this) {
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
