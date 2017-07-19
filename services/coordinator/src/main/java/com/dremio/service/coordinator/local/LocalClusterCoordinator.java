/*
 * Copyright (C) 2017 Dremio Corporation
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
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.service.coordinator.AbstractServiceSet;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.DistributedSemaphore;
import com.dremio.service.coordinator.ServiceSet;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;

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

  @Override
  public void close() throws Exception {
    logger.info("Stopping Local Cluster Coordinator");
    AutoCloseables.close(serviceSets.values());
    logger.info("Stopped Local Cluster Coordinator");
  }

  @Override
  public void start() throws Exception {
    logger.info("Local Cluster Coordinator is up.");
    for(ClusterCoordinator.Role role : ClusterCoordinator.Role.values()) {
      serviceSets.put(role, new LocalServiceSet());
    }
  }

  @Override
  public ServiceSet getServiceSet(Role role) {
    return serviceSets.get(role);
  }

  private static final class LocalServiceSet extends AbstractServiceSet implements AutoCloseable {
    private final Map<RegistrationHandle, NodeEndpoint> endpoints = new ConcurrentHashMap<>();

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

    @Override
    public RegistrationHandle register(NodeEndpoint endpoint) {
      logger.debug("Endpoint registered {}.", endpoint);
      final Handle h = new Handle();
      endpoints.put(h, endpoint);
      return h;
    }

    @Override
    public Collection<NodeEndpoint> getAvailableEndpoints() {
      return Collections.unmodifiableCollection(endpoints.values());
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

  private class LocalSemaphore implements DistributedSemaphore {
    private final Semaphore semaphore;
    private final LocalLease localLease = new LocalLease();

    public LocalSemaphore(final int size) {
      semaphore = new Semaphore(size);
    }

    @Override
    public DistributedLease acquire(final long timeout, final TimeUnit timeUnit) throws Exception {
      if (!semaphore.tryAcquire(timeout, timeUnit)) {
        return null;
      } else {
        return localLease;
      }
    }

    private class LocalLease implements DistributedLease {
      @Override
      public void close() throws Exception {
        semaphore.release();
      }
    }
  }
}
