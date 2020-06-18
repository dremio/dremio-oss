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
import java.util.Collections;

import com.dremio.exec.proto.CoordinationProtos;

/**
 * A {@code ClusterCoordinator} no-op implementation for executor nodes in DCS.
 */
public class NoOpClusterCoordinator extends ClusterCoordinator {
  private static final org.slf4j.Logger logger =
    org.slf4j.LoggerFactory.getLogger(NoOpClusterCoordinator.class);
  private static final NoOpServiceSet NO_OP_SERVICE_SET = new NoOpServiceSet();

  public NoOpClusterCoordinator() {
    logger.info("NoOp Cluster coordinator is up.");
  }

  @Override
  public void start() throws Exception {
  }

  @Override
  public ServiceSet getServiceSet(Role role) {
    return NO_OP_SERVICE_SET;
  }

  @Override
  public ServiceSet getOrCreateServiceSet(String serviceName) {
    return NO_OP_SERVICE_SET;
  }

  @Override
  public Iterable<String> getServiceNames() throws Exception {
    return Collections.emptySet();
  }

  @Override
  public DistributedSemaphore getSemaphore(String name, int maximumLeases) {
    throw new UnsupportedOperationException("getSemaphore not implemented in " +
      "NoOpClusterCoordinator");
  }

  @Override
  public ElectionRegistrationHandle joinElection(String name, ElectionListener listener) {
    throw new UnsupportedOperationException("joinElection not implemented in " +
      "NoOpClusterCoordinator");
  }

  @Override
  public void close() throws Exception {
    logger.info("Stopped NoOp Cluster coordinator.");
  }

  private static final class NoOpServiceSet extends AbstractServiceSet implements AutoCloseable {
    @Override
    public RegistrationHandle register(CoordinationProtos.NodeEndpoint endpoint) {
      return () -> {};
    }

    @Override
    public Collection<CoordinationProtos.NodeEndpoint> getAvailableEndpoints() {
      return Collections.emptySet();
    }

    @Override
    public void close() {
    }
  };
}
