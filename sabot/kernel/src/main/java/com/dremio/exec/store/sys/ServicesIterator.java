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
package com.dremio.exec.store.sys;

import com.dremio.config.DremioConfig;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.sys.ServicesIterator.ServiceSetInfo;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.ClusterCoordinator.Role;
import com.dremio.service.coordinator.ServiceSet;
import com.google.common.collect.ImmutableSet;
import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.stream.StreamSupport;

/**
 * Iterator that returns a {@link ServiceSetInfo} for every ServiceSet in a {@link
 * ClusterCoordinator}
 */
public class ServicesIterator implements Iterator<ServiceSetInfo> {
  public static final Set<String> SERVICE_BLACKLIST =
      ImmutableSet.of(
          ClusterCoordinator.Role.COORDINATOR.toString(),
          ClusterCoordinator.Role.EXECUTOR.toString(),
          "leader-latch",
          "semaphore",
          "clustered_singleton");

  private final Iterator<ServiceSetInfo> serviceNames;

  /** Basic ServiceSet info */
  @SuppressWarnings("checkstyle:VisibilityModifier")
  public static class ServiceSetInfo {
    public final String service;
    public final String hostname;
    public final int user_port;
    public final int fabric_port;

    public ServiceSetInfo(String service, final Optional<CoordinationProtos.NodeEndpoint> ep) {
      this.service = service;
      this.hostname = ep.map(CoordinationProtos.NodeEndpoint::getAddress).orElse("");
      this.user_port = ep.map(CoordinationProtos.NodeEndpoint::getUserPort).orElse(0);
      this.fabric_port = ep.map(CoordinationProtos.NodeEndpoint::getFabricPort).orElse(0);
    }
  }

  public ServicesIterator(final SabotContext dbContext) {
    ClusterCoordinator clusterCoordinator = dbContext.getClusterCoordinator();
    DremioConfig dremioConfig = dbContext.getDremioConfig();
    Iterator<ServiceSetInfo> srv;
    try {

      srv =
          StreamSupport.stream(clusterCoordinator.getServiceNames().spliterator(), false)
              .filter((p) -> !SERVICE_BLACKLIST.contains(p))
              .map(
                  s -> new ServiceSetInfo(s, getServiceLeader(dremioConfig, clusterCoordinator, s)))
              .iterator();
    } catch (Exception e) {
      srv = Collections.emptyIterator();
    }

    this.serviceNames = srv;
  }

  /** To return task leader nodeEndpoint if masterless mode is on otherwise return master */
  private static Optional<NodeEndpoint> getServiceLeader(
      DremioConfig dremioConfig, ClusterCoordinator clusterCoordinator, String serviceName) {
    ServiceSet serviceSet;
    if (dremioConfig.isMasterlessEnabled()) {
      serviceSet = clusterCoordinator.getOrCreateServiceSet(serviceName);
    } else {
      serviceSet = clusterCoordinator.getServiceSet(Role.MASTER);
    }
    return Optional.ofNullable(serviceSet.getAvailableEndpoints())
        .flatMap(nodeEndpoints -> nodeEndpoints.stream().findFirst());
  }

  @Override
  public boolean hasNext() {
    return serviceNames.hasNext();
  }

  @Override
  public ServiceSetInfo next() {
    return serviceNames.next();
  }
}
