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
package com.dremio.exec.store.sys;

import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.stream.StreamSupport;

import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.server.SabotContext;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.google.common.collect.ImmutableSet;

/**
 * Iterator that returns a {@link ServiceSetInfo} for every ServiceSet in a {@link ClusterCoordinator}
 */
public class ServicesIterator implements Iterator<Object> {
  public static final Set<String> SERVICE_BLACKLIST = ImmutableSet.of(
    ClusterCoordinator.Role.COORDINATOR.toString(),
    ClusterCoordinator.Role.EXECUTOR.toString(),
    "leader-latch",
    "semaphore");

  private final SabotContext dbContext;
  private final Iterator<String> serviceNames;

  /**
   * Basic ServiceSet info
   */
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

  ServicesIterator(final SabotContext dbContext) {
    this.dbContext = dbContext;

    Iterator<String> srv;
    try {
      srv = StreamSupport.stream(dbContext.getClusterCoordinator()
        .getServiceNames().spliterator(), false)
        .filter((p) -> !SERVICE_BLACKLIST.contains(p))
        .iterator();
    } catch (Exception e) {
      srv = Collections.emptyIterator();
    }

    this.serviceNames = srv;
  }

  @Override
  public boolean hasNext() {
    return serviceNames.hasNext();
  }

  @Override
  public Object next() {
    String serv = serviceNames.next();
    return new ServiceSetInfo(serv, dbContext.getServiceLeader(serv));
  }
}
