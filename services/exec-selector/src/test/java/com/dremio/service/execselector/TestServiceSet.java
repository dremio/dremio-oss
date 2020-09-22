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
package com.dremio.service.execselector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.dremio.common.util.DremioVersionInfo;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.service.coordinator.NodeStatusListener;
import com.dremio.service.coordinator.RegistrationHandle;
import com.dremio.service.coordinator.ServiceSet;
import com.google.common.collect.ImmutableSet;

/**
 * Unit test helper: provides a service set that can be directly manipulated by the unit test
 */
public class TestServiceSet implements ServiceSet {
  private Set<CoordinationProtos.NodeEndpoint> endpoints = new HashSet<>();
  private NodeStatusListener listener;

  public RegistrationHandle register(CoordinationProtos.NodeEndpoint endpoint) {
    throw new UnsupportedOperationException("never invoked");
  }

  public Collection<CoordinationProtos.NodeEndpoint> getAvailableEndpoints() {
    return endpoints;
  }

  public void addNodeStatusListener(NodeStatusListener listener) {
    assertNull(this.listener);
    this.listener = listener;
  }

  public void removeNodeStatusListener(NodeStatusListener listener) {
    assertEquals(this.listener, listener);
    this.listener = null;
  }

  void testAddNode(String address) {
    testAddNodeWithDremioVersion(address, DremioVersionInfo.getVersion());
  }

  void testAddNodeWithDremioVersion(String address, String dremioVersion) {
    CoordinationProtos.NodeEndpoint newNode = CoordinationProtos.NodeEndpoint.newBuilder()
        .setAddress(address)
        .setDremioVersion(dremioVersion)
        .build();
    endpoints.add(newNode);
    if (listener != null) {
      listener.nodesRegistered(ImmutableSet.of(newNode));
    }
  }

  void testRemoveNode(String address) {
    testRemoveNodeWithDremioVersion(address, DremioVersionInfo.getVersion());
  }

  void testRemoveNodeWithDremioVersion(String address, String dremioVersion) {
    CoordinationProtos.NodeEndpoint removedNode = CoordinationProtos.NodeEndpoint.newBuilder()
        .setAddress(address)
        .setDremioVersion(dremioVersion)
        .build();
    endpoints.remove(removedNode);
    if (listener != null) {
      listener.nodesUnregistered(ImmutableSet.of(removedNode));
    }
  }

  void testAddNode(String address, String tag) {
    CoordinationProtos.NodeEndpoint newNode = CoordinationProtos.NodeEndpoint.newBuilder()
        .setAddress(address)
        .setNodeTag(tag)
        .setDremioVersion(DremioVersionInfo.getVersion())
        .build();
    endpoints.add(newNode);
    if (listener != null) {
      listener.nodesRegistered(ImmutableSet.of(newNode));
    }
  }

  void testAddNode(String address, String tag, int port) {
    CoordinationProtos.NodeEndpoint newNode = CoordinationProtos.NodeEndpoint.newBuilder()
      .setAddress(address)
      .setNodeTag(tag)
      .setUserPort(port)
      .setDremioVersion(DremioVersionInfo.getVersion())
      .build();
    endpoints.add(newNode);
    if (listener != null) {
      listener.nodesRegistered(ImmutableSet.of(newNode));
    }
  }

  void testRemoveNode(String address, String tag) {
    CoordinationProtos.NodeEndpoint removedNode = CoordinationProtos.NodeEndpoint.newBuilder()
        .setAddress(address)
        .setNodeTag(tag)
        .setDremioVersion(DremioVersionInfo.getVersion())
        .build();

    endpoints.remove(removedNode);
    if (listener != null) {
      listener.nodesUnregistered(ImmutableSet.of(removedNode));
    }
  }
}
