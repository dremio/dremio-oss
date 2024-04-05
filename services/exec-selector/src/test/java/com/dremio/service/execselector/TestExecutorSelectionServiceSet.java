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

import com.dremio.common.util.DremioVersionInfo;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.service.coordinator.NodeStatusListener;
import com.dremio.service.coordinator.RegistrationHandle;
import com.dremio.service.coordinator.ServiceSet;
import com.google.common.collect.ImmutableSet;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Implementation of a {@link ServiceSet} that allows manual addition and removal of endpoints The
 * endpoints thus added only contain an address, with the rest of the {@link NodeEndpoint} proto not
 * set
 */
public class TestExecutorSelectionServiceSet implements ServiceSet {
  private Set<NodeEndpoint> endpoints = new HashSet<>();
  private NodeStatusListener listener;

  @Override
  public RegistrationHandle register(NodeEndpoint endpoint) {
    throw new UnsupportedOperationException("never invoked");
  }

  @Override
  public Collection<NodeEndpoint> getAvailableEndpoints() {
    return endpoints;
  }

  @Override
  public void addNodeStatusListener(NodeStatusListener listener) {
    assertNull(this.listener);
    this.listener = listener;
  }

  @Override
  public void removeNodeStatusListener(NodeStatusListener listener) {
    assertEquals(this.listener, listener);
    this.listener = null;
  }

  void testAddNode(String address) {
    testAddNode(address, DremioVersionInfo.getVersion());
  }

  void testAddNode(String address, String dremioVersion) {
    NodeEndpoint newNode =
        NodeEndpoint.newBuilder().setAddress(address).setDremioVersion(dremioVersion).build();
    endpoints.add(newNode);
    if (listener != null) {
      listener.nodesRegistered(ImmutableSet.of(newNode));
    }
  }

  void testRemoveNode(String address) {
    testRemoveNode(address, DremioVersionInfo.getVersion());
  }

  void testRemoveNode(String address, String dremioVersion) {
    NodeEndpoint removedNode =
        NodeEndpoint.newBuilder().setAddress(address).setDremioVersion(dremioVersion).build();
    endpoints.remove(removedNode);
    if (listener != null) {
      listener.nodesUnregistered(ImmutableSet.of(removedNode));
    }
  }
}
