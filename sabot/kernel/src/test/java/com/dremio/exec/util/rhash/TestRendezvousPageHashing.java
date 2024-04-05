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
package com.dremio.exec.util.rhash;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.io.file.Path;
import com.dremio.service.coordinator.NodeStatusListener;
import com.dremio.service.coordinator.RegistrationHandle;
import com.dremio.service.coordinator.ServiceSet;
import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.junit.Test;

/** Test the rendezvous page hash function */
@SuppressWarnings("serial")
public class TestRendezvousPageHashing {
  private static final String NODE = "node";
  private final TestServiceSet serviceSet = new TestServiceSet();
  private final RendezvousPageHasher rendezvousPageHasher = new RendezvousPageHasher(serviceSet);

  private void addNodes(int numNodes) {
    for (int i = 0; i < numNodes; i++) {
      serviceSet.addNode(NODE + i);
    }
  }

  private void addNodesInReverseOrder(int numNodes) {
    for (int i = numNodes; i > 0; i--) {
      serviceSet.addNode(NODE + i);
    }
  }

  private void clearServiceSet() {
    serviceSet.clearEndPoints();
  }

  private String[] getEndpointAddress(CoordinationProtos.NodeEndpoint[] nodeEndpoints) {
    return Arrays.stream(nodeEndpoints).map(ep -> ep.getAddress()).toArray(String[]::new);
  }

  @Test
  public void testMultipleOffsets() {
    addNodes(128);
    Path path = Path.of("/cloud/test/file");
    long offset = 1 << 24; // 16M
    CoordinationProtos.NodeEndpoint[] endpoint_16m =
        rendezvousPageHasher.getEndpoints(path, offset);
    String[] addresses_16m = getEndpointAddress(endpoint_16m);
    offset = 1 << 28; // 1M
    CoordinationProtos.NodeEndpoint[] endpoint_256m =
        rendezvousPageHasher.getEndpoints(path, offset);
    String[] addresses_256m = getEndpointAddress(endpoint_256m);
    assertFalse(
        "Endpoints should be different for different offsets",
        Arrays.equals(addresses_16m, addresses_256m));
    clearServiceSet();
  }

  @Test
  public void testConsistentAfterRemove() {
    addNodes(128);
    Path path = Path.of("/cloud/test/file");
    long offset = 1 << 28; // 256M

    CoordinationProtos.NodeEndpoint[] endpoints_before_removal =
        rendezvousPageHasher.getEndpoints(path, offset);
    String[] address_list_before_removal = getEndpointAddress(endpoints_before_removal);
    serviceSet.removeNode(address_list_before_removal[0]);

    CoordinationProtos.NodeEndpoint[] endpoints_after_removal =
        rendezvousPageHasher.getEndpoints(path, offset);
    String[] address_list_after_removal = getEndpointAddress(endpoints_after_removal);
    assertFalse(Arrays.equals(address_list_before_removal, address_list_after_removal));
    assertEquals(address_list_after_removal[0], address_list_before_removal[1]);
    assertEquals(address_list_after_removal[1], address_list_before_removal[2]);

    serviceSet.addNode(address_list_before_removal[0]);
    CoordinationProtos.NodeEndpoint[] endpoints_after_addition =
        rendezvousPageHasher.getEndpoints(path, offset);
    String[] address_list_after_addition = getEndpointAddress(endpoints_after_addition);
    assertTrue(Arrays.equals(address_list_before_removal, address_list_after_addition));
    clearServiceSet();
  }

  @Test
  public void testNodeOrder() {
    addNodes(128);
    Path path = Path.of("/cloud/test/file");
    long offset = 1 << 28; // 256M
    CoordinationProtos.NodeEndpoint[] endpoint_inc_order =
        rendezvousPageHasher.getEndpoints(path, offset);
    String[] address_inc_order = getEndpointAddress(endpoint_inc_order);
    clearServiceSet();

    addNodesInReverseOrder(128);
    CoordinationProtos.NodeEndpoint[] endpoint_dec_order =
        rendezvousPageHasher.getEndpoints(path, offset);
    String[] address_dec_order = getEndpointAddress(endpoint_dec_order);
    assertTrue(Arrays.equals(address_inc_order, address_dec_order));
    clearServiceSet();
  }

  private class TestServiceSet implements ServiceSet {

    private Set<CoordinationProtos.NodeEndpoint> endpoints = new HashSet<>();
    private NodeStatusListener listener;

    @Override
    public RegistrationHandle register(CoordinationProtos.NodeEndpoint endpoint) {
      throw new UnsupportedOperationException("never invoked");
    }

    @Override
    public Collection<CoordinationProtos.NodeEndpoint> getAvailableEndpoints() {
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

    void addNode(String address) {
      CoordinationProtos.NodeEndpoint newNode =
          CoordinationProtos.NodeEndpoint.newBuilder().setAddress(address).build();
      endpoints.add(newNode);
      if (listener != null) {
        listener.nodesRegistered(ImmutableSet.of(newNode));
      }
    }

    void removeNode(String address) {
      CoordinationProtos.NodeEndpoint removedNode =
          CoordinationProtos.NodeEndpoint.newBuilder().setAddress(address).build();
      endpoints.remove(removedNode);
      if (listener != null) {
        listener.nodesUnregistered(ImmutableSet.of(removedNode));
      }
    }

    void clearEndPoints() {
      endpoints.clear();
    }
  }
}
