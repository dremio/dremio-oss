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
package com.dremio.service.conduit;

import java.net.InetAddress;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.service.DirectProvider;
import com.dremio.service.conduit.ConduitTestServiceGrpc.ConduitTestServiceBlockingStub;
import com.dremio.service.conduit.client.ConduitProvider;
import com.dremio.service.conduit.client.ConduitProviderImpl;
import com.dremio.service.conduit.server.ConduitServer;
import com.dremio.service.conduit.server.ConduitServerTestUtils;
import com.dremio.service.conduit.server.ConduitServiceRegistry;
import com.dremio.service.conduit.server.ConduitServiceRegistryImpl;
import com.google.common.collect.Sets;

import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import io.grpc.testing.GrpcCleanupRule;

/**
 * Test conduit.
 */
public class TestConduit {

  @Rule
  public final GrpcCleanupRule grpcCleanupRule = new GrpcCleanupRule();

  @Rule
  public final ExpectedException thrownException = ExpectedException.none();

  private ConduitServer conduitServer1;
  private NodeEndpoint endpoint1;

  private ConduitServer conduitServer2;
  private NodeEndpoint endpoint2;

  private ConduitProviderImpl conduitProvider;

  @Before
  public void setUp() throws Exception {
    final ConduitServiceRegistry serviceRegistry1 = new ConduitServiceRegistryImpl();
    serviceRegistry1.registerService(new ConduitTestService());

    conduitServer1 = new ConduitServer(DirectProvider.wrap(serviceRegistry1), 0, Optional.empty()
      , UUID.randomUUID().toString());
    conduitServer1.start();
    grpcCleanupRule.register(ConduitServerTestUtils.getServer(conduitServer1));

    final ConduitServiceRegistry serviceRegistry2 = new ConduitServiceRegistryImpl();
    conduitServer2 = new ConduitServer(DirectProvider.wrap(serviceRegistry2), 0, Optional.empty()
      , UUID.randomUUID().toString());
    conduitServer2.start();
    grpcCleanupRule.register(ConduitServerTestUtils.getServer(conduitServer2));

    final String address = InetAddress.getLocalHost().getCanonicalHostName();
    endpoint1 = NodeEndpoint.newBuilder()
      .setAddress(address)
      .setConduitPort(conduitServer1.getPort())
      .build();
    endpoint2 = NodeEndpoint.newBuilder()
      .setAddress(address)
      .setConduitPort(conduitServer2.getPort())
      .build();

    final Set<NodeEndpoint> endpoints = Sets.newHashSet(endpoint1, endpoint2);

    conduitProvider = new ConduitProviderImpl(DirectProvider.wrap(endpoint1), Optional.empty());
  }

  @After
  public void tearDown() throws Exception {
    if (conduitServer1 != null) {
      conduitServer1.close();
    }
    if (conduitServer2 != null) {
      conduitServer2.close();
    }
    if (conduitProvider != null) {
      conduitProvider.close();
    }
  }

  public ConduitProvider getConduitProvider() {
    return conduitProvider;
  }

  public NodeEndpoint getEndpoint1() {
    return endpoint1;
  }

  public NodeEndpoint getEndpoint2() {
    return endpoint2;
  }

  @Test
  public void increment() {
    final ManagedChannel channel = getConduitProvider().getOrCreateChannel(getEndpoint1());
    grpcCleanupRule.register(channel);

    final ConduitTestServiceBlockingStub stub =
      ConduitTestServiceGrpc.newBlockingStub(channel);

    Assert.assertEquals(2, stub.increment(IncrementRequest.newBuilder().setI(1).build()).getI());
  }

  @Test
  public void unimplemented() {
    thrownException.expect(StatusRuntimeException.class);
    thrownException.expectMessage("Method not found");

    final ManagedChannel channel = getConduitProvider().getOrCreateChannel(getEndpoint2());
    grpcCleanupRule.register(channel);

    final ConduitTestServiceBlockingStub stub =
      ConduitTestServiceGrpc.newBlockingStub(channel);

    //noinspection ResultOfMethodCallIgnored
    stub.increment(IncrementRequest.newBuilder().setI(1).build());
  }
}
