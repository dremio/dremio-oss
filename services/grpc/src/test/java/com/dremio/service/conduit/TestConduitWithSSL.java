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

import com.dremio.config.DremioConfig;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.rpc.ssl.SSLConfigurator;
import com.dremio.service.DirectProvider;
import com.dremio.service.conduit.client.ConduitProvider;
import com.dremio.service.conduit.client.ConduitProviderImpl;
import com.dremio.service.conduit.server.ConduitServer;
import com.dremio.service.conduit.server.ConduitServerTestUtils;
import com.dremio.service.conduit.server.ConduitServiceRegistry;
import com.dremio.service.conduit.server.ConduitServiceRegistryImpl;
import com.dremio.ssl.SSLEngineFactory;
import com.google.common.collect.Sets;
import java.net.InetAddress;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.After;
import org.junit.Before;

/** Test conduit with SSL. */
public class TestConduitWithSSL extends TestConduit {

  private ConduitServer conduitServer1;
  private NodeEndpoint endpoint1;

  private ConduitServer conduitServer2;
  private NodeEndpoint endpoint2;

  private ConduitProviderImpl conduitProvider;

  @Before
  @Override
  public void setUp() throws Exception {
    final String address = InetAddress.getLocalHost().getCanonicalHostName();

    DremioConfig config =
        DremioConfig.create()
            .withValue(ConduitUtils.CONDUIT_SSL_PREFIX + DremioConfig.SSL_ENABLED, true)
            .withValue(
                ConduitUtils.CONDUIT_SSL_PREFIX + DremioConfig.SSL_AUTO_GENERATED_CERTIFICATE,
                true);
    final SSLConfigurator sslConfigurator =
        new SSLConfigurator(config, () -> null, ConduitUtils.CONDUIT_SSL_PREFIX, "test-conduit");
    final Optional<SSLEngineFactory> conduitSslEngineFactory =
        SSLEngineFactory.create(sslConfigurator.getSSLConfig(false, address));

    final ConduitServiceRegistry serviceRegistry1 = new ConduitServiceRegistryImpl();
    serviceRegistry1.registerService(new ConduitTestService());

    conduitServer1 =
        new ConduitServer(
            DirectProvider.wrap(serviceRegistry1),
            0,
            conduitSslEngineFactory,
            UUID.randomUUID().toString());
    conduitServer1.start();
    grpcCleanupRule.register(ConduitServerTestUtils.getServer(conduitServer1));

    final ConduitServiceRegistry serviceRegistry2 = new ConduitServiceRegistryImpl();
    conduitServer2 =
        new ConduitServer(
            DirectProvider.wrap(serviceRegistry2),
            0,
            conduitSslEngineFactory,
            UUID.randomUUID().toString());
    conduitServer2.start();
    grpcCleanupRule.register(ConduitServerTestUtils.getServer(conduitServer2));

    endpoint1 =
        NodeEndpoint.newBuilder()
            .setAddress(address)
            .setConduitPort(conduitServer1.getPort())
            .build();
    endpoint2 =
        NodeEndpoint.newBuilder()
            .setAddress(address)
            .setConduitPort(conduitServer2.getPort())
            .build();

    final Set<NodeEndpoint> endpoints = Sets.newHashSet(endpoint1, endpoint2);

    conduitProvider =
        new ConduitProviderImpl(DirectProvider.wrap(endpoint1), conduitSslEngineFactory);
  }

  @After
  @Override
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

  @Override
  public ConduitProvider getConduitProvider() {
    return conduitProvider;
  }

  @Override
  public NodeEndpoint getEndpoint1() {
    return endpoint1;
  }

  @Override
  public NodeEndpoint getEndpoint2() {
    return endpoint2;
  }
}
