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
package com.dremio.services.credentials;

import static com.dremio.services.credentials.CredentialsServiceUtils.REMOTE_LOOKUP_ENABLED;
import static com.dremio.services.credentials.CredentialsTestingUtils.getLeaderConfig;
import static com.dremio.services.credentials.CredentialsTestingUtils.getNonLeaderConfig;
import static com.dremio.services.credentials.CredentialsTestingUtils.isRemoteResolved;
import static com.dremio.services.credentials.TestingCredentialsProvider.isResolved;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dremio.config.DremioConfig;
import com.dremio.options.OptionManager;
import com.dremio.services.credentials.proto.CredentialsServiceGrpc;
import com.dremio.test.DremioTest;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Providers;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for {@link CredentialsServiceImpl} that involve a leader and a non-leader instance to
 * simulate remote calls between nodes.
 */
public class TestCredentialsServiceImplRemote extends DremioTest {
  @ClassRule public static final GrpcCleanupRule GRPC_CLEANUP_RULE = new GrpcCleanupRule();
  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();
  private static ManagedChannel channelToLeader;
  private static CredentialsServiceImpl leader;
  private CredentialsServiceImpl nonLeader;
  private OptionManager optionManager;

  @BeforeClass
  public static void beforeClass() throws Exception {
    final String serverName = InProcessServerBuilder.generateName();
    final Injector injector =
        Guice.createInjector(
            new CredentialsProviderModule(
                getLeaderConfig(), ImmutableSet.of(TestingCredentialsProvider.class)));
    leader = injector.getInstance(CredentialsServiceImpl.class);

    // Configure a mock gRPC service that uses the leader credentials and marks the resolved secret
    // as remotely resolved
    final CredentialsServiceGrpc.CredentialsServiceImplBase serviceImpl =
        new CredentialsTestingUtils.TestingCredentialsServiceGrpcServer(leader);
    GRPC_CLEANUP_RULE.register(
        InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .addService(serviceImpl)
            .build()
            .start());
    channelToLeader =
        GRPC_CLEANUP_RULE.register(
            InProcessChannelBuilder.forName(serverName).directExecutor().build());
  }

  @AfterClass
  public static void afterClass() throws Exception {
    leader.close();
  }

  @Before
  public void before() throws Exception {
    optionManager = mock(OptionManager.class);
    final DremioConfig conf = getNonLeaderConfig();
    final Injector injector =
        Guice.createInjector(
            new CredentialsProviderModule(conf, ImmutableSet.of(TestingCredentialsProvider.class)),
            new CredentialsProviderContextModule(
                conf, Providers.of(optionManager), Providers.of(channelToLeader)));
    nonLeader = injector.getInstance(CredentialsServiceImpl.class);
  }

  @After
  public void after() throws Exception {
    nonLeader.close();
  }

  @Test
  public void testRemoteLookupDisabled() throws Exception {
    when(optionManager.getOption(REMOTE_LOOKUP_ENABLED)).thenReturn(false);
    final String resolved = nonLeader.lookup(TestingCredentialsProvider.SCHEME + ":testSecret");
    assertTrue(isResolved(resolved));
    assertFalse(isRemoteResolved(resolved));
  }

  @Test
  public void testRemoteLookupEnabled() throws Exception {
    when(optionManager.getOption(REMOTE_LOOKUP_ENABLED)).thenReturn(true);
    final String resolved = nonLeader.lookup(TestingCredentialsProvider.SCHEME + ":testSecret");
    assertTrue(isResolved(resolved));
    assertTrue(isRemoteResolved(resolved));
  }

  @Test
  public void testLeaderLookup() throws Exception {
    final String resolved = leader.lookup(TestingCredentialsProvider.SCHEME + ":testSecret");
    assertTrue(isResolved(resolved));
    assertFalse(isRemoteResolved(resolved));
  }
}
