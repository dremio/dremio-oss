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

import static com.dremio.services.credentials.ExecutorCredentialsService.REMOTE_LOOKUP_ENABLED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.URI;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.config.DremioConfig;
import com.dremio.options.OptionManager;
import com.dremio.service.SingletonRegistry;
import com.dremio.services.credentials.proto.CredentialsServiceGrpc;
import com.dremio.services.credentials.proto.LookupRequest;
import com.dremio.services.credentials.proto.LookupResponse;
import com.dremio.test.DremioTest;
import com.dremio.test.TemporaryEnvironment;

import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;

public class TestExecutorCredentialsService extends DremioTest {
  @ClassRule
  public static final TemporaryEnvironment temporaryEnvironment = new TemporaryEnvironment();
  @ClassRule
  public static final GrpcCleanupRule grpcCleanupRule = new GrpcCleanupRule();
  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();
  private static CredentialsServiceGrpc.CredentialsServiceBlockingStub credentialsStub;
  private OptionManager mockOptionManager;
  private SingletonRegistry registry;
  private ExecutorCredentialsService credentialsService;

  @BeforeClass
  public static void beforeClass() throws Exception {
    final CredentialsServiceGrpc.CredentialsServiceImplBase serviceImpl = new CredentialsServiceGrpc.CredentialsServiceImplBase() {
      @Override
      public void lookup(final LookupRequest request, final StreamObserver<LookupResponse> responseObserver) {
        responseObserver.onNext(LookupResponse.newBuilder()
          .setSecret("REMOTE-RESOLVE: " + request.getPattern())
          .build());
        responseObserver.onCompleted();
      }
    };

    grpcCleanupRule.register(InProcessServerBuilder.forName("exec-cred-test")
      .directExecutor()
      .addService(serviceImpl)
      .build()
      .start());
    final ManagedChannel channel = grpcCleanupRule.register(
      InProcessChannelBuilder.forName("exec-cred-test").directExecutor().build());
    credentialsStub = CredentialsServiceGrpc.newBlockingStub(channel);
  }

  @Before
  public void setUp() throws Exception {
    mockOptionManager = mock(OptionManager.class);
    when(mockOptionManager.getOption(REMOTE_LOOKUP_ENABLED)).thenReturn(true);

    final DremioConfig conf = DEFAULT_DREMIO_CONFIG
      .withValue(DremioConfig.ENABLE_COORDINATOR_BOOL, false)
      .withValue(DremioConfig.ENABLE_EXECUTOR_BOOL, true);

    registry = new SingletonRegistry();
    registry.bind(OptionManager.class, mockOptionManager);
    registry.bind(CredentialsServiceGrpc.CredentialsServiceBlockingStub.class,
      credentialsStub);
    registry.start();

    credentialsService = ExecutorCredentialsService.newInstance(
      conf, CLASSPATH_SCAN_RESULT, () -> credentialsStub, () -> mockOptionManager);
  }

  @After
  public void after() throws Exception {
    if (registry != null) {
      registry.close();
    }
  }

  @Test
  public void testCoordinatorConstruction() {
    final DremioConfig conf = DEFAULT_DREMIO_CONFIG
      .withValue(DremioConfig.ENABLE_COORDINATOR_BOOL, true)
      .withValue(DremioConfig.ENABLE_EXECUTOR_BOOL, false);
    assertTrue(CredentialsService.newInstance(conf, registry, CLASSPATH_SCAN_RESULT)
      instanceof SimpleCredentialsService);
  }

  @Test
  public void testExecutorConstruction() {
    final DremioConfig conf = DEFAULT_DREMIO_CONFIG
      .withValue(DremioConfig.ENABLE_COORDINATOR_BOOL, false)
      .withValue(DremioConfig.ENABLE_EXECUTOR_BOOL, true);
    assertTrue(CredentialsService.newInstance(conf, registry, CLASSPATH_SCAN_RESULT)
      instanceof ExecutorCredentialsService);  }

  @Test
  public void testCPlusEConstruction() {
    final DremioConfig conf = DEFAULT_DREMIO_CONFIG
      .withValue(DremioConfig.ENABLE_COORDINATOR_BOOL, true)
      .withValue(DremioConfig.ENABLE_EXECUTOR_BOOL, true);
    assertTrue(CredentialsService.newInstance(conf, registry, CLASSPATH_SCAN_RESULT)
      instanceof SimpleCredentialsService);
  }

  private boolean isRemoteResolved(String resolvedSecret) {
    return resolvedSecret.contains("REMOTE-RESOLVE");
  }

  @Test
  public void testRemoteLookupDisabled() throws Exception {
    // Disable remote lookup
    when(mockOptionManager.getOption(REMOTE_LOOKUP_ENABLED)).thenReturn(false);

    // Set up a Local Secret
    final URI encrypted = setupAndCheckLocalSecret();

    // Ensure remote path was not taken
    assertFalse(isRemoteResolved(credentialsService.lookup(encrypted.toString())));
  }

  @Test
  public void testRemoteLookupEnabled() throws Exception {
    // Remote lookup is enabled in setup
    // Set up a Local Secret
    final URI encrypted = setupAndCheckLocalSecret();

    // Ensure remote path was not taken
    assertTrue(isRemoteResolved(credentialsService.lookup(encrypted.toString())));
  }

  @Test
  public void testRemoteRestrictedProviders() throws Exception {
    temporaryEnvironment.setEnvironmentVariable("testEnvKey", "testEnvValue");
    assertFalse(isRemoteResolved(credentialsService.lookup("env:testEnvKey")));
    assertFalse(isRemoteResolved(credentialsService.lookup("data:text/plain;base64,test")));
  }


  /**
   * Set up an actual encrypted local secret, and validates the lookup works
   */
  private URI setupAndCheckLocalSecret() throws Exception {
    final String originalString = "secretEncryptDecryptValidation";

    // emulate "dremio-admin encrypt secret"
    final LocalSecretCredentialsProvider credentialsProvider = credentialsService.findProvider(LocalSecretCredentialsProvider.class);
    final URI encrypted = credentialsProvider.encrypt(originalString);
    assertEquals("secret", encrypted.getScheme());

    final String secret = credentialsProvider.lookup(encrypted);
    assertEquals(originalString, secret);
    return encrypted;
  }
}
