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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import com.dremio.config.DremioConfig;
import com.dremio.options.OptionManager;
import com.dremio.services.credentials.proto.CredentialsServiceGrpc;
import com.dremio.services.credentials.proto.LookupRequest;
import com.dremio.services.credentials.proto.LookupResponse;
import com.dremio.test.DremioTest;

import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;

public class TestExecutorCredentialsService extends DremioTest {
  @ClassRule
  public static final GrpcCleanupRule grpcCleanupRule = new GrpcCleanupRule();


  private CredentialsService mockDelegate;
  private CredentialsService executorCredentialsService;
  private CredentialsServiceGrpc.CredentialsServiceBlockingStub credentialsStub;
  private CredentialsServiceGrpc.CredentialsServiceImplBase grpcCredentialsService;
  private OptionManager mockOptionManager;


  @Before
  public void setUp() throws Exception {
    mockDelegate = mock(CredentialsService.class);
    mockOptionManager = mock(OptionManager.class);

    grpcCredentialsService = mock(CredentialsServiceGrpc.CredentialsServiceImplBase.class);
    final String channelName = InProcessServerBuilder.generateName();
    grpcCleanupRule.register(InProcessServerBuilder.forName(channelName)
      .directExecutor()
      .addService(grpcCredentialsService)
      .build()
      .start());
    ManagedChannel channel = grpcCleanupRule.register(InProcessChannelBuilder.forName(channelName).directExecutor().build());
    credentialsStub = CredentialsServiceGrpc.newBlockingStub(channel);


    DremioConfig execConfig = DEFAULT_DREMIO_CONFIG
      .withValue(DremioConfig.ENABLE_COORDINATOR_BOOL, false)
      .withValue(DremioConfig.ENABLE_EXECUTOR_BOOL, true);
    executorCredentialsService = new ExecutorCredentialsService(
      mockDelegate,
      () -> credentialsStub,
      () -> mockOptionManager);

    when(mockDelegate.lookup(any())).thenReturn("someSecret");
  }

  @Test
  public void testRemoteLookupEnabled() throws Exception {
    when(mockOptionManager.getOption(REMOTE_LOOKUP_ENABLED)).thenReturn(true);
    doAnswer(invocation -> {
      LookupRequest request = invocation.getArgument(0, LookupRequest.class);
      StreamObserver observer = invocation.getArgument(1, StreamObserver.class);
      observer.onNext(LookupResponse.newBuilder().setSecret("someSecret").build());
      observer.onCompleted();
      return null;
    }).when(grpcCredentialsService).lookup(any(), any());

    assertEquals("someSecret", executorCredentialsService.lookup("anyPattern"));
  }

  @Test
  public void testRemoteLookupDisabled() throws Exception {
    when(mockOptionManager.getOption(REMOTE_LOOKUP_ENABLED)).thenReturn(false);
    executorCredentialsService.lookup("anyPattern");

    verify(mockDelegate,times(1)).lookup(any());
  }
}
