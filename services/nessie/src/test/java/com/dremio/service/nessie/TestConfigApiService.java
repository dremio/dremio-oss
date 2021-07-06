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
package com.dremio.service.nessie;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.projectnessie.services.rest.ConfigResource;

import com.dremio.service.nessieapi.NessieConfiguration;
import com.google.protobuf.Empty;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

/**
 * Unit tests for the TreeApiService
 */
@RunWith(MockitoJUnitRunner.class)
public class TestConfigApiService {
  private ConfigApiService configApiService;
  @Mock private ConfigResource configResource;

  @Before
  public void setup() {
    configApiService = new ConfigApiService(() -> configResource);
  }

  @Test
  public void getConfig() {
    final org.projectnessie.model.NessieConfiguration config = createNessieConfiguration("main", "1.0");
    when(configResource.getConfig()).thenReturn(config);

    final StreamObserver streamObserver = mock(StreamObserver.class);
    configApiService.getConfig(Empty.getDefaultInstance(), streamObserver);
    verify(streamObserver, never()).onError(any(Throwable.class));

    final InOrder inOrder = inOrder(streamObserver);
    final ArgumentCaptor<NessieConfiguration> configCaptor = ArgumentCaptor.forClass(NessieConfiguration.class);
    inOrder.verify(streamObserver).onNext(configCaptor.capture());

    final NessieConfiguration nessieConfiguration = configCaptor.getValue();
    assertEquals("main", nessieConfiguration.getDefaultBranch());
    assertEquals("1.0", nessieConfiguration.getVersion());

    inOrder.verify(streamObserver).onCompleted();
  }

  @Test
  public void getDefaultBranchUnexpectedException() {
    when(configResource.getConfig()).thenThrow(new RuntimeException("Unexpected exception"));
    final StreamObserver streamObserver = mock(StreamObserver.class);
    configApiService.getConfig(Empty.getDefaultInstance(), streamObserver);

    final ArgumentCaptor<Throwable> errorCaptor = ArgumentCaptor.forClass(Throwable.class);
    verify(streamObserver).onError(errorCaptor.capture());

    final StatusRuntimeException status = (StatusRuntimeException) errorCaptor.getValue();
    assertEquals(Status.UNKNOWN.getCode(), status.getStatus().getCode());
  }

  private org.projectnessie.model.NessieConfiguration createNessieConfiguration(String defaultBranch, String version) {
    return org.projectnessie.model.ImmutableNessieConfiguration.builder().defaultBranch(defaultBranch).version(version).build();
  }
}
