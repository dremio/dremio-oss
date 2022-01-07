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
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.projectnessie.api.ContentsApi;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Contents;
import org.projectnessie.model.ImmutableIcebergTable;

import com.dremio.service.nessieapi.ContentsKey;
import com.dremio.service.nessieapi.GetContentsRequest;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

/**
 * Unit tests for the ContentsApiService
 */
@RunWith(MockitoJUnitRunner.class)
public class TestContentsApiService {
  private ContentsApiService contentsApiService;
  @Mock private ContentsApi contentsResource;

  private static final String BRANCH = "foo";
  private static final String HASH = "bar";
  private static final String METADATA_LOCATION = "here";
  private static final String ID_GENERATOR = "X";

  @Before
  public void setup() {
    contentsApiService = new ContentsApiService(() -> contentsResource);
  }

  @Test
  public void getContentsFound() throws Exception {
    final ContentsKey contentsKey = ContentsKey.newBuilder().addElements("a").addElements("b").build();
    final org.projectnessie.model.ContentsKey nessieContentsKey = org.projectnessie.model.ContentsKey.of("a", "b");
    final GetContentsRequest request = GetContentsRequest
        .newBuilder()
        .setContentsKey(contentsKey)
        .setRef(BRANCH)
        .setHashOnRef(HASH)
        .build();
    final Contents contents = ImmutableIcebergTable.builder()
      .metadataLocation(METADATA_LOCATION).idGenerators(ID_GENERATOR).build();
    when(contentsResource.getContents(nessieContentsKey, BRANCH, HASH)).thenReturn(contents);

    StreamObserver responseObserver = mock(StreamObserver.class);
    contentsApiService.getContents(request, responseObserver);

    final InOrder inOrder = inOrder(responseObserver);
    final ArgumentCaptor<com.dremio.service.nessieapi.Contents> contentsCaptor =
        ArgumentCaptor.forClass(com.dremio.service.nessieapi.Contents.class);
    inOrder.verify(responseObserver).onNext(contentsCaptor.capture());

    final com.dremio.service.nessieapi.Contents actualContents = contentsCaptor.getValue();
    assertEquals(com.dremio.service.nessieapi.Contents.Type.ICEBERG_TABLE, actualContents.getType());
    assertEquals(METADATA_LOCATION, actualContents.getIcebergTable().getMetadataLocation());

    inOrder.verify(responseObserver).onCompleted();
  }

  @Test
  public void getContentsNotFound() throws NessieNotFoundException {
    final ContentsKey contentsKey = ContentsKey.newBuilder().addElements("a").addElements("b").build();
    final org.projectnessie.model.ContentsKey nessieContentsKey = org.projectnessie.model.ContentsKey.of("a", "b");
    final GetContentsRequest request = GetContentsRequest
      .newBuilder()
      .setContentsKey(contentsKey)
      .setRef(BRANCH)
      .setHashOnRef(HASH)
      .build();
    when(contentsResource.getContents(nessieContentsKey, BRANCH, HASH))
      .thenThrow(new NessieNotFoundException("Not found"));

    final StreamObserver responseObserver = mock(StreamObserver.class);
    contentsApiService.getContents(request, responseObserver);

    final ArgumentCaptor<Throwable> errorCaptor = ArgumentCaptor.forClass(Throwable.class);
    verify(responseObserver).onError(errorCaptor.capture());

    final StatusRuntimeException status = (StatusRuntimeException) errorCaptor.getValue();
    assertEquals(Status.NOT_FOUND.getCode(), status.getStatus().getCode());
  }

  @Test
  public void getContentsUnexpectedException() throws NessieNotFoundException {
    final ContentsKey contentsKey = ContentsKey.newBuilder().addElements("a").addElements("b").build();
    final org.projectnessie.model.ContentsKey nessieContentsKey = org.projectnessie.model.ContentsKey.of("a", "b");
    final GetContentsRequest request = GetContentsRequest
      .newBuilder()
      .setContentsKey(contentsKey)
      .setRef(BRANCH)
      .setHashOnRef(HASH)
      .build();
    when(contentsResource.getContents(nessieContentsKey, BRANCH, HASH)).thenThrow(new RuntimeException("Not found"));

    final StreamObserver responseObserver = mock(StreamObserver.class);
    contentsApiService.getContents(request, responseObserver);

    final ArgumentCaptor<Throwable> errorCaptor = ArgumentCaptor.forClass(Throwable.class);
    verify(responseObserver).onError(errorCaptor.capture());

    final StatusRuntimeException status = (StatusRuntimeException) errorCaptor.getValue();
    assertEquals(Status.UNKNOWN.getCode(), status.getStatus().getCode());
  }
}
