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
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
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
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Contents;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.ImmutableIcebergTable;
import org.projectnessie.services.rest.ContentsResource;

import com.dremio.service.nessieapi.ContentsKey;
import com.dremio.service.nessieapi.GetContentsRequest;
import com.dremio.service.nessieapi.SetContentsRequest;
import com.google.common.collect.Lists;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

/**
 * Unit tests for the ContentsApiService
 */
@RunWith(MockitoJUnitRunner.class)
public class TestContentsApiService {
  private ContentsApiService contentsApiService;
  @Mock private ContentsResource contentsResource;

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
        .setRef("foo")
        .build();
    final Contents contents = ImmutableIcebergTable.builder().metadataLocation("here").build();
    when(contentsResource.getContents(nessieContentsKey, "foo")).thenReturn(contents);

    StreamObserver responseObserver = mock(StreamObserver.class);
    contentsApiService.getContents(request, responseObserver);

    final InOrder inOrder = inOrder(responseObserver);
    final ArgumentCaptor<com.dremio.service.nessieapi.Contents> contentsCaptor =
        ArgumentCaptor.forClass(com.dremio.service.nessieapi.Contents.class);
    inOrder.verify(responseObserver).onNext(contentsCaptor.capture());

    final com.dremio.service.nessieapi.Contents actualContents = contentsCaptor.getValue();
    assertEquals(com.dremio.service.nessieapi.Contents.Type.ICEBERG_TABLE, actualContents.getType());
    assertEquals("here", actualContents.getIcebergTable().getMetadataLocation());

    inOrder.verify(responseObserver).onCompleted();
  }

  @Test
  public void getContentsNotFound() throws NessieNotFoundException {
    final ContentsKey contentsKey = ContentsKey.newBuilder().addElements("a").addElements("b").build();
    final org.projectnessie.model.ContentsKey nessieContentsKey = org.projectnessie.model.ContentsKey.of("a", "b");
    final GetContentsRequest request = GetContentsRequest
      .newBuilder()
      .setContentsKey(contentsKey)
      .setRef("foo")
      .build();
    when(contentsResource.getContents(nessieContentsKey, "foo")).thenThrow(new NessieNotFoundException("Not found"));

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
      .setRef("foo")
      .build();
    when(contentsResource.getContents(nessieContentsKey, "foo")).thenThrow(new RuntimeException("Not found"));

    final StreamObserver responseObserver = mock(StreamObserver.class);
    contentsApiService.getContents(request, responseObserver);

    final ArgumentCaptor<Throwable> errorCaptor = ArgumentCaptor.forClass(Throwable.class);
    verify(responseObserver).onError(errorCaptor.capture());

    final StatusRuntimeException status = (StatusRuntimeException) errorCaptor.getValue();
    assertEquals(Status.UNKNOWN.getCode(), status.getStatus().getCode());
  }

  @Test
  public void setContents() throws Exception {
    final SetContentsRequest request = SetContentsRequest
        .newBuilder()
        .setBranch("foo")
        .setHash("0011223344556677")
        .setMessage("bar")
        .setContentsKey(ContentsKey.newBuilder().addElements("a").addElements("b"))
        .setContents(com.dremio.service.nessieapi.Contents
            .newBuilder()
            .setType(com.dremio.service.nessieapi.Contents.Type.ICEBERG_TABLE)
        .setIcebergTable(com.dremio.service.nessieapi.Contents.IcebergTable.newBuilder().setMetadataLocation("here")))
        .build();

    StreamObserver responseObserver = mock(StreamObserver.class);
    contentsApiService.setContents(request, responseObserver);

    final ArgumentCaptor<org.projectnessie.model.ContentsKey> contentsKeyCaptor =
        ArgumentCaptor.forClass(org.projectnessie.model.ContentsKey.class);
    final ArgumentCaptor<Contents> contentsCaptor = ArgumentCaptor.forClass(Contents.class);
    verify(contentsResource).setContents(
        contentsKeyCaptor.capture(),
        eq("foo"),
        eq("0011223344556677"),
        eq("bar"),
        contentsCaptor.capture()
    );

    final org.projectnessie.model.ContentsKey actualContentsKey = contentsKeyCaptor.getValue();
    assertEquals(Lists.newArrayList("a", "b"), actualContentsKey.getElements());

    assertTrue(contentsCaptor.getValue() instanceof IcebergTable);
    final IcebergTable icebergTable = (IcebergTable) contentsCaptor.getValue();
    assertEquals("here", icebergTable.getMetadataLocation());
  }

  @Test
  public void setContentsConflict() throws Exception {
    final SetContentsRequest request = SetContentsRequest
      .newBuilder()
      .setBranch("foo")
      .setHash("0011223344556677")
      .setMessage("bar")
      .setContentsKey(ContentsKey.newBuilder().addElements("a").addElements("b"))
      .setContents(com.dremio.service.nessieapi.Contents
        .newBuilder()
        .setType(com.dremio.service.nessieapi.Contents.Type.ICEBERG_TABLE)
        .setIcebergTable(com.dremio.service.nessieapi.Contents.IcebergTable.newBuilder().setMetadataLocation("here")))
      .build();

    doThrow(new NessieConflictException("foo")).when(contentsResource).setContents(
      any(org.projectnessie.model.ContentsKey.class), anyString(), anyString(), anyString(), any(Contents.class));

    final StreamObserver streamObserver = mock(StreamObserver.class);
    contentsApiService.setContents(request, streamObserver);

    final ArgumentCaptor<Throwable> errorCaptor = ArgumentCaptor.forClass(Throwable.class);
    verify(streamObserver).onError(errorCaptor.capture());

    StatusRuntimeException status = (StatusRuntimeException) errorCaptor.getValue();
    assertEquals(Status.ABORTED.getCode(), status.getStatus().getCode());
  }

  @Test
  public void setContentsUnexpectedException() throws Exception {
    final SetContentsRequest request = SetContentsRequest
      .newBuilder()
      .setBranch("foo")
      .setHash("0011223344556677")
      .setMessage("bar")
      .setContentsKey(ContentsKey.newBuilder().addElements("a").addElements("b"))
      .setContents(com.dremio.service.nessieapi.Contents
        .newBuilder()
        .setType(com.dremio.service.nessieapi.Contents.Type.ICEBERG_TABLE)
        .setIcebergTable(com.dremio.service.nessieapi.Contents.IcebergTable.newBuilder().setMetadataLocation("here")))
      .build();

    doThrow(new RuntimeException("foo")).when(contentsResource).setContents(
      any(org.projectnessie.model.ContentsKey.class), anyString(), anyString(), anyString(), any(Contents.class));

    final StreamObserver streamObserver = mock(StreamObserver.class);
    contentsApiService.setContents(request, streamObserver);

    final ArgumentCaptor<Throwable> errorCaptor = ArgumentCaptor.forClass(Throwable.class);
    verify(streamObserver).onError(errorCaptor.capture());

    StatusRuntimeException status = (StatusRuntimeException) errorCaptor.getValue();
    assertEquals(Status.UNKNOWN.getCode(), status.getStatus().getCode());
  }
}
