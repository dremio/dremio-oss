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
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
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
import org.projectnessie.api.TreeApi;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.ImmutableIcebergTable;
import org.projectnessie.model.Operations;

import com.dremio.service.nessieapi.Branch;
import com.dremio.service.nessieapi.CommitMultipleOperationsRequest;
import com.dremio.service.nessieapi.Contents;
import com.dremio.service.nessieapi.ContentsKey;
import com.dremio.service.nessieapi.CreateReferenceRequest;
import com.dremio.service.nessieapi.GetReferenceByNameRequest;
import com.dremio.service.nessieapi.Operation;
import com.dremio.service.nessieapi.Reference;
import com.google.common.collect.Lists;
import com.google.protobuf.Empty;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

/**
 * Unit tests for the TreeApiService
 */
@RunWith(MockitoJUnitRunner.class)
public class TestTreeApiService {
  private TreeApiService treeApiService;
  @Mock private TreeApi treeResource;

  @Before
  public void setup() {
    treeApiService = new TreeApiService(() -> treeResource);
  }

  @Test
  public void createReference() throws Exception {
    final Branch branch = Branch.newBuilder().setName("foo").setHash("0011223344556677").build();
    final Reference branchRef = Reference.newBuilder().setBranch(branch).build();
    final CreateReferenceRequest request = CreateReferenceRequest
      .newBuilder()
      .setReference(branchRef)
      .setSourceRefName("sourceRef")
      .build();

    StreamObserver responseObserver = mock(StreamObserver.class);
    treeApiService.createReference(request, responseObserver);

    final ArgumentCaptor<org.projectnessie.model.Reference> referenceCaptor =
      ArgumentCaptor.forClass(org.projectnessie.model.Reference.class);
    final ArgumentCaptor<String> sourceRefNameCaptor = ArgumentCaptor.forClass(String.class);
    verify(treeResource).createReference(sourceRefNameCaptor.capture(), referenceCaptor.capture());

    final org.projectnessie.model.Reference actualReference = referenceCaptor.getValue();
    assertEquals("foo", actualReference.getName());
    assertEquals("0011223344556677", actualReference.getHash());
    assertEquals("sourceRef", sourceRefNameCaptor.getValue());
  }

  @Test
  public void createReferenceAlreadyExists() throws Exception {
    final Branch branch = Branch.newBuilder().setName("foo").setHash("0011223344556677").build();
    final Reference branchRef = Reference.newBuilder().setBranch(branch).build();

    doThrow(new NessieConflictException("already exists")).when(treeResource)
      .createReference(anyString(), any(org.projectnessie.model.Reference.class));

    final StreamObserver streamObserver = mock(StreamObserver.class);
    treeApiService.createReference(CreateReferenceRequest.newBuilder().setReference(branchRef).build(), streamObserver);

    final ArgumentCaptor<Throwable> errorCaptor = ArgumentCaptor.forClass(Throwable.class);
    verify(streamObserver).onError(errorCaptor.capture());

    final StatusRuntimeException status = (StatusRuntimeException) errorCaptor.getValue();
    assertEquals(Status.ALREADY_EXISTS.getCode(), status.getStatus().getCode());
  }

  @Test
  public void createReferenceUnexpectedException() throws Exception {
    final Branch branch = Branch.newBuilder().setName("foo").setHash("0011223344556677").build();
    final Reference branchRef = Reference.newBuilder().setBranch(branch).build();

    doThrow(new RuntimeException("Unexpected exception")).when(treeResource)
      .createReference(anyString(), any(org.projectnessie.model.Reference.class));

    final StreamObserver streamObserver = mock(StreamObserver.class);
    treeApiService.createReference(CreateReferenceRequest.newBuilder().setReference(branchRef).build(), streamObserver);

    final ArgumentCaptor<Throwable> errorCaptor = ArgumentCaptor.forClass(Throwable.class);
    verify(streamObserver).onError(errorCaptor.capture());

    final StatusRuntimeException status = (StatusRuntimeException) errorCaptor.getValue();
    assertEquals(Status.UNKNOWN.getCode(), status.getStatus().getCode());
  }

  @Test
  public void getReferenceByNameFound() throws NessieNotFoundException {
    final org.projectnessie.model.Reference nessieRef = createTagReference("foo", "0011223344556677");
    when(treeResource.getReferenceByName("foo")).thenReturn(nessieRef);
    final StreamObserver streamObserver = mock(StreamObserver.class);

    treeApiService.getReferenceByName(GetReferenceByNameRequest.newBuilder().setRefName("foo").build(), streamObserver);
    verify(streamObserver, never()).onError(any(Throwable.class));

    final InOrder inOrder = inOrder(streamObserver);
    final ArgumentCaptor<Reference> referenceCaptor = ArgumentCaptor.forClass(Reference.class);
    inOrder.verify(streamObserver).onNext(referenceCaptor.capture());

    final Reference ref = referenceCaptor.getValue();
    assertTrue(ref.hasTag());
    assertEquals("foo", ref.getTag().getName());
    assertEquals("0011223344556677", ref.getTag().getHash());

    inOrder.verify(streamObserver).onCompleted();
  }

  @Test
  public void getReferenceByNameNotFound() throws NessieNotFoundException {
    when(treeResource.getReferenceByName("foo")).thenThrow(new NessieNotFoundException("Not found"));
    final StreamObserver streamObserver = mock(StreamObserver.class);
    treeApiService.getReferenceByName(GetReferenceByNameRequest.newBuilder().setRefName("foo").build(), streamObserver);

    final ArgumentCaptor<Throwable> errorCaptor = ArgumentCaptor.forClass(Throwable.class);
    verify(streamObserver).onError(errorCaptor.capture());

    final StatusRuntimeException status = (StatusRuntimeException) errorCaptor.getValue();
    assertEquals(Status.NOT_FOUND.getCode(), status.getStatus().getCode());
  }

  @Test
  public void getReferenceByNameUnexpectedException() throws NessieNotFoundException {
    when(treeResource.getReferenceByName("foo")).thenThrow(new RuntimeException("Unexpected exception"));
    final StreamObserver streamObserver = mock(StreamObserver.class);
    treeApiService.getReferenceByName(GetReferenceByNameRequest.newBuilder().setRefName("foo").build(), streamObserver);

    final ArgumentCaptor<Throwable> errorCaptor = ArgumentCaptor.forClass(Throwable.class);
    verify(streamObserver).onError(errorCaptor.capture());

    final StatusRuntimeException status = (StatusRuntimeException) errorCaptor.getValue();
    assertEquals(Status.UNKNOWN.getCode(), status.getStatus().getCode());
  }

  @Test
  public void getDefaultBranchFound() throws NessieNotFoundException {
    final org.projectnessie.model.Branch defaultBranch = createBranch("foo", "0011223344556677");
    when(treeResource.getDefaultBranch()).thenReturn(defaultBranch);

    final StreamObserver streamObserver = mock(StreamObserver.class);
    treeApiService.getDefaultBranch(Empty.getDefaultInstance(), streamObserver);
    verify(streamObserver, never()).onError(any(Throwable.class));

    final InOrder inOrder = inOrder(streamObserver);
    final ArgumentCaptor<Branch> branchCaptor = ArgumentCaptor.forClass(Branch.class);
    inOrder.verify(streamObserver).onNext(branchCaptor.capture());

    final Branch branch = branchCaptor.getValue();
    assertEquals("foo", branch.getName());
    assertEquals("0011223344556677", branch.getHash());

    inOrder.verify(streamObserver).onCompleted();
  }

  @Test
  public void getDefaultBranchNotFound() throws NessieNotFoundException {
    when(treeResource.getDefaultBranch()).thenThrow(new NessieNotFoundException("Not found"));
    final StreamObserver streamObserver = mock(StreamObserver.class);
    treeApiService.getDefaultBranch(Empty.getDefaultInstance(), streamObserver);

    final ArgumentCaptor<Throwable> errorCaptor = ArgumentCaptor.forClass(Throwable.class);
    verify(streamObserver).onError(errorCaptor.capture());

    final StatusRuntimeException status = (StatusRuntimeException) errorCaptor.getValue();
    assertEquals(Status.NOT_FOUND.getCode(), status.getStatus().getCode());
  }

  @Test
  public void getDefaultBranchUnexpectedException() throws NessieNotFoundException {
    when(treeResource.getDefaultBranch()).thenThrow(new RuntimeException("Unexpected exception"));
    final StreamObserver streamObserver = mock(StreamObserver.class);
    treeApiService.getDefaultBranch(Empty.getDefaultInstance(), streamObserver);

    final ArgumentCaptor<Throwable> errorCaptor = ArgumentCaptor.forClass(Throwable.class);
    verify(streamObserver).onError(errorCaptor.capture());

    final StatusRuntimeException status = (StatusRuntimeException) errorCaptor.getValue();
    assertEquals(Status.UNKNOWN.getCode(), status.getStatus().getCode());
  }

  @Test
  public void commitMultipleOperationsSucceeds() throws Exception {
    final Contents.IcebergTable icebergTable = Contents.IcebergTable.newBuilder().setMetadataLocation("here").build();
    final CommitMultipleOperationsRequest request = createCommitMulitpleOperationsRequest(icebergTable);

    final StreamObserver streamObserver = mock(StreamObserver.class);
    treeApiService.commitMultipleOperations(request, streamObserver);

    verify(streamObserver, never()).onError(any(Throwable.class));

    final ArgumentCaptor<Operations> operationsCaptor = ArgumentCaptor.forClass(Operations.class);
    verify(streamObserver).onNext(any(Empty.class));
    verify(streamObserver).onCompleted();

    verify(treeResource).commitMultipleOperations(
        eq("foo"),
        eq("bar"),
        operationsCaptor.capture()
    );

    assertEquals(1, operationsCaptor.getValue().getOperations().size());
    final org.projectnessie.model.Operation operation = operationsCaptor.getValue().getOperations().get(0);
    assertTrue(operation instanceof org.projectnessie.model.Operation.Put);
    final org.projectnessie.model.Operation.Put put = (org.projectnessie.model.Operation.Put) operation;

    assertEquals(Lists.newArrayList("a", "b"), put.getKey().getElements());

    assertTrue(put.getContents() instanceof ImmutableIcebergTable);
    final ImmutableIcebergTable table = (ImmutableIcebergTable) put.getContents();
    assertEquals(icebergTable.getMetadataLocation(), table.getMetadataLocation());
  }

  @Test
  public void commitMulipleOperationsNotFound() throws Exception {
    final Contents.IcebergTable icebergTable = Contents.IcebergTable.newBuilder().setMetadataLocation("here").build();
    final CommitMultipleOperationsRequest request = createCommitMulitpleOperationsRequest(icebergTable);

    doThrow(new NessieNotFoundException("foo")).when(treeResource).commitMultipleOperations(
      anyString(), anyString(), any(Operations.class));
    final StreamObserver streamObserver = mock(StreamObserver.class);
    treeApiService.commitMultipleOperations(request, streamObserver);

    final ArgumentCaptor<Throwable> errorCaptor = ArgumentCaptor.forClass(Throwable.class);
    verify(streamObserver).onError(errorCaptor.capture());

    StatusRuntimeException status = (StatusRuntimeException) errorCaptor.getValue();
    assertEquals(Status.NOT_FOUND.getCode(), status.getStatus().getCode());
  }

  @Test
  public void commitMulipleOperationsConflict() throws Exception {
    final Contents.IcebergTable icebergTable = Contents.IcebergTable.newBuilder().setMetadataLocation("here").build();
    final CommitMultipleOperationsRequest request = createCommitMulitpleOperationsRequest(icebergTable);

    doThrow(new NessieConflictException("foo")).when(treeResource).commitMultipleOperations(
      anyString(), anyString(), any(Operations.class));
    final StreamObserver streamObserver = mock(StreamObserver.class);
    treeApiService.commitMultipleOperations(request, streamObserver);

    final ArgumentCaptor<Throwable> errorCaptor = ArgumentCaptor.forClass(Throwable.class);
    verify(streamObserver).onError(errorCaptor.capture());

    StatusRuntimeException status = (StatusRuntimeException) errorCaptor.getValue();
    assertEquals(Status.ABORTED.getCode(), status.getStatus().getCode());
  }

  @Test
  public void commitMulipleOperationsUnexpectedException() throws Exception {
    final Contents.IcebergTable icebergTable = Contents.IcebergTable.newBuilder().setMetadataLocation("here").build();
    final CommitMultipleOperationsRequest request = createCommitMulitpleOperationsRequest(icebergTable);

    doThrow(new RuntimeException("Unexpected exception")).when(treeResource).commitMultipleOperations(
      anyString(), anyString(), any(Operations.class));
    final StreamObserver streamObserver = mock(StreamObserver.class);
    treeApiService.commitMultipleOperations(request, streamObserver);

    final ArgumentCaptor<Throwable> errorCaptor = ArgumentCaptor.forClass(Throwable.class);
    verify(streamObserver).onError(errorCaptor.capture());

    StatusRuntimeException status = (StatusRuntimeException) errorCaptor.getValue();
    assertEquals(Status.UNKNOWN.getCode(), status.getStatus().getCode());
  }

  private org.projectnessie.model.Branch createBranch(String name, String hash) {
    return org.projectnessie.model.ImmutableBranch.builder().name(name).hash(hash).build();
  }

  private org.projectnessie.model.Reference createBranchReference(String name, String hash) {
    return createBranch(name, hash);
  }

  private org.projectnessie.model.Reference createTagReference(String name, String hash) {
    return org.projectnessie.model.ImmutableTag.builder().name(name).hash(hash).build();
  }

  private CommitMultipleOperationsRequest createCommitMulitpleOperationsRequest(Contents.IcebergTable icebergTable) {
    return CommitMultipleOperationsRequest
      .newBuilder()
      .setBranchName("foo")
      .setExpectedHash("bar")
      .setMessage("message")
      .addOperations(Operation
        .newBuilder()
        .setType(Operation.Type.PUT)
        .setContentsKey(ContentsKey.newBuilder().addElements("a").addElements("b"))
        .setContents(Contents.newBuilder().setType(Contents.Type.ICEBERG_TABLE).setIcebergTable(icebergTable)))
      .build();
  }
}
