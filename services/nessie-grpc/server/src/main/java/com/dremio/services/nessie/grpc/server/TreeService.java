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
package com.dremio.services.nessie.grpc.server;

import static com.dremio.services.nessie.grpc.ProtoUtil.fromProto;
import static com.dremio.services.nessie.grpc.ProtoUtil.refFromProto;
import static com.dremio.services.nessie.grpc.ProtoUtil.refToProto;
import static com.dremio.services.nessie.grpc.ProtoUtil.toProto;
import static com.dremio.services.nessie.grpc.client.GrpcExceptionMapper.handle;

import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.projectnessie.api.TreeApi;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Detached;
import org.projectnessie.model.ImmutableMerge;
import org.projectnessie.model.ImmutableTransplant;
import org.projectnessie.model.Tag;

import com.dremio.services.nessie.grpc.ProtoUtil;
import com.dremio.services.nessie.grpc.api.AssignReferenceRequest;
import com.dremio.services.nessie.grpc.api.CommitLogRequest;
import com.dremio.services.nessie.grpc.api.CommitLogResponse;
import com.dremio.services.nessie.grpc.api.CommitRequest;
import com.dremio.services.nessie.grpc.api.CreateReferenceRequest;
import com.dremio.services.nessie.grpc.api.DeleteReferenceRequest;
import com.dremio.services.nessie.grpc.api.Empty;
import com.dremio.services.nessie.grpc.api.EntriesRequest;
import com.dremio.services.nessie.grpc.api.EntriesResponse;
import com.dremio.services.nessie.grpc.api.GetAllReferencesRequest;
import com.dremio.services.nessie.grpc.api.GetAllReferencesResponse;
import com.dremio.services.nessie.grpc.api.GetReferenceByNameRequest;
import com.dremio.services.nessie.grpc.api.MergeRequest;
import com.dremio.services.nessie.grpc.api.Reference;
import com.dremio.services.nessie.grpc.api.TransplantRequest;
import com.dremio.services.nessie.grpc.api.TreeServiceGrpc;

import io.grpc.stub.StreamObserver;

/**
 * The gRPC service implementation for the Tree-API.
 */
public class TreeService extends TreeServiceGrpc.TreeServiceImplBase {

  private final Supplier<TreeApi> bridge;

  public TreeService(Supplier<TreeApi> bridge) {
    this.bridge = bridge;
  }

  @Override
  public void getAllReferences(GetAllReferencesRequest request,
    StreamObserver<GetAllReferencesResponse> observer) {
    handle(
      () ->
        GetAllReferencesResponse.newBuilder()
          .addAllReference(
            bridge.get().getAllReferences(fromProto(request)).getReferences().stream()
              .map(ProtoUtil::refToProto)
              .collect(Collectors.toList()))
          .build(),
      observer);
  }

  @Override
  public void getReferenceByName(
    GetReferenceByNameRequest request, StreamObserver<Reference> observer) {
    handle(() -> refToProto(bridge.get().getReferenceByName(fromProto(request))), observer);
  }

  @Override
  public void createReference(CreateReferenceRequest request, StreamObserver<Reference> observer) {
    handle(
      () ->
        refToProto(
          bridge.get().createReference(
            "".equals(request.getSourceRefName()) ? null : request.getSourceRefName(),
            refFromProto(request.getReference()))),
      observer);
  }

  @Override
  public void getDefaultBranch(Empty request, StreamObserver<Reference> observer) {
    handle(() -> refToProto(bridge.get().getDefaultBranch()), observer);
  }

  @Override
  public void assignReference(AssignReferenceRequest request, StreamObserver<Empty> observer) {
    handle(
      () -> {
        org.projectnessie.model.Reference ref;
        if (request.hasBranch()) {
          ref = Branch.of(request.getBranch().getName(), request.getBranch().hasHash() ? request.getBranch().getHash() : null);
        } else if (request.hasTag()) {
          ref = Tag.of(request.getTag().getName(), request.getTag().hasHash() ? request.getTag().getHash() : null);
        } else if (request.hasDetached()) {
          ref = Detached.of(request.getDetached().getHash());
        } else {
          throw new IllegalArgumentException("assignTo must be either a Branch or Tag or Detached");
        }
        bridge.get().assignReference(fromProto(request.getReferenceType()), request.getNamedRef(), request.getOldHash(), ref);
        return Empty.getDefaultInstance();
      },
      observer);
  }

  @Override
  public void deleteReference(DeleteReferenceRequest request, StreamObserver<Empty> observer) {
    handle(
      () -> {
        bridge.get().deleteReference(fromProto(request.getReferenceType()), request.getNamedRef(), request.getHash());
        return Empty.getDefaultInstance();
      },
      observer);
  }

  @Override
  public void getCommitLog(CommitLogRequest request, StreamObserver<CommitLogResponse> observer) {
    handle(
      () -> toProto(bridge.get().getCommitLog(request.getNamedRef(), fromProto(request))),
      observer);
  }

  @Override
  public void getEntries(EntriesRequest request, StreamObserver<EntriesResponse> observer) {
    handle(
      () -> toProto(bridge.get().getEntries(request.getNamedRef(), fromProto(request))),
      observer);
  }

  @Override
  public void transplantCommitsIntoBranch(
    TransplantRequest request, StreamObserver<Empty> observer) {
    handle(
      () -> {
        bridge.get().transplantCommitsIntoBranch(
          request.getBranchName(),
          request.getHash(),
          request.getMessage(),
          ImmutableTransplant.builder()
            .hashesToTransplant(request.getHashesToTransplantList())
            .fromRefName(request.getFromRefName())
            .build());
        return Empty.getDefaultInstance();
      },
      observer);
  }

  @Override
  public void mergeRefIntoBranch(MergeRequest request, StreamObserver<Empty> observer) {
    handle(
      () -> {
        bridge.get().mergeRefIntoBranch(
          request.getToBranch(),
          request.getExpectedHash(),
          ImmutableMerge.builder()
            .fromHash(request.getFromHash())
            .fromRefName(request.getFromRefName())
            .build());
        return Empty.getDefaultInstance();
      },
      observer);
  }

  @Override
  public void commitMultipleOperations(
    CommitRequest request, StreamObserver<com.dremio.services.nessie.grpc.api.Branch> observer) {
    handle(
      () ->
        toProto(
          bridge.get().commitMultipleOperations(
            request.getBranch(),
            request.getHash(),
            fromProto(request.getCommitOperations()))),
      observer);
  }
}
