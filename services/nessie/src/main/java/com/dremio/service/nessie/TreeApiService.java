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

import java.util.function.Supplier;

import org.projectnessie.api.TreeApi;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ImmutableOperations;

import com.dremio.service.nessieapi.Branch;
import com.dremio.service.nessieapi.CommitMultipleOperationsRequest;
import com.dremio.service.nessieapi.CreateReferenceRequest;
import com.dremio.service.nessieapi.GetReferenceByNameRequest;
import com.dremio.service.nessieapi.Reference;
import com.dremio.service.nessieapi.TreeApiGrpc;
import com.google.protobuf.Empty;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

/**
 * Translates gRPC requests for the TreeApiService and forwards them to the TreeResource.
 */
class TreeApiService extends TreeApiGrpc.TreeApiImplBase {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TreeApiService.class);

  private final Supplier<TreeApi> treeResource;

  TreeApiService(Supplier<TreeApi> treeResource) {
    this.treeResource = treeResource;
  }

  @Override
  public void createReference(CreateReferenceRequest request, StreamObserver<Empty> responseObserver) {
    logger.debug("[gRPC] CreateReference (ref: {})", request.getReference());
    try {
      treeResource.get().createReference(request.getSourceRefName(), GrpcNessieConverter.fromGrpc(request.getReference()));
      responseObserver.onNext(Empty.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (IllegalArgumentException e) {
      logger.error("CreateReference failed with a IllegalArgumentException.", e);
      responseObserver.onError(new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).withCause(e)));
    }  catch (NessieConflictException e) {
      logger.error("CreateReference failed with a NessieConflictException.", e);
      responseObserver.onError(new StatusRuntimeException(Status.ALREADY_EXISTS.withDescription(e.getMessage()).withCause(e)));
    } catch (Exception e) {
      logger.error("CreateReference failed with an unexpected exception.", e);
      responseObserver.onError(new StatusRuntimeException(Status.UNKNOWN.withDescription(e.getMessage()).withCause(e)));
    }
  }

  @Override
  public void getDefaultBranch(Empty request, StreamObserver<Branch> responseObserver) {
    logger.debug("[gRPC] GetDefaultBranch)");
    try {
      final org.projectnessie.model.Branch branch = treeResource.get().getDefaultBranch();
      responseObserver.onNext(GrpcNessieConverter.toGrpc(branch));
      responseObserver.onCompleted();
    } catch (NessieNotFoundException e) {
      logger.error("GetDefaultBranch failed with a NessieNotFoundException.", e);
      responseObserver.onError(new StatusRuntimeException(Status.NOT_FOUND.withDescription(e.getMessage()).withCause(e)));
    } catch (IllegalArgumentException e) {
      logger.error("GetDefaultBranch failed with a IllegalArgumentException.", e);
      responseObserver.onError(new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).withCause(e)));
    } catch (Exception e) {
      logger.error("GetDefaultBranch failed with an unexpected exception.", e);
      responseObserver.onError(new StatusRuntimeException(Status.UNKNOWN.withDescription(e.getMessage()).withCause(e)));
    }
  }

  @Override
  public void getReferenceByName(GetReferenceByNameRequest request, StreamObserver<Reference> responseObserver) {
    logger.debug("[gRPC] GetReferenceByName (refName: {})", request.getRefName());
    try {
      final org.projectnessie.model.Reference reference = treeResource.get().getReferenceByName(request.getRefName());
      responseObserver.onNext(GrpcNessieConverter.toGrpc(reference));
      responseObserver.onCompleted();
    } catch (NessieNotFoundException e) {
      logger.error("GetReferenceByName failed with a NessieNotFoundException.", e);
      responseObserver.onError(new StatusRuntimeException(Status.NOT_FOUND.withDescription(e.getMessage()).withCause(e)));
    } catch (IllegalArgumentException e) {
      logger.error("GetReferenceByName failed with a IllegalArgumentException.", e);
      responseObserver.onError(new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).withCause(e)));
    } catch (Exception e) {
      logger.error("GetReferenceByName failed with an unexpected exception.", e);
      responseObserver.onError(new StatusRuntimeException(Status.UNKNOWN.withDescription(e.getMessage()).withCause(e)));
    }
  }

  @Override
  public void commitMultipleOperations(CommitMultipleOperationsRequest request, StreamObserver<Empty> responseObserver) {
    logger.debug("[gRPC] CommitMultipleOperations (branch: {}, expectedHash: {}, message: {})",
      request.getBranchName(), request.getExpectedHash(), request.getMessage());
    logger.debug("Operations:");
    request.getOperationsList().forEach(o -> logger.debug("type: {}, contentKey: {}, contents: {}",
      o.getType(), o.getContentsKey(), o.getContents()));

    final ImmutableOperations.Builder operationsBuilder = ImmutableOperations.builder();
    request.getOperationsList().forEach(o -> operationsBuilder.addOperations(GrpcNessieConverter.fromGrpc(o)));
    operationsBuilder.commitMeta(CommitMeta.fromMessage(request.getMessage()));

    try {
      treeResource.get().commitMultipleOperations(
          request.getBranchName(),
          request.getExpectedHash(),
          operationsBuilder.build());

      responseObserver.onNext(Empty.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (NessieNotFoundException e) {
      logger.error("CommitMultipleOperations failed with a NessieNotFoundException.", e);
      responseObserver.onError(new StatusRuntimeException(Status.NOT_FOUND.withDescription(e.getMessage()).withCause(e)));
    } catch (NessieConflictException e) {
      logger.error("CommitMultipleOperations failed with a NessieConflictException.", e);
      responseObserver.onError(new StatusRuntimeException(Status.ABORTED.withDescription(e.getMessage()).withCause(e)));
    } catch (IllegalArgumentException e) {
      logger.error("CommitMultipleOperations failed with a IllegalArgumentException.", e);
      responseObserver.onError(new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).withCause(e)));
    } catch (Exception e) {
      logger.error("CommitMultipleOperations failed with an unexpected exception.", e);
      responseObserver.onError(new StatusRuntimeException(Status.UNKNOWN.withDescription(e.getMessage()).withCause(e)));
    }
  }
}
