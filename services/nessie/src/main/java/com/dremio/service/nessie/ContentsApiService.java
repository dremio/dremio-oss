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

import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Contents;
import org.projectnessie.services.rest.ContentsResource;

import com.dremio.service.nessieapi.ContentsApiGrpc;
import com.dremio.service.nessieapi.GetContentsRequest;
import com.dremio.service.nessieapi.SetContentsRequest;
import com.google.protobuf.Empty;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

/**
 * Translates gRPC requests for the ContentsApiService and forwards them to the ContentsResource.
 */
class ContentsApiService extends ContentsApiGrpc.ContentsApiImplBase {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ContentsApiService.class);

  private final Supplier<ContentsResource> contentsResource;

  ContentsApiService(Supplier<ContentsResource> contentsResource) {
    this.contentsResource = contentsResource;
  }

  @Override
  public void getContents(GetContentsRequest request, StreamObserver<com.dremio.service.nessieapi.Contents> responseObserver) {
    logger.debug("[gRPC] GetContents (contentsKey: {}, ref: {})", request.getContentsKey(), request.getRef());
    try {
      final Contents contents = contentsResource.get().getContents(GrpcNessieConverter.fromGrpc(request.getContentsKey()), request.getRef());
      responseObserver.onNext(GrpcNessieConverter.toGrpc(contents));
      responseObserver.onCompleted();
    } catch (NessieNotFoundException e) {
      logger.error("GetContents failed with a NessieNotFoundException.", e);
      responseObserver.onError(new StatusRuntimeException(Status.NOT_FOUND.withDescription(e.getMessage()).withCause(e)));
    } catch (IllegalArgumentException e) {
      logger.error("GetContents failed with a IllegalArgumentException.", e);
      responseObserver.onError(new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).withCause(e)));
    } catch (Exception e) {
      logger.error("GetContents failed with an unexpected exception.", e);
      responseObserver.onError(new StatusRuntimeException(Status.UNKNOWN.withDescription(e.getMessage()).withCause(e)));
    }
  }

  @Override
  public void setContents(SetContentsRequest request, StreamObserver<Empty> responseObserver) {
    logger.debug("[gRPC] SetContents (branch: {}, contentsKey: {}, hash: {})", request.getBranch(), request.getContentsKey(), request.getHash());
    try {
      contentsResource.get().setContents(
         GrpcNessieConverter.fromGrpc(request.getContentsKey()),
         request.getBranch(),
         request.getHash(),
         request.getMessage(),
         GrpcNessieConverter.fromGrpc(request.getContents())
      );

      responseObserver.onNext(Empty.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (NessieNotFoundException e) {
      logger.error("SetContents failed with a NessieNotFoundException.", e);
      responseObserver.onError(new StatusRuntimeException(Status.NOT_FOUND.withDescription(e.getMessage()).withCause(e)));
    } catch (NessieConflictException e) {
      logger.error("SetContents failed with a NessieConflictException.", e);
      responseObserver.onError(new StatusRuntimeException(Status.ABORTED.withDescription(e.getMessage()).withCause(e)));
    } catch (IllegalArgumentException e) {
      logger.error("SetContents failed with a IllegalArgumentException.", e);
      responseObserver.onError(new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).withCause(e)));
    } catch (Exception e) {
      logger.error("SetContents failed with an unexpected exception.", e);
      responseObserver.onError(new StatusRuntimeException(Status.UNKNOWN.withDescription(e.getMessage()).withCause(e)));
    }
  }
}
