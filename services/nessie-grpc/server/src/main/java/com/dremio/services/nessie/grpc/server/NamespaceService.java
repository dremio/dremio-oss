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

import static com.dremio.services.nessie.grpc.GrpcExceptionMapper.handle;
import static com.dremio.services.nessie.grpc.ProtoUtil.fromProto;
import static com.dremio.services.nessie.grpc.ProtoUtil.toProto;

import com.dremio.services.nessie.grpc.api.Empty;
import com.dremio.services.nessie.grpc.api.MultipleNamespacesRequest;
import com.dremio.services.nessie.grpc.api.MultipleNamespacesResponse;
import com.dremio.services.nessie.grpc.api.Namespace;
import com.dremio.services.nessie.grpc.api.NamespaceRequest;
import com.dremio.services.nessie.grpc.api.NamespaceServiceGrpc.NamespaceServiceImplBase;
import com.dremio.services.nessie.grpc.api.NamespaceUpdateRequest;
import com.google.common.collect.ImmutableSet;
import io.grpc.stub.StreamObserver;
import java.util.function.Supplier;

/** The gRPC service implementation for the RefLog-API. */
public class NamespaceService extends NamespaceServiceImplBase {

  private final Supplier<? extends org.projectnessie.services.spi.NamespaceService> bridge;

  public NamespaceService(
      Supplier<? extends org.projectnessie.services.spi.NamespaceService> bridge) {
    this.bridge = bridge;
  }

  @Override
  public void createNamespace(NamespaceRequest request, StreamObserver<Namespace> observer) {
    handle(
        () ->
            toProto(
                bridge
                    .get()
                    .createNamespace(request.getNamedRef(), fromProto(request.getNamespace()))),
        observer);
  }

  @Override
  public void deleteNamespace(NamespaceRequest request, StreamObserver<Empty> observer) {
    handle(
        () -> {
          bridge.get().deleteNamespace(request.getNamedRef(), fromProto(request.getNamespace()));
          return Empty.getDefaultInstance();
        },
        observer);
  }

  @Override
  public void getNamespace(NamespaceRequest request, StreamObserver<Namespace> observer) {
    handle(
        () ->
            toProto(
                bridge
                    .get()
                    .getNamespace(
                        request.getNamedRef(),
                        fromProto(request::hasHashOnRef, request::getHashOnRef),
                        fromProto(request.getNamespace()))),
        observer);
  }

  @Override
  public void getNamespaces(
      MultipleNamespacesRequest request, StreamObserver<MultipleNamespacesResponse> observer) {
    handle(
        () ->
            toProto(
                bridge
                    .get()
                    .getNamespaces(
                        request.getNamedRef(),
                        fromProto(request::hasHashOnRef, request::getHashOnRef),
                        fromProto(request.getNamespace()))),
        observer);
  }

  @Override
  public void updateProperties(NamespaceUpdateRequest request, StreamObserver<Empty> observer) {
    handle(
        () -> {
          bridge
              .get()
              .updateProperties(
                  request.getNamespaceRequest().getNamedRef(),
                  fromProto(request.getNamespaceRequest().getNamespace()),
                  request.getPropertyUpdatesMap(),
                  ImmutableSet.<String>builder().addAll(request.getPropertyRemovalsList()).build());
          return Empty.getDefaultInstance();
        },
        observer);
  }
}
