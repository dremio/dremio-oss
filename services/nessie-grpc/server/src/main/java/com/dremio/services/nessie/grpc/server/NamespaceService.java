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
import static com.dremio.services.nessie.grpc.ProtoUtil.toProto;
import static com.dremio.services.nessie.grpc.client.GrpcExceptionMapper.handle;

import java.util.function.Supplier;

import org.projectnessie.api.NamespaceApi;
import org.projectnessie.api.params.ImmutableNamespaceUpdate;

import com.dremio.services.nessie.grpc.api.Empty;
import com.dremio.services.nessie.grpc.api.MultipleNamespacesRequest;
import com.dremio.services.nessie.grpc.api.MultipleNamespacesResponse;
import com.dremio.services.nessie.grpc.api.Namespace;
import com.dremio.services.nessie.grpc.api.NamespaceRequest;
import com.dremio.services.nessie.grpc.api.NamespaceServiceGrpc.NamespaceServiceImplBase;
import com.dremio.services.nessie.grpc.api.NamespaceUpdateRequest;

import io.grpc.stub.StreamObserver;

/**
 * The gRPC service implementation for the RefLog-API.
 */
public class NamespaceService extends NamespaceServiceImplBase {

  private final Supplier<NamespaceApi> bridge;

  public NamespaceService(Supplier<NamespaceApi> bridge) {
    this.bridge = bridge;
  }

  @Override
  public void createNamespace(NamespaceRequest request, StreamObserver<Namespace> observer) {
    handle(() -> toProto(bridge.get().createNamespace(fromProto(request), fromProto(request.getNamespace()))), observer);
  }

  @Override
  public void deleteNamespace(NamespaceRequest request, StreamObserver<Empty> observer) {
    handle(() -> {
      bridge.get().deleteNamespace(fromProto(request));
      return Empty.getDefaultInstance();
    }, observer);
  }

  @Override
  public void getNamespace(NamespaceRequest request, StreamObserver<Namespace> observer) {
    handle(() -> toProto(bridge.get().getNamespace(fromProto(request))), observer);
  }

  @Override
  public void getNamespaces(MultipleNamespacesRequest request,
    StreamObserver<MultipleNamespacesResponse> observer) {
    handle(() -> toProto(bridge.get().getNamespaces(fromProto(request))), observer);
  }

  @Override
  public void updateProperties(NamespaceUpdateRequest request, StreamObserver<Empty> observer) {
    handle(() -> {
      bridge.get().updateProperties(fromProto(request.getNamespaceRequest()),
        ImmutableNamespaceUpdate.builder()
          .propertyUpdates(request.getPropertyUpdatesMap())
          .propertyRemovals(request.getPropertyRemovalsList())
          .build());
      return Empty.getDefaultInstance();
    }, observer);
  }
}
