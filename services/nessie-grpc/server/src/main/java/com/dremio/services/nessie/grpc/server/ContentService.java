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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.projectnessie.api.ContentApi;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.GetMultipleContentsRequest;

import com.dremio.services.nessie.grpc.api.Content;
import com.dremio.services.nessie.grpc.api.ContentRequest;
import com.dremio.services.nessie.grpc.api.ContentServiceGrpc;
import com.dremio.services.nessie.grpc.api.MultipleContentsRequest;
import com.dremio.services.nessie.grpc.api.MultipleContentsResponse;

import io.grpc.stub.StreamObserver;

/**
 * The gRPC service implementation for the Content-API.
 */
public class ContentService extends ContentServiceGrpc.ContentServiceImplBase {

  private final Supplier<ContentApi> bridge;

  public ContentService(Supplier<ContentApi> bridge) {
    this.bridge = bridge;
  }

  @Override
  public void getContent(ContentRequest request, StreamObserver<Content> observer) {
    handle(
      () ->
        toProto(
          bridge.get().getContent(
            fromProto(request.getContentKey()),
            request.getRef(),
            getHashOnRefFromProtoRequest(request.getHashOnRef()))),
      observer);
  }

  @Override
  public void getMultipleContents(
    MultipleContentsRequest request, StreamObserver<MultipleContentsResponse> observer) {
    handle(
      () -> {
        List<ContentKey> requestedKeys = new ArrayList<>();
        request.getRequestedKeysList().forEach(k -> requestedKeys.add(fromProto(k)));
        return toProto(
          bridge.get().getMultipleContents(
            request.getRef(),
            getHashOnRefFromProtoRequest(request.getHashOnRef()),
            GetMultipleContentsRequest.of(requestedKeys)));
      },
      observer);
  }

  private String getHashOnRefFromProtoRequest(String hashOnRef) {
    return "".equals(hashOnRef) ? null : hashOnRef;
  }
}
