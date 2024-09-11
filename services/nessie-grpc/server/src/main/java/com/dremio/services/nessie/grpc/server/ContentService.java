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

import com.dremio.services.nessie.grpc.api.ContentServiceGrpc;
import com.dremio.services.nessie.grpc.api.MultipleContentsRequest;
import com.dremio.services.nessie.grpc.api.MultipleContentsResponse;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.projectnessie.model.ContentKey;

/** The gRPC service implementation for the Content-API. */
public class ContentService extends ContentServiceGrpc.ContentServiceImplBase {

  private final Supplier<? extends org.projectnessie.services.spi.ContentService> bridge;

  public ContentService(Supplier<? extends org.projectnessie.services.spi.ContentService> bridge) {
    this.bridge = bridge;
  }

  @Override
  public void getMultipleContents(
      MultipleContentsRequest request, StreamObserver<MultipleContentsResponse> observer) {
    handle(
        () -> {
          List<ContentKey> requestedKeys = new ArrayList<>();
          request.getRequestedKeysList().forEach(k -> requestedKeys.add(fromProto(k)));
          return toProto(
              bridge
                  .get()
                  .getMultipleContents(
                      getRefFromProtoRequest(request.getRef()),
                      getHashOnRefFromProtoRequest(request.getHashOnRef()),
                      requestedKeys,
                      false, // TODO: support withDocumentation
                      request.getForWrite()));
        },
        observer);
  }

  private String getHashOnRefFromProtoRequest(String hashOnRef) {
    return "".equals(hashOnRef) ? null : hashOnRef;
  }

  private String getRefFromProtoRequest(String ref) {
    return "".equals(ref) ? null : ref;
  }
}
