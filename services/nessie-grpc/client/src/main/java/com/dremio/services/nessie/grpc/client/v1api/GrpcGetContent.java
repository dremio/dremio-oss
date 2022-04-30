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
package com.dremio.services.nessie.grpc.client.v1api;

import static com.dremio.services.nessie.grpc.ProtoUtil.fromProto;
import static com.dremio.services.nessie.grpc.ProtoUtil.toProto;
import static com.dremio.services.nessie.grpc.client.GrpcExceptionMapper.handleNessieNotFoundEx;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.projectnessie.client.api.GetContentBuilder;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.GetMultipleContentsResponse;
import org.projectnessie.model.GetMultipleContentsResponse.ContentWithKey;
import org.projectnessie.model.ImmutableGetMultipleContentsRequest;

import com.dremio.services.nessie.grpc.api.ContentServiceGrpc.ContentServiceBlockingStub;

final class GrpcGetContent implements GetContentBuilder {

  private final ContentServiceBlockingStub stub;
  private final ImmutableGetMultipleContentsRequest.Builder request =
    ImmutableGetMultipleContentsRequest.builder();
  private String refName;
  private String hashOnRef;

  public GrpcGetContent(ContentServiceBlockingStub stub) {
    this.stub = stub;
  }

  @Override
  public GetContentBuilder key(ContentKey key) {
    request.addRequestedKeys(key);
    return this;
  }

  @Override
  public GetContentBuilder keys(List<ContentKey> keys) {
    request.addAllRequestedKeys(keys);
    return this;
  }

  @Override
  public GetContentBuilder refName(String refName) {
    this.refName = refName;
    return this;
  }

  @Override
  public GetContentBuilder hashOnRef(@Nullable String hashOnRef) {
    this.hashOnRef = hashOnRef;
    return this;
  }

  @Override
  public Map<ContentKey, Content> get() throws NessieNotFoundException {
    return handleNessieNotFoundEx(
      () -> {
        GetMultipleContentsResponse response =
          fromProto(stub.getMultipleContents(toProto(refName, hashOnRef, request.build())));
        return response.getContents().stream()
          .collect(Collectors.toMap(ContentWithKey::getKey, ContentWithKey::getContent));
      });
  }
}
