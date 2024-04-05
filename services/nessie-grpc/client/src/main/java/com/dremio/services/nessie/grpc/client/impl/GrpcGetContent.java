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
package com.dremio.services.nessie.grpc.client.impl;

import static com.dremio.services.nessie.grpc.GrpcExceptionMapper.handleNessieNotFoundEx;
import static com.dremio.services.nessie.grpc.ProtoUtil.fromProto;
import static com.dremio.services.nessie.grpc.ProtoUtil.toProto;

import com.dremio.services.nessie.grpc.api.ContentServiceGrpc.ContentServiceBlockingStub;
import java.util.Map;
import org.projectnessie.client.builder.BaseGetContentBuilder;
import org.projectnessie.error.NessieContentNotFoundException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.ContentResponse;
import org.projectnessie.model.GetMultipleContentsResponse;

final class GrpcGetContent extends BaseGetContentBuilder {

  private final ContentServiceBlockingStub stub;

  public GrpcGetContent(ContentServiceBlockingStub stub) {
    this.stub = stub;
  }

  @Override
  public Map<ContentKey, Content> get() throws NessieNotFoundException {
    return getWithResponse().toContentsMap();
  }

  @Override
  public ContentResponse getSingle(ContentKey key) throws NessieNotFoundException {
    if (!request.build().getRequestedKeys().isEmpty()) {
      throw new IllegalStateException(
          "Must not use getSingle() with key() or keys(), pass the single key to getSingle()");
    }

    GetMultipleContentsResponse multi =
        handleNessieNotFoundEx(
            () ->
                fromProto(
                    stub.getMultipleContents(
                        toProto(refName, hashOnRef, request.build().withRequestedKeys(key)))));

    Content content = multi.toContentsMap().get(key);
    if (content == null) {
      throw new NessieContentNotFoundException(key, refName);
    }

    return ContentResponse.builder()
        .content(content)
        .effectiveReference(multi.getEffectiveReference())
        .build();
  }

  @Override
  public GetMultipleContentsResponse getWithResponse() throws NessieNotFoundException {
    return handleNessieNotFoundEx(
        () -> fromProto(stub.getMultipleContents(toProto(refName, hashOnRef, request.build()))));
  }
}
