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
import static com.dremio.services.nessie.grpc.ProtoUtil.toProtoDiffRequest;

import com.dremio.services.nessie.grpc.api.DiffRequest;
import com.dremio.services.nessie.grpc.api.DiffServiceGrpc.DiffServiceBlockingStub;
import org.projectnessie.client.builder.BaseGetDiffBuilder;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.DiffResponse;

/** Returns the diff for two given references. */
public class GrpcGetDiff extends BaseGetDiffBuilder<DiffRequest> {

  private final DiffServiceBlockingStub stub;

  public GrpcGetDiff(DiffServiceBlockingStub stub) {
    super(
        (request, pageToken) -> {
          DiffRequest.Builder builder = request.toBuilder().clearPageToken();
          if (pageToken != null) {
            builder.setPageToken(pageToken);
          }
          return builder.build();
        });
    this.stub = stub;
  }

  @Override
  protected DiffRequest params() {
    return toProtoDiffRequest(
        fromRefName,
        fromHashOnRef,
        toRefName,
        toHashOnRef,
        maxRecords,
        minKey,
        maxKey,
        prefixKey,
        keys,
        filter);
  }

  @Override
  protected DiffResponse get(DiffRequest request) throws NessieNotFoundException {
    return handleNessieNotFoundEx(() -> fromProto(stub.getDiff(request)));
  }
}
