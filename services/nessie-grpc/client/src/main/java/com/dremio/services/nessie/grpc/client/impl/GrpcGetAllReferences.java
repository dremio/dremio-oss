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

import static com.dremio.services.nessie.grpc.GrpcExceptionMapper.handleNessieRuntimeEx;

import com.dremio.services.nessie.grpc.ProtoUtil;
import com.dremio.services.nessie.grpc.api.GetAllReferencesResponse;
import com.dremio.services.nessie.grpc.api.TreeServiceGrpc.TreeServiceBlockingStub;
import org.projectnessie.api.v1.params.ReferencesParams;
import org.projectnessie.client.builder.BaseGetAllReferencesBuilder;
import org.projectnessie.model.ImmutableReferencesResponse;
import org.projectnessie.model.ReferencesResponse;

final class GrpcGetAllReferences extends BaseGetAllReferencesBuilder<ReferencesParams> {

  private final TreeServiceBlockingStub stub;

  public GrpcGetAllReferences(TreeServiceBlockingStub stub) {
    super(ReferencesParams::forNextPage);
    this.stub = stub;
  }

  @Override
  protected ReferencesParams params() {
    return ReferencesParams.builder()
        .maxRecords(maxRecords)
        .fetchOption(fetchOption)
        .filter(filter)
        .build();
  }

  @Override
  protected ReferencesResponse get(ReferencesParams p) {
    return handleNessieRuntimeEx(
        () -> {
          GetAllReferencesResponse response = stub.getAllReferences(ProtoUtil.toProto(p));
          ImmutableReferencesResponse.Builder builder = ReferencesResponse.builder();
          response.getReferenceList().stream()
              .map(ProtoUtil::refFromProto)
              .forEach(builder::addReferences);
          builder.isHasMore(response.getHasMore());
          if (response.hasPageToken()) {
            builder.token(response.getPageToken());
          }
          return builder.build();
        });
  }
}
