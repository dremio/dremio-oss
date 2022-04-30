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

import java.util.List;
import java.util.stream.Collectors;

import org.projectnessie.api.params.FetchOption;
import org.projectnessie.api.params.ReferencesParams;
import org.projectnessie.client.api.GetAllReferencesBuilder;
import org.projectnessie.model.Reference;
import org.projectnessie.model.ReferencesResponse;

import com.dremio.services.nessie.grpc.ProtoUtil;
import com.dremio.services.nessie.grpc.api.TreeServiceGrpc.TreeServiceBlockingStub;

final class GrpcGetAllReferences implements GetAllReferencesBuilder {

  private final TreeServiceBlockingStub stub;
  private final ReferencesParams.Builder params = ReferencesParams.builder();

  public GrpcGetAllReferences(TreeServiceBlockingStub stub) {
    this.stub = stub;
  }

  @Override
  public GetAllReferencesBuilder maxRecords(int maxRecords) {
    params.maxRecords(maxRecords);
    return this;
  }

  @Override
  public GetAllReferencesBuilder pageToken(String pageToken) {
    params.pageToken(pageToken);
    return this;
  }

  public GetAllReferencesBuilder fetch(FetchOption fetchOption) {
    params.fetch(fetchOption);
    return this;
  }

  @Override
  public GetAllReferencesBuilder filter(String filter) {
    params.filter(filter);
    return this;
  }

  @Override
  public ReferencesResponse get() {
    List<Reference> result = stub.getAllReferences(ProtoUtil.toProto(params.build()))
      .getReferenceList()
      .stream()
      .map(ProtoUtil::refFromProto)
      .collect(Collectors.toList());
    return ReferencesResponse.builder().addAllReferences(result).build();
  }
}
