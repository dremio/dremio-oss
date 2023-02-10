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
import java.util.stream.Stream;

import org.projectnessie.api.params.FetchOption;
import org.projectnessie.api.params.ReferencesParams;
import org.projectnessie.api.params.ReferencesParamsBuilder;
import org.projectnessie.client.StreamingUtil;
import org.projectnessie.client.api.GetAllReferencesBuilder;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Reference;
import org.projectnessie.model.ReferencesResponse;

import com.dremio.services.nessie.grpc.ProtoUtil;
import com.dremio.services.nessie.grpc.api.TreeServiceGrpc.TreeServiceBlockingStub;

final class GrpcGetAllReferences implements GetAllReferencesBuilder {

  private final TreeServiceBlockingStub stub;
  private final ReferencesParamsBuilder params = ReferencesParams.builder();

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
    params.fetchOption(fetchOption);
    return this;
  }

  @Override
  public GetAllReferencesBuilder filter(String filter) {
    params.filter(filter);
    return this;
  }

  @Override
  public ReferencesResponse get() {
    return get(params.build());
  }

  private ReferencesResponse get(ReferencesParams p) {
    List<Reference> result = stub.getAllReferences(ProtoUtil.toProto(p))
      .getReferenceList()
      .stream()
      .map(ProtoUtil::refFromProto)
      .collect(Collectors.toList());
    return ReferencesResponse.builder().addAllReferences(result).build();
  }

  @Override
  public Stream<Reference> stream() throws NessieNotFoundException {
    ReferencesParams p = params.build();
    return StreamingUtil.generateStream(
      ReferencesResponse::getReferences, pageToken -> get(p.forNextPage(pageToken)));
  }
}
