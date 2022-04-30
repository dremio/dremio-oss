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

import static com.dremio.services.nessie.grpc.ProtoUtil.refFromProto;
import static com.dremio.services.nessie.grpc.ProtoUtil.toProto;
import static com.dremio.services.nessie.grpc.client.GrpcExceptionMapper.handleNessieNotFoundEx;

import org.projectnessie.api.params.FetchOption;
import org.projectnessie.api.params.GetReferenceParams;
import org.projectnessie.client.api.GetReferenceBuilder;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Reference;

import com.dremio.services.nessie.grpc.api.TreeServiceGrpc.TreeServiceBlockingStub;

final class GrpcGetReference implements GetReferenceBuilder {

  private final TreeServiceBlockingStub stub;
  private final GetReferenceParams.Builder builder = GetReferenceParams.builder();

  public GrpcGetReference(TreeServiceBlockingStub stub) {
    this.stub = stub;
  }

  @Override
  public GetReferenceBuilder refName(String refName) {
    builder.refName(refName);
    return this;
  }

  @Override
  public GetReferenceBuilder fetch(FetchOption fetchOption) {
    builder.fetch(fetchOption);
    return this;
  }

  @Override
  public Reference get() throws NessieNotFoundException {
    return handleNessieNotFoundEx(
      () ->
        refFromProto(
          stub.getReferenceByName(toProto(builder.build()))));
  }
}
