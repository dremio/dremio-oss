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

import org.projectnessie.api.params.DiffParams;
import org.projectnessie.client.api.GetDiffBuilder;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.DiffResponse;

import com.dremio.services.nessie.grpc.api.DiffServiceGrpc.DiffServiceBlockingStub;

/**
 * Returns the diff for two given references.
 */
public class GrpcGetDiff implements GetDiffBuilder {

  private final DiffServiceBlockingStub stub;
  private final DiffParams.Builder builder = DiffParams.builder();

  public GrpcGetDiff(DiffServiceBlockingStub stub) {
    this.stub = stub;
  }

  @Override
  public GetDiffBuilder fromRefName(String fromRefName) {
    builder.fromRef(fromRefName);
    return this;
  }

  @Override
  public GetDiffBuilder fromHashOnRef(String fromHashOnRef) {
    builder.fromHashOnRef(fromHashOnRef);
    return this;
  }

  @Override
  public GetDiffBuilder toRefName(String toRefName) {
    builder.toRef(toRefName);
    return this;
  }

  @Override
  public GetDiffBuilder toHashOnRef(String toHashOnRef) {
    builder.toHashOnRef(toHashOnRef);
    return this;
  }

  @Override
  public DiffResponse get() throws NessieNotFoundException {
    return handleNessieNotFoundEx(() -> fromProto(stub.getDiff(toProto(builder.build()))));
  }
}
