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

import static com.dremio.services.nessie.grpc.ProtoUtil.refFromProto;
import static com.dremio.services.nessie.grpc.ProtoUtil.refToProto;
import static com.dremio.services.nessie.grpc.client.GrpcExceptionMapper.handle;

import org.projectnessie.client.builder.BaseCreateReferenceBuilder;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Reference;

import com.dremio.services.nessie.grpc.api.CreateReferenceRequest;
import com.dremio.services.nessie.grpc.api.TreeServiceGrpc.TreeServiceBlockingStub;

final class GrpcCreateReference extends BaseCreateReferenceBuilder {

  private final TreeServiceBlockingStub stub;

  public GrpcCreateReference(TreeServiceBlockingStub stub) {
    this.stub = stub;
  }

  @Override
  public Reference create() throws NessieNotFoundException, NessieConflictException {
    return handle(
      () -> {
        CreateReferenceRequest.Builder builder =
          CreateReferenceRequest.newBuilder().setReference(refToProto(reference));
        if (null != sourceRefName) {
          builder.setSourceRefName(sourceRefName);
        }
        return refFromProto(stub.createReference(builder.build()));
      });
  }
}
