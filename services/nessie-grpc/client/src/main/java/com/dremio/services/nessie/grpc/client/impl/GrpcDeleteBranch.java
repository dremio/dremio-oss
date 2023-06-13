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

import static com.dremio.services.nessie.grpc.ProtoUtil.refFromProtoResponse;
import static com.dremio.services.nessie.grpc.client.GrpcExceptionMapper.handle;

import org.projectnessie.client.api.DeleteBranchBuilder;
import org.projectnessie.client.builder.BaseOnBranchBuilder;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;

import com.dremio.services.nessie.grpc.api.DeleteReferenceRequest;
import com.dremio.services.nessie.grpc.api.ReferenceResponse;
import com.dremio.services.nessie.grpc.api.ReferenceType;
import com.dremio.services.nessie.grpc.api.TreeServiceGrpc.TreeServiceBlockingStub;

final class GrpcDeleteBranch extends BaseOnBranchBuilder<DeleteBranchBuilder>
    implements DeleteBranchBuilder {

  private final TreeServiceBlockingStub stub;

  public GrpcDeleteBranch(TreeServiceBlockingStub stub) {
    this.stub = stub;
  }

  @Override
  public Branch getAndDelete() throws NessieConflictException, NessieNotFoundException {
    return handle(
      () ->
      {
        ReferenceResponse response = stub.deleteReference(
          DeleteReferenceRequest.newBuilder()
            .setReferenceType(ReferenceType.BRANCH)
            .setNamedRef(branchName)
            .setHash(hash)
            .build());
        return (Branch) refFromProtoResponse(response);
      });
  }

  @Override
  public void delete() throws NessieConflictException, NessieNotFoundException {
    getAndDelete();
  }
}
