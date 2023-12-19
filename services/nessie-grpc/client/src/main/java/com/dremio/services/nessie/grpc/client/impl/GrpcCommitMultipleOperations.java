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

import static com.dremio.services.nessie.grpc.GrpcExceptionMapper.handle;
import static com.dremio.services.nessie.grpc.ProtoUtil.fromProto;
import static com.dremio.services.nessie.grpc.ProtoUtil.toProto;

import java.util.List;

import org.projectnessie.client.api.CommitMultipleOperationsBuilder;
import org.projectnessie.client.builder.BaseCommitMultipleOperationsBuilder;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.CommitResponse;
import org.projectnessie.model.ImmutableOperations;
import org.projectnessie.model.Operation;

import com.dremio.services.nessie.grpc.api.CommitRequest;
import com.dremio.services.nessie.grpc.api.TreeServiceGrpc.TreeServiceBlockingStub;

final class GrpcCommitMultipleOperations extends BaseCommitMultipleOperationsBuilder {

  private final TreeServiceBlockingStub stub;
  private final ImmutableOperations.Builder operations = ImmutableOperations.builder();
  private String branchName;
  private String hash;

  public GrpcCommitMultipleOperations(TreeServiceBlockingStub stub) {
    this.stub = stub;
  }

  @Override
  public CommitMultipleOperationsBuilder commitMeta(CommitMeta commitMeta) {
    operations.commitMeta(commitMeta);
    return this;
  }

  @Override
  public CommitMultipleOperationsBuilder operations(List<Operation> operations) {
    this.operations.addAllOperations(operations);
    return this;
  }

  @Override
  public CommitMultipleOperationsBuilder operation(Operation operation) {
    operations.addOperations(operation);
    return this;
  }

  @Override
  public CommitMultipleOperationsBuilder branchName(String branchName) {
    this.branchName = branchName;
    return this;
  }

  @Override
  public CommitMultipleOperationsBuilder hash(String hash) {
    this.hash = hash;
    return this;
  }

  @Override
  public Branch commit() throws NessieNotFoundException, NessieConflictException {
    return commitWithResponse().getTargetBranch();
  }

  @Override
  public CommitResponse commitWithResponse() throws NessieNotFoundException, NessieConflictException {
    return handle(
      () -> {
        CommitRequest.Builder request = CommitRequest.newBuilder()
          .setBranch(branchName)
          .setCommitOperations(toProto(operations.build()));

        if (hash != null) {
          request.setHash(hash);
        }

        return fromProto(stub.commitMultipleOperations(request.build()));
      });
  }
}
