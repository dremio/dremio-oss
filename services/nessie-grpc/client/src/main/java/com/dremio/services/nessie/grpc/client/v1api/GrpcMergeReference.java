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

import static com.dremio.services.nessie.grpc.ProtoUtil.toProto;
import static com.dremio.services.nessie.grpc.client.GrpcExceptionMapper.handle;

import org.projectnessie.client.api.MergeReferenceBuilder;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.ImmutableMerge;

import com.dremio.services.nessie.grpc.api.TreeServiceGrpc.TreeServiceBlockingStub;

final class GrpcMergeReference implements MergeReferenceBuilder {

  private final TreeServiceBlockingStub stub;
  private String branchName;
  private String hash;
  private final ImmutableMerge.Builder merge = ImmutableMerge.builder();

  public GrpcMergeReference(TreeServiceBlockingStub stub) {
    this.stub = stub;
  }

  @Override
  public MergeReferenceBuilder fromHash(String fromHash) {
    merge.fromHash(fromHash);
    return this;
  }

  @Override
  public MergeReferenceBuilder fromRefName(String fromRefName) {
    merge.fromRefName(fromRefName);
    return this;
  }

  @Override
  public MergeReferenceBuilder keepIndividualCommits(boolean keepIndividualCommits) {
    merge.keepIndividualCommits(keepIndividualCommits);
    return this;
  }

  @Override
  public MergeReferenceBuilder branchName(String branchName) {
    this.branchName = branchName;
    return this;
  }

  @Override
  public MergeReferenceBuilder hash(String hash) {
    this.hash = hash;
    return this;
  }

  @Override
  public void merge() throws NessieNotFoundException, NessieConflictException {
    handle(
      () -> stub.mergeRefIntoBranch(toProto(branchName, hash, merge.build())));
  }
}
