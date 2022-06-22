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

import java.util.List;

import org.projectnessie.client.api.TransplantCommitsBuilder;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.ImmutableTransplant;

import com.dremio.services.nessie.grpc.api.TreeServiceGrpc.TreeServiceBlockingStub;

final class GrpcTransplantCommits implements TransplantCommitsBuilder {

  private final TreeServiceBlockingStub stub;
  private String branchName;
  private String hash;
  private String message;
  private final ImmutableTransplant.Builder transplant = ImmutableTransplant.builder();

  public GrpcTransplantCommits(TreeServiceBlockingStub stub) {
    this.stub = stub;
  }

  @Override
  public TransplantCommitsBuilder branchName(String branchName) {
    this.branchName = branchName;
    return this;
  }

  @Override
  public TransplantCommitsBuilder hash(String hash) {
    this.hash = hash;
    return this;
  }

  @Override
  public TransplantCommitsBuilder message(String message) {
    this.message = message;
    return this;
  }

  @Override
  public TransplantCommitsBuilder fromRefName(String fromRefName) {
    transplant.fromRefName(fromRefName);
    return this;
  }

  @Override
  public TransplantCommitsBuilder keepIndividualCommits(boolean keepIndividualCommits) {
    transplant.keepIndividualCommits(keepIndividualCommits);
    return this;
  }

  @Override
  public TransplantCommitsBuilder hashesToTransplant(List<String> hashesToTransplant) {
    transplant.hashesToTransplant(hashesToTransplant);
    return this;
  }

  @Override
  public void transplant() throws NessieNotFoundException, NessieConflictException {
    handle(
      () -> stub.transplantCommitsIntoBranch(toProto(branchName, hash, message, transplant.build())));
  }
}
