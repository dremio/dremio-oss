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

import org.projectnessie.client.builder.BaseMergeReferenceBuilder;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.MergeResponse;

import com.dremio.services.nessie.grpc.api.TreeServiceGrpc.TreeServiceBlockingStub;

final class GrpcMergeReference extends BaseMergeReferenceBuilder {

  private final TreeServiceBlockingStub stub;

  public GrpcMergeReference(TreeServiceBlockingStub stub) {
    this.stub = stub;
  }

  @Override
  public MergeResponse merge() throws NessieNotFoundException, NessieConflictException {
    return handle(
      () -> fromProto(
        stub.mergeRefIntoBranch(
          toProto(branchName, hash, message, commitMeta, fromRefName, fromHash, keepIndividualCommits, dryRun,
            returnConflictAsResult, fetchAdditionalInfo, defaultMergeMode,
            mergeModes == null ? null : mergeModes.values()))));
  }
}
