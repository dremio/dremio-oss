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

import static com.dremio.services.nessie.grpc.ProtoUtil.fromProto;
import static com.dremio.services.nessie.grpc.ProtoUtil.toProto;
import static com.dremio.services.nessie.grpc.client.GrpcExceptionMapper.handle;

import org.projectnessie.api.v1.params.ImmutableMerge;
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
    ImmutableMerge.Builder merge =
      ImmutableMerge.builder()
        .fromHash(fromHash)
        .fromRefName(fromRefName)
        .isDryRun(dryRun)
        .isReturnConflictAsResult(returnConflictAsResult)
        .isFetchAdditionalInfo(fetchAdditionalInfo)
        .keepIndividualCommits(keepIndividualCommits);

    if (defaultMergeMode != null) {
      merge.defaultKeyMergeMode(defaultMergeMode);
    }

    if (mergeModes != null) {
      merge.keyMergeModes(mergeModes.values());
    }

    return handle(
      () -> fromProto(
        stub.mergeRefIntoBranch(
          toProto(branchName, hash, merge.build(), message, commitMeta))));
  }
}
