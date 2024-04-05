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

import com.dremio.services.nessie.grpc.api.TreeServiceGrpc.TreeServiceBlockingStub;
import org.projectnessie.client.api.DeleteBranchBuilder;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Reference;

final class GrpcDeleteBranch extends BaseGrpcDeleteReference<Branch, DeleteBranchBuilder>
    implements DeleteBranchBuilder {

  public GrpcDeleteBranch(TreeServiceBlockingStub stub) {
    super(stub);
    refType(Reference.ReferenceType.BRANCH);
  }

  @Override
  public DeleteBranchBuilder branchName(String branchName) {
    return refName(branchName);
  }
}
