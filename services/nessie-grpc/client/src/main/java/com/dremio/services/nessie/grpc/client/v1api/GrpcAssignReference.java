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

import static com.dremio.services.nessie.grpc.client.GrpcExceptionMapper.handle;

import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Detached;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Tag;

import com.dremio.services.nessie.grpc.ProtoUtil;
import com.dremio.services.nessie.grpc.api.AssignReferenceRequest;
import com.dremio.services.nessie.grpc.api.ReferenceType;
import com.dremio.services.nessie.grpc.api.TreeServiceGrpc.TreeServiceBlockingStub;

abstract class GrpcAssignReference {

  private final TreeServiceBlockingStub stub;
  private Reference assignTo;
  private String refName;
  private String hash;

  GrpcAssignReference(TreeServiceBlockingStub stub) {
    this.stub = stub;
  }

  void setAssignTo(Reference assignTo) {
    this.assignTo = assignTo;
  }

  void setRefName(String refName) {
    this.refName = refName;
  }

  void setHash(String hash) {
    this.hash = hash;
  }

  void assign(ReferenceType refType) throws NessieNotFoundException, NessieConflictException {
    handle(
      () -> {
        AssignReferenceRequest.Builder builder = AssignReferenceRequest.newBuilder()
            .setReferenceType(refType)
            .setNamedRef(refName)
            .setOldHash(hash);
        if (assignTo instanceof Branch) {
          builder.setBranch(ProtoUtil.toProto((Branch) assignTo));
        } else if (assignTo instanceof Tag) {
          builder.setTag(ProtoUtil.toProto((Tag) assignTo));
        } else if (assignTo instanceof Detached) {
          builder.setDetached(ProtoUtil.toProto((Detached) assignTo));
        }
        return stub.assignReference(builder.build());
      });
  }
}
