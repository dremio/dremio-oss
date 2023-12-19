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
import static com.dremio.services.nessie.grpc.ProtoUtil.refFromProtoResponse;

import org.projectnessie.client.builder.BaseAssignReferenceBuilder;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Detached;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Tag;

import com.dremio.services.nessie.grpc.ProtoUtil;
import com.dremio.services.nessie.grpc.api.AssignReferenceRequest;
import com.dremio.services.nessie.grpc.api.ReferenceResponse;
import com.dremio.services.nessie.grpc.api.ReferenceType;
import com.dremio.services.nessie.grpc.api.TreeServiceGrpc.TreeServiceBlockingStub;

abstract class BaseGrpcAssignReference<T extends Reference, R> extends BaseAssignReferenceBuilder<R> {

  private final TreeServiceBlockingStub stub;

  protected BaseGrpcAssignReference(TreeServiceBlockingStub stub) {
    this.stub = stub;
  }

  public T assignAndGet() throws NessieNotFoundException, NessieConflictException {
    return handle(
      () -> {
        AssignReferenceRequest.Builder builder = AssignReferenceRequest.newBuilder()
            .setNamedRef(refName);

        if (expectedHash != null) {
          builder.setOldHash(expectedHash);
        }

        if (type != null) {
          builder.setReferenceType(ReferenceType.valueOf(type.name()));
        }

        if (assignTo instanceof Branch) {
          builder.setBranch(ProtoUtil.toProto((Branch) assignTo));
        } else if (assignTo instanceof Tag) {
          builder.setTag(ProtoUtil.toProto((Tag) assignTo));
        } else if (assignTo instanceof Detached) {
          builder.setDetached(ProtoUtil.toProto((Detached) assignTo));
        }

        ReferenceResponse response = stub.assignReference(builder.build());
        //noinspection unchecked
        return (T) refFromProtoResponse(response);
      });
  }

  public void assign() throws NessieNotFoundException, NessieConflictException {
    assignAndGet();
  }
}
