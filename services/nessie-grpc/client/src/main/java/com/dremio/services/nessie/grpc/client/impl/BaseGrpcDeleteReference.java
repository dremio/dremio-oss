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

import com.dremio.services.nessie.grpc.api.DeleteReferenceRequest;
import com.dremio.services.nessie.grpc.api.ReferenceResponse;
import com.dremio.services.nessie.grpc.api.ReferenceType;
import com.dremio.services.nessie.grpc.api.TreeServiceGrpc.TreeServiceBlockingStub;
import org.projectnessie.client.builder.BaseChangeReferenceBuilder;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Reference;

abstract class BaseGrpcDeleteReference<T extends Reference, R>
    extends BaseChangeReferenceBuilder<R> {

  private final TreeServiceBlockingStub stub;

  protected BaseGrpcDeleteReference(TreeServiceBlockingStub stub) {
    this.stub = stub;
  }

  public T getAndDelete() throws NessieConflictException, NessieNotFoundException {
    return handle(
        () -> {
          DeleteReferenceRequest.Builder request =
              DeleteReferenceRequest.newBuilder().setNamedRef(refName);

          if (expectedHash != null) {
            request.setHash(expectedHash);
          }

          if (type != null) {
            request.setReferenceType(ReferenceType.valueOf(type.name()));
          }

          ReferenceResponse response = stub.deleteReference(request.build());
          //noinspection unchecked
          return (T) refFromProtoResponse(response);
        });
  }

  public void delete() throws NessieConflictException, NessieNotFoundException {
    getAndDelete();
  }
}
