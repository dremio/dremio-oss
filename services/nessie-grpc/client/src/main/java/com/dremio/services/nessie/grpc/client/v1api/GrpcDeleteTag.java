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

import org.projectnessie.client.api.DeleteTagBuilder;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;

import com.dremio.services.nessie.grpc.api.DeleteReferenceRequest;
import com.dremio.services.nessie.grpc.api.ReferenceType;
import com.dremio.services.nessie.grpc.api.TreeServiceGrpc.TreeServiceBlockingStub;

final class GrpcDeleteTag implements DeleteTagBuilder {

  private final TreeServiceBlockingStub stub;
  private String tagName;
  private String hash;

  public GrpcDeleteTag(TreeServiceBlockingStub stub) {
    this.stub = stub;
  }

  @Override
  public DeleteTagBuilder tagName(String tagName) {
    this.tagName = tagName;
    return this;
  }

  @Override
  public DeleteTagBuilder hash(String hash) {
    this.hash = hash;
    return this;
  }

  @Override
  public void delete() throws NessieConflictException, NessieNotFoundException {
    handle(
      () ->
        stub.deleteReference(
          DeleteReferenceRequest.newBuilder().setReferenceType(ReferenceType.TAG).setNamedRef(tagName).setHash(hash).build()));
  }
}
