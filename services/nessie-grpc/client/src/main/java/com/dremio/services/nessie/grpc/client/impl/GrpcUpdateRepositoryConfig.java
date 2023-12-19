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

import static com.dremio.services.nessie.grpc.GrpcExceptionMapper.handleNessieRuntimeEx;
import static com.dremio.services.nessie.grpc.ProtoUtil.fromProto;

import org.projectnessie.client.api.UpdateRepositoryConfigBuilder;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.model.RepositoryConfig;
import org.projectnessie.model.UpdateRepositoryConfigResponse;

import com.dremio.services.nessie.grpc.ProtoUtil;
import com.dremio.services.nessie.grpc.api.ConfigServiceGrpc;

public class GrpcUpdateRepositoryConfig implements UpdateRepositoryConfigBuilder {

  private final ConfigServiceGrpc.ConfigServiceBlockingStub stub;

  private RepositoryConfig update;

  public GrpcUpdateRepositoryConfig(ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub) {
    this.stub = configServiceBlockingStub;
  }

  @Override
  public UpdateRepositoryConfigBuilder repositoryConfig(RepositoryConfig update) {
    if (this.update != null) {
      throw new IllegalStateException("repository config to update has already been set");
    }
    this.update = update;
    return this;
  }

  @Override
  public UpdateRepositoryConfigResponse update() throws NessieConflictException {
    return handleNessieRuntimeEx(
      () -> fromProto(stub.updateRepositoryConfig(ProtoUtil.toProtoRepoConfigUpdate(update))));
  }
}
