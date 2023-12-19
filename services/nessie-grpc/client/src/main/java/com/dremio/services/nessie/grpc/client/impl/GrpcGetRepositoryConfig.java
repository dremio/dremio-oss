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
import static java.util.Objects.requireNonNull;

import java.util.HashSet;
import java.util.Set;

import org.projectnessie.client.api.GetRepositoryConfigBuilder;
import org.projectnessie.model.RepositoryConfig;
import org.projectnessie.model.RepositoryConfigResponse;

import com.dremio.services.nessie.grpc.ProtoUtil;
import com.dremio.services.nessie.grpc.api.ConfigServiceGrpc.ConfigServiceBlockingStub;

public class GrpcGetRepositoryConfig implements GetRepositoryConfigBuilder {

  private final ConfigServiceBlockingStub stub;

  private final Set<RepositoryConfig.Type> types = new HashSet<>();

  public GrpcGetRepositoryConfig(ConfigServiceBlockingStub stub) {
    this.stub = stub;
  }

  @Override
  public GetRepositoryConfigBuilder type(RepositoryConfig.Type type) {
    types.add(requireNonNull(type));
    return this;
  }

  @Override
  public RepositoryConfigResponse get() {
    return handleNessieRuntimeEx(
      () -> fromProto(stub.getRepositoryConfig(ProtoUtil.toProtoRepoConfigRequest(types))));
  }
}
