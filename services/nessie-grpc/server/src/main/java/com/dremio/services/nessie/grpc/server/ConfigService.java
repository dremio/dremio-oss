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
package com.dremio.services.nessie.grpc.server;

import static com.dremio.services.nessie.grpc.ProtoUtil.toProto;
import static com.dremio.services.nessie.grpc.client.GrpcExceptionMapper.handle;

import java.util.function.Supplier;

import org.projectnessie.api.ConfigApi;

import com.dremio.services.nessie.grpc.api.ConfigServiceGrpc;
import com.dremio.services.nessie.grpc.api.Empty;
import com.dremio.services.nessie.grpc.api.NessieConfiguration;

import io.grpc.stub.StreamObserver;

/**
 * The gRPC service implementation for the Config-API.
 */
public class ConfigService extends ConfigServiceGrpc.ConfigServiceImplBase {

  private final Supplier<ConfigApi> bridge;

  public ConfigService(Supplier<ConfigApi> bridge) {
    this.bridge = bridge;
  }

  @Override
  public void getConfig(Empty request, StreamObserver<NessieConfiguration> observer) {
    handle(() -> toProto(bridge.get().getConfig()), observer);
  }
}
