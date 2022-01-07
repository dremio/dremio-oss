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
package com.dremio.service.nessie;

import java.util.function.Supplier;

import org.projectnessie.api.ConfigApi;

import com.dremio.service.nessieapi.ConfigApiGrpc;
import com.dremio.service.nessieapi.NessieConfiguration;
import com.google.protobuf.Empty;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

/**
 * Translates gRPC requests for the ConfigApiService and forwards them to the ConfigResource.
 */
class ConfigApiService extends ConfigApiGrpc.ConfigApiImplBase {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ConfigApiService.class);

  private final Supplier<ConfigApi> configResource;

  ConfigApiService(Supplier<ConfigApi> configResource) {
    this.configResource = configResource;
  }

  @Override
  public void getConfig(Empty request, StreamObserver<NessieConfiguration> responseObserver) {
    logger.debug("[gRPC] GetConfig)");
    try {
      final org.projectnessie.model.NessieConfiguration config = configResource.get().getConfig();
      responseObserver.onNext(GrpcNessieConverter.toGrpc(config));
      responseObserver.onCompleted();
    } catch (Exception e) {
      logger.error("GetConfig failed with an unexpected exception.", e);
      responseObserver.onError(new StatusRuntimeException(Status.UNKNOWN.withDescription(e.getMessage()).withCause(e)));
    }
  }
}

