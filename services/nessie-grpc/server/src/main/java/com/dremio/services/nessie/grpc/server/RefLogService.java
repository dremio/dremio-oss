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

import static com.dremio.services.nessie.grpc.ProtoUtil.fromProto;
import static com.dremio.services.nessie.grpc.ProtoUtil.toProto;
import static com.dremio.services.nessie.grpc.client.GrpcExceptionMapper.handle;

import java.util.function.Supplier;

import com.dremio.services.nessie.grpc.api.RefLogParams;
import com.dremio.services.nessie.grpc.api.RefLogResponse;
import com.dremio.services.nessie.grpc.api.RefLogServiceGrpc.RefLogServiceImplBase;

import io.grpc.stub.StreamObserver;

/**
 * The gRPC service implementation for the RefLog-API.
 */
public class RefLogService extends RefLogServiceImplBase {

  private final Supplier<? extends org.projectnessie.services.spi.RefLogService> bridge;

  public RefLogService(Supplier<? extends org.projectnessie.services.spi.RefLogService> bridge) {
    this.bridge = bridge;
  }

  @Override
  public void getRefLog(RefLogParams params, StreamObserver<RefLogResponse> observer) {
    handle(() -> toProto(bridge.get().getRefLog(
      fromProto(params::hasStartHash, params::getStartHash),
      fromProto(params::hasEndHash, params::getEndHash),
      fromProto(params::hasFilter, params::getFilter),
      fromProto(params::hasMaxRecords, params::getMaxRecords),
      fromProto(params::hasPageToken, params::getPageToken))
      ), observer);
  }
}
