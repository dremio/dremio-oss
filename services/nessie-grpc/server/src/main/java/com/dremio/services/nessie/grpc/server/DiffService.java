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

import org.projectnessie.api.DiffApi;

import com.dremio.services.nessie.grpc.api.DiffRequest;
import com.dremio.services.nessie.grpc.api.DiffResponse;
import com.dremio.services.nessie.grpc.api.DiffServiceGrpc.DiffServiceImplBase;

import io.grpc.stub.StreamObserver;

/**
 * The gRPC service implementation for the Diff-API.
 */
public class DiffService extends DiffServiceImplBase {

  private final Supplier<DiffApi> bridge;

  public DiffService(Supplier<DiffApi> bridge) {
    this.bridge = bridge;
  }

  @Override
  public void getDiff(DiffRequest request, StreamObserver<DiffResponse> observer) {
    handle(() -> toProto(bridge.get().getDiff(fromProto(request))), observer);
  }
}
