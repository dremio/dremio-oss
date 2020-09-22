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
package com.dremio.service.grpc;

import io.grpc.ManagedChannelBuilder;

/**
 * Interface for gRPC channel builder factory.
 */
public interface GrpcChannelBuilderFactory {
  int MAX_RETRY = Integer.parseInt(System.getProperty("dremio.grpc.retry_max", "10"));

  /**
   * Returns a new gRPC ManagedChannelBuilder with instrumentation.
   */
  ManagedChannelBuilder<?> newManagedChannelBuilder(String host, int port);

  /**
   * Returns a new gRPC ManagedChannelBuilder with instrumentation.
   */
  ManagedChannelBuilder<?> newManagedChannelBuilder(String target);

  /**
   * Returns a new gRPC InProcessChannelBuilder with instrumentation.
   */
  ManagedChannelBuilder<?> newInProcessChannelBuilder(String processName);
}
