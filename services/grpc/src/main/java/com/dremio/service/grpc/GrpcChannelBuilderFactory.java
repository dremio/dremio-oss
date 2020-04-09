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

import com.dremio.telemetry.utils.GrpcTracerFacade;
import com.dremio.telemetry.utils.TracerFacade;

import io.grpc.ManagedChannelBuilder;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.opentracing.Tracer;
import io.opentracing.contrib.grpc.TracingClientInterceptor;

/**
 * Factory for creating gRPC ChannelBuilder in services.
 */
public final class GrpcChannelBuilderFactory {

  private static final int MAX_RETRY = Integer.parseInt(System.getProperty("dremio.grpc.retry_max", "10"));

  private final Tracer tracer;

  public GrpcChannelBuilderFactory(Tracer tracer) {
    this.tracer = new GrpcTracerFacade((TracerFacade) tracer);
  }

  /**
   * Returns a new gRPC ManagedChannelBuilder with instrumentation.
   */
  public ManagedChannelBuilder<?> newManagedChannelBuilder(String host, int port) {
    final ManagedChannelBuilder<?> builder = ManagedChannelBuilder.forAddress(host, port);
    addDefaultBuilderProperties(builder);

    return builder;
  }

  /**
   * Returns a new gRPC ManagedChannelBuilder with instrumentation.
   */
  public ManagedChannelBuilder<?> newManagedChannelBuilder(String target) {
    final ManagedChannelBuilder<?> builder = ManagedChannelBuilder.forTarget(target);
    addDefaultBuilderProperties(builder);

    return builder;
  }

  /**
   * Returns a new gRPC InProcessChannelBuilder with instrumentation.
   */
  public ManagedChannelBuilder<?> newInProcessChannelBuilder(String processName) {
    final ManagedChannelBuilder<?> builder = InProcessChannelBuilder.forName(processName);
    addDefaultBuilderProperties(builder);

    return builder;
  }

  /* Decorates a ManagedChannelBuilder with default properties.   */
  private void addDefaultBuilderProperties(ManagedChannelBuilder<?> builder) {
    final TracingClientInterceptor interceptor = TracingClientInterceptor
      .newBuilder()
      .withTracer(tracer)
      .build();

    builder.intercept(interceptor)
       .enableRetry()
       .maxRetryAttempts(MAX_RETRY);
  }
}
