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

import java.util.Set;

import com.dremio.telemetry.utils.GrpcTracerFacade;
import com.dremio.telemetry.utils.TracerFacade;
import com.google.common.collect.Sets;

import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptor;
import io.grpc.inprocess.InProcessServerBuilder;
import io.opentracing.Tracer;
import io.opentracing.contrib.grpc.TracingServerInterceptor;

/**
 * Factory for creating gRPC ServerBuilders in services.
 */
public final class GrpcServerBuilderFactory {

  private final Tracer tracer;
  private final Set<ServerInterceptor> interceptors;

  public GrpcServerBuilderFactory(Tracer tracer) {
    this(new GrpcTracerFacade((TracerFacade) tracer), Sets.newHashSet());
  }

  public GrpcServerBuilderFactory(GrpcTracerFacade tracer, Set<ServerInterceptor> interceptors) {
    this.tracer = tracer;
    this.interceptors = interceptors;
  }

  /**
   * Returns a new gRPC ServerBuilder with instrumentation.
   */
  public ServerBuilder<?> newServerBuilder() {
    return newServerBuilder(0);
  }

  /**
   * Returns a new gRPC ServerBuilder with instrumentation.
   */
  public ServerBuilder<?> newServerBuilder(int port) {
    final ServerBuilder<?> builder = ServerBuilder.forPort(port);
    addInterceptors(builder);
    return builder;
  }

  /**
   * Returns a new gRPC InProcessServerBuilder with instrumentation.
   */
  public ServerBuilder<?> newInProcessServerBuilder(String processName) {
    final ServerBuilder<?> builder = InProcessServerBuilder.forName(processName);
    addInterceptors(builder);

    return builder;
  }

  /* Decorates a ServerBuilder with additional interceptors.*/
  private void addInterceptors(ServerBuilder<?> builder) {
    final TracingServerInterceptor interceptor = TracingServerInterceptor
      .newBuilder()
      .withTracer(tracer)
      .build();

    builder.intercept(interceptor);

    for (ServerInterceptor intercept : interceptors) {
      builder.intercept(intercept);
    }
  }
}
