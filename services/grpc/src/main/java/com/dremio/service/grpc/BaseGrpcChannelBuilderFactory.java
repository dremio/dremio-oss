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

import java.util.Collections;
import java.util.Set;

import com.dremio.telemetry.utils.GrpcTracerFacade;
import com.dremio.telemetry.utils.TracerFacade;
import com.google.common.collect.Sets;

import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannelBuilder;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.opentracing.Tracer;
import io.opentracing.contrib.grpc.TracingClientInterceptor;

/**
 * Base class for grpc channel builder factory.
 */
class BaseGrpcChannelBuilderFactory implements GrpcChannelBuilderFactory {
  private final Tracer tracer;
  private final Set<ClientInterceptor> interceptors;

  BaseGrpcChannelBuilderFactory(Tracer tracer) {
    this(tracer, Collections.emptySet());
  }

  BaseGrpcChannelBuilderFactory(Tracer tracer, Set<ClientInterceptor> interceptors) {
    this.tracer = new GrpcTracerFacade((TracerFacade) tracer);
    this.interceptors = Sets.newHashSet(interceptors);
  }

  /**
   * Returns a new gRPC ManagedChannelBuilder with instrumentation.
   */
  @Override
  public ManagedChannelBuilder<?> newManagedChannelBuilder(String host, int port) {
    final ManagedChannelBuilder<?> builder = ManagedChannelBuilder.forAddress(host, port);
    addDefaultBuilderProperties(builder);

    return builder;
  }

  /**
   * Returns a new gRPC ManagedChannelBuilder with instrumentation.
   */
  @Override
  public ManagedChannelBuilder<?> newManagedChannelBuilder(String target) {
    final ManagedChannelBuilder<?> builder = ManagedChannelBuilder.forTarget(target);
    addDefaultBuilderProperties(builder);

    return builder;
  }

  /**
   * Returns a new gRPC InProcessChannelBuilder with instrumentation.
   */
  @Override
  public ManagedChannelBuilder<?> newInProcessChannelBuilder(String processName) {
    final ManagedChannelBuilder<?> builder = InProcessChannelBuilder.forName(processName);
    addDefaultBuilderProperties(builder);

    return builder;
  }

  /* Decorates a ManagedChannelBuilder with default properties.   */
  private void addDefaultBuilderProperties(ManagedChannelBuilder<?> builder) {
    final TracingClientInterceptor tracingInterceptor = TracingClientInterceptor
      .newBuilder()
      .withTracer(tracer)
      .build();

    for (ClientInterceptor interceptor : interceptors) {
      builder.intercept(interceptor);
    }
    builder.intercept(tracingInterceptor)
      .enableRetry()
      .maxRetryAttempts(MAX_RETRY);
  }
}
