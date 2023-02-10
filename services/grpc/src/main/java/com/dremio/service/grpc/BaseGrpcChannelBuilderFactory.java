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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.inject.Provider;

import com.dremio.telemetry.utils.GrpcTracerFacade;
import com.dremio.telemetry.utils.TracerFacade;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannelBuilder;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.netty.NettyChannelBuilder;
import io.opentracing.Tracer;
import io.opentracing.contrib.grpc.TracingClientInterceptor;

/**
 * Base class for grpc channel builder factory.
 */
public class BaseGrpcChannelBuilderFactory implements GrpcChannelBuilderFactory {
  private final Tracer tracer;
  private final Set<ClientInterceptor> interceptors;
  private final Provider<Map<String, Object>> defaultServiceConfigProvider;
  private Integer idleTimeoutSeconds;

  public BaseGrpcChannelBuilderFactory(Tracer tracer) {
    this(tracer, Collections.emptySet(), () -> Maps.newHashMap());
  }

  public BaseGrpcChannelBuilderFactory(Tracer tracer, Set<ClientInterceptor> interceptors,
                                Provider<Map<String, Object>> defaultServiceConfigProvider) {
    this.tracer = new GrpcTracerFacade((TracerFacade) tracer);
    this.interceptors = Sets.newHashSet(interceptors);
    this.defaultServiceConfigProvider = defaultServiceConfigProvider;
  }

  public BaseGrpcChannelBuilderFactory withIdleTimeout(int idleTimeoutSeconds) {
    this.idleTimeoutSeconds = idleTimeoutSeconds;
    return this;
  }

  /**
   * Returns a new gRPC ManagedChannelBuilder with instrumentation.
   */
  @Override
  public ManagedChannelBuilder<?> newManagedChannelBuilder(String host, int port) {
    return newManagedChannelBuilder(host, port, defaultServiceConfigProvider.get());
  }

  /**
   * Returns a new gRPC ManagedChannelBuilder overriding the default service configuration
   */
  @Override
  public ManagedChannelBuilder<?> newManagedChannelBuilder(String host, int port, Map<String, Object> defaultServiceConfigProvider) {
    final ManagedChannelBuilder<?> builder = ManagedChannelBuilder.forAddress(host, port);
    addDefaultBuilderProperties(builder, defaultServiceConfigProvider);

    return builder;
  }

  /**
   * Returns a new gRPC ManagedChannelBuilder with instrumentation.
   */
  @Override
  public ManagedChannelBuilder<?> newManagedChannelBuilder(String target) {
    return newManagedChannelBuilder(target, defaultServiceConfigProvider.get());
  }

  @Override
  public ManagedChannelBuilder<?> newManagedChannelBuilder(String target, Map<String, Object> defaultServiceConfigProvider) {
    final ManagedChannelBuilder<?> builder = ManagedChannelBuilder.forTarget(target);
    addDefaultBuilderProperties(builder, defaultServiceConfigProvider);

    return builder;
  }

  /**
   * Returns a new gRPC InProcessChannelBuilder with instrumentation.
   */
  @Override
  public ManagedChannelBuilder<?> newInProcessChannelBuilder(String processName) {
    return newInProcessChannelBuilder(processName, defaultServiceConfigProvider.get());
  }

  public ManagedChannelBuilder<?> newInProcessChannelBuilder(String processName, Map<String, Object> defaultServiceConfigProvider) {
    final ManagedChannelBuilder<?> builder = InProcessChannelBuilder.forName(processName);
    addDefaultBuilderProperties(builder, defaultServiceConfigProvider);

    return builder;
  }

  /* Decorates a ManagedChannelBuilder with default properties.   */
  private void addDefaultBuilderProperties(ManagedChannelBuilder<?> builder, Map<String, Object> defaultServiceConfigProvider) {
    final int setMaxMetaDataSizeToEightMB = 8388608;
    final int defaultMaxInboundMessageSize = Integer.MAX_VALUE;
    final TracingClientInterceptor tracingInterceptor = TracingClientInterceptor
      .newBuilder()
      .withTracer(tracer)
      .build();

    for (ClientInterceptor interceptor : interceptors) {
      builder.intercept(interceptor);
    }
    builder.intercept(tracingInterceptor)
      .enableRetry()
      .defaultServiceConfig(defaultServiceConfigProvider)
      .maxInboundMetadataSize(setMaxMetaDataSizeToEightMB)
      .maxInboundMessageSize(defaultMaxInboundMessageSize)
      .maxRetryAttempts(MAX_RETRY);

    if (idleTimeoutSeconds != null) {
      builder.idleTimeout(idleTimeoutSeconds, TimeUnit.SECONDS);
    }

    if (builder instanceof NettyChannelBuilder) {
      // disable auto flow control
      ((NettyChannelBuilder) builder).flowControlWindow(NettyChannelBuilder.DEFAULT_FLOW_CONTROL_WINDOW);
    }
  }
}
