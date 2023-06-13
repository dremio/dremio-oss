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

import java.util.Map;

import javax.inject.Provider;

import com.google.common.collect.Sets;

import io.grpc.ClientInterceptor;
import io.opentracing.Tracer;

/**
 * gRPC channel factory with multi-tenancy support.
 */
public final class MultiTenantGrpcChannelBuilderFactory extends BaseGrpcChannelBuilderFactory {
  private static final ClientInterceptor mtInterceptor =
    ContextualizedClientInterceptor.buildMultiTenantClientInterceptor();

  public MultiTenantGrpcChannelBuilderFactory(Tracer tracer, Provider<Map<String, Object>> defaultServiceConfigProvider) {
    super(tracer, Sets.newHashSet(mtInterceptor), defaultServiceConfigProvider);
  }

  public MultiTenantGrpcChannelBuilderFactory(
    Tracer tracer,
    Provider<Map<String, Object>> defaultServiceConfigProvider,
    ClientInterceptor interceptor)
  {
    super(tracer, Sets.newHashSet(mtInterceptor, interceptor), defaultServiceConfigProvider);
  }
}
