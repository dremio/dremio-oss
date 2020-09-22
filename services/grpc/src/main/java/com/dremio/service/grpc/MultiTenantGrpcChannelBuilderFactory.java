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

import com.google.common.collect.Sets;

import io.grpc.ClientInterceptor;
import io.opentracing.Tracer;

/**
 * gRPC channel factory with multi-tenancy support.
 */
public final class MultiTenantGrpcChannelBuilderFactory extends BaseGrpcChannelBuilderFactory {
  private static final ClientInterceptor mtInterceptor = new MultiTenantClientInterceptor();

  public MultiTenantGrpcChannelBuilderFactory(Tracer tracer) {
    super(tracer, Sets.newHashSet(mtInterceptor));
  }
}
