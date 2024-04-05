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

import com.dremio.context.SupportContext;
import com.dremio.context.TenantContext;
import com.dremio.context.UserContext;
import com.dremio.telemetry.utils.GrpcTracerFacade;
import com.dremio.telemetry.utils.TracerFacade;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import io.grpc.ServerInterceptor;
import io.opentracing.Tracer;

/** gRPC server factory with multi-tenancy support. */
public final class MultiTenantGrpcServerBuilderFactory extends BaseGrpcServerBuilderFactory {
  private static final ServerInterceptor mtInterceptor =
      new ContextualizedServerInterceptor(
          ImmutableList.of(
              new TenantContext.Transformer(),
              new UserContext.Transformer(),
              new SupportContext.Transformer()));

  public MultiTenantGrpcServerBuilderFactory(Tracer tracer) {
    super(new GrpcTracerFacade((TracerFacade) tracer), Sets.newHashSet(mtInterceptor));
  }
}
