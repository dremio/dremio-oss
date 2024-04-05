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
import io.grpc.ServerInterceptor;
import io.opentracing.Tracer;
import java.util.Set;

/** Grpc server factory helper. Most behavior depends on registered interceptors. */
public final class SimpleGrpcServerBuilderFactory extends BaseGrpcServerBuilderFactory {
  public SimpleGrpcServerBuilderFactory(Tracer tracer) {
    super(tracer);
  }

  public SimpleGrpcServerBuilderFactory(
      TracerFacade tracer, Set<ServerInterceptor> serverInterceptorSet) {
    super(new GrpcTracerFacade(tracer), serverInterceptorSet);
  }

  public SimpleGrpcServerBuilderFactory(
      GrpcTracerFacade tracer, Set<ServerInterceptor> serverInterceptorSet) {
    super(tracer, serverInterceptorSet);
  }
}
