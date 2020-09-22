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
package com.dremio.telemetry.utils;

import io.opentracing.Scope;
import io.opentracing.ScopeManager;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.noop.NoopTracerFactory;
import io.opentracing.propagation.Format;

/**
 * Tracer facade class to disable OpenTracing context propagation when OpenCensus is in use.
 * OpenCensus context is automatically propagated.
 */
public class GrpcTracerFacade implements Tracer {
  private final TracerFacade tracer;

  public GrpcTracerFacade(TracerFacade tracer) {
    this.tracer = tracer;
  }

  private Tracer getTracer() {
    return tracer.getTracer() instanceof OpenCensusTracerAdapter ? NoopTracerFactory.create() :
      tracer;
  }

  @Override
  public ScopeManager scopeManager() {
    return getTracer().scopeManager();
  }

  @Override
  public Span activeSpan() {
    return getTracer().activeSpan();
  }

  @Override
  public Scope activateSpan(Span span) {
    return getTracer().activateSpan(span);
  }

  @Override
  public SpanBuilder buildSpan(String operationName) {
    return getTracer().buildSpan(operationName);
  }

  @Override
  public <C> void inject(SpanContext spanContext, Format<C> format, C carrier) {
    getTracer().inject(spanContext, format, carrier);
  }

  @Override
  public <C> SpanContext extract(Format<C> format, C carrier) {
    return getTracer().extract(format, carrier);
  }

  @Override
  public void close() {
    tracer.close();
  }
}
