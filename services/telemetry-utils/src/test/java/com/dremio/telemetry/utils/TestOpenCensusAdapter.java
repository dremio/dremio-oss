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

import static io.opentracing.propagation.Format.Builtin.HTTP_HEADERS;
import static org.junit.Assert.assertEquals;

import com.dremio.telemetry.utils.OpenCensusTracerAdapter.OpenCensusContextAdapter;
import com.dremio.telemetry.utils.OpenCensusTracerAdapter.OpenCensusSpanAdapter;
import io.opencensus.exporter.trace.logging.LoggingTraceExporter;
import io.opencensus.trace.Span;
import io.opencensus.trace.Tracing;
import io.opentracing.Scope;
import io.opentracing.Tracer;
import io.opentracing.noop.NoopSpan;
import io.opentracing.propagation.TextMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test the open census adapter to ensure its behavior is aligned with user expectations of
 * opentracing.
 */
public class TestOpenCensusAdapter {

  private static Tracer outerTracer;

  @BeforeClass
  public static void setup() {
    LoggingTraceExporter.register();

    outerTracer = new OpenCensusTracerAdapter(Tracing.getTracer());
  }

  private Span getInnerActiveSpan() {
    return ((OpenCensusSpanAdapter) outerTracer.activeSpan()).getOCSpan();
  }

  @Test
  public void testActivatingNonOpenCensusSpanResultsInNoopSpan() {

    try (Scope s = outerTracer.activateSpan(NoopSpan.INSTANCE)) {
      assertEquals(NoopSpan.INSTANCE, outerTracer.activeSpan());
    }

    // Noop Span should be active if there is no real active span.
    assertEquals(NoopSpan.INSTANCE, outerTracer.activeSpan());
  }

  @Test
  public void testActivatingOpenCensusSpan() {
    io.opentracing.Span span = outerTracer.buildSpan("testActivating").start();
    try (Scope s = outerTracer.activateSpan(span)) {
      assertEquals(getInnerActiveSpan(), ((OpenCensusSpanAdapter) span).getOCSpan());
    }
    assertEquals(NoopSpan.INSTANCE, outerTracer.activeSpan());
  }

  class LogTextMap implements TextMap {

    private Map<String, String> map = new HashMap<>();

    @Override
    public void put(String key, String value) {
      map.put(key, value);
    }

    @Override
    public Iterator<Map.Entry<String, String>> iterator() {
      return map.entrySet().iterator();
    }
  }

  @Test
  public void testTextFormatInjectionExtractionRoundTrip() {
    io.opentracing.Span span = outerTracer.buildSpan("testActivating").start();

    LogTextMap logger = new LogTextMap();
    outerTracer.inject(span.context(), HTTP_HEADERS, logger);

    assertEquals(
        ((OpenCensusContextAdapter) span.context()).getOcContext(),
        ((OpenCensusContextAdapter) outerTracer.extract(HTTP_HEADERS, logger)).getOcContext());
  }

  @Test
  public void testParentSpanByDefault() {
    final io.opentracing.Span parentSpan = outerTracer.buildSpan("test").start();

    final io.opentracing.Span childSpan;

    try (Scope s = outerTracer.activateSpan(parentSpan)) {
      childSpan = outerTracer.buildSpan("child").start();
    }

    Span innerChild = ((OpenCensusSpanAdapter) childSpan).getOCSpan();
    Span innerParent = ((OpenCensusSpanAdapter) parentSpan).getOCSpan();

    assertEquals(innerParent.getContext().getTraceId(), innerChild.getContext().getTraceId());
  }

  @Test
  public void testParentSpanExplicit() {
    final io.opentracing.Span parentSpan = outerTracer.buildSpan("test").start();

    final io.opentracing.Span childSpan;

    childSpan = outerTracer.buildSpan("child").asChildOf(parentSpan).start();

    Span innerChild = ((OpenCensusSpanAdapter) childSpan).getOCSpan();
    Span innerParent = ((OpenCensusSpanAdapter) parentSpan).getOCSpan();

    assertEquals(innerParent.getContext().getTraceId(), innerChild.getContext().getTraceId());
  }
}
