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

import static io.opencensus.trace.SpanContext.INVALID;

import com.google.errorprone.annotations.MustBeClosed;
import io.opencensus.trace.AttributeValue;
import io.opencensus.trace.BlankSpan;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import io.opencensus.trace.propagation.SpanContextParseException;
import io.opencensus.trace.propagation.TextFormat;
import io.opencensus.trace.propagation.TextFormat.Setter;
import io.opencensus.trace.samplers.Samplers;
import io.opentracing.Scope;
import io.opentracing.ScopeManager;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.noop.NoopSpan;
import io.opentracing.propagation.Format;
import io.opentracing.propagation.TextMapExtract;
import io.opentracing.propagation.TextMapInject;
import io.opentracing.tag.Tag;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Hides open census objects behind the open tracing interfaces. Not all functionality is supported.
 * I took shortcuts based on the functionality I know we use.
 */
public class OpenCensusTracerAdapter implements io.opentracing.Tracer {

  private final Tracer ocTracer;

  public OpenCensusTracerAdapter(Tracer ocTracer) {
    this.ocTracer = ocTracer;
  }

  @Override
  public ScopeManager scopeManager() {
    throw new UnsupportedOperationException("Adapter does not support scopeManager");
  }

  @Override
  public Span activeSpan() {
    io.opencensus.trace.Span inner = ocTracer.getCurrentSpan();
    if (inner == null) {
      return null;
    }

    // An open census blank span is never sampled.
    // Moreover, gRPC creates non-blank spans even though it doesn't want to sample them.
    // The default policy is to inherit the sampling policy from the parent span.
    // Therefore, gRPC spans will not be sampled when we create blank parents.
    // Further reading:
    // https://github.com/census-instrumentation/opencensus-specs/blob/master/trace/Sampling.md
    final boolean shouldNotSample = !inner.getContext().getTraceOptions().isSampled();

    // Our traces will not sample if we use a noop.
    return shouldNotSample ? NoopSpan.INSTANCE : new OpenCensusSpanAdapter(inner);
  }

  @SuppressWarnings("MustBeClosedChecker")
  @MustBeClosed
  @Override
  public Scope activateSpan(Span span) {
    final io.opencensus.trace.Span newSpan;

    if (span instanceof OpenCensusSpanAdapter) {
      final OpenCensusSpanAdapter realSpan = (OpenCensusSpanAdapter) span;
      newSpan = realSpan.getOCSpan();
    } else {
      // Cannot activate non open census spans.
      // Also, noop spans should be remembered internally as blank spans.
      newSpan = BlankSpan.INSTANCE;
    }

    // This can only be called with an adapted span.
    return new OpenCensusScopeAdapter(ocTracer.withSpan(newSpan));
  }

  @Override
  public SpanBuilder buildSpan(String opName) {
    return new OpenCensusSpanBuilderAdapter(ocTracer, opName);
  }

  /**
   * Currently only supports text formats. Will need to update for binary. If open census context is
   * not used then we will inject nothing.
   *
   * @param context Either opencensus wrapped context or open tracing context.
   * @param format Binary or Text instance.
   * @param carrier Container of data in format format.
   * @param <C> Format type Binary or Text.
   */
  @Override
  public <C> void inject(SpanContext context, Format<C> format, C carrier) {
    OpenCensusContextAdapter adapter = ((OpenCensusContextAdapter) context);
    if (adapter == null) {
      return;
    }

    if (!(carrier instanceof TextMapInject)) {
      throw new UnsupportedOperationException(
          String.format("Injection format %s not supported", format));
    }

    TextMapInject map = (TextMapInject) carrier;

    final Setter<TextMapInject> setter =
        new Setter<TextMapInject>() {
          @Override
          public void put(TextMapInject carrier, String key, String value) {
            carrier.put(key, value);
          }
        };

    Tracing.getPropagationComponent().getB3Format().inject(adapter.getOcContext(), map, setter);
  }

  @Override
  public <C> SpanContext extract(Format<C> format, C carrier) {

    if (!(carrier instanceof TextMapExtract)) {
      throw new UnsupportedOperationException(
          String.format("Extraction format %s not supported", format));
    }

    TextMapExtract textMap = (TextMapExtract) carrier;

    Map<String, String> map = new HashMap<>();

    textMap.forEach((entry) -> map.put(entry.getKey(), entry.getValue()));

    final TextFormat.Getter<Map<String, String>> getter =
        new TextFormat.Getter<Map<String, String>>() {
          @Nullable
          @Override
          public String get(Map<String, String> carrier, String key) {
            return carrier.getOrDefault(key, null);
          }
        };

    io.opencensus.trace.SpanContext ocContext;
    try {
      return new OpenCensusContextAdapter(
          Tracing.getPropagationComponent().getB3Format().extract(map, getter));
    } catch (SpanContextParseException e) {
      return NoopSpan.INSTANCE.context();
    }
  }

  @Override
  public void close() {
    // Do nothing since underlying tracer cannot close.
    // Assume that close is only called when we switch the tracer out.
    // Therefore, this tracer will not receive anymore calls.
  }

  /**
   * Maps OT span to OC span. OT has a more generic interface that we won't support at this point.
   */
  public static class OpenCensusSpanAdapter implements io.opentracing.Span {
    private final io.opencensus.trace.Span ocSpan;

    OpenCensusSpanAdapter(io.opencensus.trace.Span span) {
      this.ocSpan = span;
    }

    io.opencensus.trace.Span getOCSpan() {
      return ocSpan;
    }

    @Override
    public SpanContext context() {
      return new OpenCensusContextAdapter(ocSpan.getContext());
    }

    @Override
    public Span setTag(String key, String value) {
      ocSpan.putAttribute(key, AttributeValue.stringAttributeValue(value));
      return this;
    }

    @Override
    public Span setTag(String key, boolean bool) {
      ocSpan.putAttribute(key, AttributeValue.booleanAttributeValue(bool));
      return this;
    }

    @Override
    public Span setTag(String key, Number num) {
      // Open census only supports long nums.
      ocSpan.putAttribute(key, AttributeValue.longAttributeValue(num.longValue()));
      return this;
    }

    @Override
    public <T> Span setTag(Tag<T> tag, T value) {
      throw new UnsupportedOperationException(
          "Adapter span only supports boolean, number, and string tags");
    }

    @Override
    public Span log(Map<String, ?> fields) {
      throw new UnsupportedOperationException("Adapter span does not support log on maps");
    }

    @Override
    public Span log(long timestamp, Map<String, ?> events) {
      throw new UnsupportedOperationException("Adapter span does not support log on maps");
    }

    @Override
    public Span log(String event) {
      ocSpan.addAnnotation(event);
      return this;
    }

    @Override
    public Span log(long ts, String event) {
      throw new UnsupportedOperationException(
          "Adapter span does not support custom timestamp logs");
    }

    @Override
    public Span setBaggageItem(String key, String value) {
      throw new UnsupportedOperationException("Adapter span does not support baggage");
    }

    @Override
    public String getBaggageItem(String key) {
      throw new UnsupportedOperationException("Adapter span does not support baggage");
    }

    @Override
    public Span setOperationName(String name) {
      // Open census doesn't have the concept of an operation name available on the span.
      ocSpan.addAnnotation(name);
      return this;
    }

    @Override
    public void finish() {
      ocSpan.end();
    }

    @Override
    public void finish(long ts) {
      throw new UnsupportedOperationException("Adapter span does not support custom finish times");
    }
  }

  /** OT to OC context adapter. */
  public static class OpenCensusContextAdapter implements SpanContext {
    private final io.opencensus.trace.SpanContext ocContext;

    OpenCensusContextAdapter(io.opencensus.trace.SpanContext ocContext) {
      this.ocContext = ocContext;
    }

    io.opencensus.trace.SpanContext getOcContext() {
      return ocContext;
    }

    @Override
    public String toTraceId() {
      return ocContext.getTraceId().toString();
    }

    @Override
    public String toSpanId() {
      return ocContext.getSpanId().toString();
    }

    @Override
    public Iterable<Map.Entry<String, String>> baggageItems() {
      throw new UnsupportedOperationException("No baggage items for open census");
    }
  }

  /** Wraps Opencensus scope behind Opentracing scope interface. */
  public static class OpenCensusScopeAdapter implements Scope {

    private final io.opencensus.common.Scope ocScope;

    OpenCensusScopeAdapter(io.opencensus.common.Scope ocScope) {
      this.ocScope = ocScope;
    }

    @Override
    public void close() {
      ocScope.close();
    }
  }

  /**
   * Maps behavior of Opencensus span building to Opentracing spanbuilder. In OC, the method called
   * on the tracer differs depending on the parent child relationship you want. Example:
   * ocTracer.spanBuilderWithRemoteParent(...); However, OT only has one method to get a builder,
   * once we have the builder we can customize the relationship. In order to preserve that control
   * over the parent relationship, we defer invoking the builder method on the ocTracer.
   */
  public static class OpenCensusSpanBuilderAdapter implements SpanBuilder {
    private final Tracer tracer;
    private String opName;
    private io.opencensus.trace.SpanContext parentContext;
    private io.opencensus.trace.Span parentSpan;
    private boolean ignoreActiveSpan = false;

    private Map<String, AttributeValue> attributes = new HashMap<>();

    OpenCensusSpanBuilderAdapter(Tracer tracer, String opName) {
      this.tracer = tracer;
      this.opName = opName;
    }

    @Override
    public SpanBuilder asChildOf(SpanContext parent) {
      if (!(parent instanceof OpenCensusContextAdapter)) {
        // Cannot parent spans across opencensus/tracing boundary.
        return this;
      }
      OpenCensusContextAdapter real = (OpenCensusContextAdapter) parent;
      ignoreActiveSpan = true;
      parentContext = real.getOcContext();
      return this;
    }

    @Override
    public SpanBuilder asChildOf(Span parent) {
      if (!(parent instanceof OpenCensusSpanAdapter)) {
        // Cannot make non open census span parent.
        return this;
      }
      ignoreActiveSpan = true;
      parentSpan = ((OpenCensusSpanAdapter) parent).getOCSpan();
      return this;
    }

    @Override
    public SpanBuilder addReference(String referenceType, SpanContext referencedContext) {
      throw new UnsupportedOperationException("Adapter does not support references");
    }

    @Override
    public SpanBuilder ignoreActiveSpan() {
      ignoreActiveSpan = true;
      return this;
    }

    private SpanBuilder addAttribute(String key, AttributeValue value) {
      attributes.put(key, value);
      return this;
    }

    @Override
    public SpanBuilder withTag(String key, String value) {
      return addAttribute(key, AttributeValue.stringAttributeValue(value));
    }

    @Override
    public SpanBuilder withTag(String key, boolean value) {
      return addAttribute(key, AttributeValue.booleanAttributeValue(value));
    }

    @Override
    public SpanBuilder withTag(String key, Number value) {
      return addAttribute(key, AttributeValue.longAttributeValue(value.longValue()));
    }

    @Override
    public <T> SpanBuilder withTag(Tag<T> key, T value) {
      throw new UnsupportedOperationException("Adapter does not support generic tags");
    }

    @Override
    public SpanBuilder withStartTimestamp(long microseconds) {
      throw new UnsupportedOperationException("Adapter does not support custom time");
    }

    @Override
    public Span start() {
      final io.opencensus.trace.SpanBuilder sb;

      if (ignoreActiveSpan) {

        if (parentSpan != null) {
          sb = tracer.spanBuilderWithExplicitParent(opName, parentSpan);
        } else if (parentContext != null) {
          sb = tracer.spanBuilderWithRemoteParent(opName, parentContext);
        } else {
          sb = tracer.spanBuilderWithRemoteParent(opName, INVALID);
        }

      } else {
        sb = tracer.spanBuilder(opName);
      }
      // We only use the always on sampler for our spans.
      // We could set it globally, but some other libraries (gRPC)
      // create spans through the opencensus tracing global and we don't want them always sampled.
      sb.setSampler(Samplers.alwaysSample());

      final io.opencensus.trace.Span span = sb.startSpan();
      span.putAttributes(attributes);
      return new OpenCensusSpanAdapter(span);
    }
  }
}
