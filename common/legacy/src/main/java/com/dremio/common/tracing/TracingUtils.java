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
package com.dremio.common.tracing;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.base.Preconditions;

import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.noop.NoopSpan;
import io.opentracing.noop.NoopSpanBuilder;

/**
 * Common functions for tracing.
 */
public final class TracingUtils {

  private TracingUtils() {

  }

  /**
   * Creates a builder for a child span with spanName and tags.
   * @param tracer current tracer
   * @param spanName the desired name of the child span
   * @param tags an even length list of tags on the span.
   * @return A noop builder if there is no traced active span. Otherwise, a real span builder.
   */
  public static Tracer.SpanBuilder childSpanBuilder(Tracer tracer, String spanName, String... tags) {
    return childSpanBuilder(tracer, tracer.activeSpan(), spanName, tags);
  }

  /**
   * Creates a span builder for a tracer with some parent.
   * @param tracer current tracer
   * @param parent parent span the span will eventually attach to. Note: we don't create asChildOf relationship
   *               because we don't want to create an additional child relationship in the case where the parent
   *               span is equivalent to the active span.
   * @param spanName name to be given to span
   * @param tags tags given to span builder
   * @return noop if parent is noop or null. Otherwise, a real span builder.
   */
  public static Tracer.SpanBuilder childSpanBuilder(Tracer tracer, Span parent, String spanName, String... tags) {
    Preconditions.checkArgument(tags.length % 2 == 0);
    if (parent == null || parent.equals(NoopSpan.INSTANCE)) {
      return NoopSpanBuilder.INSTANCE;
    }

    Tracer.SpanBuilder builder = tracer.buildSpan(spanName);

    for (int i = 0; i < tags.length; i += 2) {
      builder.withTag(tags[i], tags[i + 1]);
    }
    return builder;
  }


  /**
   * Builds a real span if the operation is being traced. Otherwise, noop.
   * @param tracer current tracer
   * @param spanName the desired name of the child span
   * @param tags an even length list of tags on the span.
   * @return A child span or a noop span.
   */
  public static Span buildChildSpan(Tracer tracer, String spanName, String... tags) {
    return childSpanBuilder(tracer, spanName, tags).start();
  }

  /**
   * Creates a child span with operation name and tags.
   * Child span is active and lasts as long as the work.
   * @param <R> - return type.
   * @param work - Some user function. Accepts the child span.
   * @param tracer - tracer
   * @param operation - operation name
   * @param tags - an even length list of tags.
   * @return - whatever the user function "work" returns.
   */
  public static <R> R trace(Function<Span, R> work, Tracer tracer, String operation, String... tags) {
    Span span = TracingUtils.buildChildSpan(tracer, operation, tags);
    try (Scope s = tracer.activateSpan(span)) {
      return work.apply(span);
    } finally {
      span.finish();
    }
  }

  /**
   * Map supplier to function to use a single implementation of trace.
   * @param work - Work that returns some R
   * @param tracer - The tracer
   * @param operation - operation name
   * @param tags - an even length list of tags.
   * @param <R> - Return type
   * @return work.get()
   */
  public static <R> R trace(Supplier<R> work, Tracer tracer, String operation, String... tags) {
    return trace((span) -> {
      return work.get();
    }, tracer, operation, tags);
  }

  /**
   * Map Consumer to Function trace impl.
   * @param work - operation to be traced
   * @param tracer - tracer
   * @param operation - operation name
   * @param tags - tags on child span. Must be even length of list.
   */
  public static void trace(Consumer<Span> work, Tracer tracer, String operation, String... tags) {
    trace(
      (span) -> {
        work.accept(span);
        return null;
      },
      tracer,
      operation,
      tags);
  }

  /**
   * Map runnable to function trace implementation.
   * @param work runnable to be traced
   * @param tracer tracer
   * @param operation operation name
   * @param tags even length list of tags for the child span.
   */
  public static void trace(Runnable work, Tracer tracer, String operation, String... tags) {
    trace(
      (span) -> {
        work.run();
        return null;
      },
      tracer,
      operation,
      tags);
  }
}
