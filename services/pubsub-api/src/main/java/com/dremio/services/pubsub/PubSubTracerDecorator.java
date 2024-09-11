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
package com.dremio.services.pubsub;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public final class PubSubTracerDecorator {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(PubSubTracerDecorator.class);

  private PubSubTracerDecorator() {}

  /** Decorates the call to publish.apply() with open telemetry tracing. */
  public static CompletableFuture<String> addPublishTracing(
      Tracer openTelemetryTracer,
      String topicName,
      Function<Span, CompletableFuture<String>> publish) {
    Span span =
        openTelemetryTracer
            .spanBuilder(topicName + ".publisher")
            .setSpanKind(SpanKind.PRODUCER)
            .startSpan();
    try {
      CompletableFuture<String> future = publish.apply(span);
      return future.whenComplete(
          (messageId, throwable) -> {
            try {
              if (throwable == null) {
                return;
              }
              span.recordException(throwable);
            } finally {
              span.end();
            }
          });
    } catch (Exception e) {
      logger.error("Failed trying to publish a message", e);
      throw new RuntimeException(e);
    }
  }

  /** Decorates the call to receiveMessage.apply() with open telemetry tracing. */
  public static void addSubscribeTracing(
      Tracer openTelemetryTracer,
      String subscriptionName,
      Context parentContext,
      Function<Span, Void> receiveMessage) {
    Span span =
        openTelemetryTracer
            .spanBuilder(subscriptionName + ".subscriber")
            .setSpanKind(SpanKind.CONSUMER)
            .setParent(parentContext)
            .startSpan();
    try {
      receiveMessage.apply(span);
    } catch (Exception e) {
      span.recordException(e);
      logger.error("Failed to process message", e);
      throw new RuntimeException(e);
    } finally {
      span.end();
    }
  }
}
