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
package com.dremio.dac.server.tracing;

import javax.annotation.Priority;
import javax.ws.rs.Priorities;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.container.DynamicFeature;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.FeatureContext;
import javax.ws.rs.core.MultivaluedMap;

import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.noop.NoopSpan;
import io.opentracing.noop.NoopSpanContext;
import io.opentracing.propagation.Format;
import io.opentracing.tag.Tags;

/**
 * Dynamic feature for Jersey request tracing.
 */
public class ServerTracingDynamicFeature implements DynamicFeature {
  private Tracer tracer;

  public ServerTracingDynamicFeature(Tracer tracer) {
    this.tracer = tracer;
  }

  @Override
  public void configure(ResourceInfo resourceInfo, FeatureContext context) {
    context.register(new ServerTracingFilter());
  }

  /**
   * Request filter for server request tracing.
   */
  @Priority(Priorities.HEADER_DECORATOR)
  private class ServerTracingFilter implements ContainerRequestFilter, ContainerResponseFilter {

    @Override
    public void filter(ContainerRequestContext containerRequestContext) {
      // Check if there is already a Span context.
      if(TracingUtils.getResourceSpan(containerRequestContext).isPresent()) {
        return;
      }

      // Check if there is an existing span to join.
      final MultivaluedMap<String, String> headers = containerRequestContext.getHeaders();
      final SpanContext parentSpanContext = tracer.extract(Format.Builtin.HTTP_HEADERS, new RequestHeaderTextMap(headers));
      final boolean shouldTrace = Boolean.parseBoolean(headers.getFirst(TracingUtils.TRACING_HEADER));

      final Span span;
      if ((parentSpanContext == null || parentSpanContext instanceof NoopSpanContext) && !shouldTrace) {
        span = NoopSpan.INSTANCE;
      } else {
        span = tracer.buildSpan("http-request")
          .ignoreActiveSpan()
          .asChildOf(parentSpanContext)
          .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_SERVER)
          .withTag(Tags.HTTP_METHOD.getKey(), containerRequestContext.getMethod())
          .withTag(Tags.HTTP_URL.getKey(), containerRequestContext.getUriInfo().getRequestUri().toASCIIString())
          .start();
      }

      containerRequestContext.setProperty(TracingUtils.SPAN_CONTEXT_PROPERTY, new SpanHolder(span, tracer.activateSpan(span)));
    }

    @Override
    public void filter(ContainerRequestContext containerRequestContext, ContainerResponseContext containerResponseContext) {
      final SpanHolder spanHolder = getSpanHolder(containerRequestContext);
      if (spanHolder == null) {
        return;
      }

      final Span span = spanHolder.getSpan();
      final int responseStatus = containerResponseContext.getStatus();

      span.setTag(Tags.HTTP_STATUS.getKey(), responseStatus);
      if(responseStatus >= 400) {
        span.setTag(Tags.ERROR.getKey(), true);
      }
    }

    private SpanHolder getSpanHolder(final ContainerRequestContext containerRequestContext) {
      final Object spanHolder = containerRequestContext.getProperty(TracingUtils.SPAN_CONTEXT_PROPERTY);
      if (spanHolder instanceof SpanHolder) {
        return (SpanHolder) spanHolder;
      }

      return null;
    }
  }
}
