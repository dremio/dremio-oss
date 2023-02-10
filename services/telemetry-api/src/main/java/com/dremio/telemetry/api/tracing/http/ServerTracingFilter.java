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
package com.dremio.telemetry.api.tracing.http;

import java.io.IOException;
import java.util.UUID;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.Priority;
import javax.ws.rs.Priorities;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.MultivaluedMap;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.logging.StructuredLogger;
import com.dremio.telemetry.api.Telemetry;
import com.dremio.telemetry.api.log.RequestTracingLogProtobuf.RequestTracingLog;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.semconv.trace.attributes.SemanticAttributes;


/**
 * Container Filter to start and end spans for http requests. This is intended to be used only with Rest Resources.
 */
@Priority(Priorities.HEADER_DECORATOR)
public class ServerTracingFilter implements ContainerRequestFilter, ContainerResponseFilter {
  private static final Logger logger = LoggerFactory.getLogger(ServerTracingFilter.class);
  //Atlantis initialization is whacked. It doesnt conform to the Guice initialization flow.
  //Hence the hack to lazily instantiate the tracer.
  private static Tracer tracer;
  public static final String TRACING_SCOPE_CONTEXT_PROPERTY = "tracing-scope";
  public static final String TRACING_SPAN_CONTEXT_PROPERTY = "tracing-span";
  public static final String REQUEST_ID_CONTEXT_PROPERTY = "request_id";
  public static final String REQUEST_ID_HEADER = "X-Request-ID";
  public static final String REQUEST_TRACING_LOGGER = "tracing.logger";

  private static final StructuredLogger structuredLogger =
      StructuredLogger.get(RequestTracingLog.class, ServerTracingFilter.class.getName());

  @javax.ws.rs.core.Context
  private ResourceInfo resourceInfo;

  private final boolean checkForParentContextEnabled;
  private final String forcedSamplingHeader;

  /**
   * Create a new filter with <code>checkForParentSpansEnabled</code> disabled.
   */
  public ServerTracingFilter() {
    this(false);
  }

  public ServerTracingFilter(boolean checkForParentSpansEnabled) {
    this(checkForParentSpansEnabled, null);
  }

  /**
   * new Filter with give settings.
   *
   * @param checkForParentSpansEnabled if <code>true</code> will check for parent spans to join.
   * @param forceSamplingHeader if not null, will check for presence of this header to start a span.
   */
  public ServerTracingFilter(boolean checkForParentSpansEnabled, String forceSamplingHeader) {
    this.checkForParentContextEnabled = checkForParentSpansEnabled;
    this.forcedSamplingHeader = forceSamplingHeader;
  }

  public boolean isCheckForParentContextEnabled() {
    return checkForParentContextEnabled;
  }

  private static Tracer getTracer() {
    if (tracer == null) {
      tracer = GlobalOpenTelemetry.getTracer("com.dremio.server.http");
    }
    return tracer;
  }

  @SuppressWarnings("MustBeClosedChecker")
  @Override
  public void filter(ContainerRequestContext requestContext) throws IOException {
    String spanName;
    if (resourceInfo != null) {
      spanName = resourceInfo.getResourceClass().getName() + "." + resourceInfo.getResourceMethod().getName();
    } else {
      spanName = "http-request";
    }
    SpanBuilder spanBuilder = getTracer().spanBuilder(requestContext.getMethod() + " " + spanName);
    spanBuilder.setSpanKind(SpanKind.SERVER);
    spanBuilder.setAttribute(SemanticAttributes.HTTP_METHOD, requestContext.getMethod());
    spanBuilder.setAttribute(SemanticAttributes.HTTP_URL, requestContext.getUriInfo().getRequestUri().toASCIIString());
    MultivaluedMap<String, String> pathParams = requestContext.getUriInfo().getPathParameters();
    if (!pathParams.isEmpty()) {
      pathParams.forEach((key, value) -> spanBuilder.setAttribute(key, String.join(",", value)));
    }
    if (forcedSamplingHeader != null && requestContext.getHeaders().containsKey(forcedSamplingHeader)) {
      logger.debug("Found Header in request - '{}' : '{}'",
        forcedSamplingHeader, requestContext.getHeaders().getFirst(forcedSamplingHeader));

      boolean headerValue = Boolean.parseBoolean(""+requestContext.getHeaders().getFirst(forcedSamplingHeader));
      if (headerValue) {
        spanBuilder.setAttribute(Telemetry.FORCE_SAMPLING_ATTRIBUTE, true);
      }
    }

    if (isCheckForParentContextEnabled()) {
      Context parentContext = extractParentContext(requestContext);
      spanBuilder.setParent(parentContext);
    }

    Span span = spanBuilder.startSpan();
    requestContext.setProperty(TRACING_SPAN_CONTEXT_PROPERTY, span);

    requestContext.setProperty(TRACING_SCOPE_CONTEXT_PROPERTY, span.makeCurrent());

    if (shouldHandleRequestId(span)) {
      String requestId = getOrCreateRequestId(requestContext);
      requestContext.setProperty(REQUEST_ID_CONTEXT_PROPERTY, requestId);
      logRequestTraceInfo(requestId, span);
    }
  }

  @Override
  public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext) throws IOException {
    final int responseStatus = responseContext.getStatus();
    final Scope scope = (Scope) requestContext.getProperty(TRACING_SCOPE_CONTEXT_PROPERTY);
    final Span span = (Span) requestContext.getProperty(TRACING_SPAN_CONTEXT_PROPERTY);
    if (scope != null) {
      span.setAttribute(SemanticAttributes.HTTP_STATUS_CODE, responseStatus);
      span.end();
      scope.close();
    }

    if (shouldHandleRequestId(span)) {
      final String requestId = (String) requestContext.getProperty(REQUEST_ID_CONTEXT_PROPERTY);
      attachRequestIdToResponseIfNotExists(responseContext, requestId);
    }
  }

  private Context extractParentContext(ContainerRequestContext requestContext) {
    TextMapGetter<ContainerRequestContext> getter = new TextMapGetter<ContainerRequestContext>() {
      @Override
      public Iterable<String> keys(ContainerRequestContext carrier) {
        return carrier.getHeaders().keySet();
      }
      @Nullable
      @Override
      public String get(@Nullable ContainerRequestContext carrier, @Nonnull String key) {
        if (carrier != null) {
          return carrier.getHeaderString(key);
        }
        return null;
      }
    };

    return GlobalOpenTelemetry.get().getPropagators()
      .getTextMapPropagator()
      .extract(Context.current(), requestContext, getter);
  }

  private void attachRequestIdToResponseIfNotExists(ContainerResponseContext responseContext, String requestId) {
    if (StringUtils.isBlank(responseContext.getHeaderString(REQUEST_ID_HEADER))) {
      responseContext.getHeaders().add(REQUEST_ID_HEADER, requestId);
    }
  }

  private void logRequestTraceInfo(String requestId, Span span) {
    String traceId = span.getSpanContext().getTraceId();
    String spanId = span.getSpanContext().getSpanId();
    structuredLogger.info(RequestTracingLog.newBuilder()
                .setRequestId(requestId)
                .setTraceId(traceId)
                .setSpanId(spanId)
                .build(),
            "Using request id [{}]; trace id [{}]; span id [{}] for the request.",
            requestId,
            traceId,
            traceId);
  }

  private boolean shouldHandleRequestId(Span span) {
    return span != null && span.getSpanContext().isSampled();
  }

  private String getOrCreateRequestId(ContainerRequestContext requestContext) {
    final String requestId = requestContext.getHeaderString(REQUEST_ID_HEADER);
    if (StringUtils.isNotBlank(requestId)) {
      return requestId;
    }
    // this is fallback
    final String newRequestId =  UUID.randomUUID().toString();
    logger.debug("Request ID header is missing on the request. Generating new on instead: {}", newRequestId);
    return newRequestId;
  }
}
