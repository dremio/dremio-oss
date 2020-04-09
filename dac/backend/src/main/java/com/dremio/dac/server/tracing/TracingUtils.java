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

import java.util.Optional;

import javax.ws.rs.container.ContainerRequestContext;

import io.opentracing.Span;

/**
 * Utilities for easily tracing Jersey requests.
 */
public class TracingUtils {
  static final String SPAN_CONTEXT_PROPERTY = "span";
  public static final String TRACING_HEADER = "x-tracing-enabled";

  public static Optional<Span> getResourceSpan(final ContainerRequestContext context) {
    return Optional.ofNullable(context).map(c -> {
      final Object spanHolder = c.getProperty(SPAN_CONTEXT_PROPERTY);
      if(spanHolder instanceof SpanHolder) {
        return ((SpanHolder) spanHolder).getSpan();
      }
      return null;
    });
  }
}
