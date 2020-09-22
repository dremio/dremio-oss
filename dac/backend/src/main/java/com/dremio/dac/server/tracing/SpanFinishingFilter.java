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

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import io.opentracing.tag.Tags;

/**
 * Filter for completing trace spans in Jetty.
 */
public class SpanFinishingFilter implements Filter {
  public SpanFinishingFilter() {}

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {}

  @Override
  public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
    final HttpServletRequest httpServletRequest = (HttpServletRequest) servletRequest;
    final HttpServletResponse httpServletResponse = (HttpServletResponse) servletResponse;

    try {
      filterChain.doFilter(servletRequest, servletResponse);
    } catch (Exception ex) {
      final SpanHolder spanHolder = getSpanHolder(httpServletRequest);
      if (spanHolder != null) {
        spanHolder.getSpan()
          .setTag(Tags.ERROR.getKey(), true);
      }
      throw ex;
    } finally {
      final SpanHolder spanHolder = getSpanHolder(httpServletRequest);
      if (spanHolder != null) {
        spanHolder.getSpan()
          .setTag(Tags.HTTP_STATUS.getKey(), httpServletResponse.getStatus());
        spanHolder.finish();
      }
    }
  }

  @Override
  public void destroy() {

  }

  private SpanHolder getSpanHolder(final HttpServletRequest httpServletRequest) {
    final Object spanHolder = httpServletRequest.getAttribute(TracingUtils.SPAN_CONTEXT_PROPERTY);
    if (spanHolder instanceof SpanHolder) {
      return (SpanHolder) spanHolder;
    }

    return null;
  }
}
