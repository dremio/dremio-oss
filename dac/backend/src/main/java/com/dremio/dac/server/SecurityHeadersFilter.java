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
package com.dremio.dac.server;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;

/**
 * Filter that adds several security related HTTP headers
 */
public class SecurityHeadersFilter implements Filter {
  private static final long STS_MAX_AGE = TimeUnit.SECONDS.toDays(356);
  private static final String CSP_DEFAULT_HEADER = "default-src 'self' 'unsafe-inline' 'unsafe-eval'"
    + " blob: ws: *.dremio.com *.bm4u.net *.mktoresp.com *.cloudfront.net *.marketo.com *.sentry.io *.intercom.io"
    + " *.walkme.com *.intercomcdn.com *.io *.marketo.net *.bootstrapcdn.com *.googletagmanager.com; img-src 'self'"
    + " blob: data: *.cloudfront.net *.amazonaws.com; font-src 'self' data: *.bootstrapcdn.com;";

  @Override
  public void init(FilterConfig filterConfig) {
  }

  @Override
  public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
    final HttpServletResponse response = (HttpServletResponse) servletResponse;

    response.setHeader("x-content-type-options", "nosniff");
    response.setHeader("x-frame-options", "SAMEORIGIN");
    response.setHeader("x-xss-protection", "1; mode=block");
    response.setHeader("Content-Security-Policy", System.getProperty("dremio.ui.csp-header", CSP_DEFAULT_HEADER));

    if (servletRequest.isSecure()) {
      // https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Strict-Transport-Security
      response.setHeader("strict-transport-security", "max-age=" + STS_MAX_AGE);
    }

    filterChain.doFilter(servletRequest, servletResponse);
  }

  @Override
  public void destroy() {
  }
}
