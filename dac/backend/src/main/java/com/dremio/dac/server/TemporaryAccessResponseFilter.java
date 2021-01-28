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

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.Provider;

import com.dremio.dac.annotations.APIResource;
import com.dremio.dac.annotations.RestResource;
import com.dremio.dac.annotations.TemporaryAccess;

/**
 * Response filter designed to prevent referer header to be sent for subresources
 * fetched from the current resource as it might leak the authentication token to
 * 3rd parties.
 *
 * This filter automatically registers itself with v2 and v3 API servers.
 */
@TemporaryAccess
@RestResource
@APIResource
@Provider
public class TemporaryAccessResponseFilter implements ContainerResponseFilter {
  /*
   * Header specified at https://www.w3.org/TR/referrer-policy/
   * Examples and browser support available at https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Referrer-Policy
   */
  private static final String REFERRER_POLICY_HEADER = "Referrer-Policy";

  /*
   * Some of the accepted policy for Referrer-Policy header
   */
  private static final String STRICT_ORIGIN_WHEN_CROSS_ORIGIN_POLICY = "strict-origin-when-cross-origin";
  private static final String NO_REFERRER_POLICY = "no-referrer";

  @Override
  public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext)
      throws IOException {
    final MultivaluedMap<String, Object> headers = responseContext.getHeaders();
    headers.addAll(REFERRER_POLICY_HEADER, NO_REFERRER_POLICY, STRICT_ORIGIN_WHEN_CROSS_ORIGIN_POLICY);
  }
}
