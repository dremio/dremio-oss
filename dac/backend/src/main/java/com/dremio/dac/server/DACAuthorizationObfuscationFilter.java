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

import javax.annotation.Priority;
import javax.ws.rs.Priorities;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.ext.Provider;

import com.dremio.dac.annotations.Secured;

/**
 * Move token from query parameter to header.
 */
@Secured
@Provider
@PreMatching
@Priority(Priorities.AUTHENTICATION)
public class DACAuthorizationObfuscationFilter implements ContainerRequestFilter {

  public DACAuthorizationObfuscationFilter() {
  }

  @Override
  public void filter(ContainerRequestContext requestContext) {
    //Remove Authorization token from Uri if exists.
    MultivaluedMap<String, String> queryParams = requestContext.getUriInfo().getQueryParameters();
    if (queryParams.containsKey(HttpHeaders.AUTHORIZATION)) {
      String token = queryParams.getFirst(org.apache.http.HttpHeaders.AUTHORIZATION);
      UriInfo uriInfo = requestContext.getUriInfo();
      UriBuilder builder = uriInfo.getBaseUriBuilder().path(uriInfo.getPath());
      queryParams.forEach((k, v) -> { // v is a list
        if (!HttpHeaders.AUTHORIZATION.equals(k)) {
          v.forEach(e -> builder.queryParam(k, e));
        }
      });
      requestContext.setRequestUri(builder.build());
      requestContext.getHeaders().putSingle(org.apache.http.HttpHeaders.AUTHORIZATION, token);
    }
  }
}
