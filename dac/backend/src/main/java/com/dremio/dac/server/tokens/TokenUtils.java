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
package com.dremio.dac.server.tokens;

import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.container.ContainerRequestContext;

import org.apache.http.HttpHeaders;

/**
 * Utility methods for tokens.
 */
public final class TokenUtils {

  public static final String AUTH_HEADER_PREFIX = "_dremio";

  /**
   * Attempt to read an auth token from an input string
   *
   * @param input String containing auth token
   * @return the token if it exists, otherwise null
   */
  private static String getToken(final String input) {
    if (input != null && input.startsWith(AUTH_HEADER_PREFIX)) {
      return input.substring(AUTH_HEADER_PREFIX.length()).trim();
    }
    return null;
  }

  /**
   * Get token from the authorization header.
   *
   * @param authHeader authorization header
   * @return token
   * @throws NotAuthorizedException if header format is incorrect
   */
  public static String getTokenFromAuthHeader(final String authHeader) throws NotAuthorizedException {
    final String token = getToken(authHeader);
    if (token == null) {
      throw new NotAuthorizedException("Authorization header must be provided");
    }
    return token;
  }

  private TokenUtils() {
  }

  /**
   * Get token from the authorization header or from the query parameters.
   *
   * @param context The request context
   * @return token
   * @throws NotAuthorizedException if header format is incorrect and the token is not supplied as a query param
   */
  public static String getTokenFromAuthHeaderOrQueryParameter(final ContainerRequestContext context)
    throws NotAuthorizedException {

    final String authHeader = getToken(context.getHeaderString(HttpHeaders.AUTHORIZATION));
    if (authHeader != null) {
      return authHeader;
    }

    final String token = getToken(context.getUriInfo().getQueryParameters().getFirst(HttpHeaders.AUTHORIZATION));
    if (token != null) {
      return token;
    }

    throw new NotAuthorizedException("Authorization header or access token must be provided");
  }
}
