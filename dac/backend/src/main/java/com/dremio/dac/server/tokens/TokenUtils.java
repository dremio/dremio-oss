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

import java.text.ParseException;

import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.container.ContainerRequestContext;

import org.apache.http.HttpHeaders;

/**
 * Utility methods for tokens.
 */
public final class TokenUtils {

  public static final String AUTH_HEADER_PREFIX = "_dremio";
  private static final String BEARER_TOKEN_PREFIX = "bearer";
  public static final String TOKEN_QUERY_PARAM = ".token";
  private static final String TOKEN_QUERY_CONTEXT_KEY = "TemporaryToken";

  /**
   * Attempt to read an auth token from an input string
   *
   * @param input String containing auth token
   * @return the token if it exists, otherwise null
   */
  public static String getToken(final String input) {
    if (input == null) {
      return null;
    }
    return input.startsWith(AUTH_HEADER_PREFIX)? input.substring(AUTH_HEADER_PREFIX.length()).trim() : input.trim();
  }

  /**
   * Get auth token from request context. Only the first value is considered,
   * other values are simply ignored.
   * @param context request-specific information
   * @return token string. Return null if authorization header is not present.
   */
  public static String getAuthHeaderToken(ContainerRequestContext context) {
    final String authHeader = context.getHeaders().getFirst(HttpHeaders.AUTHORIZATION);
    if (authHeader == null) {
      return null;
    }

    return authHeader.startsWith(AUTH_HEADER_PREFIX)? authHeader.substring(AUTH_HEADER_PREFIX.length()).trim() : authHeader.trim();
  }

  /**
   * Get bearer token from authHeader.
   * @param authHeader authorization header value
   * @return token string
   * @throws NotAuthorizedException if token does not exist
   * @throws ParseException if authHeader is not null and not a bearer token, caller
   * will catch this exception and fall back to non-DCS token validation.
   */
  public static String getBearerTokenFromAuthHeader(final String authHeader)
    throws NotAuthorizedException, ParseException {
    if (authHeader == null) {
      throw new NotAuthorizedException("Unauthorized.");
    }
    String[] splitToken = authHeader.split(" +");
    if (splitToken.length == 2 && splitToken[0].equalsIgnoreCase(BEARER_TOKEN_PREFIX)) {
      return splitToken[1];
    }
    throw new ParseException("Invalid bearer token.", 0);
  }

  private TokenUtils() {
  }

  /**
   * Return token from property. If not present, return null.
   */
  public static String getTemporaryToken(ContainerRequestContext context) {
    return getToken((String) context.getProperty(TOKEN_QUERY_CONTEXT_KEY));
  }

  /**
   * Set the request context property with the temporary token.
   */
  public static void setTemporaryToken(ContainerRequestContext context, String temporaryToken) {
    context.setProperty(TokenUtils.TOKEN_QUERY_CONTEXT_KEY, temporaryToken);
  }
}
