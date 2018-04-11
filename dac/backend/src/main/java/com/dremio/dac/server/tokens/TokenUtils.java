/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

/**
 * Utility methods for tokens.
 */
public final class TokenUtils {

  public static final String TOKENS_TABLE_NAME = "tokens";

  public static final String AUTH_HEADER_PREFIX = "_dremio";

  /**
   * Get token from the authorization header.
   *
   * @param authHeader authorization header
   * @return token
   * @throws NotAuthorizedException if header format is incorrect
   */
  public static String getTokenFromAuthHeader(final String authHeader) throws NotAuthorizedException {
    if (authHeader == null || !authHeader.startsWith(AUTH_HEADER_PREFIX)) {
      throw new NotAuthorizedException("Authorization header must be provided");
    }
    return authHeader.substring(AUTH_HEADER_PREFIX.length()).trim();
  }

  private TokenUtils() {
  }
}
