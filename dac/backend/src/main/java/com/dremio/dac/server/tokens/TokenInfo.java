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

import java.util.Objects;
import java.util.function.Supplier;

import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;

import org.immutables.value.Value;

import com.dremio.service.tokens.TokenDetails;

/**
 * TokenInfo represents the required information of a token that can be injectable into any resource.
 */
@Value.Immutable
public interface TokenInfo {
  /**
   * Gets the token related username.
   *
   * @return a username
   */
  String getUsername();

  /**
   * Gets the token related expiration timestamp.
   *
   * @return an expiration timestamp
   */
  long getExpiresAt();

  /**
   * Builds an immutable TokenInfo instance containing the token related username
   * and expiration timestamp.
   *
   * @param username  the token related username
   * @param expiresAt the token related expiration timestamp
   * @return          an immutable TokenInfo instance
   */
  static TokenInfo of(String username, long expiresAt) {
    return new ImmutableTokenInfo.Builder().setUsername(username).setExpiresAt(expiresAt).build();
  }


  /**
   * Sets the token detailed information in the defined request context.
   *
   * @param context the request context instance
   * @param token   the token detailed information containing the token itself,
   *                the token related username and the expiration timestamp
   */
  static void setContext(ContainerRequestContext context, TokenDetails token) {
    Objects.requireNonNull(token);

    context.setProperty(Factory.CONTEXT_KEY, TokenInfo.of(token.username, token.expiresAt));
  }

  /**
   * Factory to extract TokenInfo from the current request context.
   */
  class Factory implements Supplier<TokenInfo> {
    /**
     * The defined default context property key to be set on request context objects.
     */
    private static final String CONTEXT_KEY = "dremio.token.info";

    private final ContainerRequestContext requestContext;

    @Inject
    public Factory(ContainerRequestContext requestContext) {
      this.requestContext = requestContext;
    }

    /**
     * Gets the current request context token information containing the token related username
     * and expiration timestamp.
     *
     * @return the TokenInfo instance set on the current request context
     */
    @Override
    public TokenInfo get() {
      return (TokenInfo) requestContext.getProperty(CONTEXT_KEY);
    }
  }
}
