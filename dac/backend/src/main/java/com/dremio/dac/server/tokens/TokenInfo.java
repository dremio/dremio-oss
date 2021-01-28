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
 * Required info of a token that is injectable into any resource.
 */
@Value.Immutable
public interface TokenInfo {
  String getUsername();

  long getExpiresAt();

  static TokenInfo of(String username, long expiresAt) {
    return new ImmutableTokenInfo.Builder().setUsername(username).setExpiresAt(expiresAt).build();
  }


  /**
   * Set token information in the current request context
   *
   * @param context
   * @param token
   */
  static void setContext(ContainerRequestContext context, TokenDetails token) {
    Objects.requireNonNull(token);

    context.setProperty(Factory.CONTEXT_KEY, TokenInfo.of(token.username, token.expiresAt));
  }

  /**
   * Factory to extract TokenInfo from the current request context.
   *
   */
  class Factory implements Supplier<TokenInfo> {
    private static final String CONTEXT_KEY = "dremio.token.info";

    private final ContainerRequestContext requestContext;

    @Inject
    public Factory(ContainerRequestContext requestContext) {
      this.requestContext = requestContext;
    }

    @Override
    public TokenInfo get() {
      return (TokenInfo) requestContext.getProperty(CONTEXT_KEY);
    }
  }
}
