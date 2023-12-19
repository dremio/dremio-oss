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
package com.dremio.service.tokens;

import java.util.Date;

import javax.inject.Provider;

import org.jetbrains.annotations.NotNull;

import com.dremio.authenticator.AuthException;
import com.dremio.authenticator.AuthProvider;
import com.dremio.authenticator.AuthRequest;
import com.dremio.authenticator.AuthResult;

/**
 * Validates Dremio access token.
 */
public class DremioAccessTokenAuthProvider implements AuthProvider {
  public static final String TOKEN_TYPE = "access_token";
  private final Provider<TokenManager> tokenManagerProvider;

  private TokenManager tokenManager;

  public DremioAccessTokenAuthProvider(
    Provider<TokenManager> tokenManagerProvider
  ) {
    this.tokenManagerProvider = tokenManagerProvider;
  }

  @Override
  public void start() {
    tokenManager = tokenManagerProvider.get();
  }

  /**
   * Returns true if token type is supported
   */
  @Override
  public boolean isSupported(String tokenType) {
    return TOKEN_TYPE.equalsIgnoreCase(tokenType);
  }

  /**
   * Validates Dremio access token. (Currently only used by Tableau)
   */
  @Override
  @NotNull
  public AuthResult validate(AuthRequest request) throws AuthException {
    final TokenDetails tokenDetails;
    try {
      tokenDetails = tokenManager.validateToken(request.getToken());
    } catch (IllegalArgumentException e) {
      throw new AuthException(request, e);
    }
    if (!tokenDetails.getScopes().contains("dremio.all")) {
      throw new AuthException(request, "Invalid scope.");
    }
    return AuthResult.builder()
      .setUserName(tokenDetails.username)
      .setExpiresAt(new Date(tokenDetails.expiresAt))
      .setTokenType(TOKEN_TYPE)
      .build();
  }
}
