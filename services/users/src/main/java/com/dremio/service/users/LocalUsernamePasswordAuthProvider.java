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
package com.dremio.service.users;

import javax.inject.Provider;

import org.jetbrains.annotations.NotNull;

import com.dremio.authenticator.AuthException;
import com.dremio.authenticator.AuthProvider;
import com.dremio.authenticator.AuthRequest;
import com.dremio.authenticator.AuthResult;

/**
 * Validate username/local password
 */
public class LocalUsernamePasswordAuthProvider implements AuthProvider {
  public static final String TOKEN_TYPE = "local_password";
  private final Provider<SimpleUserService> simpleUserServiceProvider;

  private SimpleUserService simpleUserService;

  public LocalUsernamePasswordAuthProvider(
    Provider<SimpleUserService> simpleUserServiceProvider
  ) {
    this.simpleUserServiceProvider = simpleUserServiceProvider;
  }

  @Override
  public void start() {
    simpleUserService = simpleUserServiceProvider.get();
  }

  /**
   * Returns true if token type is supported
   */
  @Override
  public boolean isSupported(String tokenType) {
    return TOKEN_TYPE.equalsIgnoreCase(tokenType);
  }

  /**
   * Validates user credentials (password/token).
   */
  @Override
  @NotNull
  public AuthResult validate(AuthRequest request) throws AuthException {
    final String userName = request.getUsername();
    final String password = request.getToken();
    try {
      final com.dremio.service.users.AuthResult authResult =
        simpleUserService.authenticate(userName, password);
      return AuthResult.builder()
        .setUserName(authResult.getUserName())
        .setUserId(authResult.getUserId())
        .setTokenType(TOKEN_TYPE)
        .build();
    } catch (UserLoginException e) {
      throw new AuthException(request, e);
    }
  }
}
