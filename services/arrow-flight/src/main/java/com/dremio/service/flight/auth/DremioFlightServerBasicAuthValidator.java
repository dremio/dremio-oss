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

package com.dremio.service.flight.auth;

import java.util.Optional;

import javax.inject.Provider;

import org.apache.arrow.flight.auth.BasicServerAuthHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.service.flight.DremioFlightSessionsManager;
import com.dremio.service.flight.utils.DremioFlightAuthUtils;
import com.dremio.service.tokens.TokenManager;
import com.dremio.service.users.UserService;
import com.google.common.base.Charsets;

/**
 * Dremio authentication specialized implementation of BasicAuthValidator. Authenticates with provided
 * credentials, creates a new UserSession, creates and validates associated bearer token.
 */
public class DremioFlightServerBasicAuthValidator implements BasicServerAuthHandler.BasicAuthValidator {
  private static final Logger LOGGER = LoggerFactory.getLogger(DremioFlightServerBasicAuthValidator.class);
  private final Provider<UserService> userServiceProvider;
  private final Provider<TokenManager> tokenManagerProvider;
  private final DremioFlightSessionsManager dremioFlightSessionsManager;

  public DremioFlightServerBasicAuthValidator(Provider<UserService> userServiceProvider,
                                              Provider<TokenManager> tokenManagerProvider,
                                              DremioFlightSessionsManager dremioFlightSessionsManager) {
    this.userServiceProvider = userServiceProvider;
    this.tokenManagerProvider = tokenManagerProvider;
    this.dremioFlightSessionsManager = dremioFlightSessionsManager;
  }

  @Override
  public byte[] getToken(String username, String password) {
    final String token = DremioFlightAuthUtils.authenticateAndCreateSession(
      userServiceProvider,
      tokenManagerProvider,
      dremioFlightSessionsManager,
      username, password, LOGGER);

    return token.getBytes(Charsets.UTF_8);
  }

  @Override
  public Optional<String> isValid(byte[] bytes) {
    final String token = new String(bytes, Charsets.UTF_8);
    try {
      tokenManagerProvider.get().validateToken(token);
      return Optional.of(token);
    } catch (IllegalArgumentException e) {
      LOGGER.error("Token validation failed.", e);
      return Optional.empty();
    }
  }
}
