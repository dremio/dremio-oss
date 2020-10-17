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

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.auth.BasicServerAuthHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.service.flight.DremioFlightSessionsManager;
import com.dremio.service.tokens.TokenManager;
import com.dremio.service.users.UserService;
import com.google.common.base.Charsets;

/**
 * Implements BasicAuthValidator, and returns a token if the credentials are valid
 * and also checks token validity.
 */
public class DremioFlightServerAuthValidator implements BasicServerAuthHandler.BasicAuthValidator {
  private static final Logger logger = LoggerFactory.getLogger(DremioFlightServerAuthValidator.class);
  private final Provider<UserService> userServiceProvider;
  private final Provider<TokenManager> tokenManagerProvider;
  private final DremioFlightSessionsManager dremioFlightSessionsManager;

  public DremioFlightServerAuthValidator(Provider<UserService> userServiceProvider,
                                         Provider<TokenManager> tokenManagerProvider,
                                         DremioFlightSessionsManager dremioFlightSessionsManager) {
    this.userServiceProvider = userServiceProvider;
    this.tokenManagerProvider = tokenManagerProvider;
    this.dremioFlightSessionsManager = dremioFlightSessionsManager;
  }

  @Override
  public byte[] getToken(String username, String password) throws Exception {
    try {
      if (dremioFlightSessionsManager.reachedMaxNumberOfSessions()) {
        final String errorMessage = "Reached the maximum number of allowed sessions: " + dremioFlightSessionsManager.getMaxSessions();
        logger.error(errorMessage);
        throw CallStatus.UNAUTHENTICATED.withDescription(errorMessage).toRuntimeException();
      }
      userServiceProvider.get().authenticate(username, password);
      // TODO: DX-25278: Add ClientAddress information while creating a Token in DremioFlightServerAuthValidator
      // Currently, we don't have access to the CallContext or CallHeaders from the ServerAuthHandler.
      // We will need changes in the Arrow-Flight Core to pass this information to ServerAuthHandler.
      // Changes in https://issues.apache.org/jira/browse/ARROW-9804 add the ability to get this information,
      // as we pass the whole CallHeaders to the ServerAuthHandler.
      final String token = tokenManagerProvider.get().createToken(username, null).token;
      dremioFlightSessionsManager.createUserSession(token, username);
      return token.getBytes(Charsets.UTF_8);
    } catch (Exception e) {
      if (e instanceof RuntimeException) {
        throw e;
      }
      logger.error("Unable to authenticate user {}", username, e);
      final String errorMessage = "Unable to authenticate user " + username + ", exception: " + e.getMessage();
      throw CallStatus.UNAUTHENTICATED.withCause(e).withDescription(errorMessage).toRuntimeException();
    }
  }

  @Override
  public Optional<String> isValid(byte[] bytes) {
    final String token = new String(bytes, Charsets.UTF_8);
    try {
      tokenManagerProvider.get().validateToken(token);
      return Optional.of(token);
    } catch (IllegalArgumentException e) {
      logger.error("Token validation failed.", e);
      return Optional.empty();
    }
  }
}
