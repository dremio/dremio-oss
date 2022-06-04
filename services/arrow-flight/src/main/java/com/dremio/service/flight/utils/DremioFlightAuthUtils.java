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
package com.dremio.service.flight.utils;

import javax.inject.Provider;

import org.apache.arrow.flight.CallStatus;
import org.slf4j.Logger;

import com.dremio.service.flight.DremioFlightSessionsManager;
import com.dremio.service.tokens.TokenManager;
import com.dremio.service.users.AuthResult;
import com.dremio.service.users.UserService;

/**
 * A collection of common Dremio Flight server authentication methods.
 */
public final class DremioFlightAuthUtils {
  private DremioFlightAuthUtils() {}

  /**
   * Authenticates provided credentials against Dremio and create an associated Bearer token if
   * authentication is successful. Returns the token associated with the user authenticated.
   *
   * @param userServiceProvider Provider of UserService.
   * @param tokenManagerProvider Provider of TokenManager.
   * @param dremioFlightSessionsManager The DremioFlightSessionsManager.
   * @param username Dremio username.
   * @param password Dremio password.
   * @param logger The logger.
   * @return session token associated with the provided user.
   * @throws org.apache.arrow.flight.FlightRuntimeException with status code {@code UNAUTHENTICATED}
   *         - If the number of UserSession has reached its maximum.
   *         - If unable to authenticate against Dremio with the provided credentials.
   */
  public static String authenticateAndCreateToken(Provider<UserService> userServiceProvider,
                                                    Provider<TokenManager> tokenManagerProvider,
                                                    DremioFlightSessionsManager dremioFlightSessionsManager,
                                                    String username, String password, Logger logger) {
    AuthResult authResult = authenticateCredentials(userServiceProvider, username, password, logger);
    return createToken(
      tokenManagerProvider,
      authResult.getUserName());
  }

  /**
   * Authenticate against Dremio with the provided credentials.
   *
   * @param userServiceProvider UserService Provider.
   * @param username Dremio username.
   * @param password Dremio password.
   * @param logger the slf4j logger for logging.
   * @throws org.apache.arrow.flight.FlightRuntimeException if unable to authenticate against Dremio
   * with the provided credentials.
   */
  public static AuthResult authenticateCredentials(Provider<UserService> userServiceProvider,
                                                   String username, String password, Logger logger) {
    try {
      return userServiceProvider.get().authenticate(username, password);
    } catch (Exception e) {
      logger.error("Unable to authenticate user {}", username, e);
      final String errorMessage = "Unable to authenticate user " + username + ", exception: " + e.getMessage();
      throw CallStatus.UNAUTHENTICATED.withCause(e).withDescription(errorMessage).toRuntimeException();
    }
  }

  /**
   * Create a new token with the TokenManager and create a new UserSession object associated with
   * the authenticated username.
   *
   * @param tokenManagerProvider Provider of TokenManager.
   * @param username Drmeio username.
   * @return session token associated with the provided user.
   */
  public static String createUserSessionWithTokenAndProperties(Provider<TokenManager> tokenManagerProvider,
                                                               String username) {
    return createToken(
      tokenManagerProvider,
      username);
  }

  /**
   * Creates a new Bearer Token. Returns the bearer token associated with the User.
   *
   * @param tokenManagerProvider the TokenManager Provider.
   * @param username the user to create a Flight server session for.
   * @return the token associated with the UserSession created.
   */
  private static String createToken(Provider<TokenManager> tokenManagerProvider,
                                          String username) {
    // TODO: DX-25278: Add ClientAddress information while creating a Token in DremioFlightServerAuthValidator
    return tokenManagerProvider.get().createToken(username, null).token;
  }
}
