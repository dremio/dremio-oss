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

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallStatus;
import org.slf4j.Logger;

import com.dremio.service.flight.DremioFlightSessionsManager;
import com.dremio.service.tokens.TokenManager;
import com.dremio.service.users.UserService;

/**
 * A collection of common Dremio Flight server authentication methods.
 */
public final class DremioFlightAuthUtils {
  private DremioFlightAuthUtils() {}

  /**
   * Authenticates provided credentials against Dremio and create an associated UserSession if
   * authentication is successful. Returns the token associated with the session created.
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
  public static String authenticateAndCreateSession(Provider<UserService> userServiceProvider,
                                                    Provider<TokenManager> tokenManagerProvider,
                                                    DremioFlightSessionsManager dremioFlightSessionsManager,
                                                    String username, String password, Logger logger) {
    checkUserSessionLimit(dremioFlightSessionsManager, logger);
    authenticateCredentials(userServiceProvider, username, password, logger);
    return createUserSession(
      tokenManagerProvider,
      dremioFlightSessionsManager,
      null,
      username);
  }

  /**
   * Create a new token with the TokenManager and create a new UserSession object associated with
   * the authenticated username.
   *
   * @param tokenManagerProvider Provider of TokenManager.
   * @param dremioFlightSessionsManager The DremioFlightSessionManager.
   * @param username Drmeio username.
   * @param logger The logger.
   * @return session token associated with the provided user.
   */
  public static String createUserSessionWithTokenAndProperties(Provider<TokenManager> tokenManagerProvider,
                                                               DremioFlightSessionsManager dremioFlightSessionsManager,
                                                               CallHeaders incomingHeaders,
                                                               String username, Logger logger) {
    checkUserSessionLimit(dremioFlightSessionsManager, logger);
    return createUserSession(
      tokenManagerProvider,
      dremioFlightSessionsManager,
      incomingHeaders,
      username);
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
  public static void authenticateCredentials(Provider<UserService> userServiceProvider,
                                             String username, String password, Logger logger) {
    try {
      userServiceProvider.get().authenticate(username, password);
    } catch (Exception e) {
      logger.error("Unable to authenticate user {}", username, e);
      final String errorMessage = "Unable to authenticate user " + username + ", exception: " + e.getMessage();
      throw CallStatus.UNAUTHENTICATED.withCause(e).withDescription(errorMessage).toRuntimeException();
    }
  }

  /**
   * Checks whether the number of UserSession has reached its maximum. If so, a FlightRuntimeException
   * is thrown with status code {@code UNAUTHENTICATED}.
   *
   * @param dremioFlightSessionsManager the DremioFlightSessionManager.
   * @param logger the slf4j logger for logging.
   * @throws org.apache.arrow.flight.FlightRuntimeException if the number of UserSession has reached its
   * maximum.
   */
  private static void checkUserSessionLimit(DremioFlightSessionsManager dremioFlightSessionsManager,
                                           Logger logger) {
    if (dremioFlightSessionsManager.reachedMaxNumberOfSessions()) {
      final String errorMessage = "Reached the maximum number of allowed sessions: " + dremioFlightSessionsManager.getMaxSessions();
      logger.error(errorMessage);
      throw CallStatus.UNAUTHENTICATED.withDescription(errorMessage).toRuntimeException();
    }
  }

  /**
   * Creates a new Flight UserSession. Returns the bearer token associated with the user session.
   *
   * @param tokenManagerProvider the TokenManager Provider.
   * @param dremioFlightSessionsManager the DremioFlightSessionManager.
   * @param incomingHeaders the CallHeaders to parse client properties from.
   * @param username the user to create a Flight server session for.
   * @return the token associated with the UserSession created.
   */
  private static String createUserSession(Provider<TokenManager> tokenManagerProvider,
                                          DremioFlightSessionsManager dremioFlightSessionsManager,
                                          CallHeaders incomingHeaders,
                                          String username) {
    // TODO: DX-25278: Add ClientAddress information while creating a Token in DremioFlightServerAuthValidator
    // Currently, we don't have access to the CallContext or CallHeaders from the ServerAuthHandler.
    // We will need changes in the Arrow-Flight Core to pass this information to ServerAuthHandler.
    // Changes in https://issues.apache.org/jira/browse/ARROW-9804 add the ability to get this information,
    // as we pass the whole CallHeaders to the ServerAuthHandler.
    final String token = tokenManagerProvider.get().createToken(username, null).token;
    dremioFlightSessionsManager.createUserSession(token, username, incomingHeaders);
    return token;
  }
}
