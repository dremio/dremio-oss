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
package com.dremio.service.flight;

import static com.dremio.service.flight.DremioFlightService.FLIGHT_AUTH2_AUTH_MODE;
import static com.dremio.service.flight.DremioFlightService.FLIGHT_LEGACY_AUTH_MODE;

import javax.inject.Provider;

import org.apache.arrow.flight.DremioFlightServer;
import org.apache.arrow.flight.auth.BasicServerAuthHandler;

import com.dremio.config.DremioConfig;
import com.dremio.service.flight.auth.DremioFlightServerBasicAuthValidator;
import com.dremio.service.flight.auth2.DremioBearerTokenAuthenticator;
import com.dremio.service.tokens.TokenManager;
import com.dremio.service.users.UserService;

/**
 * Adds the appropriate authentication handler for the Dremio software release.
 */
public class DremioFlightAuthProviderImpl implements DremioFlightAuthProvider {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DremioFlightAuthProviderImpl.class);

  private final Provider<DremioConfig> configProvider;
  private final Provider<UserService> userServiceProvider;
  private final Provider<TokenManager> tokenManagerProvider;

  public DremioFlightAuthProviderImpl(Provider<DremioConfig> configProvider,
                                      Provider<UserService> userServiceProvider,
                                      Provider<TokenManager> tokenManagerProvider) {
    this.configProvider = configProvider;
    this.userServiceProvider = userServiceProvider;
    this.tokenManagerProvider = tokenManagerProvider;
  }

  @Override
  public void addAuthHandler(DremioFlightServer.Builder builder, DremioFlightSessionsManager dremioFlightSessionsManager) {
    final String authMode = configProvider.get().getString(DremioConfig.FLIGHT_SERVICE_AUTHENTICATION_MODE);
    if (FLIGHT_LEGACY_AUTH_MODE.equals(authMode)) {
      builder.authHandler(new BasicServerAuthHandler(
        createBasicAuthValidator(userServiceProvider, tokenManagerProvider, dremioFlightSessionsManager)));
      logger.info("Using basic authentication with ServerAuthHandler.");
    } else if (FLIGHT_AUTH2_AUTH_MODE.equals(authMode)) {
      builder.headerAuthenticator(new DremioBearerTokenAuthenticator(userServiceProvider,
        tokenManagerProvider, dremioFlightSessionsManager));
      logger.info("Using bearer token authentication with CallHeaderAuthenticator.");
    } else {
      throw new RuntimeException(authMode
        + " is not a supported authentication mode for the Dremio FlightServer Endpoint.");
    }
  }

  /**
   * Factory method for creating an instance of BasicAuthValidator.
   *
   * @param userServiceProvider         The UserService Provider.
   * @param tokenManagerProvider        The TokenManager Provider.
   * @param dremioFlightSessionsManager An instance of DremioFlightSessionsManager.
   * @return An Instance of BasicAuthValidator.
   */
  protected BasicServerAuthHandler.BasicAuthValidator createBasicAuthValidator(Provider<UserService> userServiceProvider,
                                                                               Provider<TokenManager> tokenManagerProvider,
                                                                               DremioFlightSessionsManager dremioFlightSessionsManager) {
    return new DremioFlightServerBasicAuthValidator(userServiceProvider, tokenManagerProvider, dremioFlightSessionsManager);
  }
}
