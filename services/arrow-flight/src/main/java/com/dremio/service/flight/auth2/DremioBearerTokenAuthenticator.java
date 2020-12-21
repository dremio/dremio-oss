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
package com.dremio.service.flight.auth2;

import javax.inject.Provider;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.auth2.Auth2Constants;
import org.apache.arrow.flight.auth2.AuthUtilities;
import org.apache.arrow.flight.auth2.BasicCallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.apache.arrow.util.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.service.flight.DremioFlightSessionsManager;
import com.dremio.service.flight.utils.DremioFlightAuthUtils;
import com.dremio.service.tokens.TokenManager;
import com.dremio.service.users.UserService;

/**
 * Dremio's custom implementation of CallHeaderAuthenticator for bearer token authentication.
 * This class implements CallHeaderAuthenticator rather than BearerTokenAuthenticator. Dremio
 * creates UserSession objects when the bearer token is created and requires access to the CallHeaders
 * in getAuthResultWithBearerToken.
 */
public class DremioBearerTokenAuthenticator implements CallHeaderAuthenticator {
  private static final Logger LOGGER = LoggerFactory.getLogger(DremioBearerTokenAuthenticator.class);

  private final CallHeaderAuthenticator initialAuthenticator;
  private final Provider<TokenManager> tokenManagerProvider;
  private final DremioFlightSessionsManager dremioFlightSessionsManager;

  public DremioBearerTokenAuthenticator(Provider<UserService> userServiceProvider,
                                        Provider<TokenManager> tokenManagerProvider,
                                        DremioFlightSessionsManager dremioFlightSessionsManager) {
    this.initialAuthenticator = new BasicCallHeaderAuthenticator(
      new DremioCredentialValidator(userServiceProvider));
    this.tokenManagerProvider = tokenManagerProvider;
    this.dremioFlightSessionsManager = dremioFlightSessionsManager;
  }

  /**
   * If no bearer token is provided, the method initiates initial password and username
   * authentication. Once authenticated, client properties are retrieved from incoming CallHeaders.
   * Then it generates a token and creates a UserSession with the retrieved client properties.
   * associated with it.
   *
   * If a bearer token is provided, the method validates the provided token.
   *
   * @param incomingHeaders call headers to retrieve client properties and auth headers from.
   * @return an AuthResult with the bearer token and peer identity.
   */
  @Override
  public AuthResult authenticate(CallHeaders incomingHeaders) {
    final String bearerToken = AuthUtilities.getValueFromAuthHeader(incomingHeaders,
      Auth2Constants.BEARER_PREFIX);

    if (bearerToken != null) {
      return validateBearer(bearerToken);
    } else {
      final AuthResult result = initialAuthenticator.authenticate(incomingHeaders);
      return getAuthResultWithBearerToken(result, incomingHeaders);
    }
  }

  /**
   * Validates provided token.
   *
   * @param token the token to validate.
   * @return an AuthResult with the bearer token and peer identity.
   */
  @VisibleForTesting
  AuthResult validateBearer(String token) {
    try {
      tokenManagerProvider.get().validateToken(token);
      return createAuthResultWithBearerToken(token);
    } catch (IllegalArgumentException e) {
      LOGGER.error("Bearer token validation failed.", e);
      throw CallStatus.UNAUTHENTICATED.toRuntimeException();
    }
  }

  /**
   * Generates a bearer token, parses client properties from incoming headers, then creates a
   * UserSession associated with the generated token and client properties.
   *
   * @param authResult the AuthResult from initial authentication, with peer identity captured.
   * @param incomingHeaders the CallHeaders to parse client properties from.
   * @return an an AuthResult with the bearer token and peer identity.
   */
  @VisibleForTesting
  AuthResult getAuthResultWithBearerToken(AuthResult authResult, CallHeaders incomingHeaders) {
    final String username = authResult.getPeerIdentity();
    final String token = DremioFlightAuthUtils.createUserSessionWithTokenAndProperties(
      tokenManagerProvider,
      dremioFlightSessionsManager,
      incomingHeaders,
      username,
      LOGGER);

    return createAuthResultWithBearerToken(token);
  }

  /**
   * Helper method to create an AuthResult.
   *
   * @param token the token to create a UserSession for.
   * @return a new AuthResult with functionality to add given bearer token to the outgoing header.
   */
  private AuthResult createAuthResultWithBearerToken(String token) {
    return new AuthResult() {
      @Override
      public void appendToOutgoingHeaders(CallHeaders outgoingHeaders) {
        outgoingHeaders.insert(Auth2Constants.AUTHORIZATION_HEADER,
          Auth2Constants.BEARER_PREFIX + token);
      }

      @Override
      public String getPeerIdentity() {
        return token;
      }
    };
  }
}
