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
package com.dremio.authenticator;

import org.jetbrains.annotations.NotNull;

import com.dremio.service.Service;

/**
 * This interface defines an API for authentication.
 * Implementations of this interface are responsible for performing authentication,
 * retrieving information about the authenticated user and auditing.
 * <p>
 * Authentication involves validating user credentials (e.g., username/password)
 * or verifying the authenticity of a token.
 * <p>
 * Example usage:
 * <pre>
 *     Authenticator authenticator = new BasicAuthenticator();
 *     AuthRequest authRequest = AuthRequest.builder()
 *       .setUsername("USERNAME HERE")
 *       .setToken("PASSWORD HERE")
 *       .setResource(AuthRequest.Resource.USER_RPC)
 *       .build();
 *     try {
 *       AuthResult authResult = authenticator.authenticate(authRequest);
 *       authResult.getUserName();
 *       // Perform authorized actions for the authenticated user.
 *     } catch (AuthException e) {
 *       // Handle authentication failure.
 *     }
 * </pre>
 */
public interface Authenticator extends Service {

  /**
   * Validates user credentials (e.g., username/password)
   * or verifies the authenticity of a token.
   *
   * @param request contains
   *                - a password or a token is required
   *                - a username is required when a password is provided, optional otherwise
   *                - token_type is optional but recommended
   *                - resource type based on the client type is required
   * @return Authenticate result
   * - a valid userID is required
   * - a valid username is required
   * - the type of token validation that had successfully validated the credentials is required
   * - expiry is optional
   * @throws AuthException                 if the authentication failed.
   * @throws UnsupportedOperationException if requested operation is not supported.
   */
  @NotNull
  AuthResult authenticate(AuthRequest request) throws AuthException, UnsupportedOperationException;

  /**
   * Make sure all required {@link AuthProvider} and services are available.
   * @throws RuntimeException if the required {@link AuthProvider} and services cannot be initialized.
   */
  @Override
  void start();

  /**
   * NOOP
   */
  @Override
  default void close() {}
}
