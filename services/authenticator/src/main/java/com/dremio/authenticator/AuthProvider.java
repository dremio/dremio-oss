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

import com.dremio.service.Service;
import org.jetbrains.annotations.NotNull;

/**
 * This interface defines an API for an authentication mechanism provider. Implementations of this
 * interface are responsible for performing authentication based on a specific token type.
 * Implementations of this interface are NOT responsible for retrieving information about the
 * authenticated user except the Username/Password Auth Provider(s).
 *
 * <p>Authentication involves validating user credentials (e.g., username/password) or verifying the
 * authenticity of a token.
 *
 * <p>Example usage:
 *
 * <pre>
 *     AuthProvider authProvider = new MyAuthProvider();
 *     AuthRequest authRequest = AuthRequest.builder()
 *       .setUsername("USERNAME HERE")
 *       .setToken("CREDENTIALS HERE")
 *       .setResource(AuthRequest.Resource.USER_RPC)
 *       .build();
 *     if (authProvider.isSupported(authRequest.getTokenType())) { // Optional check
 *       try {
 *         authResult = authProvider.validate(request);
 *         // Retrieve information about the authenticated user with the username or user ID from authResult.
 *         // Perform authorized actions for the authenticated user.
 *       } catch (AuthException e) {
 *         // Handle authentication failure.
 *       }
 *     } else {
 *       // Handle unsupported token type
 *     }
 * </pre>
 */
public interface AuthProvider extends Service {

  /** Returns true if token type is supported */
  boolean isSupported(String tokenType);

  /**
   * Validates credentials (password/token).
   *
   * @param request contains - a password or a token is required - a username is required when a
   *     password is provided, optional otherwise - resource type based on the client type is
   *     required
   * @return Authenticate result - either a userID or username is required - the type of token
   *     validation that had successfully validated the credentials is required - expiry is optional
   * @throws AuthException if the authentication failed.
   * @throws UnsupportedOperationException if requested operation is not supported.
   */
  @NotNull
  AuthResult validate(AuthRequest request) throws AuthException;

  /**
   * Make sure all required services are available.
   *
   * @throws RuntimeException if the required services cannot be initialized.
   */
  @Override
  void start();

  /** NOOP */
  @Override
  default void close() {}
}
