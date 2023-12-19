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

/**
 * Exception thrown by {@link AuthProvider} and {@link Authenticator}.
 */
public class AuthException extends Exception {

  /**
   * Helper method for including the auth request and exception's message in the error message
   * and propagating the original exception.
   */
  public AuthException(AuthRequest authRequest, Exception e) {
    super(String.format("Authentication failed for user %s with type %s for resource %s. Reason: %s",
      authRequest.getUsername(), authRequest.getTokenType(),
      authRequest.getResource(), e.getMessage()), e);
  }

  /**
   * Helper method for including the auth request in the error message and a specific instruction for the end user.
   * Usually this is used when no need to propagate the original exception or the original exception has been handled by the server.
   */
  public AuthException(AuthRequest authRequest, String error) {
    super(String.format("Authentication failed for user %s with type %s for resource %s. Reason: %s",
      authRequest.getUsername(), authRequest.getTokenType(),
      authRequest.getResource(), error));
  }

  /**
   * Helper method for including an invalid user's username in the error message.
   */
  public AuthException(String username) {
    super(String.format("User %s was either not found or in an inactive state.",
      username));
  }

  /**
   * Helper method for propagating the original exception.
   */
  public AuthException(Exception e) {
    super(e);
  }
}
