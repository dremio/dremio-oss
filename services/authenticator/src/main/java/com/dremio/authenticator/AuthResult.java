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

import java.util.Date;
import org.immutables.value.Value;

/** Result of Authentication. */
@Value.Immutable
public interface AuthResult {

  /**
   * @return a username. Non-empty when returned by {@link Authenticator#authenticate(AuthRequest)}.
   *     This maybe null or empty when returned by {@link AuthProvider#validate(AuthRequest)}.
   */
  String getUserName();

  /**
   * @return expiry of the session. This may be null depending on the token type. If null, up to the
   *     caller to decide expiry of the session.
   */
  Date getExpiresAt();

  /**
   * @return a userID. Non-empty when returned by {@link Authenticator#authenticate(AuthRequest)}.
   *     This maybe null or empty when returned by {@link AuthProvider#validate(AuthRequest)}.
   */
  String getUserId();

  /**
   * @return (required) a validation mechanism that has successfully validated the token.
   */
  String getTokenType();

  /**
   * Helper method to build an AuthResult instance.
   *
   * @return ImmutableAuthResult.Builder
   */
  static ImmutableAuthResult.Builder builder() {
    return new ImmutableAuthResult.Builder();
  }
}
