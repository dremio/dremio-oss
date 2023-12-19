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

import org.immutables.value.Value;

/**
 * Request of Authentication contains credentials and/or user info.
 */
@Value.Immutable
public abstract class AuthRequest {

  /**
   * Specifies token type. If absent (empty or null), uses default behavior.
   */
  public abstract String getTokenType();

  /**
   * Specifies the username of the Dremio user. Can be absent (null or empty) based on token type.
   */
  public abstract String getUsername();

  /**
   * Required. Can be a password or a token.
   */
  public abstract String getToken();

  /**
   * Types of resource.
   */
  public enum Resource {
    UNSPECIFIED,
    HTTP,
    FLIGHT,
    USER_RPC
  }

  /**
   * Required. Returns the type of resource. Usually determined by the type of client.
   */
  public abstract Resource getResource();

  /**
   * Helper method to build an AuthRequest instance.
   *
   * @return ImmutableAuthRequest.Builder
   */
  public static ImmutableAuthRequest.Builder builder() {
    return new ImmutableAuthRequest.Builder();
  }

  /**
   * Returns a string version of an instance of AuthRequest. Do not show sensitive info.
   */
  @Override
  public String toString() {
    return String.format("AuthRequest[username:%s, tokenType:%s, resource:%s]",
      getUsername(), getTokenType(), getResource());
  }
}
