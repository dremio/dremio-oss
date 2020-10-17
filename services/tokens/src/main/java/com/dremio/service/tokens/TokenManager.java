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
package com.dremio.service.tokens;

import com.dremio.service.Service;

/**
 * Token manager.
 */
public interface TokenManager extends Service {

  /**
   * Create a token for the session, and return details about the token.
   *
   * @param username user name
   * @param clientAddress client address
   * @return token details
   */
  TokenDetails createToken(String username, String clientAddress);

  /**
   * Validate the token, and return details about the token.
   *
   * @param token session token
   * @return token details
   * @throws IllegalArgumentException if the token is invalid or expired
   */
  TokenDetails validateToken(String token) throws IllegalArgumentException;

  /**
   * Invalidate the token.
   *
   * @param token session token
   */
  void invalidateToken(String token);

}
