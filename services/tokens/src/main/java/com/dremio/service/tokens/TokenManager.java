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

import java.util.List;
import java.util.Map;

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
   * Create a temporary token for certain API requests that require ui to make
   * a non-ajax call.
   *
   * @param username user name
   * @param path designated REST API url path
   * @param queryParams designated REST API url query params
   * @param durationMillis duration of the temporary token in milliseconds
   * @return token details
   */
  TokenDetails createTemporaryToken(String username,
                                    String path,
                                    Map<String, List<String>> queryParams,
                                    long durationMillis);

  /**
   * Validate the token, and return details about the token.
   *
   * @param token session token
   * @return token details
   * @throws IllegalArgumentException if the token is invalid or expired
   */
  TokenDetails validateToken(String token) throws IllegalArgumentException;

  /**
   * Validate the temporary token, and return details about the token.
   *
   * @param token temporary token
   * @param path incoming REST API url path
   * @param queryParams incoming REST API url query parameters
   * @return token details
   * @throws IllegalArgumentException if the token is invalid or expired
   */
  TokenDetails validateTemporaryToken(String token,
                                      String path,
                                      Map<String, List<String>> queryParams);

  /**
   * Invalidate the token.
   *
   * @param token session token
   */
  void invalidateToken(String token);

}
