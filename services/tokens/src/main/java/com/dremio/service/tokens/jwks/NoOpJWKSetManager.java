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
package com.dremio.service.tokens.jwks;

import com.dremio.service.tokens.jwt.JWTSigner;
import com.dremio.service.tokens.jwt.JWTValidator;
import java.util.Map;

public class NoOpJWKSetManager implements JWKSetManager {
  @Override
  public void start() throws Exception {}

  @Override
  public void close() throws Exception {}

  @Override
  public JWTSigner getSigner() {
    throw new UnsupportedOperationException(
        "NoOpJWKSetManager does not support retrieving a JWTSigner");
  }

  @Override
  public JWTValidator getValidator() {
    throw new UnsupportedOperationException(
        "NoOpJWKSetManager does not support retrieving a JWTValidator");
  }

  @Override
  public Map<String, Object> getPublicJWKS() {
    throw new UnsupportedOperationException(
        "NoOpJWKSetManager does not support retrieving the public JWKS");
  }
}
