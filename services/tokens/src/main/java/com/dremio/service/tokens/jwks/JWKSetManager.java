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

import com.dremio.service.Service;
import com.dremio.service.tokens.jwt.JWTSigner;
import com.dremio.service.tokens.jwt.JWTValidator;
import java.util.Map;

/**
 * This class manages the JSON Web Key Set (JWKS) used to sign and validate Dremio JWTs, allows
 * callers to retrieve the public keys to be exposed on Dremio's JWKS endpoint, as well as sign and
 * validate Dremio JWTs.
 */
public interface JWKSetManager extends Service {
  /**
   * @return A signer that can be used to sign Dremio JWTs.
   */
  JWTSigner getSigner();

  /**
   * @return A validator that can be used to validate signed Dremio JWTs.
   */
  JWTValidator getValidator();

  /**
   * @return Dremio's current public JWKS.
   */
  Map<String, Object> getPublicJWKS();
}
