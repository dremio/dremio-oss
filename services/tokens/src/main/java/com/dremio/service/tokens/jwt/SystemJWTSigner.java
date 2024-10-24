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
package com.dremio.service.tokens.jwt;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JOSEObjectType;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

/** Signs JWTs using the given signing key available on the local system. */
public class SystemJWTSigner implements JWTSigner {
  private final JWSSigner signer;
  private final String signingKeyId;

  public SystemJWTSigner(JWSSigner signer, String signingKeyId) {
    this.signer = signer;
    this.signingKeyId = signingKeyId;
  }

  @Override
  public String sign(JWTClaims jwt) {
    final JWTClaimsSet claimsSet =
        new JWTClaimsSet.Builder()
            .issuer(jwt.getIssuer())
            .subject(jwt.getSubject())
            .audience(jwt.getAudience())
            .expirationTime(jwt.getExpirationTime())
            .notBeforeTime(jwt.getNotBeforeTime())
            .issueTime(jwt.getIssueTime())
            .jwtID(jwt.getJWTId())
            .build();

    final SignedJWT signedJWT =
        new SignedJWT(
            new JWSHeader.Builder(JWSAlgorithm.ES256)
                .keyID(signingKeyId)
                .type(JOSEObjectType.JWT)
                .build(),
            claimsSet);

    try {
      signedJWT.sign(signer);
    } catch (JOSEException e) {
      throw new RuntimeException("Failed to sign JWT", e);
    }

    return signedJWT.serialize();
  }
}
