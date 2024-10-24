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

import static com.google.common.base.Preconditions.checkArgument;

import com.dremio.service.tokens.TokenDetails;
import com.dremio.service.users.User;
import com.dremio.service.users.UserNotFoundException;
import com.dremio.service.users.UserResolver;
import com.dremio.service.users.proto.UID;
import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.proc.BadJOSEException;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.JWT;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.JWTParser;
import com.nimbusds.jwt.proc.JWTProcessor;
import java.text.ParseException;
import javax.inject.Provider;

public class JWTValidatorImpl implements JWTValidator {
  private final Provider<UserResolver> userResolverProvider;
  private final JWTProcessor<SecurityContext> jwtProcessor;

  public JWTValidatorImpl(
      Provider<UserResolver> userResolverProvider, JWTProcessor<SecurityContext> jwtProcessor) {
    this.userResolverProvider = userResolverProvider;
    this.jwtProcessor = jwtProcessor;
  }

  @Override
  public TokenDetails validate(String jwtString) throws ParseException {
    checkArgument(jwtString != null, "invalid token");
    final JWT jwt = JWTParser.parse(jwtString);

    final JWTClaimsSet jwtClaimsSet;
    try {
      jwtClaimsSet = jwtProcessor.process(jwt, null);
    } catch (JOSEException | BadJOSEException e) {
      throw new IllegalArgumentException("Invalid token", e);
    }

    final User user;
    try {
      user = userResolverProvider.get().getUser(new UID(jwtClaimsSet.getSubject()));
    } catch (UserNotFoundException e) {
      throw new IllegalArgumentException("Invalid token", e);
    }

    return TokenDetails.of(
        jwtString, user.getUserName(), jwtClaimsSet.getExpirationTime().getTime());
  }
}
