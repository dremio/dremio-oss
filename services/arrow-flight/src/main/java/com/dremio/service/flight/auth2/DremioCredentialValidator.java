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
package com.dremio.service.flight.auth2;

import javax.inject.Provider;

import org.apache.arrow.flight.auth2.BasicCallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator.AuthResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.service.flight.utils.DremioFlightAuthUtils;
import com.dremio.service.users.UserService;

/**
 * Dremio authentication specialized CredentialValidator implementation.
 */
public class DremioCredentialValidator implements BasicCallHeaderAuthenticator.CredentialValidator {
  private static final Logger LOGGER = LoggerFactory.getLogger(DremioCredentialValidator.class);

  private final Provider<UserService> userServiceProvider;

  DremioCredentialValidator(Provider<UserService> userServiceProvider) {
    this.userServiceProvider = userServiceProvider;
  }

  /**
   * Authenticates against Dremio with the provided username and password.
   *
   * @param username Dremio username.
   * @param password Dremio user password.
   * @return AuthResult with username as the peer identity.
   */
  @Override
  public AuthResult validate(String username, String password) {
    DremioFlightAuthUtils.authenticateCredentials(userServiceProvider, username, password, LOGGER);
    return () -> username;
  }
}
