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

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.ErrorFlightMetadata;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator.AuthResult;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.dremio.service.flight.BasicFlightAuthenticationTest;
import com.dremio.service.users.UserLoginException;

/**
 * Unit tests for DremioCredentialValidator.
 */
public class TestDremioCredentialValidator extends BasicFlightAuthenticationTest {
  private DremioCredentialValidator credentialValidator;

  @Before
  @Override
  public void setup() {
    super.setup();
    credentialValidator = new DremioCredentialValidator(getMockUserServiceProvider());
  }

  @After
  public void tearDown() throws Exception {
    credentialValidator = null;
  }

  @Test
  public void testAuthenticateWithValidCredentials() {
    // Act
    AuthResult actual = credentialValidator.validate(USERNAME, PASSWORD);

    // Validate - Check no-op CallHeaders
    CallHeaders testCallHeaders = new ErrorFlightMetadata();
    actual.appendToOutgoingHeaders(testCallHeaders);

    // Validate - Check peer identity and CallHeader
    assertEquals(USERNAME, actual.getPeerIdentity());
    assertEquals(0, testCallHeaders.keys().size());
  }

  @Test
  public void testAuthenticateWithInvalidCredentials() throws Exception {
    // Arrange
    final String badUsername = "BadUserName";
    final String badPassword = "BadPassword";
    doThrow(new UserLoginException(badUsername, "Invalid User credentials")).when(getMockUserService())
      .authenticate(eq(badUsername), eq(badPassword));

   // Act
    try {
      credentialValidator.validate("BadUserName", "BadPassword");
      testFailed();
    } catch (FlightRuntimeException exception) {
      assertEquals(FlightStatusCode.UNAUTHENTICATED, exception.status().code());
    }
  }
}
