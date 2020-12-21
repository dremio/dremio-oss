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
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.ErrorFlightMetadata;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.auth2.Auth2Constants;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator.AuthResult;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.dremio.service.flight.BasicFlightAuthenticationTest;

/**
 * Unit tests for DremioFlightServerBearerTokenAuthenticator.
 */
public class TestDremioBearerTokenAuthenticator extends BasicFlightAuthenticationTest {
  private DremioBearerTokenAuthenticator bearerTokenAuthenticator;

  @Before
  @Override
  public void setup() {
    super.setup();
    bearerTokenAuthenticator = new DremioBearerTokenAuthenticator(
      getMockUserServiceProvider(),
      getMockTokenManagerProvider(),
      getMockDremioFlightSessionsManager());
  }

  @After
  public void tearDown() throws Exception {
    bearerTokenAuthenticator = null;
  }

  @Test
  public void testGetAuthResultWithInitialAuth() {
    // Act
    final CallHeaders incomingTestHeaders = new ErrorFlightMetadata();
    incomingTestHeaders.insert(Auth2Constants.AUTHORIZATION_HEADER, Auth2Constants.BASIC_PREFIX +
      Base64.getEncoder().encodeToString(String.format("%s:%s", USERNAME, PASSWORD).getBytes(StandardCharsets.UTF_8)));
    final AuthResult bearerTokenAuthResult = bearerTokenAuthenticator.authenticate(incomingTestHeaders);
    final CallHeaders bearerTokenHeader = new ErrorFlightMetadata();
    bearerTokenAuthResult.appendToOutgoingHeaders(bearerTokenHeader);

    // Validate
    assertEquals(TOKEN, bearerTokenAuthResult.getPeerIdentity());
    assertTrue(bearerTokenHeader.containsKey(Auth2Constants.AUTHORIZATION_HEADER));
    assertEquals(Auth2Constants.BEARER_PREFIX + TOKEN,
      bearerTokenHeader.get(Auth2Constants.AUTHORIZATION_HEADER));
  }

  @Test
  public void testGetAuthResultWithBearerToken() {
    // Arrange
    when(getMockTokenManager().validateToken(eq(TOKEN))).thenReturn(TOKEN_DETAILS);

    // Act
    final CallHeaders incomingTestHeaders = new ErrorFlightMetadata();
    incomingTestHeaders.insert(Auth2Constants.AUTHORIZATION_HEADER,
      Auth2Constants.BEARER_PREFIX + TOKEN);
    final AuthResult bearerTokenAuthResult = bearerTokenAuthenticator.authenticate(incomingTestHeaders);
    final CallHeaders bearerTokenHeader = new ErrorFlightMetadata();
    bearerTokenAuthResult.appendToOutgoingHeaders(bearerTokenHeader);

    // Validate
    assertEquals(TOKEN, bearerTokenAuthResult.getPeerIdentity());
    assertTrue(bearerTokenHeader.containsKey(Auth2Constants.AUTHORIZATION_HEADER));
    assertEquals(Auth2Constants.BEARER_PREFIX + TOKEN,
      bearerTokenHeader.get(Auth2Constants.AUTHORIZATION_HEADER));
  }

  @Test
  public void testAuthenticateWithTooManyUserSessions() throws Exception {
    // Arrange
    when(getMockDremioFlightSessionsManager().reachedMaxNumberOfSessions()).thenReturn(Boolean.TRUE);

    // Act
    try {
      final CallHeaders incomingTestHeaders = new ErrorFlightMetadata();
      incomingTestHeaders.insert(Auth2Constants.AUTHORIZATION_HEADER, Auth2Constants.BASIC_PREFIX +
        Base64.getEncoder().encodeToString(String.format("%s:%s", USERNAME, PASSWORD).getBytes(StandardCharsets.UTF_8)));
      bearerTokenAuthenticator.authenticate(incomingTestHeaders);
      testFailed();
    } catch (FlightRuntimeException exception) {
      assertEquals(FlightStatusCode.UNAUTHENTICATED, exception.status().code());
    }
  }

  @Test
  public void testValidateBearerWithValidToken() {
    // Arrange
    when(getMockTokenManager().validateToken(eq(TOKEN))).thenReturn(TOKEN_DETAILS);

    // Act
    final AuthResult actualAuthResult = bearerTokenAuthenticator.validateBearer(TOKEN);

    // Validate
    assertEquals(TOKEN, actualAuthResult.getPeerIdentity());
  }

  @Test
  public void testValidateBearerWithInvalidToken() throws Exception {
    // Arrange
    doThrow(IllegalArgumentException.class).when(getMockTokenManager()).validateToken(anyString());

    // Act
    try {
      bearerTokenAuthenticator.validateBearer(TOKEN);
      testFailed();
    } catch(FlightRuntimeException exception) {
      assertEquals(FlightStatusCode.UNAUTHENTICATED, exception.status().code());
    }
  }
}
