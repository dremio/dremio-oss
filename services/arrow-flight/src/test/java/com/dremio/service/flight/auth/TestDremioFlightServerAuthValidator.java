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
package com.dremio.service.flight.auth;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;

import javax.inject.Provider;

import org.apache.arrow.flight.FlightRuntimeException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.AdditionalMatchers;

import com.dremio.common.AutoCloseables;
import com.dremio.service.flight.DremioFlightSessionsManager;
import com.dremio.service.tokens.TokenDetails;
import com.dremio.service.tokens.TokenManager;
import com.dremio.service.users.UserLoginException;
import com.dremio.service.users.UserService;
import com.google.common.base.Charsets;

/**
 * Unit tests for DremioFlightServerAuthValidator
 */
public class TestDremioFlightServerAuthValidator {
  private static final String USERNAME = "MY_USER";
  private static final String PASSWORD = "MY_PASS";
  private static final String TOKEN = "VALID_TOKEN";
  private static final long MAX_NUMBER_OF_SESSIONS = 2L;
  private static final TokenDetails TOKEN_DETAILS = TokenDetails.of(
    TOKEN, USERNAME, System.currentTimeMillis() + 1000);

  private DremioFlightServerAuthValidator dremioFlightServerAuthValidator;
  private final TokenManager mockTokenManager = mock(TokenManager.class);
  private final UserService mockUserService = mock(UserService.class);
  private final DremioFlightSessionsManager mockDremioFlightSessionsManager = mock(DremioFlightSessionsManager.class);

  @Before
  public void setup() {
    final Provider<UserService> mockUserServiceProvider = mock(Provider.class);
    final Provider<TokenManager> mockTokenManagerProvider = mock(Provider.class);

    when(mockUserServiceProvider.get()).thenReturn(mockUserService);
    when(mockTokenManagerProvider.get()).thenReturn((mockTokenManager));
    when(mockTokenManager.createToken(eq(USERNAME), eq(null))).thenReturn(TOKEN_DETAILS);
    dremioFlightServerAuthValidator = new DremioFlightServerAuthValidator(
      mockUserServiceProvider, mockTokenManagerProvider, mockDremioFlightSessionsManager);
    doReturn(MAX_NUMBER_OF_SESSIONS).when(mockDremioFlightSessionsManager).getMaxSessions();
  }

  @After
  public void tearDown() throws Exception {
    AutoCloseables.close(mockDremioFlightSessionsManager);
    dremioFlightServerAuthValidator = null;
  }

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void getTokenWithValidCredentials() throws Exception {
    // Arrange
    final byte[] expectedToken = TOKEN.getBytes(Charsets.UTF_8);

    // Act
    final byte[] actualToken = dremioFlightServerAuthValidator.getToken(USERNAME, PASSWORD);

    // Assert
    Assert.assertArrayEquals(expectedToken, actualToken);
  }

  @Test
  public void getTokenWithInvalidCredentialsThrowsException() throws Exception {
    // Arrange
    thrown.expect(FlightRuntimeException.class);
    thrown.expectMessage("Unable to authenticate user " + USERNAME +
      ", exception: Invalid User credentials, user " + USERNAME);
    doThrow(new UserLoginException(USERNAME, "Invalid User credentials")).when(mockUserService)
      .authenticate(eq(USERNAME), AdditionalMatchers.not(eq(PASSWORD)));

    // Act
    dremioFlightServerAuthValidator.getToken(USERNAME, "INVALID_PASSWORD");
  }

  @Test
  public void getTokenWithMaxNumberOfSessionsThrowsException() throws Exception {
    // Arrange
    thrown.expect(FlightRuntimeException.class);
    thrown.expectMessage("Reached the maximum number of allowed sessions: " + MAX_NUMBER_OF_SESSIONS);
    when(mockDremioFlightSessionsManager.reachedMaxNumberOfSessions()).thenReturn(Boolean.TRUE);

    // Act
    dremioFlightServerAuthValidator.getToken(USERNAME, PASSWORD);
  }

  @Test
  public void isValidWithValidTokenReturnsUserName() {
    // Arrange
    final byte[] token = TOKEN.getBytes(Charsets.UTF_8);
    when(mockTokenManager.validateToken(eq(TOKEN))).thenReturn(TOKEN_DETAILS);

    // Act
    final Optional<String> tokenAsString = dremioFlightServerAuthValidator.isValid(token);

    // Assert
    Assert.assertEquals(Optional.of(TOKEN), tokenAsString);
  }

  @Test
  public void isValidWithInvalidTokenReturnsEmptyOptionalString() {
    // Arrange
    final byte[] token = "INVALID_TOKEN".getBytes(Charsets.UTF_8);
    final Optional<String> expectedResult = Optional.empty();
    doThrow(IllegalArgumentException.class).when(mockTokenManager).validateToken(anyString());

    // Act
    final Optional<String> actualResult = dremioFlightServerAuthValidator.isValid(token);

    // Assert
    Assert.assertEquals(expectedResult, actualResult);
  }
}
