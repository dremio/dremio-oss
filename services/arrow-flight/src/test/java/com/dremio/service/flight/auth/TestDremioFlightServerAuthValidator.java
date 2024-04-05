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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

import com.dremio.service.flight.BasicFlightAuthenticationTest;
import com.dremio.service.users.UserLoginException;
import java.util.Optional;
import org.apache.arrow.flight.FlightRuntimeException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.AdditionalMatchers;

/** Unit tests for DremioFlightServerAuthValidator */
public class TestDremioFlightServerAuthValidator extends BasicFlightAuthenticationTest {
  private DremioFlightServerBasicAuthValidator dremioFlightServerAuthValidator;

  @Before
  @Override
  public void setup() throws UserLoginException {
    super.setup();
    dremioFlightServerAuthValidator =
        new DremioFlightServerBasicAuthValidator(
            getMockUserServiceProvider(),
            getMockTokenManagerProvider(),
            getMockDremioFlightSessionsManager());
  }

  @After
  public void tearDown() throws Exception {
    dremioFlightServerAuthValidator = null;
  }

  @Test
  public void getTokenWithValidCredentials() throws Exception {
    // Arrange
    final byte[] expectedToken = TOKEN.getBytes(UTF_8);

    // Act
    final byte[] actualToken = dremioFlightServerAuthValidator.getToken(USERNAME, PASSWORD);

    // Assert
    Assert.assertArrayEquals(expectedToken, actualToken);
  }

  @Test
  public void getTokenWithInvalidCredentialsThrowsException() throws Exception {
    // Arrange
    doThrow(new UserLoginException(USERNAME, "Invalid User credentials"))
        .when(getMockUserService())
        .authenticate(eq(USERNAME), AdditionalMatchers.not(eq(PASSWORD)));

    // Act
    assertThatThrownBy(() -> dremioFlightServerAuthValidator.getToken(USERNAME, "INVALID_PASSWORD"))
        .isInstanceOf(FlightRuntimeException.class)
        .hasMessageContaining(
            "Unable to authenticate user "
                + USERNAME
                + ", exception: Invalid User credentials, user "
                + USERNAME);
  }

  @Test
  public void isValidWithValidTokenReturnsUserName() {
    // Arrange
    final byte[] token = TOKEN.getBytes(UTF_8);
    when(getMockTokenManager().validateToken(eq(TOKEN))).thenReturn(TOKEN_DETAILS);

    // Act
    final Optional<String> tokenAsString = dremioFlightServerAuthValidator.isValid(token);

    // Assert
    Assert.assertEquals(Optional.of(TOKEN), tokenAsString);
  }

  @Test
  public void isValidWithInvalidTokenReturnsEmptyOptionalString() {
    // Arrange
    final byte[] token = "INVALID_TOKEN".getBytes(UTF_8);
    final Optional<String> expectedResult = Optional.empty();
    doThrow(IllegalArgumentException.class).when(getMockTokenManager()).validateToken(anyString());

    // Act
    final Optional<String> actualResult = dremioFlightServerAuthValidator.isValid(token);

    // Assert
    Assert.assertEquals(expectedResult, actualResult);
  }
}
