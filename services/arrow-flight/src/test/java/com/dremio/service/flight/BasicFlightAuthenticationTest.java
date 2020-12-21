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
package com.dremio.service.flight;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import javax.inject.Provider;

import org.apache.arrow.flight.FlightStatusCode;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import com.dremio.service.tokens.TokenDetails;
import com.dremio.service.tokens.TokenManager;
import com.dremio.service.users.UserService;

/**
 * Base class for all Flight Authentication unit tests.
 */
public abstract class BasicFlightAuthenticationTest {
  protected static final String USERNAME = "MY_USER";
  protected static final String PASSWORD = "MY_PASS";
  protected static final String TOKEN = "VALID_TOKEN";
  protected static final long MAX_NUMBER_OF_SESSIONS = 2L;
  protected static final TokenDetails TOKEN_DETAILS = TokenDetails.of(
    TOKEN, USERNAME, System.currentTimeMillis() + 1000);

  private final Provider<UserService> mockUserServiceProvider = mock(Provider.class);
  private final Provider<TokenManager> mockTokenManagerProvider = mock(Provider.class);
  private final TokenManager mockTokenManager = mock(TokenManager.class);
  private final UserService mockUserService = mock(UserService.class);
  private final DremioFlightSessionsManager mockDremioFlightSessionsManager = mock(DremioFlightSessionsManager.class);

  protected static void testFailed() throws Exception {
    throw new Exception("Test failed. Expected FlightRuntimeException with status code: "
      + FlightStatusCode.UNAUTHENTICATED + ", but none was thrown.");
  }

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setup() {
    when(mockUserServiceProvider.get()).thenReturn(mockUserService);
    when(mockTokenManagerProvider.get()).thenReturn((mockTokenManager));
    when(mockTokenManager.createToken(eq(USERNAME), eq(null))).thenReturn(TOKEN_DETAILS);
    doReturn(MAX_NUMBER_OF_SESSIONS).when(mockDremioFlightSessionsManager).getMaxSessions();
  }

  public Provider<UserService> getMockUserServiceProvider() {
    return mockUserServiceProvider;
  }

  public Provider<TokenManager> getMockTokenManagerProvider() {
    return mockTokenManagerProvider;
  }

  public TokenManager getMockTokenManager() {
    return mockTokenManager;
  }

  public UserService getMockUserService() {
    return mockUserService;
  }

  public DremioFlightSessionsManager getMockDremioFlightSessionsManager() {
    return mockDremioFlightSessionsManager;
  }
}
