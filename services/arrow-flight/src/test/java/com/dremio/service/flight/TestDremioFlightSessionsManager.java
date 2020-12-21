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

import static com.dremio.service.flight.DremioFlightServiceOptions.SESSION_EXPIRATION_TIME_MINUTES;
import static com.dremio.service.flight.DremioFlightSessionsManager.MAX_SESSIONS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import javax.inject.Provider;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.ErrorFlightMetadata;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserProtos;
import com.dremio.exec.server.SabotContext;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValidatorListing;
import com.dremio.options.OptionValue;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.flight.client.properties.DremioFlightClientProperties;
import com.dremio.service.tokens.TokenManager;

/**
 * Unit tests for DremioFlightSessionsManager
 */
public class TestDremioFlightSessionsManager {
  private static final String TOKEN1 = "TOKEN_1";
  private static final String TOKEN2 = "TOKEN_2";
  private static final String USERNAME1 = "MY_USER1";
  private static final String USERNAME2 = "MY_USER2";

  private static final boolean DEFAULT_SUPPORT_COMPLEX_TYPES = true;
  private static final String DEFAULT_RPC_ENDPOINT_INFO_NAME = "Arrow Flight";

  private static final UserSession USER1_SESSION = UserSession.Builder.newBuilder()
    .withCredentials(UserBitShared.UserCredentials.newBuilder().setUserName(USERNAME1).build())
    .build();
  private static final UserSession USER2_SESSION = UserSession.Builder.newBuilder()
    .withCredentials(UserBitShared.UserCredentials.newBuilder().setUserName(USERNAME2).build())
    .build();

  private static final long TOKEN_EXPIRATION_MINS = 60L;
  private static final long MAX_NUMBER_OF_SESSIONS = 2L;

  private DremioFlightSessionsManager dremioFlightSessionsManager;
  private DremioFlightSessionsManager spyDremioFlightSessionsManager;
  private final OptionManager mockOptionManager = mock(OptionManager.class);
  private final TokenManager mockTokenManager = mock(TokenManager.class);

  @Before
  public void setup() {
    final Provider<SabotContext> mockSabotContextProvider = mock(Provider.class);
    final SabotContext mockSabotContext = mock(SabotContext.class);

    final Provider<TokenManager> mockTokenManagerProvider = mock(Provider.class);

    when(mockSabotContextProvider.get()).thenReturn(mockSabotContext);
    when(mockSabotContextProvider.get().getOptionManager()).thenReturn(mockOptionManager);
    when(mockSabotContextProvider.get().getOptionManager().getOption(SESSION_EXPIRATION_TIME_MINUTES))
      .thenReturn(TOKEN_EXPIRATION_MINS);
    when(mockTokenManagerProvider.get()).thenReturn(mockTokenManager);
    when(mockSabotContextProvider.get().getOptionValidatorListing())
      .thenReturn(mock(OptionValidatorListing.class));
    when(mockOptionManager.getOption("client.max_metadata_count"))
      .thenReturn(OptionValue.createLong(OptionValue.OptionType.SESSION, "dummy", 0L));
    when(mockOptionManager.getOption(MAX_SESSIONS)).thenReturn(MAX_NUMBER_OF_SESSIONS);
    dremioFlightSessionsManager =
      new DremioFlightSessionsManager(mockSabotContextProvider, mockTokenManagerProvider);
    spyDremioFlightSessionsManager = spy(dremioFlightSessionsManager);

    doReturn(USER1_SESSION)
      .when(spyDremioFlightSessionsManager).buildUserSession(USERNAME1, null);

    doReturn(USER2_SESSION)
      .when(spyDremioFlightSessionsManager).buildUserSession(USERNAME2, null);
  }

  @After
  public void tearDown() throws Exception {
    AutoCloseables.close(dremioFlightSessionsManager, spyDremioFlightSessionsManager);
    dremioFlightSessionsManager = null;
    spyDremioFlightSessionsManager = null;
  }

  @Test
  public void creatingUserSessionsAddsToUserSessionCache() {
    // Arrange
    final long expectedSize = 2L;

    // Act
    spyDremioFlightSessionsManager.createUserSession(TOKEN1, USERNAME1, null);
    spyDremioFlightSessionsManager.createUserSession(TOKEN2, USERNAME2, null);
    final long actualSize = spyDremioFlightSessionsManager.getNumberOfUserSessions();

    // Assert
    assertEquals(expectedSize, actualSize);
  }

  @Test
  public void getMaxSessionsReturnsValueOfTheSupportKey() {
    // Arrange
    when(mockOptionManager.getOption(MAX_SESSIONS)).thenReturn(MAX_NUMBER_OF_SESSIONS);
    final long expectedValue = MAX_NUMBER_OF_SESSIONS;

    // Act
    final long actualValue = spyDremioFlightSessionsManager.getMaxSessions();

    // Assert
    assertEquals(expectedValue, actualValue);
  }

  @Test
  public void reachedMaxNumberOfSessionsReturnsFalseWhenMaxSessionsNotReached() {
    // Arrange
    spyDremioFlightSessionsManager.createUserSession(TOKEN1, USERNAME1, null);

    // Act
    final boolean actual = spyDremioFlightSessionsManager.reachedMaxNumberOfSessions();

    // Assert
    Assert.assertFalse(actual);
  }

  @Test
  public void reachedMaxNumberOfSessionsReturnsTrueWhenMaxSessionsAreReached() {
    // Arrange
    spyDremioFlightSessionsManager.createUserSession(TOKEN1, USERNAME1, null);
    spyDremioFlightSessionsManager.createUserSession(TOKEN2, USERNAME2, null);

    // Act
    final boolean actual = spyDremioFlightSessionsManager.reachedMaxNumberOfSessions();

    // Assert
    assertTrue(actual);
  }

  @Test
  public void resolveDistinctSessions() {
    // Arrange
    spyDremioFlightSessionsManager.createUserSession(TOKEN1, USERNAME1, null);
    spyDremioFlightSessionsManager.createUserSession(TOKEN2, USERNAME2, null);

    // Act
    final UserSession actualUser1 = spyDremioFlightSessionsManager.getUserSession(TOKEN1, null);
    final UserSession actualUser2 = spyDremioFlightSessionsManager.getUserSession(TOKEN2, null);

    // Assert
    assertEquals(USER1_SESSION, actualUser1);
    assertEquals(USER2_SESSION, actualUser2);
  }

  @Test
  public void invalidateTokenAfterExpiration() throws Exception {
    // Arrange
    doAnswer(invocationOnMock -> {
      assertEquals(TOKEN1, invocationOnMock.getArguments()[0]);
      return null;
    }).when(mockTokenManager).invalidateToken(TOKEN1);

    // Test
    try {
      spyDremioFlightSessionsManager.getUserSession(TOKEN1, null);
      throw new Exception("Test failed. Expected FlightRuntimeException.");
    } catch (FlightRuntimeException ex) {
      assertEquals(FlightStatusCode.UNAUTHENTICATED, ex.status().code());
    } finally {
      verify(mockTokenManager, times(1)).invalidateToken(TOKEN1);
    }
  }

  @Test
  public void buildUserSessionWithClientProperties() {
    // Arrange
    final String username = "tempUser";
    final String testSchema = "test.catalog.table";
    final String testRoutingTag = "test-tag";
    final String testRoutingQueue = "test-queue-name";

    final CallHeaders callheaders = new ErrorFlightMetadata();
    callheaders.insert(UserSession.SCHEMA, testSchema);
    callheaders.insert(UserSession.ROUTING_TAG, testRoutingTag);
    callheaders.insert(UserSession.ROUTING_QUEUE, testRoutingQueue);

    // Act
    final UserSession actual = spyDremioFlightSessionsManager.buildUserSession(username, callheaders);

    // Verify
    assertEquals(testSchema, String.join(".", actual.getDefaultSchemaPath().getPathComponents()));
    assertEquals(testRoutingTag, actual.getRoutingTag());
    assertEquals(testRoutingQueue, actual.getRoutingQueue());
    assertEquals(DEFAULT_RPC_ENDPOINT_INFO_NAME, actual.getClientInfos().getName());
    assertTrue(DEFAULT_SUPPORT_COMPLEX_TYPES);
  }

  @Test
  public void buildUserSessionWithNullCallHeaders() {
    final UserSession actual = spyDremioFlightSessionsManager.buildUserSession("tempUser", null);

    assertNull(actual.getDefaultSchemaPath());
    assertNull(actual.getRoutingTag());
    assertNull(actual.getRoutingQueue());
    assertEquals(DEFAULT_RPC_ENDPOINT_INFO_NAME, actual.getClientInfos().getName());
    assertTrue(DEFAULT_SUPPORT_COMPLEX_TYPES);
  }

  @Test
  public void getUserSessionWithClientProperties() {
    // Arrange
    final String testUser = "tempUser";
    final String testSchema = "test.catalog.table";
    final String testRoutingTag = "test-tag";
    final String testRoutingQueue = "test-queue-name";
    final UserSession userSession = UserSession.Builder.newBuilder()
      .withUserProperties(
        UserProtos.UserProperties.newBuilder()
          .addProperties(DremioFlightClientProperties.createUserProperty(UserSession.SCHEMA, testSchema))
          .addProperties(DremioFlightClientProperties.createUserProperty(UserSession.ROUTING_TAG, testRoutingTag))
          .addProperties(DremioFlightClientProperties.createUserProperty(UserSession.ROUTING_QUEUE, testRoutingQueue))
          .build())
      .build();

    doReturn(userSession)
      .when(spyDremioFlightSessionsManager).buildUserSession(testUser, null);

    final String testPeerIdentity = "tempToken";
    final String newSchema = "new.catalog.table";
    final CallHeaders callHeaders = new ErrorFlightMetadata();
    callHeaders.insert(UserSession.SCHEMA, newSchema);

    // Act
    spyDremioFlightSessionsManager.createUserSession(testPeerIdentity, testUser, null);
    final UserSession actual = spyDremioFlightSessionsManager.getUserSession(testPeerIdentity, callHeaders);

    // Verify
    assertEquals(newSchema, String.join(".", actual.getDefaultSchemaPath().getPathComponents()));
    assertEquals(testRoutingTag, actual.getRoutingTag());
    assertEquals(testRoutingQueue, actual.getRoutingQueue());
  }

  @Test
  public void getUserSessionWithNullClientProperties() {
    // Arrange
    final String testUser = "tempUser";
    final String testSchema = "test.catalog.table";
    final String testRoutingTag = "test-tag";
    final String testRoutingQueue = "test-queue-name";
    final UserSession userSession = UserSession.Builder.newBuilder()
      .withUserProperties(
        UserProtos.UserProperties.newBuilder()
          .addProperties(DremioFlightClientProperties.createUserProperty(UserSession.SCHEMA, testSchema))
          .addProperties(DremioFlightClientProperties.createUserProperty(UserSession.ROUTING_TAG, testRoutingTag))
          .addProperties(DremioFlightClientProperties.createUserProperty(UserSession.ROUTING_QUEUE, testRoutingQueue))
          .build())
      .build();

    doReturn(userSession)
      .when(spyDremioFlightSessionsManager).buildUserSession(testUser, null);

    final String testPeerIdentity = "tempToken";

    // Act
    spyDremioFlightSessionsManager.createUserSession(testPeerIdentity, testUser, null);
    final UserSession actual = spyDremioFlightSessionsManager.getUserSession(testPeerIdentity, null);

    // Verify
    assertEquals(testSchema, String.join(".", actual.getDefaultSchemaPath().getPathComponents()));
    assertEquals(testRoutingTag, actual.getRoutingTag());
    assertEquals(testRoutingQueue, actual.getRoutingQueue());
  }
}
