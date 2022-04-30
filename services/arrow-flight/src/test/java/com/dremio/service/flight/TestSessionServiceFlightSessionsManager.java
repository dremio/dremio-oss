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
import static com.dremio.service.flight.SessionServiceFlightSessionsManager.COOKIE_HEADER;
import static com.dremio.service.flight.SessionServiceFlightSessionsManager.MAX_SESSIONS;
import static com.dremio.service.flight.SessionServiceFlightSessionsManager.SESSION_ID_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import javax.inject.Provider;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.ErrorFlightMetadata;
import org.apache.arrow.flight.FlightCallHeaders;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.dremio.common.AutoCloseables;
import com.dremio.datastore.api.options.ImmutableVersionOption;
import com.dremio.datastore.api.options.VersionOption;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserProtos;
import com.dremio.exec.server.SabotContext;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValidatorListing;
import com.dremio.options.OptionValue;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.flight.client.properties.DremioFlightClientProperties;
import com.dremio.service.tokens.TokenDetails;
import com.dremio.service.tokens.TokenManager;
import com.dremio.service.usersessions.UserSessionService;

/**
 * Unit tests for DremioFlightSessionsManager
 */
public class TestSessionServiceFlightSessionsManager {
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

  private SessionServiceFlightSessionsManager sessionsManager;
  private SessionServiceFlightSessionsManager spySessionsManager;
  private UserSessionService mockUserSessionService;
  private final OptionManager mockOptionManager = mock(OptionManager.class);
  private final TokenManager mockTokenManager = mock(TokenManager.class);

  @Before
  public void setup() throws Exception {
    final Provider<SabotContext> mockSabotContextProvider = mock(Provider.class);
    final SabotContext mockSabotContext = mock(SabotContext.class);
    mockUserSessionService = mock(UserSessionService.class);

    final Provider<TokenManager> mockTokenManagerProvider = mock(Provider.class);
    final Provider<UserSessionService> mockUserSessionServiceProvider = mock(Provider.class);

    when(mockSabotContextProvider.get()).thenReturn(mockSabotContext);
    when(mockSabotContextProvider.get().getOptionManager()).thenReturn(mockOptionManager);
    when(mockSabotContextProvider.get().getOptionManager().getOption(SESSION_EXPIRATION_TIME_MINUTES))
      .thenReturn(TOKEN_EXPIRATION_MINS);
    when(mockTokenManagerProvider.get()).thenReturn(mockTokenManager);
    when(mockUserSessionServiceProvider.get()).thenReturn(mockUserSessionService);
    when(mockSabotContextProvider.get().getOptionValidatorListing())
      .thenReturn(mock(OptionValidatorListing.class));
    when(mockOptionManager.getOption("client.max_metadata_count"))
      .thenReturn(OptionValue.createLong(OptionValue.OptionType.SESSION, "dummy", 0L));
    when(mockOptionManager.getOption(MAX_SESSIONS)).thenReturn(MAX_NUMBER_OF_SESSIONS);
    sessionsManager =
      new SessionServiceFlightSessionsManager(mockSabotContextProvider, mockTokenManagerProvider, mockUserSessionServiceProvider);
    spySessionsManager = spy(sessionsManager);

    doReturn(USER1_SESSION)
      .when(spySessionsManager).buildUserSession(USERNAME1, null);

    doReturn(USER2_SESSION)
      .when(spySessionsManager).buildUserSession(USERNAME2, null);
  }

  @After
  public void tearDown() throws Exception {
    AutoCloseables.close(sessionsManager, spySessionsManager);
    sessionsManager = null;
    spySessionsManager = null;
  }

  @Test
  public void resolveDistinctSessions() throws Exception {
    // Arrange
    final String sessionId1 = "1";
    final String sessionId2 = "2";
    final CallHeaders headers1 = new FlightCallHeaders();
    final CallHeaders headers2 = new FlightCallHeaders();
    final VersionOption version = new ImmutableVersionOption.Builder().setTag("version").build();

    doReturn(TokenDetails.of(TOKEN1, USERNAME1, 100))
      .when(mockTokenManager).validateToken(TOKEN1);
    doReturn(TokenDetails.of(TOKEN2, USERNAME2, 100))
      .when(mockTokenManager).validateToken(TOKEN2);

    doReturn(new UserSessionService.SessionIdAndVersion(sessionId1, version))
      .when(mockUserSessionService).putSession(USER1_SESSION);
    doReturn(new UserSessionService.SessionIdAndVersion(sessionId2, version))
      .when(mockUserSessionService).putSession(USER2_SESSION);

    spySessionsManager.createUserSession(TOKEN1, null);
    spySessionsManager.createUserSession(TOKEN2, null);

    headers1.insert(COOKIE_HEADER, String.format("%s=%s", SESSION_ID_KEY, sessionId1));
    headers2.insert(COOKIE_HEADER, String.format("%s=%s", SESSION_ID_KEY, sessionId2));

    doReturn(new UserSessionService.UserSessionAndVersion(USER1_SESSION, version))
      .when(mockUserSessionService).getSession(sessionId1);
    doReturn(new UserSessionService.UserSessionAndVersion(USER2_SESSION, version))
      .when(mockUserSessionService).getSession(sessionId2);

    // Act
    final UserSession actualUser1 = spySessionsManager.getUserSession(TOKEN1, headers1).getSession();
    final UserSession actualUser2 = spySessionsManager.getUserSession(TOKEN2, headers2).getSession();

    // Assert
    assertEquals(USER1_SESSION, actualUser1);
    assertEquals(USER2_SESSION, actualUser2);
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
    final UserSession actual = spySessionsManager.buildUserSession(username, callheaders);

    // Verify
    assertEquals(testSchema, String.join(".", actual.getDefaultSchemaPath().getPathComponents()));
    assertEquals(testRoutingTag, actual.getRoutingTag());
    assertEquals(testRoutingQueue, actual.getRoutingQueue());
    assertEquals(DEFAULT_RPC_ENDPOINT_INFO_NAME, actual.getClientInfos().getName());
    assertTrue(DEFAULT_SUPPORT_COMPLEX_TYPES);
  }

  @Test
  public void buildUserSessionWithNullCallHeaders() {
    final UserSession actual = spySessionsManager.buildUserSession("tempUser", null);

    assertNull(actual.getDefaultSchemaPath());
    assertNull(actual.getRoutingTag());
    assertNull(actual.getRoutingQueue());
    assertEquals(DEFAULT_RPC_ENDPOINT_INFO_NAME, actual.getClientInfos().getName());
    assertTrue(DEFAULT_SUPPORT_COMPLEX_TYPES);
  }

  @Test
  public void getUserSessionWithClientProperties() throws Exception {
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

    final String testPeerIdentity = "tempToken";
    final String newSchema = "new.catalog.table";

    final String sessionId = "sessionId";
    final VersionOption version = new ImmutableVersionOption.Builder().setTag("version").build();
    final CallHeaders incomingCallHeaders = new FlightCallHeaders();
    incomingCallHeaders.insert(COOKIE_HEADER, String.format("%s=%s", SESSION_ID_KEY, sessionId));
    incomingCallHeaders.insert(UserSession.SCHEMA, newSchema);

    doReturn(TokenDetails.of(testPeerIdentity, testUser, 100))
      .when(mockTokenManager).validateToken(testPeerIdentity);
    doReturn(userSession)
      .when(spySessionsManager).buildUserSession(testUser, incomingCallHeaders);
    doReturn(new UserSessionService.SessionIdAndVersion(sessionId, version))
      .when(mockUserSessionService).putSession(userSession);
    doReturn(new UserSessionService.UserSessionAndVersion(userSession, version))
      .when(mockUserSessionService).getSession(sessionId);

    // Act
    spySessionsManager.createUserSession(testPeerIdentity, incomingCallHeaders);
    final UserSession actual = spySessionsManager.getUserSession(testPeerIdentity, incomingCallHeaders).getSession();

    // Verify
    assertEquals(newSchema, String.join(".", actual.getDefaultSchemaPath().getPathComponents()));
    assertEquals(testRoutingTag, actual.getRoutingTag());
    assertEquals(testRoutingQueue, actual.getRoutingQueue());
  }

  @Test
  public void getUserSessionWithNullClientProperties() throws Exception {
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


    final String testPeerIdentity = "tempToken";
    final String sessionId = "sessionId";
    final VersionOption version = new ImmutableVersionOption.Builder().setTag("version").build();
    final CallHeaders incomingCallHeaders = new FlightCallHeaders();
    incomingCallHeaders.insert(COOKIE_HEADER, String.format("%s=%s", SESSION_ID_KEY, sessionId));

    doReturn(userSession)
      .when(spySessionsManager).buildUserSession(testUser, incomingCallHeaders);

    doReturn(TokenDetails.of(testPeerIdentity, testUser, 100))
      .when(mockTokenManager).validateToken(testPeerIdentity);

    doReturn(new UserSessionService.SessionIdAndVersion(sessionId, version))
      .when(mockUserSessionService).putSession(userSession);

    doReturn(new UserSessionService.UserSessionAndVersion(userSession, version))
      .when(mockUserSessionService).getSession(sessionId);

    // Act
    spySessionsManager.createUserSession(testPeerIdentity, incomingCallHeaders);
    final UserSession actual = spySessionsManager.getUserSession(testPeerIdentity, incomingCallHeaders).getSession();

    // Verify
    assertEquals(testSchema, String.join(".", actual.getDefaultSchemaPath().getPathComponents()));
    assertEquals(testRoutingTag, actual.getRoutingTag());
    assertEquals(testRoutingQueue, actual.getRoutingQueue());
  }
}
