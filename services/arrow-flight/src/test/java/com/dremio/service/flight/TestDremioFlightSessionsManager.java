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

import static com.dremio.service.flight.DremioFlightSessionsManager.MAX_SESSIONS;
import static com.dremio.service.tokens.TokenManagerImpl.TOKEN_EXPIRATION_TIME_MINUTES;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import javax.inject.Provider;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.server.SabotContext;
import com.dremio.options.OptionManager;
import com.dremio.sabot.rpc.user.UserSession;

/**
 * Unit tests for DremioFlightSessionsManager
 */
public class TestDremioFlightSessionsManager {

  private static final String PEERID1 = "PEER_ID_1";
  private static final String PEERID2 = "PEER_ID_2";
  private static final String USERNAME1 = "MY_USER1";
  private static final String USERNAME2 = "MY_USER2";

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

  @Before
  public void setup() {
    final Provider<SabotContext> mockSabotContextProvider = mock(Provider.class);
    final SabotContext mockSabotContext = mock(SabotContext.class);

    when(mockSabotContextProvider.get()).thenReturn(mockSabotContext);
    when(mockSabotContextProvider.get().getOptionManager()).thenReturn(mockOptionManager);
    when(mockSabotContextProvider.get().getOptionManager().getOption(TOKEN_EXPIRATION_TIME_MINUTES))
      .thenReturn(TOKEN_EXPIRATION_MINS);
    when(mockOptionManager.getOption(MAX_SESSIONS)).thenReturn(MAX_NUMBER_OF_SESSIONS);
    dremioFlightSessionsManager = new DremioFlightSessionsManager(mockSabotContextProvider);
    spyDremioFlightSessionsManager = spy(dremioFlightSessionsManager);

    doReturn(USER1_SESSION)
      .when(spyDremioFlightSessionsManager).buildUserSession(USERNAME1);

    doReturn(USER2_SESSION)
      .when(spyDremioFlightSessionsManager).buildUserSession(USERNAME2);
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
    spyDremioFlightSessionsManager.createUserSession(PEERID1, USERNAME1);
    spyDremioFlightSessionsManager.createUserSession(PEERID2, USERNAME2);
    final long actualSize = spyDremioFlightSessionsManager.getNumberOfUserSessions();

    // Assert
    Assert.assertEquals(expectedSize, actualSize);
  }

  @Test
  public void getMaxSessionsReturnsValueOfTheSupportKey() {
    // Arrange
    when(mockOptionManager.getOption(MAX_SESSIONS)).thenReturn(MAX_NUMBER_OF_SESSIONS);
    final long expectedValue = MAX_NUMBER_OF_SESSIONS;

    // Act
    final long actualValue = spyDremioFlightSessionsManager.getMaxSessions();

    // Assert
    Assert.assertEquals(expectedValue, actualValue);
  }

  @Test
  public void reachedMaxNumberOfSessionsReturnsFalseWhenMaxSessionsNotReached() {
    // Arrange
    spyDremioFlightSessionsManager.createUserSession(PEERID1, USERNAME1);

    // Act
    final boolean actual = spyDremioFlightSessionsManager.reachedMaxNumberOfSessions();

    // Assert
    Assert.assertFalse(actual);
  }

  @Test
  public void reachedMaxNumberOfSessionsReturnsTrueWhenMaxSessionsAreReached() {
    // Arrange
    spyDremioFlightSessionsManager.createUserSession(PEERID1, USERNAME1);
    spyDremioFlightSessionsManager.createUserSession(PEERID2, USERNAME2);

    // Act
    final boolean actual = spyDremioFlightSessionsManager.reachedMaxNumberOfSessions();

    // Assert
    Assert.assertTrue(actual);
  }

  @Test
  public void resolveDistinctSessions() {
    // Arrange
    spyDremioFlightSessionsManager.createUserSession(PEERID1, USERNAME1);
    spyDremioFlightSessionsManager.createUserSession(PEERID2, USERNAME2);

    // Act
    final UserSession actualUser1 = spyDremioFlightSessionsManager.getUserSession(PEERID1);
    final UserSession actualUser2 = spyDremioFlightSessionsManager.getUserSession(PEERID2);

    // Assert
    Assert.assertEquals(USER1_SESSION, actualUser1);
    Assert.assertEquals(USER2_SESSION, actualUser2);
  }
}
