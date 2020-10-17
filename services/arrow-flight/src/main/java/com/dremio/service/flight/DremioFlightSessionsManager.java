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

import static com.dremio.service.tokens.TokenManagerImpl.TOKEN_EXPIRATION_TIME_MINUTES;

import java.util.concurrent.TimeUnit;

import javax.inject.Provider;

import org.apache.arrow.flight.CallStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserProtos;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.SessionOptionManagerImpl;
import com.dremio.options.OptionManager;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators;
import com.dremio.sabot.rpc.user.UserSession;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * Manages UserSession creation and UserSession cache.
 */
@Options
public class DremioFlightSessionsManager implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(DremioFlightSessionsManager.class);

  // 0 is valid value and is treated as no limit on the number of Sessions.
  public static final TypeValidators.PositiveLongValidator MAX_SESSIONS =
    new TypeValidators.PositiveLongValidator("flight.max.sessions", Long.MAX_VALUE, 0L);

  private final Cache<String, UserSession> userSessions;
  private final Provider<SabotContext> sabotContextProvider;
  private final OptionManager optionManager;

  public DremioFlightSessionsManager(Provider<SabotContext> sabotContextProvider) {
    this.sabotContextProvider = sabotContextProvider;
    this.optionManager = sabotContextProvider.get().getOptionManager();
    this.userSessions = CacheBuilder.newBuilder()
      .expireAfterWrite(optionManager.getOption(TOKEN_EXPIRATION_TIME_MINUTES), TimeUnit.MINUTES)
      .build();
  }

  public long getMaxSessions() {
    return optionManager.getOption(MAX_SESSIONS);
  }

  /**
   * Creates a UserSession object and store it in the local cache.
   *
   * @param peerIdentity The Identity used to reference this user session instance.
   * @param username The Username to build a UserSession object for.
   */
  public void createUserSession(String peerIdentity, String username) {
    userSessions.put(peerIdentity, buildUserSession(username));
  }

  /**
   * Resolves an existing UserSession for
   *
   * @param peerIdentity The identity of the user making a request.
   * @return The UserSession.
   */
  public UserSession getUserSession(String peerIdentity) {
    UserSession userSession = userSessions.getIfPresent(peerIdentity);
    if (null == userSession) {
      logger.error("UserSession is not available in SessionManager.");
      throw CallStatus.UNAUTHENTICATED.withDescription("User is not authenticated").toRuntimeException();
    }
    return userSession;
  }

  /**
   * Determines if we have reached the max number of allowed sessions.
   *
   * @return True if we have reached the max number of allowed sessions,
   * False otherwise.
   */
  public boolean reachedMaxNumberOfSessions() {
    final long maxSessions = getMaxSessions();
    return (maxSessions > 0) && (userSessions.size() >= maxSessions);
  }

  /**
   * Gets the number of items in the userSessions Cache.
   *
   * @return The size of userSessions Cache.
   */
  @VisibleForTesting
  long getNumberOfUserSessions() {
    return userSessions.size();
  }

  /**
   * Build the UserSession object using the UserSession Builder.
   *
   * @param username The username to build UserSession for.
   * @return An instance of UserSession.
   */
  @VisibleForTesting
  UserSession buildUserSession(String username) {
    return UserSession.Builder.newBuilder()
      .withSessionOptionManager(
        new SessionOptionManagerImpl(sabotContextProvider.get().getOptionValidatorListing()), optionManager)
      .withCredentials(UserBitShared.UserCredentials.newBuilder()
        .setUserName(username).build())
      .withUserProperties(UserProtos.UserProperties.getDefaultInstance())
      .setSupportComplexTypes(true)
      .withClientInfos(UserBitShared.RpcEndpointInfos.newBuilder().setName("Arrow Flight").build())
      .build();
  }

  @Override
  public void close() throws Exception {
    userSessions.invalidateAll();
  }
}
