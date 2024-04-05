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
import static com.dremio.service.flight.client.properties.DremioFlightClientProperties.applyClientPropertiesToUserSessionBuilder;
import static com.dremio.service.flight.client.properties.DremioFlightClientProperties.applyMutableClientProperties;

import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserProtos;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.SessionOptionManagerImpl;
import com.dremio.options.OptionManager;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.tokens.TokenDetails;
import com.dremio.service.tokens.TokenManager;
import com.dremio.service.usersessions.UserSessionService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.concurrent.TimeUnit;
import javax.inject.Provider;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Manages user sessions in an in memory hash with the peerIdentity as the key. */
public class TokenCacheFlightSessionManager implements DremioFlightSessionsManager {
  private static final Logger logger =
      LoggerFactory.getLogger(TokenCacheFlightSessionManager.class);

  private final Cache<String, UserSession> userSessions;
  private final Provider<SabotContext> sabotContextProvider;
  private final Provider<TokenManager> tokenManagerProvider;
  private final OptionManager optionManager;

  @SuppressWarnings("NoGuavaCacheUsage") // TODO: fix as part of DX-51884
  public TokenCacheFlightSessionManager(
      Provider<SabotContext> sabotContextProvider, Provider<TokenManager> tokenManagerProvider) {
    this.sabotContextProvider = sabotContextProvider;
    this.tokenManagerProvider = tokenManagerProvider;
    this.optionManager = sabotContextProvider.get().getOptionManager();
    this.userSessions =
        CacheBuilder.newBuilder()
            .expireAfterAccess(
                optionManager.getOption(SESSION_EXPIRATION_TIME_MINUTES), TimeUnit.MINUTES)
            .build();
  }

  public long getMaxSessions() {
    return optionManager.getOption(MAX_SESSIONS);
  }

  @Override
  public UserSessionService.UserSessionData getUserSession(
      String peerIdentity, CallHeaders incomingHeaders) {
    UserSession userSession = userSessions.getIfPresent(peerIdentity);
    if (null == userSession) {
      tokenManagerProvider.get().invalidateToken(peerIdentity);
      logger.error("UserSession is not available in SessionManager.");
      throw CallStatus.UNAUTHENTICATED
          .withDescription("User is not authenticated")
          .toRuntimeException();
    }

    return new UserSessionService.UserSessionData(userSession, null, null);
  }

  @Override
  public UserSessionService.UserSessionData createUserSession(
      String token, CallHeaders incomingHeaders) {
    final TokenDetails tokenDetails = tokenManagerProvider.get().validateToken(token);
    final UserSession session = buildUserSession(tokenDetails.username, incomingHeaders);

    if (incomingHeaders != null) {
      applyMutableClientProperties(session, incomingHeaders);
    }

    userSessions.put(token, session);
    return new UserSessionService.UserSessionData(session, null, null);
  }

  @Override
  public void decorateResponse(
      FlightProducer.CallContext callContext, UserSessionService.UserSessionData sessionData) {}

  @Override
  public void updateSession(UserSessionService.UserSessionData updatedSession) {}

  /**
   * Determines if we have reached the max number of allowed sessions.
   *
   * @return True if we have reached the max number of allowed sessions, False otherwise.
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
   * @param incomingHeaders The CallHeaders to parse client properties from.
   * @return An instance of UserSession.
   */
  @VisibleForTesting
  UserSession buildUserSession(String username, CallHeaders incomingHeaders) {
    final UserSession.Builder builder =
        UserSession.Builder.newBuilder()
            .withSessionOptionManager(
                new SessionOptionManagerImpl(
                    sabotContextProvider.get().getOptionValidatorListing()),
                optionManager)
            .withCredentials(
                UserBitShared.UserCredentials.newBuilder().setUserName(username).build())
            .withUserProperties(UserProtos.UserProperties.getDefaultInstance())
            .setSupportComplexTypes(true)
            .withClientInfos(
                UserBitShared.RpcEndpointInfos.newBuilder().setName("Arrow Flight").build());

    if (incomingHeaders != null) {
      applyClientPropertiesToUserSessionBuilder(builder, incomingHeaders);
    }

    return builder.build();
  }

  @Override
  public void close() throws Exception {
    userSessions.invalidateAll();
  }
}
