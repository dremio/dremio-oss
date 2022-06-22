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

import static com.dremio.service.flight.client.properties.DremioFlightClientProperties.applyClientPropertiesToUserSessionBuilder;
import static com.dremio.service.flight.client.properties.DremioFlightClientProperties.applyMutableClientProperties;

import javax.inject.Provider;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserProtos;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.SessionOptionManager;
import com.dremio.exec.server.options.SessionOptionManagerFactory;
import com.dremio.exec.server.options.SessionOptionManagerFactoryImpl;
import com.dremio.exec.server.options.SessionOptionManagerImpl;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValidatorListing;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.tokens.TokenDetails;
import com.dremio.service.tokens.TokenManager;
import com.dremio.service.usersessions.UserSessionService;
import com.google.common.annotations.VisibleForTesting;

/**
 * Manages UserSession creation and UserSession cache.
 */
public class SessionServiceFlightSessionsManager implements DremioFlightSessionsManager {
  private static final Logger logger = LoggerFactory.getLogger(SessionServiceFlightSessionsManager.class);

  public static final String COOKIE_HEADER = "Cookie";
  public static final String SESSION_ID_KEY = "SESSION_ID";

  private final UserSessionService userSessionService;
  private final Provider<SabotContext> sabotContextProvider;
  private final Provider<TokenManager> tokenManagerProvider;
  private final SessionOptionManagerFactory sessionOptionManagerFactory;
  private final OptionManager optionManager;

  public SessionServiceFlightSessionsManager(Provider<SabotContext> sabotContextProvider,
                                             Provider<TokenManager> tokenManagerProvider,
                                             Provider<UserSessionService> userSessionServiceProvider) throws Exception {
    this.sabotContextProvider = sabotContextProvider;
    this.tokenManagerProvider = tokenManagerProvider;
    this.optionManager = sabotContextProvider.get().getOptionManager();
    final OptionValidatorListing optionValidatorListing = sabotContextProvider.get().getOptionValidatorListing();
    this.sessionOptionManagerFactory = new SessionOptionManagerFactoryImpl(optionValidatorListing);
    this.userSessionService = userSessionServiceProvider.get();
    this.userSessionService.start();
  }

  @Override
  public UserSessionService.UserSessionData createUserSession(String peerIdentity, CallHeaders incomingHeaders) {
    final String[] peerIdentityParts = peerIdentity.split("\n");
    final String username;
    if (peerIdentityParts.length == 3) {
      username = peerIdentityParts[1];
    } else {
      final TokenDetails tokenDetails = tokenManagerProvider.get().validateToken(peerIdentity);
      username = tokenDetails.username;
    }
    final UserSession userSession = buildUserSession(username, incomingHeaders);
    applyMutableClientProperties(userSession, incomingHeaders);
    final UserSessionService.SessionIdAndVersion idAndVersion = userSessionService.putSession(userSession);

    return new UserSessionService.UserSessionData(userSession, idAndVersion.getVersion(), idAndVersion.getId());
  }

  @Override
  public UserSessionService.UserSessionData getUserSession(String peerIdentity, CallHeaders incomingHeaders) {
    final String sessionId = CookieUtils.getCookieValue(incomingHeaders, SESSION_ID_KEY);
    if (sessionId == null) {
      logger.debug("No sessionId is available in Headers.");
      return null;
    }

    UserSessionService.UserSessionAndVersion userSessionAndVersion;
    try {
      userSessionAndVersion = userSessionService.getSession(sessionId);
    } catch (Exception e) {
      final String errorDescription = "Unable to retrieve user session.";
      logger.error(errorDescription, e);
      throw CallStatus.INTERNAL.withCause(e).withDescription(errorDescription).toRuntimeException();
    }

    if (null == userSessionAndVersion) {
      logger.error("UserSession is not available in SessionManager.");
      throw CallStatus.UNAUTHENTICATED.withDescription("UserSession is not available in the cache").toRuntimeException();
    }

    UserSession userSession = userSessionAndVersion.getSession();
    final SessionOptionManager sessionOptionManager = this.sessionOptionManagerFactory.getOrCreate(sessionId);

    userSession = UserSession.Builder.newBuilder(userSession).withSessionOptionManager(sessionOptionManager, this.optionManager).build();
    return new UserSessionService.UserSessionData(userSession, userSessionAndVersion.getVersion(), sessionId);
  }

  @Override
  public void decorateResponse(FlightProducer.CallContext callContext, UserSessionService.UserSessionData sessionData) {
    addCookie(callContext, SESSION_ID_KEY, sessionData.getSessionId());
  }

  @Override
  public void updateSession(UserSessionService.UserSessionData updatedSession) {
    userSessionService.updateSession(updatedSession.getSessionId(), updatedSession.getVersion(), updatedSession.getSession());
  }

  /**
   * Build the UserSession object using the UserSession Builder.
   *
   * @param username        The username to build UserSession for.
   * @param incomingHeaders The CallHeaders to parse client properties from.
   * @return An instance of UserSession.
   */
  @VisibleForTesting
  UserSession buildUserSession(String username, CallHeaders incomingHeaders) {
    final UserSession.Builder builder = UserSession.Builder.newBuilder()
      .withSessionOptionManager(
        new SessionOptionManagerImpl(sabotContextProvider.get().getOptionValidatorListing()), optionManager)
      .withCredentials(UserBitShared.UserCredentials.newBuilder()
        .setUserName(username).build())
      .withUserProperties(UserProtos.UserProperties.getDefaultInstance())
      .setSupportComplexTypes(true)
      .withClientInfos(UserBitShared.RpcEndpointInfos.newBuilder().setName("Arrow Flight").build());

    if (incomingHeaders != null) {
      applyClientPropertiesToUserSessionBuilder(builder, incomingHeaders);
    }

    return builder.build();
  }

  @Override
  public void close() throws Exception {
  }

  /**
   * Helper method to set the outgoing cookies
   *
   * @param callContext the CallContext to get the middleware from
   * @param key         the key of the cookie
   * @param value       the value of the cookie
   */
  private void addCookie(FlightProducer.CallContext callContext, String key, String value) {
    callContext.getMiddleware(DremioFlightService.FLIGHT_CLIENT_PROPERTIES_MIDDLEWARE_KEY).addCookie(key, value);
  }
}
