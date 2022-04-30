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

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.FlightProducer;

import com.dremio.options.Options;
import com.dremio.options.TypeValidators;
import com.dremio.service.usersessions.UserSessionService;

/**
 * Manages UserSession creation and UserSession cache.
 */
@Options
public interface DremioFlightSessionsManager extends AutoCloseable {
  // 0 is valid value and is treated as no limit on the number of Sessions.
  TypeValidators.PositiveLongValidator MAX_SESSIONS =
    new TypeValidators.PositiveLongValidator("flight.max.sessions", Long.MAX_VALUE, 0L);

  /**
   * Resolves an existing UserSession for the given token. Null if the UserSession has expired and is no
   * longer in the cache.
   * <p>
   *
   * @param peerIdentity identity after authorization
   * @param incomingHeaders The CallHeaders to parse client properties from.
   * @return The UserSession or null if no sessionId is given.
   */
  UserSessionService.UserSessionData getUserSession(String peerIdentity, CallHeaders incomingHeaders);

  /**
   * Creates a UserSession object and store it in the local cache, assuming that the token was already validated.
   *
   * @param peerIdentity   identity after authorization
   * @param incomingHeaders The CallHeaders to parse client properties from.
   */
  UserSessionService.UserSessionData createUserSession(String peerIdentity, CallHeaders incomingHeaders);

  /**
   * Decorates the response to go back to the client.
   * @param callContext request call context
   * @param sessionData data for the session
   */
  void decorateResponse(FlightProducer.CallContext callContext, UserSessionService.UserSessionData sessionData);

  /**
   * Updates a UserSession in the cache.
   * @param updatedSession the updated session data, including the session id, the previous version, and the changed session.
   */
  void updateSession(UserSessionService.UserSessionData updatedSession);
}
