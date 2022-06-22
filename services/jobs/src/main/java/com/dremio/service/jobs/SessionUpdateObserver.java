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
package com.dremio.service.jobs;

import org.apache.arrow.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.usersessions.UserSessionService;
import com.dremio.service.usersessions.UserSessionService.UserSessionAndVersion;

/**
 * Implementation of {@link SessionObserver} that will update the session id in the store
 * when the job is completed.
 */
public class SessionUpdateObserver implements SessionObserver {
  private static final Logger logger = LoggerFactory.getLogger(SessionUpdateObserver.class);

  private final UserSessionService userSessionService;
  private UserSession session;
  private final String sessionId;

  public SessionUpdateObserver(UserSessionService userSessionService, String sessionId) {
    this.userSessionService = Preconditions.checkNotNull(userSessionService,
      "UserSessionService must not be null");
    this.sessionId = Preconditions.checkNotNull(sessionId,
      "SessionId must not be null");
  }

  @Override
  public void onCompleted() {
    logger.debug("Job completed. Updating session.");
    update();
  }

  @Override
  public void onError(Exception e) {
    logger.debug("Job failed. Updating session.", e);
    update();
  }

  @Override
  public void onSessionModified(UserSession session) {
    this.session = session;
  }

  private synchronized void update() {
    final UserSessionAndVersion sessionAndVersion = userSessionService.getSession(sessionId);
    if (sessionAndVersion == null) {
      String errMessage = "The session expired while the query was running";
      logger.debug(errMessage);
      throw new RuntimeException(errMessage);
    } else {
      logger.debug("Updating session");
      if (session == null) {
        session = sessionAndVersion.getSession();
      }
      userSessionService.updateSession(sessionId, sessionAndVersion.getVersion(), session);
    }
  }
}
