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
package com.dremio.service.usersessions;

import com.dremio.datastore.api.options.VersionOption;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.Service;

/**
 * Defines the interface for UserSessionStore services
 */
public interface UserSessionService extends Service {
  /**
   * Object holding UserSesssion, associated session id and VersionOption
   */
  class UserSessionData {
    private final UserSession session;
    private final VersionOption version;
    private final String sessionId;

    public UserSessionData(UserSession session, VersionOption version, String sessionId)  {
      this.session = session;
      this.version = version;
      this.sessionId = sessionId;
    }
    public UserSession getSession() { return this.session; }
    public VersionOption getVersion() { return this.version; }
    public String getSessionId() { return this.sessionId; }
  }

  /**
   * Object holding the UserSession and VersionOption
   */
  class UserSessionAndVersion {
    private final UserSession session;
    private final VersionOption version;

    public UserSessionAndVersion(UserSession session, VersionOption version) {
      this.session = session;
      this.version = version;
    }

    public UserSession getSession() { return this.session; }
    public VersionOption getVersion() { return this.version; }
  }

  /**
   * Object holding the session id and version
   */
  class SessionIdAndVersion {
    private final String id;
    private final VersionOption version;

    public SessionIdAndVersion(String id, VersionOption version) {
      this.id = id;
      this.version = version;
    }

    public String getId() { return this.id; }
    public VersionOption getVersion() { return this.version; }
  }

  /**
   * Puts a session into the store and returns a UUID encoded as a string
   * @param session the user session to put in the store
   * @return the session id and version
   */
   SessionIdAndVersion putSession(UserSession session);

  /**
   * This will update a session
   * @param sessionId the sessionId to update
   * @param version the version of the session to update
   * @param newSession the user session to put in the store
   * @return the session version
   * @throws java.util.ConcurrentModificationException if the version is out of data and the @param newSession is not the same as the session currently stored
   */
  VersionOption updateSession(String sessionId, VersionOption version, UserSession newSession);

  /**
   * Returns the session data. null if it does not exist
   * @param sessionId the sessionId to retrieve
   * @return an object containing the data stored if sessionId exists and is valid and null if not
   */
   UserSessionAndVersion getSession(String sessionId);
}
