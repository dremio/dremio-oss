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
package com.dremio.exec.server.options;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.dremio.options.OptionValidatorListing;
import com.google.common.annotations.VisibleForTesting;

/**
 * SessionOptionManagerFactory for software. Maintains a map of sessionIds to
 * associated SessionOptionManagers.
 *
 * Provides functionality to get the associated SessionOptionManager for the
 * provided session id if it exists in the map, else
 */
public class SessionOptionManagerFactoryImpl implements SessionOptionManagerFactory {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SessionOptionManagerFactoryImpl.class);

  // map of session ids to session option managers
  private final Map<String, SessionOptionManager> sessionOptionManagers;
  private final OptionValidatorListing optionValidatorListing;

  public SessionOptionManagerFactoryImpl(OptionValidatorListing optionValidatorListing) {
    this.sessionOptionManagers = new ConcurrentHashMap<>();
    this.optionValidatorListing = optionValidatorListing;
  }

  /**
   * Attempt to retrieve the SessionOptionManager corresponding to the given
   * sessionId. If it doesn't exist in the map, create a new SessionOptionManager
   * and add it to the map.
   *
   * @param sessionId session Id
   * @return a SessionOptionManager for the current session
   */
  @Override
  public SessionOptionManager getOrCreate(String sessionId) {
    return sessionOptionManagers.computeIfAbsent(sessionId, id ->
      new SessionOptionManagerImpl(optionValidatorListing));
  }

  /**
   * Delete the SessionOptionManager associated with the given id, if it exists.
   *
   * @param sessionId session ID
   */
  @Override
  public void delete(String sessionId) {
    if (sessionOptionManagers.remove(sessionId) == null) {
      logger.warn(String.format("Could not find session with sessionId %s.", sessionId));
    }
  }

  @VisibleForTesting
  public boolean contains(String sessionId) {
    return sessionOptionManagers.containsKey(sessionId);
  }
}
