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

import com.dremio.options.OptionValidatorListing;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.annotations.VisibleForTesting;
import java.time.Duration;

/**
 * SessionOptionManagerFactory for software. Maintains a map of sessionIds to associated
 * SessionOptionManagers.
 *
 * <p>Provides functionality to get the associated SessionOptionManager for the provided session id
 * if it exists in the map, else
 */
public class SessionOptionManagerFactoryWithExpiration implements SessionOptionManagerFactory {
  private final LoadingCache<String, SessionOptionManager> cache;

  public SessionOptionManagerFactoryWithExpiration(
      OptionValidatorListing optionValidatorListing, Duration cacheExpiration, int maxCacheSize) {
    // Cache with limited capacity, user session managers otherwise accumulate indefinitely. The
    // default TTL for
    // user session is 2 minutes. In one example, the number of session managers grew to ~2M.
    this.cache =
        Caffeine.newBuilder()
            .expireAfterAccess(cacheExpiration)
            .maximumSize(maxCacheSize)
            .build(k -> new SessionOptionManagerImpl(optionValidatorListing));
  }

  /**
   * Attempt to retrieve the SessionOptionManager corresponding to the given sessionId. If it
   * doesn't exist in the map, create a new SessionOptionManager and add it to the map.
   *
   * @param sessionId session Id
   * @return a SessionOptionManager for the current session
   */
  @Override
  public SessionOptionManager getOrCreate(String sessionId) {
    return cache.get(sessionId);
  }

  /**
   * Delete the SessionOptionManager associated with the given id, if it exists.
   *
   * @param sessionId session ID
   */
  @Override
  public void delete(String sessionId) {
    cache.invalidate(sessionId);
  }

  @VisibleForTesting
  public boolean contains(String sessionId) {
    return cache.getIfPresent(sessionId) != null;
  }
}
