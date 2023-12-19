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
package com.dremio.service.sqlrunner;

/**
 * Defines the interface for SQLRunner Session services
 */
public interface SQLRunnerSessionService {
  /**
   * Returns the Sql Runner session.
   * @param userId the userId to retrieve
   * @return a SQLRunnerSession object containing the SQL Runner session of the user
   */
  SQLRunnerSession getSession(String userId) throws SQLRunnerSessionNotSupportedException;

  /**
   * Update a SQl Runner session
   * @param newSession the SQL Runner session to put in the store
   */
  SQLRunnerSession updateSession(SQLRunnerSession newSession) throws SQLRunnerSessionNotSupportedException;

  /**
   * Update a SQl Runner session
   * @param userId the owner userId of the SQL Runner Session
   */
  void deleteSession(String userId) throws SQLRunnerSessionNotSupportedException;

  /**
   * Add a tab to SQl Runner session
   * @param userId the userId
   */
  SQLRunnerSession newTab(String userId, String scriptId);

  /**
   * Delete a tab in SQl Runner session
   * @param userId the userId to
   */
  void deleteTab(String userId, String scriptId);

  /**
   * Delete expired SQl Runner sessions
   */
  int deleteExpired();
}
