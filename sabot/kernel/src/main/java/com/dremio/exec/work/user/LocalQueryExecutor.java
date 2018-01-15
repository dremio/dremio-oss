/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.exec.work.user;

import com.dremio.exec.planner.observer.QueryObserver;
import com.dremio.exec.proto.UserBitShared.ExternalId;

/**
 * Will submit a query locally without going through the client
 */
public interface LocalQueryExecutor {

  /**
   * Will submit a query locally without going through the client.
   * @param observer QueryObserver used to get notifications about the queryJob.
   *                    Overrides the use of QueryObserverFactory defined in the context
   * @param query the query definition
   * @param prepare whether this is a prepared statement
   * @param config local execution config
   */
  void submitLocalQuery(
      ExternalId externalId,
      QueryObserver observer,
      Object query,
      boolean prepare,
      LocalExecutionConfig config);

  /**
   * Cancel a locally running query.
   * @param externalId QueryId of the query to cancel.
   */
  void cancelLocalQuery(ExternalId externalId);

}
