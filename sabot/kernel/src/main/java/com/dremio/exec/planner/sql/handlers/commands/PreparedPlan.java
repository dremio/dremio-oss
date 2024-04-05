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
package com.dremio.exec.planner.sql.handlers.commands;

import com.dremio.exec.physical.PhysicalPlan;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.planner.sql.handlers.commands.HandlerToPreparePlanBase.RecordingObserver;
import com.dremio.exec.proto.UserBitShared.QueryId;

/** Represents a previous planned query generated through creating a prepared statement. */
public class PreparedPlan {
  private final QueryId prepareId;
  private final String username;
  private final boolean queryRequiresGroupsInfo;
  private final String query;
  private final PhysicalPlan plan;
  private final RecordingObserver observer;

  public PreparedPlan(
      QueryId prepareId,
      String username,
      boolean queryRequiresGroupsInfo,
      String query,
      PhysicalPlan plan,
      RecordingObserver observer) {
    this.prepareId = prepareId;
    this.username = username;
    this.queryRequiresGroupsInfo = queryRequiresGroupsInfo;
    this.query = query;
    this.plan = plan;
    this.observer = observer;
  }

  public void replay(AttemptObserver observer) {
    this.observer.replay(observer);
  }

  public String getUsername() {
    return username;
  }

  public PhysicalPlan getPlan() {
    return plan;
  }

  public QueryId getPrepareId() {
    return prepareId;
  }

  public String getQuery() {
    return query;
  }

  public boolean getQueryRequiresGroupsInfo() {
    return queryRequiresGroupsInfo;
  }
}
