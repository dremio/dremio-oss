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

import com.dremio.exec.planner.PlannerPhase;
import com.google.common.base.Strings;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;

public class JoinPlanningMultiJoinPlanCaptureListener implements PlanTransformationListener {
  private volatile String plan;

  @Override
  public void onPhaseCompletion(
      final PlannerPhase phase, final RelNode before, final RelNode after, final long millisTaken) {
    if (!Strings.isNullOrEmpty(plan)) {
      return;
    }

    if (phase == PlannerPhase.JOIN_PLANNING_MULTI_JOIN) {
      plan = RelOptUtil.dumpPlan("", after, SqlExplainFormat.TEXT, SqlExplainLevel.ALL_ATTRIBUTES);
    }
  }

  public String getPlan() {
    return plan;
  }
}
