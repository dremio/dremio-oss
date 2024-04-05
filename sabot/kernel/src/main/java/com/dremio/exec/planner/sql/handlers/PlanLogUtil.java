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
package com.dremio.exec.planner.sql.handlers;

import com.dremio.exec.physical.PhysicalPlan;
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.planner.PlannerType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Stopwatch;
import java.util.concurrent.TimeUnit;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlExplainLevel;
import org.slf4j.Logger;

public class PlanLogUtil {
  public static void log(
      final PlannerType plannerType,
      final PlannerPhase phase,
      final RelNode node,
      final Logger logger,
      Stopwatch watch) {
    if (logger.isDebugEnabled()) {
      log(plannerType.name() + ":" + phase.description, node, logger, watch);
    }
  }

  public static void log(
      final String description, final RelNode node, final Logger logger, Stopwatch watch) {
    if (logger.isDebugEnabled()) {
      final String plan = RelOptUtil.toString(node, SqlExplainLevel.ALL_ATTRIBUTES);
      final String time =
          watch == null ? "" : String.format(" (%dms)", watch.elapsed(TimeUnit.MILLISECONDS));
      logger.debug(String.format("%s%s:\n%s", description, time, plan));
    }
  }

  public static void log(
      final SqlHandlerConfig config,
      final String name,
      final PhysicalPlan plan,
      final Logger logger)
      throws JsonProcessingException {
    if (logger.isDebugEnabled()) {
      String planText = plan.unparse(config.getContext().getLpPersistence().getMapper().writer());
      logger.debug(name + " : \n" + planText);
    }
  }
}
