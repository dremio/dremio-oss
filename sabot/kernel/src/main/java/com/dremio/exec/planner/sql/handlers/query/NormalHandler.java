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
package com.dremio.exec.planner.sql.handlers.query;

import java.util.concurrent.TimeUnit;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Pair;

import com.dremio.exec.physical.PhysicalPlan;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.DremioVolcanoPlanner;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.sql.SqlExceptionHelper;
import com.dremio.exec.planner.sql.handlers.ConvertedRelNode;
import com.dremio.exec.planner.sql.handlers.PrelTransformer;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.ViewAccessEvaluator;

/**
 * The default handler for queries.
 */
public class NormalHandler implements SqlToPlanHandler {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(NormalHandler.class);

  private String textPlan;

  @Override
  public PhysicalPlan getPlan(SqlHandlerConfig config, String sql, SqlNode sqlNode) throws Exception {
    try{
      final ConvertedRelNode convertedRelNode = PrelTransformer.validateAndConvert(config, sqlNode);
      final RelDataType validatedRowType = convertedRelNode.getValidatedRowType();
      final RelNode queryRelNode = convertedRelNode.getConvertedNode();

      ViewAccessEvaluator viewAccessEvaluator = null;
      if (config.getConverter().getSubstitutionProvider().isDefaultRawReflectionEnabled()) {
        final RelNode convertedRelWithExpansionNodes = ((DremioVolcanoPlanner) queryRelNode.getCluster().getPlanner()).getOriginalRoot();
        viewAccessEvaluator = new ViewAccessEvaluator(convertedRelWithExpansionNodes, config);
        config.getContext().getExecutorService().submit(viewAccessEvaluator);
      }

      final Rel drel = PrelTransformer.convertToDrel(config, queryRelNode, validatedRowType);

      final Pair<Prel, String> convertToPrel = PrelTransformer.convertToPrel(config, drel);
      final Prel prel = convertToPrel.getKey();
      textPlan = convertToPrel.getValue();
      final PhysicalOperator pop = PrelTransformer.convertToPop(config, prel);
      final PhysicalPlan plan = PrelTransformer.convertToPlan(config, pop);
      logger.debug("Final Physical Plan {}", textPlan);
      PrelTransformer.log(config, "Dremio Plan", plan, logger);

      if (viewAccessEvaluator != null) {
        viewAccessEvaluator.getLatch().await(config.getContext().getPlannerSettings().getMaxPlanningPerPhaseMS(), TimeUnit.MILLISECONDS);
        if (viewAccessEvaluator.getException() != null) {
          throw viewAccessEvaluator.getException();
        }
      }

      return plan;
    }catch(Exception ex){
      throw SqlExceptionHelper.coerceException(logger, sql, ex, true);
    }
  }

  @Override
  public String getTextPlan() {
    return textPlan;
  }

}
