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

import static com.dremio.exec.ExecConstants.ENABLE_ICEBERG;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.exec.physical.PhysicalPlan;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.sql.SqlExceptionHelper;
import com.dremio.exec.planner.sql.handlers.direct.SimpleCommandResult;
import com.dremio.exec.planner.sql.handlers.direct.SqlNodeUtil;
import com.dremio.exec.planner.sql.handlers.query.SqlToPlanHandler;
import com.dremio.exec.planner.sql.handlers.refresh.MetadataRefreshPlanBuilder;
import com.dremio.exec.planner.sql.parser.SqlRefreshDataset;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.exec.store.pojo.PojoDataType;
import com.google.common.base.Preconditions;

import io.opentelemetry.instrumentation.annotations.WithSpan;

/**
 * Handler for internal {@link SqlRefreshDataset} command.
 */
public class RefreshDatasetHandler implements SqlToPlanHandler {
  private static final Logger logger = LoggerFactory.getLogger(RefreshDatasetHandler.class);

  private String textPlan;

  public RefreshDatasetHandler() {
    logger.info("Initialised {}", this.getClass().getName());
  }

  @WithSpan
  @Override
  public PhysicalPlan getPlan(SqlHandlerConfig config, String sql, SqlNode sqlNode) throws Exception {
    assertRefreshEnabled(config);

    // adds rowType of query result to QueryMetadata
    config.getObserver()
      .planValidated(new PojoDataType(SimpleCommandResult.class).getRowType(JavaTypeFactoryImpl.INSTANCE), sqlNode, 0);

    final SqlRefreshDataset sqlRefreshDataset = SqlNodeUtil.unwrap(sqlNode, SqlRefreshDataset.class);

    try {
      final MetadataRefreshPlanBuilder refreshDatasetPlanBuilder = MetadataRefreshPlanBuilderFactory.getPlanBuilder(config, sqlRefreshDataset);

      final Prel rootPrel = refreshDatasetPlanBuilder.buildPlan();

      final Prel finalizedPlan = (Prel) rootPrel.accept(new PrelFinalizer());
      final Pair<Prel, String> transformedPrel = PrelTransformer.applyPhysicalPrelTransformations(config, finalizedPlan);

      final PhysicalOperator pop = PrelTransformer.convertToPop(config, transformedPrel.left);
      final PhysicalPlan plan = PrelTransformer.convertToPlan(config, pop);
      if (logger.isTraceEnabled()) {
        PlanLogUtil.log(config, "Dremio Plan", plan, logger);
      }

      setTextPlan(transformedPrel.right);
      return plan;
    } catch (Exception e) {
      throw SqlExceptionHelper.coerceException(logger, sql, e, true);
    }
  }

  private void assertRefreshEnabled(SqlHandlerConfig config) {
    Preconditions.checkArgument(config.getContext().getOptions().getOption(PlannerSettings.UNLIMITED_SPLITS_SUPPORT), PlannerSettings.UNLIMITED_SPLITS_SUPPORT.getOptionName() + " Should be enabled");
    Preconditions.checkArgument(config.getContext().getOptions().getOption(ENABLE_ICEBERG), ENABLE_ICEBERG.getOptionName() + " Should be enabled");
  }

  private void setTextPlan(String textPlan) {
    this.textPlan = textPlan;
  }

  @Override
  public String getTextPlan() {
    return textPlan;
  }

  @Override
  public Rel getLogicalPlan() {
    throw new UnsupportedOperationException();
  }
}
