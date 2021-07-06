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
import static com.dremio.exec.planner.physical.PlannerSettings.ENABLE_ICEBERG_EXECUTION;

import java.io.IOException;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.exec.catalog.CatalogOptions;
import com.dremio.exec.physical.PhysicalPlan;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.sql.SqlExceptionHelper;
import com.dremio.exec.planner.sql.handlers.direct.SqlNodeUtil;
import com.dremio.exec.planner.sql.handlers.query.SqlToPlanHandler;
import com.dremio.exec.planner.sql.handlers.refresh.MetadataProvider;
import com.dremio.exec.planner.sql.handlers.refresh.RefreshDatasetIncrementalPlanBuilder;
import com.dremio.exec.planner.sql.handlers.refresh.RefreshDatasetPlanBuilder;
import com.dremio.exec.planner.sql.parser.SqlRefreshDataset;
import com.dremio.service.namespace.NamespaceException;

/**
 * Handler for internal <code>REFRESH DATASET tblname</code> command.
 */
public class RefreshDatasetHandler implements SqlToPlanHandler {
  private static final Logger logger = LoggerFactory.getLogger(RefreshDatasetHandler.class);

  private String textPlan;

  public RefreshDatasetHandler() {
    logger.info("Initialised {}", this.getClass().getName());
  }

  @Override
  public PhysicalPlan getPlan(SqlHandlerConfig config, String sql, SqlNode sqlNode) throws Exception {
    assertRefreshEnabled(config);

    final SqlRefreshDataset sqlRefreshDataset = SqlNodeUtil.unwrap(sqlNode, SqlRefreshDataset.class);

    try {
      final RefreshDatasetPlanBuilder refreshDatasetPlanBuilder = getPlanBuilder(config, sqlRefreshDataset);

      final Prel rootPrel = refreshDatasetPlanBuilder.buildPlan();
      final Prel finalizedPlan = (Prel) rootPrel.accept(new PrelFinalizer());
      final Pair<Prel, String> transformedPrel = PrelTransformer.applyPhysicalPrelTransformations(config, finalizedPlan);

      final PhysicalOperator pop = PrelTransformer.convertToPop(config, transformedPrel.left);
      final PhysicalPlan plan = PrelTransformer.convertToPlan(config, pop);

      if (logger.isTraceEnabled()) {
        PrelTransformer.log(config, "Dremio Plan", plan, logger);
      }

      setTextPlan(transformedPrel.right);
      return plan;
    } catch (Exception e) {
      throw SqlExceptionHelper.coerceException(logger, sql, e, true);
    }
  }

  private RefreshDatasetPlanBuilder getPlanBuilder(SqlHandlerConfig config, SqlRefreshDataset sqlRefreshDataset)
          throws IOException, NamespaceException {
    final MetadataProvider metadataProvider = new MetadataProvider(config, sqlRefreshDataset);
    if (metadataProvider.doesMetadataExist()) {
      return new RefreshDatasetIncrementalPlanBuilder(config, sqlRefreshDataset);
    } else {
      return new RefreshDatasetPlanBuilder(config, sqlRefreshDataset);
    }
  }

  private void assertRefreshEnabled(SqlHandlerConfig config) {
    if (!config.getContext().getOptions().getOption(CatalogOptions.DMP_METADATA_REFRESH)) {
      throw new UnsupportedOperationException("REFRESH METADATA command is not enabled.");
    }
    if (!config.getContext().getOptions().getOption(ENABLE_ICEBERG_EXECUTION) ||
            !config.getContext().getOptions().getOption(ENABLE_ICEBERG)) {
      throw new UnsupportedOperationException("REFRESH METADATA requires " + ENABLE_ICEBERG_EXECUTION.getOptionName()
              + " and " + ENABLE_ICEBERG.getOptionName() + " to be enabled.");
    }
  }

  private void setTextPlan(String textPlan) {
    this.textPlan = textPlan;
  }

  @Override
  public String getTextPlan() {
    return textPlan;
  }
}
