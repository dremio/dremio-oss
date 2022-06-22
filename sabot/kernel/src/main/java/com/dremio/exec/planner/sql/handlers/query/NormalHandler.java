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

import static com.dremio.exec.planner.physical.PlannerSettings.STORE_QUERY_RESULTS;

import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.util.Pair;

import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.physical.PhysicalPlan;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.CachedAccelDetails;
import com.dremio.exec.planner.CachedPlan;
import com.dremio.exec.planner.DremioVolcanoPlanner;
import com.dremio.exec.planner.PlanCache;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.explain.PrelSequencer;
import com.dremio.exec.planner.physical.visitor.WriterPathUpdater;
import com.dremio.exec.planner.sql.SqlExceptionHelper;
import com.dremio.exec.planner.sql.handlers.ConvertedRelNode;
import com.dremio.exec.planner.sql.handlers.PrelTransformer;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.ViewAccessEvaluator;
import com.dremio.exec.store.CatalogService;
import com.dremio.options.OptionManager;

/**
 * The default handler for queries.
 */
public class NormalHandler implements SqlToPlanHandler {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(NormalHandler.class);

  private String textPlan;

  @Override
  public PhysicalPlan getPlan(SqlHandlerConfig config, String sql, SqlNode sqlNode) throws Exception {
    try{
      final PlannerSettings plannerSettings = config.getContext().getPlannerSettings();
      final PlanCache planCache = config.getContext().getPlanCache();
      final long cachedKey = planCache.generateCacheKey(sqlNode.toSqlString(CalciteSqlDialect.DEFAULT).getSql(),
        config.getContext());
      config.getObserver().setCacheKey(cachedKey);
      final ConvertedRelNode convertedRelNode = PrelTransformer.validateAndConvert(config, sqlNode);
      final RelDataType validatedRowType = convertedRelNode.getValidatedRowType();
      final RelNode queryRelNode = convertedRelNode.getConvertedNode();
      ViewAccessEvaluator viewAccessEvaluator = null;
      if (config.getConverter().getSubstitutionProvider().isDefaultRawReflectionEnabled()) {
        final RelNode convertedRelWithExpansionNodes = ((DremioVolcanoPlanner) queryRelNode.getCluster().getPlanner()).getOriginalRoot();
        viewAccessEvaluator = new ViewAccessEvaluator(convertedRelWithExpansionNodes, config);
        config.getContext().getExecutorService().submit(viewAccessEvaluator);
      }
      final Catalog catalog = config.getContext().getCatalog();
      final CatalogService catalogService = config.getContext().getCatalogService();
      CachedPlan cachedPlan = (planCache != null) ? planCache.getIfPresentAndValid(catalog, catalogService, cachedKey) : null;
      Prel prel;
      if (!plannerSettings.isPlanCacheEnabled() || cachedPlan == null) {
        final Rel drel = PrelTransformer.convertToDrel(config, queryRelNode, validatedRowType);

        final Pair<Prel, String> convertToPrel = PrelTransformer.convertToPrel(config, drel);
        prel = convertToPrel.getKey();
        textPlan = convertToPrel.getValue();

        //after we generate a physical plan, save it in the plan cache if plan cache is present
        if (PlanCache.supportPlanCache(planCache, config, sqlNode)) {
          planCache.createNewCachedPlan(catalog, cachedKey, sql, prel, textPlan, config);
        }
      } else {
        prel = cachedPlan.getPrel();

        // After the plan has been cached during planning, the job could be canceled during execution.
        // Reset the cancel flag in cached plan, otherwise the job will always be canceled.
        prel.getCluster().getPlanner().getContext().unwrap(org.apache.calcite.util.CancelFlag.class).clearCancel();

        CachedAccelDetails accelDetails = cachedPlan.getAccelDetails();
        if (accelDetails != null && accelDetails.getSubstitutionInfo() != null) {
          config.getObserver().applyAccelDetails(accelDetails);
        }
        config.getObserver().planCacheUsed(cachedPlan.updateUseCount());
        //update writer if needed
        final OptionManager options = config.getContext().getOptions();
        final PlannerSettings.StoreQueryResultsPolicy storeQueryResultsPolicy = Optional
          .ofNullable(options.getOption(STORE_QUERY_RESULTS.getOptionName()))
          .map(o -> PlannerSettings.StoreQueryResultsPolicy.valueOf(o.getStringVal().toUpperCase(Locale.ROOT)))
          .orElse(PlannerSettings.StoreQueryResultsPolicy.NO);
        if (storeQueryResultsPolicy == PlannerSettings.StoreQueryResultsPolicy.PATH_AND_ATTEMPT_ID) {
          //update writing path for this case only
          prel = WriterPathUpdater.update(prel, config);
        }
        if (logger.isDebugEnabled() || config.getObserver() != null) {
          textPlan = PrelSequencer.setPlansWithIds(prel, SqlExplainLevel.ALL_ATTRIBUTES, config.getObserver(), 0);
          if (logger.isDebugEnabled()) {
            logger.debug(String.format("%s:\n%s", "Final Physical Transformation", textPlan));
          }
        } else {
          textPlan = "";
        }
      }
      final PhysicalOperator pop = PrelTransformer.convertToPop(config, prel);
      PhysicalPlan plan = PrelTransformer.convertToPlan(config, pop);
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
