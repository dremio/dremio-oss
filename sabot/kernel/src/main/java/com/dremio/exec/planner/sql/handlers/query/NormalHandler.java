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
import static org.slf4j.LoggerFactory.getLogger;

import com.dremio.common.logical.PlanProperties.Generator.ResultMode;
import com.dremio.common.util.Closeable;
import com.dremio.exec.catalog.CatalogOptions;
import com.dremio.exec.ops.PlannerCatalog;
import com.dremio.exec.physical.PhysicalPlan;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.events.FunctionDetectedEvent;
import com.dremio.exec.planner.events.PlannerEventBus;
import com.dremio.exec.planner.events.PlannerEventHandler;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.explain.PrelSequencer;
import com.dremio.exec.planner.physical.visitor.WriterPathUpdater;
import com.dremio.exec.planner.plancache.CachedPlan;
import com.dremio.exec.planner.plancache.PlanCache;
import com.dremio.exec.planner.plancache.PlanCacheKey;
import com.dremio.exec.planner.plancache.PlanCacheUtils;
import com.dremio.exec.planner.sql.SqlExceptionHelper;
import com.dremio.exec.planner.sql.UncacheableFunctionDetector;
import com.dremio.exec.planner.sql.handlers.ConvertedRelNode;
import com.dremio.exec.planner.sql.handlers.DrelTransformer;
import com.dremio.exec.planner.sql.handlers.PlanLogUtil;
import com.dremio.exec.planner.sql.handlers.PrelTransformer;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.SqlToRelTransformer;
import com.dremio.exec.proto.UserBitShared.AccelerationProfile;
import com.dremio.options.OptionManager;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.util.Pair;
import org.slf4j.Logger;

/** The default handler for queries. */
public class NormalHandler implements SqlToPlanHandler {
  private static final Logger LOGGER = getLogger(NormalHandler.class);

  private String textPlan;
  private Rel drel;
  private Prel prel;
  private ConvertedRelNode convertedRelNode;

  private UncacheableFunctionDetectedEventHandler uncacheableFunctionDetectedEventHandler =
      new UncacheableFunctionDetectedEventHandler();

  @WithSpan
  @Override
  public PhysicalPlan getPlan(SqlHandlerConfig config, String sql, SqlNode sqlNode)
      throws Exception {
    PlannerEventBus plannerEventBus = config.getPlannerEventBus();
    try (Closeable ignored = plannerEventBus.register(uncacheableFunctionDetectedEventHandler)) {
      Span.current()
          .setAttribute(
              "dremio.planner.workload_type", config.getContext().getWorkloadType().name());
      Span.current()
          .setAttribute(
              "dremio.planner.current_default_schema",
              config.getContext().getContextInformation().getCurrentDefaultSchema());
      final PlannerSettings plannerSettings = config.getContext().getPlannerSettings();
      final PlanCache planCache = Preconditions.checkNotNull(config.getContext().getPlanCache());

      prePlan(config, sql, sqlNode);

      convertedRelNode = SqlToRelTransformer.validateAndConvert(config, sqlNode);
      convertedRelNode = postConvertToRel(convertedRelNode);

      final RelDataType validatedRowType = convertedRelNode.getValidatedRowType();
      final RelNode queryRelNode = convertedRelNode.getConvertedNode();
      final PlannerCatalog catalog = config.getConverter().getPlannerCatalog();

      final PlanCacheKey cachedKey =
          PlanCacheUtils.generateCacheKey(sqlNode, queryRelNode, config.getContext());
      CachedPlan cachedPlan = planCache.getIfPresentAndValid(config, cachedKey);

      Span.current()
          .setAttribute("dremio.planner.cache.enabled", plannerSettings.isPlanCacheEnabled());
      Span.current()
          .setAttribute("dremio.planner.cache.plan_cache_present_and_valid", (cachedPlan != null));

      Prel prel;
      if (!plannerSettings.isPlanCacheEnabled() || cachedPlan == null) {
        drel = DrelTransformer.convertToDrel(config, queryRelNode, validatedRowType);
        drel = postConvertToDrel(drel);
        if (config.getResultMode().equals(ResultMode.LOGICAL)) {
          // we only want to do logical planning, there is no point going further in the plan
          // generation
          return null;
        }
        if (!plannerSettings.ignoreScannedColumnsLimit()) {
          long maxScannedColumns =
              config
                  .getContext()
                  .getOptions()
                  .getOption(CatalogOptions.METADATA_LEAF_COLUMN_SCANNED_MAX);
          ScanLimitValidator.ensureLimit(drel, maxScannedColumns);
        }

        final Pair<Prel, String> convertToPrel = PrelTransformer.convertToPrel(config, drel);
        prel = convertToPrel.getKey();
        textPlan = convertToPrel.getValue();

        // after we generate a physical plan, save it in the plan cache if plan cache is present
        if (PlanCacheUtils.supportPlanCache(
            config,
            sqlNode,
            catalog,
            uncacheableFunctionDetectedEventHandler.getUncacheableFunctions())) {
          planCache.putCachedPlan(config, cachedKey, prel);
        }
      } else {
        prel = cachedPlan.getPrel();

        // After the plan has been cached during planning, the job could be canceled during
        // execution.
        // Reset the cancel flag in cached plan, otherwise the job will always be canceled.
        prel.getCluster()
            .getPlanner()
            .getContext()
            .unwrap(org.apache.calcite.util.CancelFlag.class)
            .clearCancel();

        AccelerationProfile accelerationProfile = cachedPlan.getAccelerationProfile();
        config.getObserver().restoreAccelerationProfileFromCachedPlan(accelerationProfile);
        config.getObserver().planCacheUsed(cachedPlan.updateUseCount());
        Span.current()
            .setAttribute("dremio.planner.cache.plan_used_count", cachedPlan.getUseCount());
        // update writer if needed
        final OptionManager options = config.getContext().getOptions();
        final PlannerSettings.StoreQueryResultsPolicy storeQueryResultsPolicy =
            Optional.ofNullable(options.getOption(STORE_QUERY_RESULTS.getOptionName()))
                .map(
                    o ->
                        PlannerSettings.StoreQueryResultsPolicy.valueOf(
                            o.getStringVal().toUpperCase(Locale.ROOT)))
                .orElse(PlannerSettings.StoreQueryResultsPolicy.NO);
        Span.current()
            .setAttribute(
                "dremio.planner.store_query_results_policy", storeQueryResultsPolicy.name());
        if (storeQueryResultsPolicy
            == PlannerSettings.StoreQueryResultsPolicy.PATH_AND_ATTEMPT_ID) {
          // update writing path for this case only
          prel = WriterPathUpdater.update(prel, config);
        }

        textPlan = PrelSequencer.getPlanText(prel, SqlExplainLevel.ALL_ATTRIBUTES);
        final String jsonPlan = PrelSequencer.getPlanJson(prel, SqlExplainLevel.ALL_ATTRIBUTES);
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug(String.format("%s:\n%s", "Final Physical Transformation", textPlan));
        }
        config.getObserver().planText(textPlan, 0);
        config.getObserver().planJsonPlan(jsonPlan);
      }

      prel = postConvertToPrel(prel);

      PhysicalOperator pop = PrelTransformer.convertToPop(config, prel);
      pop = postConvertToPhysicalOperator(pop);

      PhysicalPlan plan = PrelTransformer.convertToPlan(config, pop);
      plan = postConvertToPhysicalPlan(plan);

      PlanLogUtil.log(config, "Dremio Plan", plan, LOGGER);
      this.prel = prel;
      return plan;
    } catch (Error ex) {
      throw SqlExceptionHelper.coerceError(sql, ex);
    } catch (Exception ex) {
      throw SqlExceptionHelper.coerceException(LOGGER, sql, ex, true);
    }
  }

  @VisibleForTesting
  public Prel getPrel() {
    return prel;
  }

  @Override
  public String getTextPlan() {
    return textPlan;
  }

  @Override
  public Rel getLogicalPlan() {
    return drel;
  }

  protected void prePlan(SqlHandlerConfig config, String sql, SqlNode sqlNode) {}

  protected ConvertedRelNode postConvertToRel(ConvertedRelNode rel) {
    return rel;
  }

  protected Rel postConvertToDrel(Rel drel) {
    return drel;
  }

  protected Prel postConvertToPrel(Prel prel) {
    return prel;
  }

  protected PhysicalOperator postConvertToPhysicalOperator(PhysicalOperator pop) {
    return pop;
  }

  protected PhysicalPlan postConvertToPhysicalPlan(PhysicalPlan plan) {
    return plan;
  }

  protected static final class UncacheableFunctionDetectedEventHandler
      implements PlannerEventHandler<FunctionDetectedEvent> {
    private final List<SqlOperator> uncacheableFunctions;

    public UncacheableFunctionDetectedEventHandler() {
      uncacheableFunctions = new ArrayList<>();
    }

    @Override
    public void handle(FunctionDetectedEvent event) {
      if (UncacheableFunctionDetector.isA(event.getSqlOperator())) {
        uncacheableFunctions.add(event.getSqlOperator());
      }
    }

    @Override
    public Class<FunctionDetectedEvent> supports() {
      return FunctionDetectedEvent.class;
    }

    public List<SqlOperator> getUncacheableFunctions() {
      return uncacheableFunctions;
    }
  }

  protected ConvertedRelNode getConvertedRelNode() {
    return convertedRelNode;
  }

  public UncacheableFunctionDetectedEventHandler getUncacheableFunctionDetectedEventHandler() {
    return uncacheableFunctionDetectedEventHandler;
  }
}
