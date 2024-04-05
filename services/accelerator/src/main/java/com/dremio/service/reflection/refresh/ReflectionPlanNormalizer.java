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
package com.dremio.service.reflection.refresh;

import static com.dremio.service.reflection.ReflectionUtils.removeUpdateColumn;

import com.dremio.common.config.SabotConfig;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.planner.acceleration.IncrementalUpdateUtils;
import com.dremio.exec.planner.acceleration.IncrementalUpdateUtils.MaterializationShuttle;
import com.dremio.exec.planner.acceleration.StrippingFactory;
import com.dremio.exec.planner.logical.EmptyRel;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.serialization.LogicalPlanSerializer;
import com.dremio.exec.planner.serialization.RelSerializerFactory;
import com.dremio.exec.planner.sql.CalciteArrowHelper;
import com.dremio.exec.planner.sql.DremioCompositeSqlOperatorTable;
import com.dremio.exec.planner.sql.handlers.RelTransformer;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.store.CatalogService;
import com.dremio.options.OptionManager;
import com.dremio.proto.model.UpdateId;
import com.dremio.service.namespace.dataset.proto.AccelerationSettings;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.RefreshMethod;
import com.dremio.service.reflection.IncrementalUpdateServiceUtils;
import com.dremio.service.reflection.ReflectionService;
import com.dremio.service.reflection.ReflectionSettings;
import com.dremio.service.reflection.ReflectionUtils;
import com.dremio.service.reflection.proto.Materialization;
import com.dremio.service.reflection.proto.ReflectionEntry;
import com.dremio.service.reflection.proto.ReflectionGoal;
import com.dremio.service.reflection.proto.ReflectionType;
import com.dremio.service.reflection.proto.RefreshDecision;
import com.dremio.service.reflection.refresh.ReflectionPlanGenerator.NonIncrementalRefreshFunctionEventHandler;
import com.dremio.service.reflection.refresh.ReflectionPlanGenerator.RefreshDecisionWrapper;
import com.dremio.service.reflection.store.DependenciesStore;
import com.dremio.service.reflection.store.MaterializationStore;
import com.google.common.base.Preconditions;
import io.protostuff.ByteString;
import java.util.Optional;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;

class ReflectionPlanNormalizer implements RelTransformer {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(ReflectionPlanNormalizer.class);

  private final SqlHandlerConfig sqlHandlerConfig;
  private final ReflectionGoal goal;
  private final ReflectionEntry entry;
  private final Materialization materialization;
  private final CatalogService catalogService;
  private final SabotConfig config;
  private final ReflectionSettings reflectionSettings;
  private final MaterializationStore materializationStore;
  private final DependenciesStore dependenciesStore;
  private final OptionManager optionManager;
  private final boolean forceFullUpdate;
  private final boolean matchingPlanOnly;
  private ByteString matchingPlanBytes;
  private RefreshDecisionWrapper refreshDecisionWrapper;
  private NonIncrementalRefreshFunctionEventHandler nonIncrementalRefreshFunctionEventHandler;

  public ReflectionPlanNormalizer(
      SqlHandlerConfig sqlHandlerConfig,
      ReflectionGoal goal,
      ReflectionEntry entry,
      Materialization materialization,
      CatalogService catalogService,
      SabotConfig config,
      ReflectionSettings reflectionSettings,
      MaterializationStore materializationStore,
      DependenciesStore dependenciesStore,
      boolean forceFullUpdate,
      boolean matchingPlanOnly,
      RefreshDecisionWrapper noDefaultReflectionDecisionWrapper,
      NonIncrementalRefreshFunctionEventHandler nonIncrementalRefreshFunctionEventHandler) {
    this.sqlHandlerConfig = sqlHandlerConfig;
    this.goal = goal;
    this.entry = entry;
    this.materialization = materialization;
    this.catalogService = catalogService;
    this.config = config;
    this.reflectionSettings = reflectionSettings;
    this.materializationStore = materializationStore;
    this.dependenciesStore = dependenciesStore;
    this.optionManager = sqlHandlerConfig.getContext().getOptions();
    this.forceFullUpdate = forceFullUpdate;
    this.matchingPlanOnly = matchingPlanOnly;
    this.refreshDecisionWrapper = noDefaultReflectionDecisionWrapper;
    this.nonIncrementalRefreshFunctionEventHandler = nonIncrementalRefreshFunctionEventHandler;
  }

  public RefreshDecisionWrapper getRefreshDecisionWrapper() {
    return refreshDecisionWrapper;
  }

  private static UserBitShared.ReflectionType mapReflectionType(ReflectionType type) {
    switch (type) {
      case AGGREGATION:
        return UserBitShared.ReflectionType.AGG;
      case EXTERNAL:
        return UserBitShared.ReflectionType.EXTERNAL;
      case RAW:
        return UserBitShared.ReflectionType.RAW;
      default:
        throw new IllegalStateException(type.name());
    }
  }

  @Override
  public RelNode transform(RelNode relNode) {
    final RelNode datasetPlan = removeUpdateColumn(relNode);
    DatasetConfig datasetConfig =
        CatalogUtil.getDatasetConfig(
            sqlHandlerConfig.getContext().getCatalog(), goal.getDatasetId());
    if (datasetConfig == null) {
      throw new IllegalStateException(
          String.format(
              "Dataset %s not found for %s", goal.getDatasetId(), ReflectionUtils.getId(goal)));
    }
    final ReflectionExpander expander = new ReflectionExpander(datasetPlan, datasetConfig);
    // Expanded to include the reflection's display or dimension fields
    final RelNode expandedPlan = expander.expand(goal);

    final boolean isLegacy =
        optionManager != null && optionManager.getOption(PlannerSettings.LEGACY_SERIALIZER_ENABLED);
    final RelSerializerFactory serializerFactory =
        isLegacy
            ? RelSerializerFactory.getLegacyPlanningFactory(
                config, sqlHandlerConfig.getScanResult())
            : RelSerializerFactory.getPlanningFactory(config, sqlHandlerConfig.getScanResult());
    final LogicalPlanSerializer serializer =
        serializerFactory.getSerializer(
            expandedPlan.getCluster(),
            DremioCompositeSqlOperatorTable.create(
                sqlHandlerConfig.getContext().getFunctionRegistry()));
    matchingPlanBytes = ByteString.copyFrom(serializer.serializeToBytes(expandedPlan));
    if (matchingPlanOnly) {
      return new EmptyRel(
          expandedPlan.getCluster(),
          expandedPlan.getTraitSet(),
          expandedPlan.getRowType(),
          CalciteArrowHelper.fromCalciteRowType(expandedPlan.getRowType()));
    }

    // we serialize the plan before normalization so we can recreate later.
    // we also store the plan with expansion nodes.
    final StrippingFactory factory = new StrippingFactory(optionManager, config);

    // normalize expanded plan to produce what we want to actually materialize
    RelNode strippedPlan =
        factory
            .strip(
                expandedPlan,
                mapReflectionType(goal.getType()),
                false,
                StrippingFactory.LATEST_STRIP_VERSION)
            .getNormalized();

    // if we detect that the plan is in fact incrementally updatable, we want to strip again with
    // isIncremental flag set to true to get the proper stripping (such as removing top level
    // projects)
    ReflectionService service =
        sqlHandlerConfig.getContext().getAccelerationManager().unwrap(ReflectionService.class);

    if (IncrementalUpdateServiceUtils.extractRefreshDetails(
                strippedPlan,
                reflectionSettings,
                service,
                optionManager,
                config,
                nonIncrementalRefreshFunctionEventHandler.isEventReceived())
            .getRefreshMethod()
        == RefreshMethod.INCREMENTAL) {
      strippedPlan =
          factory
              .strip(
                  expandedPlan,
                  mapReflectionType(goal.getType()),
                  true,
                  StrippingFactory.LATEST_STRIP_VERSION)
              .getNormalized();
    }

    Iterable<DremioTable> requestedTables =
        sqlHandlerConfig.getContext().getCatalog().getAllRequestedTables();

    if (this.refreshDecisionWrapper == null) {
      this.refreshDecisionWrapper =
          RefreshDecisionMaker.getRefreshDecision(
              entry,
              materialization,
              reflectionSettings,
              catalogService,
              materializationStore,
              dependenciesStore,
              matchingPlanBytes,
              expandedPlan,
              strippedPlan,
              requestedTables,
              forceFullUpdate,
              service,
              optionManager,
              sqlHandlerConfig,
              config,
              nonIncrementalRefreshFunctionEventHandler.isEventReceived());
    }

    final RefreshDecision refreshDecision = refreshDecisionWrapper.getRefreshDecision();
    if (refreshDecision.getNoOpRefresh()) {
      return new EmptyRel(
          expandedPlan.getCluster(),
          expandedPlan.getTraitSet(),
          expandedPlan.getRowType(),
          CalciteArrowHelper.fromCalciteRowType(expandedPlan.getRowType()));
    }
    if (isIncremental(refreshDecision) && !isSnapshotBased(refreshDecision)) {
      strippedPlan = strippedPlan.accept(getIncremental(refreshDecision));
    }

    if (isSnapshotBased(refreshDecision) && !refreshDecision.getInitialRefresh()) {
      strippedPlan =
          strippedPlan.accept(
              new IncrementalUpdateUtils.AddSnapshotDiffContextShuttle(
                  refreshDecisionWrapper.getSnapshotDiffContext()));
    }

    return strippedPlan;
  }

  private static boolean isIncremental(RefreshDecision decision) {
    return decision.getAccelerationSettings().getMethod() == RefreshMethod.INCREMENTAL;
  }

  private static boolean isSnapshotBased(RefreshDecision decision) {
    return decision.getAccelerationSettings().getSnapshotBased();
  }

  private static RelShuttle getIncremental(RefreshDecision decision) {
    Preconditions.checkArgument(isIncremental(decision) && !isSnapshotBased(decision));
    return getShuttle(
        decision.getAccelerationSettings(), decision.getInitialRefresh(), decision.getUpdateId());
  }

  private static RelShuttle getShuttle(
      AccelerationSettings settings, boolean isInitialRefresh, UpdateId updateId) {
    return new MaterializationShuttle(
        Optional.ofNullable(settings.getRefreshField())
            .orElse(IncrementalUpdateUtils.UPDATE_COLUMN),
        isInitialRefresh,
        updateId);
  }

  public ByteString getMatchingPlanBytes() {
    return matchingPlanBytes;
  }
}
