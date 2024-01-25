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

import java.util.Optional;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;

import com.dremio.common.config.SabotConfig;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.EntityExplorer;
import com.dremio.exec.ops.SnapshotDiffContext;
import com.dremio.exec.planner.acceleration.ExpansionNode;
import com.dremio.exec.planner.acceleration.IncrementalUpdateUtils;
import com.dremio.exec.planner.acceleration.IncrementalUpdateUtils.MaterializationShuttle;
import com.dremio.exec.planner.acceleration.StrippingFactory;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.serialization.RelSerializerFactory;
import com.dremio.exec.planner.sql.handlers.RelTransformer;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.store.CatalogService;
import com.dremio.options.OptionManager;
import com.dremio.proto.model.UpdateId;
import com.dremio.service.Pointer;
import com.dremio.service.namespace.dataset.proto.AccelerationSettings;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.RefreshMethod;
import com.dremio.service.reflection.IncrementalUpdateServiceUtils;
import com.dremio.service.reflection.ReflectionOptions;
import com.dremio.service.reflection.ReflectionService;
import com.dremio.service.reflection.ReflectionSettings;
import com.dremio.service.reflection.ReflectionUtils;
import com.dremio.service.reflection.proto.Materialization;
import com.dremio.service.reflection.proto.ReflectionEntry;
import com.dremio.service.reflection.proto.ReflectionGoal;
import com.dremio.service.reflection.proto.ReflectionType;
import com.dremio.service.reflection.proto.RefreshDecision;
import com.dremio.service.reflection.store.MaterializationStore;
import com.google.common.base.Preconditions;

class ReflectionPlanNormalizer implements RelTransformer {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ReflectionPlanNormalizer.class);

  private final SqlHandlerConfig sqlHandlerConfig;
  private final ReflectionGoal goal;
  private final ReflectionEntry entry;
  private final Materialization materialization;
  private final CatalogService catalogService;
  private final SabotConfig config;
  private final ReflectionSettings reflectionSettings;
  private final MaterializationStore materializationStore;
  private final OptionManager optionManager;
  private final boolean forceFullUpdate;
  private final int stripVersion;
  private final boolean isRebuildPlan;

  private RefreshDecision refreshDecision;
  private SnapshotDiffContext snapshotDiffContext = SnapshotDiffContext.NO_SNAPSHOT_DIFF;

  public ReflectionPlanNormalizer(
    SqlHandlerConfig sqlHandlerConfig,
    ReflectionGoal goal,
    ReflectionEntry entry,
    Materialization materialization,
    CatalogService catalogService,
    SabotConfig config,
    ReflectionSettings reflectionSettings,
    MaterializationStore materializationStore,
    boolean forceFullUpdate,
    int stripVersion,
    boolean isRebuildPlan) {
    this.sqlHandlerConfig = sqlHandlerConfig;
    this.goal = goal;
    this.entry = entry;
    this.materialization = materialization;
    this.catalogService = catalogService;
    this.config = config;
    this.reflectionSettings = reflectionSettings;
    this.materializationStore = materializationStore;
    this.optionManager = sqlHandlerConfig.getContext().getOptions();
    this.forceFullUpdate = forceFullUpdate;
    this.stripVersion = stripVersion;
    this.isRebuildPlan = isRebuildPlan;
  }

  public RefreshDecision getRefreshDecision() {
    return refreshDecision;
  }

  private static UserBitShared.ReflectionType mapReflectionType(ReflectionType type){
    switch(type) {
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
    // before we evaluate for reflections, we should review the tree to determine if there are any invalid nodes in the plan for reflection purposes.
    if(!ExpansionNode.findNodes(relNode, r -> r.isContextSensitive()).isEmpty()) {
      throw UserException.validationError()
        .message("Reflection could not be created as it uses context-sensitive functions. "
            + "Functions like IS_MEMBER, USER, etc. cannot be used in reflections since "
            + "they require context to complete.")
        .build(logger);
    }

    final RelNode datasetPlan = removeUpdateColumn(relNode);
    EntityExplorer catalog = CatalogUtil.getSystemCatalogForReflections(catalogService);
    DatasetConfig datasetConfig = CatalogUtil.getDatasetConfig(catalog, goal.getDatasetId());
    if (datasetConfig == null) {
      throw new IllegalStateException(String.format("Dataset %s not found for %s", goal.getDatasetId(), ReflectionUtils.getId(goal)));
    }
    final ReflectionExpander expander = new ReflectionExpander(datasetPlan, datasetConfig);
    final RelNode plan = expander.expand(goal);

    // we serialize the plan before normalization so we can recreate later.
    // we also store the plan with expansion nodes.
    final StrippingFactory factory = new StrippingFactory(optionManager, config);

    // normalize a tree without expansion nodes.
    RelNode strippedPlan = factory.strip(plan, mapReflectionType(goal.getType()), false, StrippingFactory.LATEST_STRIP_VERSION).getNormalized();

    // if we detect that the plan is in fact incrementally updateable after stripping and normalizing, we want to strip again with isIncremental flag set to true
    // to get the proper stripping
    ReflectionService service = sqlHandlerConfig.getContext().getAccelerationManager().unwrap(ReflectionService.class);

    if (IncrementalUpdateServiceUtils.extractRefreshDetails(strippedPlan, reflectionSettings, service, optionManager, isRebuildPlan, entry).getRefreshMethod() == RefreshMethod.INCREMENTAL) {
      strippedPlan = factory.strip(plan, mapReflectionType(goal.getType()), true, stripVersion).getNormalized();
    }

    Iterable<DremioTable> requestedTables = sqlHandlerConfig.getContext().getCatalog().getAllRequestedTables();

    final boolean strictRefresh = optionManager != null && optionManager.getOption(ReflectionOptions.STRICT_INCREMENTAL_REFRESH);
    final boolean isLegacy = optionManager != null && optionManager.getOption(PlannerSettings.LEGACY_SERIALIZER_ENABLED);
    final RelSerializerFactory serializerFactory =
      isLegacy ?
        RelSerializerFactory.getLegacyPlanningFactory(config, sqlHandlerConfig.getScanResult()) :
        RelSerializerFactory.getPlanningFactory(config, sqlHandlerConfig.getScanResult());


    final Pointer<SnapshotDiffContext> snapshotDiffContextPointer = new Pointer<>();
    this.refreshDecision = RefreshDecisionMaker.getRefreshDecision(
      entry,
      materialization,
      reflectionSettings,
      catalogService,
      materializationStore,
      plan,
      strippedPlan,
      requestedTables,
      serializerFactory,
      strictRefresh,
      forceFullUpdate,
      sqlHandlerConfig.getContext().getFunctionRegistry(),
      service,
      optionManager,
      snapshotDiffContextPointer,
      sqlHandlerConfig,
      isRebuildPlan);
    this.snapshotDiffContext = snapshotDiffContextPointer.value;
    if (!isRebuildPlan) {
      if (isIncremental(refreshDecision) && !isSnapshotBased(refreshDecision)) {
        strippedPlan = strippedPlan.accept(getIncremental(refreshDecision));
      }

      if (isSnapshotBased(refreshDecision) && !refreshDecision.getInitialRefresh()) {
        strippedPlan = strippedPlan.accept(new IncrementalUpdateUtils.AddSnapshotDiffContextShuttle(snapshotDiffContextPointer.value));
      }
    }

    return strippedPlan;
  }

  private static boolean isIncremental(RefreshDecision decision) {
    return decision.getAccelerationSettings().getMethod() == RefreshMethod.INCREMENTAL;
  }

  private static boolean isSnapshotBased (RefreshDecision decision) {
    return decision.getAccelerationSettings().getSnapshotBased();
  }

  private static RelShuttle getIncremental(RefreshDecision decision) {
    Preconditions.checkArgument(isIncremental(decision) && !isSnapshotBased(decision));
    return getShuttle(decision.getAccelerationSettings(), decision.getInitialRefresh(), decision.getUpdateId());
  }

  private static RelShuttle getShuttle(AccelerationSettings settings, boolean isInitialRefresh, UpdateId updateId) {
    return new MaterializationShuttle(
        Optional.ofNullable(settings.getRefreshField()).orElse(IncrementalUpdateUtils.UPDATE_COLUMN), isInitialRefresh, updateId);
  }

  public SnapshotDiffContext getSnapshotDiffContext() {
    return snapshotDiffContext;
  }

}
