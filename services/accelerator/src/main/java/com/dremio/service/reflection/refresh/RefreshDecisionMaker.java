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

import static com.dremio.exec.store.iceberg.IncrementalReflectionByPartitionUtils.toDisplayString;
import static com.google.common.base.Preconditions.checkState;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.calcite.rel.RelNode;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.types.MinorType;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.EntityExplorer;
import com.dremio.exec.expr.fn.FunctionImplementationRegistry;
import com.dremio.exec.ops.SnapshotDiffContext;
import com.dremio.exec.planner.acceleration.PlanHasher;
import com.dremio.exec.planner.serialization.LogicalPlanSerializer;
import com.dremio.exec.planner.serialization.RelSerializerFactory;
import com.dremio.exec.planner.sql.DremioCompositeSqlOperatorTable;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.store.CatalogService;
import com.dremio.options.OptionManager;
import com.dremio.proto.model.UpdateId;
import com.dremio.service.Pointer;
import com.dremio.service.job.proto.ScanPath;
import com.dremio.service.namespace.dataset.proto.AccelerationSettings;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.IcebergMetadata;
import com.dremio.service.namespace.dataset.proto.RefreshMethod;
import com.dremio.service.reflection.DatasetHashUtils;
import com.dremio.service.reflection.IncrementalUpdateServiceUtils;
import com.dremio.service.reflection.ReflectionService;
import com.dremio.service.reflection.ReflectionSettings;
import com.dremio.service.reflection.proto.Materialization;
import com.dremio.service.reflection.proto.ReflectionEntry;
import com.dremio.service.reflection.proto.Refresh;
import com.dremio.service.reflection.proto.RefreshDecision;
import com.dremio.service.reflection.store.MaterializationStore;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.base.Stopwatch;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;

import io.protostuff.ByteString;

class RefreshDecisionMaker {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RefreshDecisionMaker.class);

  /**
   * Determine whether the provided materialization will be a partial or a full along with associated updateId, seriesId, etc.
   * @return The refresh decisions made
   */
  static RefreshDecision getRefreshDecision(
    ReflectionEntry entry,
    Materialization materialization,
    ReflectionSettings reflectionSettings,
    CatalogService catalogService,
    MaterializationStore materializationStore,
    RelNode plan,
    RelNode strippedPlan,
    Iterable<DremioTable> requestedTables,
    RelSerializerFactory serializerFactory,
    boolean strictRefresh,
    boolean forceFullUpdate,
    FunctionImplementationRegistry functionImplementationRegistry,
    ReflectionService service,
    OptionManager optionManager,
    Pointer<SnapshotDiffContext> snapshotDiffContextPointer,
    final SqlHandlerConfig sqlHandlerConfig,
    boolean isRebuildPlan) {

    Stopwatch stopwatch = Stopwatch.createStarted();
    final long newSeriesId = System.currentTimeMillis();

    final RefreshDecision decision = new RefreshDecision();

    // We load settings here to determine what type of update we need to do (full or incremental)
    final IncrementalUpdateServiceUtils.RefreshDetails refreshDetails = IncrementalUpdateServiceUtils.extractRefreshDetails(
      strippedPlan, reflectionSettings, service, optionManager, isRebuildPlan, entry);
    final AccelerationSettings settings = IncrementalUpdateServiceUtils.extractRefreshSettings(refreshDetails);

    decision.setAccelerationSettings(settings);

    // For snapshot based incremental refresh, save the base table's current snapshot id.
    if (settings.getSnapshotBased()) {
      checkState(refreshDetails.getBaseTableSnapshotId() != null);
      decision.setOutputUpdateId(new UpdateId()
        .setStringUpdateId(refreshDetails.getBaseTableSnapshotId())
        .setType(MinorType.VARCHAR)
        .setUpdateIdType(UpdateId.IdType.SNAPSHOT));
    }

    if (requestedTables != null && !Iterables.isEmpty(requestedTables)) {
      // store all physical dataset paths in the refresh decision
      final List<ScanPath> scanPathsList = FluentIterable.from(requestedTables)
        .filter(new Predicate<DremioTable>() {
          @Override
          public boolean apply(DremioTable table) {
            final DatasetConfig dataset = table.getDatasetConfig();
            return dataset != null && DatasetHashUtils.isPhysicalDataset(dataset.getType());
          }
        }).transform(new Function<DremioTable, ScanPath>() {
          @Override
          public ScanPath apply(DremioTable table) {
            final List<String> datasetPath = table.getPath().getPathComponents();
            ScanPath path = new ScanPath().setPathList(datasetPath);
            if (table.getDataset().getVersionContext() != null) {
              path.setVersionContext(table.getDataset().getVersionContext().serialize());
            }
            // save the snapshot ID to the scanpath, so it can be used in the DependencyGraph for reflections
            IcebergMetadata icebergMetadata = table.getDatasetConfig().getPhysicalDataset().getIcebergMetadata();
            if (icebergMetadata != null) {
              path.setSnapshotId(icebergMetadata.getSnapshotId());
            }
            return path;
          }
        }).toList();
      decision.setScanPathsList(scanPathsList);
    }

    final LogicalPlanSerializer serializer = serializerFactory
      .getSerializer(
        plan.getCluster(),
        DremioCompositeSqlOperatorTable.create(functionImplementationRegistry));
    decision.setLogicalPlan(ByteString.copyFrom(serializer.serializeToBytes(plan)));
    decision.setLogicalPlanStrippedHash(PlanHasher.hash(strippedPlan));

    // Rebuilding of logical plan during upgrade only requires (1) logical plan and (2) logical plan stripped hash
    // from the refresh decision. No need to proceed with further calculations.
    if (isRebuildPlan) {
      return decision;
    }

    if(settings.getMethod() == RefreshMethod.FULL) {
      String fullRefreshReason = refreshDetails.getFullRefreshReason();
      if(fullRefreshReason == null || fullRefreshReason.isEmpty()){
        fullRefreshReason = "Incremental either not set or not supported for this query.";
      }
      sqlHandlerConfig.getObserver().planRefreshDecision("Full Refresh. \n" + fullRefreshReason,
        stopwatch.elapsed(TimeUnit.MILLISECONDS));
      logger.trace(fullRefreshReason);
      return decision.setInitialRefresh(true)
        .setSeriesId(newSeriesId);
    }

    // This is an incremental update dataset.
    // if we already have valid refreshes, we should use their seriesId
    final Refresh refresh = materializationStore.getMostRecentRefresh(materialization.getReflectionId());

    final Integer entryDatasetHash;
    final Integer decisionDatasetHash;
    final EntityExplorer catalog = CatalogUtil.getSystemCatalogForReflections(catalogService);
    try {
      DatasetConfig datasetConfig = CatalogUtil.getDatasetConfig(catalog, entry.getDatasetId());
      if (!strictRefresh) {
        if (entry.getShallowDatasetHash() == null && refresh != null) {
          decisionDatasetHash = DatasetHashUtils.computeDatasetHash(datasetConfig, catalogService, strippedPlan, false);
          decision.setDatasetHash(DatasetHashUtils.computeDatasetHash(datasetConfig, catalogService, strippedPlan,true));
          entryDatasetHash = entry.getDatasetHash();
        } else {
          decisionDatasetHash = DatasetHashUtils.computeDatasetHash(datasetConfig, catalogService, strippedPlan,true);
          decision.setDatasetHash(decisionDatasetHash);
          entryDatasetHash = entry.getShallowDatasetHash();
        }
      } else {
        decisionDatasetHash = DatasetHashUtils.computeDatasetHash(datasetConfig, catalogService, strippedPlan,false);
        decision.setDatasetHash(decisionDatasetHash);
        entryDatasetHash = entry.getDatasetHash();
      }
    } catch (Exception e) {
      throw UserException.validationError()
        .message("Couldn't expand a materialized view on a non existing dataset")
        .addContext("reflectionId", entry.getId().getId())
        .addContext("datasetId", entry.getDatasetId())
        .build(logger);
    }

    // if this the first refresh of this materialization, let's do a initial refresh.
    if(refresh == null) {
      sqlHandlerConfig.getObserver().planRefreshDecision("Full Refresh. \nNo existing refresh, doing an initial full refresh.", stopwatch.elapsed(TimeUnit.MILLISECONDS));
      logger.trace("No existing refresh, doing an initial refresh.");
      return decision.setInitialRefresh(true)
        .setUpdateId(new UpdateId())
        .setSeriesId(newSeriesId);
    }

    if (forceFullUpdate) {
      sqlHandlerConfig.getObserver().planRefreshDecision("Full Refresh. \nForcing full update, possible reason is switching from Non-Iceberg reflection to Iceberg reflection or vice versa.", stopwatch.elapsed(TimeUnit.MILLISECONDS));
      logger.trace("Forcing full update.");
      return decision.setInitialRefresh(true)
        .setUpdateId(new UpdateId())
        .setSeriesId(newSeriesId);
    }

    // If the refresh settings changed, do an initial refresh.
    // - Full -> Incremental
    if (entry.getRefreshMethod() != settings.getMethod() ||
      // - Refresh field changed in setting (ignored by snapshot based refresh).
      (!Objects.equal(entry.getRefreshField(), settings.getRefreshField()) && !settings.getSnapshotBased()) ||
      // - Snapshot based incremental -> non-snapshot based incremental (support key on -> off)
      (entry.getSnapshotBased() && !settings.getSnapshotBased())) {
      logger.trace("Change in refresh method, doing an initial refresh.");
      sqlHandlerConfig.getObserver().planRefreshDecision("Full Refresh. \nChange in refresh method, doing an initial refresh.", stopwatch.elapsed(TimeUnit.MILLISECONDS));
      return decision.setInitialRefresh(true)
        .setUpdateId(new UpdateId())
        .setSeriesId(newSeriesId);
    }

    // Non-snapshot based incremental -> snapshot based incremental:
    // Avoid initial full refresh during upgrade to snapshot based incremental refresh method.
    // - Current refresh will continue to use old incremental refresh method (mtime or field based).
    // - Current table snapshot id has been saved into RefreshDecision.outputUpdateId (UpdateId.IdType.SNAPSHOT).
    // - At refreshDoneHandler, RefreshDecision.outputUpdateId will be saved into refresh entry.
    // - For following refreshes, this check will fail, snapshot based incremental method will be used.
    if (settings.getSnapshotBased() &&
      (entry.getRefreshMethod() == RefreshMethod.INCREMENTAL) && !entry.getSnapshotBased() &&
      (refresh.getUpdateId().getUpdateIdType() != UpdateId.IdType.SNAPSHOT)) {
      settings.setSnapshotBased(false);
    }

    if (!Objects.equal(entryDatasetHash, decisionDatasetHash)) {
      logger.trace("Change in dataset hash, doing an initial refresh.");
      sqlHandlerConfig.getObserver().planRefreshDecision("Full Refresh. \nChange in dataset hash, doing an initial refresh.", stopwatch.elapsed(TimeUnit.MILLISECONDS));
      return decision.setInitialRefresh(true)
        .setUpdateId(new UpdateId())
        .setSeriesId(newSeriesId);
    }

    if (settings.getSnapshotBased()) {
      final SnapshotBasedIncrementalRefreshDecisionMaker decisionMaker = new SnapshotBasedIncrementalRefreshDecisionMaker(
        catalogService,
        refreshDetails.getBaseTableMetadata(),      // base table current state
        refreshDetails.getBaseTableSnapshotId(),    // base table current state
        refresh.getUpdateId().getStringUpdateId(),
        optionManager,
        materializationStore,
        materialization,
        service,
        entry,
        strippedPlan);     // base table snapshot as of the last refresh
      final SnapshotDiffContext snapshotDiffContext = decisionMaker.getDecision();
      if (snapshotDiffContext == null) {
        logger.trace("Full Refresh. \n" + decisionMaker.getFullRefreshReason());
        sqlHandlerConfig.getObserver().planRefreshDecision("Full Refresh. \n" + decisionMaker.getFullRefreshReason(), stopwatch.elapsed(TimeUnit.MILLISECONDS));
        return decision.setInitialRefresh(true)
          .setUpdateId(new UpdateId())
          .setSeriesId(newSeriesId);
      }
      snapshotDiffContextPointer.value = snapshotDiffContext;
    }

    sqlHandlerConfig.getObserver().planRefreshDecision(getIncrementalRefreshDisplayText(snapshotDiffContextPointer.value),
      stopwatch.elapsed(TimeUnit.MILLISECONDS));

    return decision.setInitialRefresh(false)
      .setUpdateId(refresh.getUpdateId())
      .setSeriesId(refresh.getSeriesId())
      .setSeriesOrdinal(refresh.getSeriesOrdinal() + 1);
  }

  private static String getIncrementalRefreshDisplayText(final SnapshotDiffContext snapshotDiffContext) {
    if (snapshotDiffContext == null) {
      return "Incremental Refresh";
    }
    switch (snapshotDiffContext.getFilterApplyOptions()) {
      case FILTER_DATA_FILES: {
        if(snapshotDiffContext.isSameSnapshot()){
          return "No changes are detected in the base dataset since the last refresh. The reflection will not be updated.";
        }
        return "Snapshot Based Incremental Refresh for Append Only workflows.";
      }
      case FILTER_PARTITIONS: {
        return "Snapshot Based Incremental Refresh by Partition." +
          "\nBase Dataset Target Partitions: " + toDisplayString(snapshotDiffContext.getBaseDatasetTargetPartitionSpec()) +
          "\nReflection Target Partitions: " + toDisplayString(snapshotDiffContext.getReflectionPartitionSpec());
      }
      default: {
        return "Incremental Refresh";
      }
    }
  }
}
