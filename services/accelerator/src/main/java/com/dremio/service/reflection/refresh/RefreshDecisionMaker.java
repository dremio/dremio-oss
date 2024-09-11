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
import static com.dremio.service.namespace.dataset.proto.DatasetType.VIRTUAL_DATASET;
import static com.google.common.base.Preconditions.checkState;

import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.types.MinorType;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.EntityExplorer;
import com.dremio.exec.catalog.TableMetadataVerifyDataModifiedRequest;
import com.dremio.exec.catalog.TableMetadataVerifyDataModifiedResult;
import com.dremio.exec.catalog.TableMetadataVerifyResult;
import com.dremio.exec.ops.SnapshotDiffContext;
import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.exec.planner.acceleration.ExpansionNode;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.options.OptionManager;
import com.dremio.proto.model.MultiDatasetUpdateId;
import com.dremio.proto.model.SingleDatasetUpdateId;
import com.dremio.proto.model.UpdateId;
import com.dremio.service.Pointer;
import com.dremio.service.job.proto.ScanPath;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.AccelerationSettings;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.IcebergMetadata;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.dataset.proto.RefreshMethod;
import com.dremio.service.reflection.DatasetHashUtils;
import com.dremio.service.reflection.DependencyEntry;
import com.dremio.service.reflection.DependencyEntry.DatasetDependency;
import com.dremio.service.reflection.DependencyEntry.ReflectionDependency;
import com.dremio.service.reflection.DependencyEntry.TableFunctionDependency;
import com.dremio.service.reflection.IncrementalUpdateServiceUtils;
import com.dremio.service.reflection.ReflectionOptions;
import com.dremio.service.reflection.ReflectionService;
import com.dremio.service.reflection.ReflectionSettings;
import com.dremio.service.reflection.proto.Materialization;
import com.dremio.service.reflection.proto.ReflectionEntry;
import com.dremio.service.reflection.proto.Refresh;
import com.dremio.service.reflection.proto.RefreshDecision;
import com.dremio.service.reflection.refresh.ReflectionPlanGenerator.RefreshDecisionWrapper;
import com.dremio.service.reflection.store.DependenciesStore;
import com.dremio.service.reflection.store.MaterializationStore;
import com.google.common.base.Objects;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;
import io.protostuff.ByteString;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.sql.SqlOperator;

class RefreshDecisionMaker {

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(RefreshDecisionMaker.class);
  private static IncrementalReflectionRefreshValidator DEFAULT_REFLECTION_DEPENDENCY_VALIDATOR =
      new IncrementalReflectionRefreshValidator();
  private static final String REFLECTION_REFRESH_DEPENDENCY_VALIDATOR =
      "dremio.reflection.refresh.reflection-refresh-dependency-validator.class";

  /**
   * Determine whether the provided materialization will be a partial or a full along with
   * associated updateId, seriesId, etc.
   *
   * @return The refresh decisions made
   */
  static RefreshDecisionWrapper getRefreshDecision(
      ReflectionEntry entry,
      Materialization materialization,
      ReflectionSettings reflectionSettings,
      CatalogService catalogService,
      MaterializationStore materializationStore,
      DependenciesStore dependencyStore,
      ByteString matchingPlanBytes,
      RelNode expandedPlan,
      RelNode strippedPlan,
      Iterable<DremioTable> requestedTables,
      boolean forceFullUpdate,
      ReflectionService service,
      OptionManager optionManager,
      final SqlHandlerConfig sqlHandlerConfig,
      final SabotConfig config,
      List<SqlOperator> nonIncrementalRefreshFunctions) {
    Stopwatch stopwatch = Stopwatch.createStarted();
    final long newSeriesId = System.currentTimeMillis();

    final RefreshDecision decision = new RefreshDecision();

    // We load settings here to determine what type of update we need to do (full or incremental)
    final IncrementalUpdateServiceUtils.RefreshDetails refreshDetails =
        IncrementalUpdateServiceUtils.extractRefreshDetails(
            strippedPlan,
            reflectionSettings,
            service,
            optionManager,
            config,
            nonIncrementalRefreshFunctions);
    final AccelerationSettings settings =
        IncrementalUpdateServiceUtils.extractRefreshSettings(refreshDetails);

    decision.setAccelerationSettings(settings);
    if (requestedTables != null && !Iterables.isEmpty(requestedTables)) {
      // store all physical dataset paths in the refresh decision
      final List<ScanPath> scanPathsList =
          StreamSupport.stream(requestedTables.spliterator(), false)
              .filter(
                  table -> {
                    final DatasetConfig dataset = table.getDatasetConfig();
                    return dataset != null && DatasetHashUtils.isPhysicalDataset(dataset.getType());
                  })
              .map(
                  table -> {
                    final List<String> datasetPath = table.getPath().getPathComponents();
                    ScanPath path = new ScanPath().setPathList(datasetPath);
                    if (table.getDataset().getVersionContext() != null) {
                      path.setVersionContext(table.getDataset().getVersionContext().serialize());
                    }
                    // save the snapshot ID to the scanpath, so it can be used in the
                    // DependencyGraph for reflections
                    IcebergMetadata icebergMetadata =
                        table.getDatasetConfig().getPhysicalDataset().getIcebergMetadata();
                    if (icebergMetadata != null) {
                      path.setSnapshotId(icebergMetadata.getSnapshotId());
                    }
                    return path;
                  })
              .collect(Collectors.toList());
      decision.setScanPathsList(scanPathsList);
    }

    final EntityExplorer catalog = sqlHandlerConfig.getContext().getCatalog();
    final Integer expandedPlanDatasetHash =
        DatasetHashUtils.computeDatasetHash(catalog, expandedPlan, true);
    decision.setDatasetHash(expandedPlanDatasetHash);
    decision.setLogicalPlan(matchingPlanBytes);

    final Refresh refresh =
        materializationStore.getMostRecentRefresh(materialization.getReflectionId());

    if (settings.getSnapshotBased() && settings.getMethod() == RefreshMethod.INCREMENTAL) {
      decision.setOutputUpdateId(
          buildSnapshotBasedOutputUpdateId(strippedPlan, refreshDetails.getBaseTableSnapshotId()));
    }

    // if this the first refresh of this materialization, let's do an initial refresh.
    if (refresh == null) {
      logger.trace("No existing refresh, doing an initial refresh.");
      return new RefreshDecisionWrapper(
          decision.setInitialRefresh(true).setUpdateId(new UpdateId()).setSeriesId(newSeriesId),
          null,
          "Full Refresh. \nNo existing refresh, doing an initial full refresh.",
          stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }

    final Integer entryDatasetHash;
    final Integer decisionDatasetHash;
    try {
      if (entry.getExpandedPlanDatasetHash() != null) {
        decisionDatasetHash = expandedPlanDatasetHash;
        entryDatasetHash = entry.getExpandedPlanDatasetHash();
      } else {
        // Deprecated dataset change detection logic based on the stripped instead of expanded
        // plan
        final DatasetConfig datasetConfig =
            CatalogUtil.getDatasetConfig(catalog, entry.getDatasetId());
        final RelNode deprecatedStrippedPlan;
        // Fix up the strippedPlan to start with the expansion node.  This only applies to
        // incremental refresh where we stripped the expansion nodes.
        if (datasetConfig.getType() == VIRTUAL_DATASET
            && MoreRelOptUtil.countRelNodes(strippedPlan, ExpansionNode.class::isInstance) == 0) {
          deprecatedStrippedPlan =
              ExpansionNode.wrap(
                  new NamespaceKey(datasetConfig.getFullPathList()),
                  strippedPlan,
                  strippedPlan.getRowType(),
                  false,
                  null,
                  null);
        } else {
          deprecatedStrippedPlan = strippedPlan;
        }
        decisionDatasetHash =
            DatasetHashUtils.computeDatasetHash(catalog, deprecatedStrippedPlan, true);
        entryDatasetHash = entry.getShallowDatasetHash();
      }
    } catch (Exception e) {
      throw UserException.validationError(e)
          .message("Couldn't expand a materialized view on a non existing dataset")
          .addContext("reflectionId", entry.getId().getId())
          .addContext("datasetId", entry.getDatasetId())
          .build(logger);
    }

    // next, make sure there were no view or table changes that would force a full refresh
    if (!Objects.equal(entryDatasetHash, decisionDatasetHash)) {
      return new RefreshDecisionWrapper(
          decision.setInitialRefresh(true).setUpdateId(new UpdateId()).setSeriesId(newSeriesId),
          null,
          "Full Refresh. \nSQL or Schema change detected in a view or table used in reflection.",
          stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }

    // If the refresh settings changed, do an initial refresh.
    // - Full -> Incremental
    if (entry.getRefreshMethod() != settings.getMethod()
        ||
        // - Refresh field changed in setting (ignored by snapshot based refresh).
        (!Objects.equal(entry.getRefreshField(), settings.getRefreshField())
            && !settings.getSnapshotBased())
        ||
        // - Snapshot based incremental -> non-snapshot based incremental (support key on -> off)
        (entry.getSnapshotBased() && !settings.getSnapshotBased())) {
      logger.trace("Change in refresh method, doing an initial refresh.");
      return new RefreshDecisionWrapper(
          decision.setInitialRefresh(true).setUpdateId(new UpdateId()).setSeriesId(newSeriesId),
          null,
          "Full Refresh. \nChange in refresh method, doing an initial refresh.",
          stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }

    String dependencyRefreshReason = "";
    if (settings.getMethod() == RefreshMethod.FULL) {
      // before doing a full refresh, check if we can skip refreshing due to no new data
      if (optionManager.getOption(ReflectionOptions.REFLECTION_MANAGER_AVOID_REDUNDANT_REFRESH)) {
        dependencyRefreshReason =
            hasNewSnapshotsForRefresh(
                entry, catalog, dependencyStore.get(entry.getId()), materializationStore);
        // reflections containing dynamic functions cannot noop refresh
        if (dependencyRefreshReason.isEmpty() && nonIncrementalRefreshFunctions.isEmpty()) {
          logger.trace("Noop refresh due to no new snapshots");
          return new RefreshDecisionWrapper(
              decision
                  .setInitialRefresh(false)
                  .setSeriesId(refresh.getSeriesId())
                  .setUpdateId(refresh.getUpdateId())
                  .setNoOpRefresh(true),
              null,
              "No changes were detected in dependencies since the last refresh. The reflection will not be updated.",
              stopwatch.elapsed(TimeUnit.MILLISECONDS));
        }
      }
      String fullRefreshReason = refreshDetails.getFullRefreshReason();
      if (fullRefreshReason == null || fullRefreshReason.isEmpty()) {
        fullRefreshReason = "Incremental either not set or not supported for this query.";
      }

      logger.trace(fullRefreshReason);
      return new RefreshDecisionWrapper(
          decision.setInitialRefresh(true).setSeriesId(newSeriesId),
          null,
          String.format("Full Refresh. \n%s\n%s", fullRefreshReason, dependencyRefreshReason),
          stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }

    if (forceFullUpdate) {
      logger.trace("Forcing full update.");
      return new RefreshDecisionWrapper(
          decision.setInitialRefresh(true).setUpdateId(new UpdateId()).setSeriesId(newSeriesId),
          null,
          "Full Refresh. \nForcing full update, possible reason is switching from Non-Iceberg reflection to Iceberg reflection or vice versa.",
          stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }

    // Non-snapshot based incremental -> snapshot based incremental:
    // Avoid initial full refresh during upgrade to snapshot based incremental refresh method.
    // - Current refresh will continue to use old incremental refresh method (mtime or field based).
    // - Current table snapshot id has been saved into RefreshDecision.outputUpdateId
    // (UpdateId.IdType.SNAPSHOT).
    // - At refreshDoneHandler, RefreshDecision.outputUpdateId will be saved into refresh entry.
    // - For following refreshes, this check will fail, snapshot based incremental method will be
    // used.
    if (settings.getSnapshotBased()
        && (entry.getRefreshMethod() == RefreshMethod.INCREMENTAL)
        && !entry.getSnapshotBased()
        && (refresh.getUpdateId().getUpdateIdType() != UpdateId.IdType.SNAPSHOT
            && refresh.getUpdateId().getUpdateIdType() != UpdateId.IdType.MULTI_DATASET)) {
      settings.setSnapshotBased(false);
    }

    Pointer<TableMetadata> tableMetadataPointer =
        new Pointer<>(refreshDetails.getBaseTableMetadata());
    Pointer<String> tableMetadataCurrentSnapshotID =
        new Pointer<>(refreshDetails.getBaseTableSnapshotId());
    Pointer<String> previousRefreshSnapshotID =
        new Pointer<>(refresh.getUpdateId().getStringUpdateId());
    SnapshotDiffContext snapshotDiffContext = null;
    if (settings.getSnapshotBased()) {
      Pointer<String> fullRefreshReason = new Pointer<>();
      IncrementalReflectionRefreshValidator reflectionRefreshDependencyValidator =
          config.getInstance(
              REFLECTION_REFRESH_DEPENDENCY_VALIDATOR,
              IncrementalReflectionRefreshValidator.class,
              DEFAULT_REFLECTION_DEPENDENCY_VALIDATOR);
      if (reflectionRefreshDependencyValidator.validate(
          strippedPlan,
          fullRefreshReason,
          tableMetadataPointer,
          tableMetadataCurrentSnapshotID,
          previousRefreshSnapshotID,
          refresh.getUpdateId())) {
        final SnapshotBasedIncrementalRefreshDecisionMaker decisionMaker =
            new SnapshotBasedIncrementalRefreshDecisionMaker(
                catalogService,
                tableMetadataPointer.value, // base table current state
                tableMetadataCurrentSnapshotID.value, // base table current state
                previousRefreshSnapshotID.value,
                optionManager,
                materializationStore,
                materialization,
                service,
                entry,
                strippedPlan); // base table snapshot as of the last refresh
        snapshotDiffContext = decisionMaker.getDecision();
        fullRefreshReason.value = decisionMaker.getFullRefreshReason();
      }
      if (snapshotDiffContext == null) {
        logger.trace("Full Refresh. \n" + fullRefreshReason.value);
        return new RefreshDecisionWrapper(
            decision.setInitialRefresh(true).setUpdateId(new UpdateId()).setSeriesId(newSeriesId),
            null,
            "Full Refresh. \n" + fullRefreshReason.value,
            stopwatch.elapsed(TimeUnit.MILLISECONDS));
      }
    }

    dependencyRefreshReason =
        hasNewSnapshotsForRefresh(
            entry, catalog, dependencyStore.get(entry.getId()), materializationStore);
    boolean canNoOpRefresh =
        optionManager.getOption(ReflectionOptions.REFLECTION_MANAGER_AVOID_REDUNDANT_REFRESH)
            ? dependencyRefreshReason.isEmpty()
            : (snapshotDiffContext != null && snapshotDiffContext.isSameSnapshot());

    if (canNoOpRefresh) {
      decision.setNoOpRefresh(true);
    }
    return new RefreshDecisionWrapper(
        decision
            .setInitialRefresh(false)
            .setUpdateId(refresh.getUpdateId())
            .setSeriesId(refresh.getSeriesId())
            .setSeriesOrdinal(refresh.getSeriesOrdinal() + 1),
        snapshotDiffContext,
        getIncrementalRefreshDisplayText(
            snapshotDiffContext,
            tableMetadataPointer.value,
            tableMetadataCurrentSnapshotID.value,
            previousRefreshSnapshotID.value,
            canNoOpRefresh),
        stopwatch.elapsed(TimeUnit.MILLISECONDS));
  }

  /**
   * Build an UpdateId for a map between a dataset IDs and their corresponding snapshot IDs If a
   * dataset is part of the plan multiple times with different snapshotIDs we will save an empty
   * snapshot id, as that case is not supported
   */
  private static UpdateId buildSnapshotBasedOutputUpdateId(RelNode plan, String currentSnapshotID) {
    Map<String, String> currentDatasetIdToSnapshotIdMap = new HashMap<>();
    plan.accept(
        new StatelessRelShuttleImpl() {
          @Override
          public RelNode visit(TableScan scan) {
            if (scan instanceof ScanRelBase) {
              final String datasetID =
                  ((ScanRelBase) scan).getTableMetadata().getDatasetConfig().getId().getId();
              String foundSnapshotID = currentDatasetIdToSnapshotIdMap.get(datasetID);
              Optional<String> currentSnapshotID =
                  IcebergUtils.getCurrentSnapshotId(
                      ((ScanRelBase) scan).getTableMetadata().getDatasetConfig());
              if (foundSnapshotID != null
                  && !foundSnapshotID.equals(currentSnapshotID.orElse(""))) {
                // the same table appears twice in the query with different snapshot IDs
                // this is unsupported time travel, so we save an empty snapshot for this table
                currentSnapshotID = Optional.of("");
              }
              currentDatasetIdToSnapshotIdMap.put(datasetID, currentSnapshotID.orElse(""));
            }
            return scan;
          }
        });

    if (currentDatasetIdToSnapshotIdMap.size() <= 1) {
      checkState(currentSnapshotID != null);
      // legacy case
      // there is no getDatasetIdToSnapshotIdMap or its size is 0 or 1
      // we don't need to use complex data structure to store the dataset snapshots
      return new UpdateId()
          .setStringUpdateId(currentSnapshotID)
          .setType(MinorType.VARCHAR)
          .setUpdateIdType(UpdateId.IdType.SNAPSHOT);
    } else {
      // if there are multiple datasets involved we need to use the
      // MultiDatasetUpdateId data structure to save the dataset state
      final UpdateId updateId = new UpdateId();
      final MultiDatasetUpdateId multiDatasetUpdateId = new MultiDatasetUpdateId();
      multiDatasetUpdateId.setSingleDatasetUpdateIdList(new ArrayList<>());
      for (Map.Entry<String, String> entry : currentDatasetIdToSnapshotIdMap.entrySet()) {
        final SingleDatasetUpdateId singleDatasetUpdateId = new SingleDatasetUpdateId();
        singleDatasetUpdateId.setDatasetId(entry.getKey());
        singleDatasetUpdateId.setSnapshotId(entry.getValue());
        multiDatasetUpdateId.getSingleDatasetUpdateIdList().add(singleDatasetUpdateId);
      }
      updateId.setMultiDatasetUpdateId(multiDatasetUpdateId);
      updateId.setUpdateIdType(UpdateId.IdType.MULTI_DATASET);
      updateId.setType(MinorType.GENERIC_OBJECT);
      return updateId;
    }
  }

  private static String getIncrementalRefreshDisplayText(
      final SnapshotDiffContext snapshotDiffContext,
      TableMetadata tableMetadata,
      String currentSnapshotId,
      String oldSnapshotID,
      boolean canNoOpRefresh) {
    if (snapshotDiffContext == null) {
      return "Incremental Refresh";
    }
    switch (snapshotDiffContext.getFilterApplyOptions()) {
      case FILTER_DATA_FILES:
        {
          if (canNoOpRefresh) {
            return "No changes are detected in the base dataset since the last refresh. The reflection will not be updated.";
          }
          return "Snapshot Based Incremental Refresh for Append Only workflows. \n"
              + getAnchorDatasetDetails(tableMetadata, currentSnapshotId, oldSnapshotID);
        }
      case FILTER_PARTITIONS:
        {
          return "Snapshot Based Incremental Refresh by Partition."
              + "\n"
              + getAnchorDatasetDetails(tableMetadata, currentSnapshotId, oldSnapshotID)
              + "\nBase Dataset Target Partitions: "
              + toDisplayString(snapshotDiffContext.getBaseDatasetTargetPartitionSpec())
              + "\nReflection Target Partitions: "
              + toDisplayString(snapshotDiffContext.getReflectionPartitionSpec());
        }
      default:
        {
          return "Incremental Refresh";
        }
    }
  }

  private static String getAnchorDatasetDetails(
      TableMetadata tableMetadata, String currentSnapshotId, String oldSnapshotID) {
    return String.format(
        "Anchor dataset %s has been updated from snapshot %s to snapshot %s",
        tableMetadata.getName(), oldSnapshotID, currentSnapshotId);
  }

  static String hasNewSnapshotsForRefresh(
      ReflectionEntry entry,
      EntityExplorer catalog,
      List<DependencyEntry> dependencies,
      MaterializationStore materializationStore) {
    // go through the dependencies and determine if there's new data for refresh
    for (DependencyEntry dependency : dependencies) {
      if (dependency instanceof TableFunctionDependency) {
        continue;
      } else if (dependency instanceof ReflectionDependency) {
        Materialization lastDone =
            materializationStore.getLastMaterializationDone(
                ((ReflectionDependency) dependency).getReflectionId());
        if (lastDone == null) {
          return String.format(
              "Reflection dependency %s did not have a valid DONE materialization",
              dependency.getId());
        }
        if (lastDone.getLastRefreshFinished() > entry.getLastSuccessfulRefresh()
            && !lastDone.getIsNoopRefresh()) {
          return String.format(
              "Reflection dependency %s refreshed since last refresh", dependency.getId());
        }
      } else {
        DatasetDependency datasetDependency = ((DatasetDependency) dependency);
        String schemaPath = datasetDependency.getNamespaceKey().getSchemaPath();
        CatalogEntityKey key =
            CatalogEntityKey.newBuilder()
                .keyComponents(dependency.getPath())
                .tableVersionContext(datasetDependency.getVersionContext())
                .build();
        DremioTable table = catalog.getTable(key);

        if (table == null) {
          return String.format(
              "Refresh couldn't be skipped because dataset dependency %s was not found",
              key.toNamespaceKey());
        }

        // Eliminating redundant refresh only works for Iceberg
        if (!DatasetHelper.isIcebergDataset(table.getDatasetConfig())
            && !DatasetHelper.isInternalIcebergTable(table.getDatasetConfig())) {
          return String.format(
              "Refresh couldn't be skipped because dataset dependency %s is not an Iceberg table",
              key.toNamespaceKey());
        }

        long snapshotId =
            Optional.of(table)
                .map(DremioTable::getDatasetConfig)
                .map(DatasetConfig::getPhysicalDataset)
                .map(PhysicalDataset::getIcebergMetadata)
                .map(IcebergMetadata::getSnapshotId)
                .orElse(0L);
        if (snapshotId != 0L && snapshotId != datasetDependency.getSnapshotId()) {
          // there are new snapshot(s). Now check that there is at least one data modifying
          // operation
          final Optional<TableMetadataVerifyResult> tableMetadataVerifyResult =
              catalog.verifyTableMetadata(
                  key,
                  new TableMetadataVerifyDataModifiedRequest(
                      String.valueOf(datasetDependency.getSnapshotId()),
                      String.valueOf(snapshotId)));

          checkState(tableMetadataVerifyResult != null);

          final TableMetadataVerifyDataModifiedResult verifyDataModifiedResult =
              tableMetadataVerifyResult
                  .filter(TableMetadataVerifyDataModifiedResult.class::isInstance)
                  .map(TableMetadataVerifyDataModifiedResult.class::cast)
                  .orElse(null);

          if (verifyDataModifiedResult == null) {
            // Something weird happened, so just refresh normally
            logger.debug(
                String.format(
                    "Fail to verify the changes between snapshots %s and %s for base table %s",
                    datasetDependency.getSnapshotId(), snapshotId, table.getPath()));
            return String.format("Metadata request for dataset dependency %s failed", schemaPath);
          }

          checkState(verifyDataModifiedResult.getResultCode() != null);

          switch (verifyDataModifiedResult.getResultCode()) {
            case NOT_DATA_MODIFIED:
              // there are new snapshots, but they didn't change table data. We may not need to
              // refresh.
              break;
            case DATA_MODIFIED:
            default:
              // In any case where we don't explicitly have NOT_DATA_MODIFIED, we should just
              // refresh.
              // This includes INVALID_BEGIN_SNAPSHOT, INVALID_END_SNAPSHOT, and NOT_ANCESTOR
              return String.format(
                  "Dataset dependency %s had new snapshot(s) that caused a refresh.\nOld Snapshot dependency: %d\nNew snapshot dependency: %d",
                  schemaPath, datasetDependency.getSnapshotId(), snapshotId);
          }
        }
      }
    }
    return "";
  }
}
