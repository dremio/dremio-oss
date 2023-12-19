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

import static com.dremio.exec.store.iceberg.IcebergIncrementalRefreshJoinKeyTableFunction.UNSUPPORTED_PARTITION_EVOLUTION;
import static com.dremio.exec.store.iceberg.IncrementalReflectionByPartitionUtils.buildCompatiblePartitions;
import static com.dremio.service.reflection.ReflectionOptions.INCREMENTAL_REFRESH_BY_PARTITION;
import static com.dremio.service.reflection.ReflectionOptions.REFLECTION_SNAPSHOT_BASED_INCREMENTAL_MAX_UNIONS;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.calcite.rel.RelNode;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.Table;

import com.dremio.catalog.model.VersionContext;
import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.catalog.model.dataset.TableVersionType;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.map.CaseInsensitiveMap;
import com.dremio.exec.catalog.CatalogUser;
import com.dremio.exec.catalog.EntityExplorer;
import com.dremio.exec.catalog.MetadataRequestOptions;
import com.dremio.exec.catalog.TableMetadataVerifyAppendOnlyRequest;
import com.dremio.exec.catalog.TableMetadataVerifyAppendOnlyResult;
import com.dremio.exec.catalog.TableMetadataVerifyResult;
import com.dremio.exec.ops.SnapshotDiffContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.TableMetadata;
import com.dremio.options.OptionManager;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.reflection.ReflectionManager;
import com.dremio.service.reflection.ReflectionService;
import com.dremio.service.reflection.ReflectionUtils;
import com.dremio.service.reflection.proto.Materialization;
import com.dremio.service.reflection.proto.ReflectionEntry;
import com.dremio.service.reflection.proto.Refresh;
import com.dremio.service.reflection.store.MaterializationStore;
import com.dremio.service.users.SystemUser;

/**
 * A class to make a decision if SnapshotBased Incremental Refresh is possible
 * The decision is in the form of SnapshotDiffContext
 * For now there are 3 possible outcomes
 * Incremental Refresh not possible - returned SnapshotDiffContext is null
 * Incremental Refresh Append only - SnapshotDiffContext has FilterApplyOptions.FILTER_DATA_FILES
 * Incremental Refresh by Partition - SnapshotDiffContext has FilterApplyOptions.FILTER_PARTITIONS
 * In addition, if we decide to not use SnapshotBased Incremental Refresh, but the settings are enabled
 * we will provide exact reason why in fullRefreshReason
 */
class SnapshotBasedIncrementalRefreshDecisionMaker {

  private final CatalogService catalogService;
  private final TableMetadata baseTableMetadata;
  private final String baseTableSnapshotId;

  private final OptionManager optionManager;
  private final String lastRefreshBaseTableSnapshotId;
  private final MaterializationStore materializationStore;
  private final Materialization materialization;
  private final ReflectionService reflectionService;
  private final ReflectionEntry reflectionEntry;
  private final RelNode strippedPlan;
  private String fullRefreshReason;

  /**
   * Determine if snapshot based incremental refresh is possible and build SnapshotDiffContext.
   *
   * @param catalogService the catalog service for this query
   * @param baseTableMetadata base table current state
   * @param baseTableSnapshotId base table current state
   * @param lastRefreshBaseTableSnapshotId last refresh base table state
   * @param optionManager the option manager to use
   * @param materializationStore the materialization store to use
   * @param materialization the current materialization for the current reflection
   * @param reflectionService the reflection service to use
   * @param reflectionEntry the reflection entry for the current reflection
   * @param strippedPlan the stripped plan for the current reflection
   */
  public SnapshotBasedIncrementalRefreshDecisionMaker(final CatalogService catalogService,
                                                      final TableMetadata baseTableMetadata,
                                                      final String baseTableSnapshotId,
                                                      final String lastRefreshBaseTableSnapshotId,
                                                      final OptionManager optionManager,
                                                      final MaterializationStore materializationStore,
                                                      final Materialization materialization,
                                                      final ReflectionService reflectionService,
                                                      final ReflectionEntry reflectionEntry,
                                                      final RelNode strippedPlan) {
    this.catalogService = catalogService;
    this.baseTableMetadata = baseTableMetadata;
    this.baseTableSnapshotId = baseTableSnapshotId;
    this.lastRefreshBaseTableSnapshotId = lastRefreshBaseTableSnapshotId;
    this.optionManager = optionManager;
    this.materializationStore = materializationStore;
    this.materialization = materialization;
    this.reflectionService = reflectionService;
    this.reflectionEntry = reflectionEntry;
    this.strippedPlan = strippedPlan;
    this.fullRefreshReason = "";
  }

  /**
   * Gets the SnapshotBased Incremental Refresh decision in the form of  SnapshotDiffContext
   * @return SnapshotDiffContext with the decision or null if SnapshotBased Incremental Refresh is not possible
   */
  public SnapshotDiffContext getDecision() {

    checkArgument(baseTableMetadata != null);
    checkArgument(baseTableSnapshotId != null && !baseTableSnapshotId.isEmpty());
    checkArgument(lastRefreshBaseTableSnapshotId != null && !lastRefreshBaseTableSnapshotId.isEmpty());

    if(baseTableSnapshotId.equals(lastRefreshBaseTableSnapshotId)){
      //skip verifying append only between snapshots if the base and the latest refresh snapshots are the same
      return new SnapshotDiffContext(baseTableMetadata, baseTableMetadata,
        SnapshotDiffContext.FilterApplyOptions.FILTER_DATA_FILES);
    }
    // Set VersionContext of Branch/Tag/Commit info as MetadataRequestOptions of EntityExplorer, so that
    // EntityExplorer#verifyTableMetadata and EntityExplorer#getTableSnapshot can resolve to correct iceberg table
    // for different Branch/Tag/Commit for versioned dataset.
    Map<String, VersionContext> versionContextMap = ReflectionUtils.buildVersionContext(
      baseTableMetadata.getDatasetConfig().getId().getId());
    CaseInsensitiveMap<VersionContext> sourceVersionMap = CaseInsensitiveMap.newHashMap();
    for (Map.Entry<String, VersionContext> versionContext : versionContextMap.entrySet()) {
      sourceVersionMap.put(versionContext.getKey(), versionContext.getValue());
    }
    final EntityExplorer entityExplorer = catalogService.getCatalog(MetadataRequestOptions.newBuilder()
      .setSchemaConfig(SchemaConfig.newBuilder(CatalogUser.from(SystemUser.SYSTEM_USERNAME)).build())
      .setSourceVersionMapping(sourceVersionMap)
      .setCheckValidity(false)
      .setNeverPromote(true)
      .setUseInternalMetadataTable(DatasetHelper.isInternalIcebergTable(baseTableMetadata.getDatasetConfig()))
      .build());

    // Verify if operations on base table were append-only since last refresh.
    final Optional<TableMetadataVerifyResult> tableMetadataVerifyResult = entityExplorer.verifyTableMetadata(
      baseTableMetadata.getName(),
      new TableMetadataVerifyAppendOnlyRequest(lastRefreshBaseTableSnapshotId, baseTableSnapshotId));

    checkState(tableMetadataVerifyResult != null);

    final TableMetadataVerifyAppendOnlyResult verifyAppendOnlyResult = tableMetadataVerifyResult
      .filter(TableMetadataVerifyAppendOnlyResult.class::isInstance)
      .map(TableMetadataVerifyAppendOnlyResult.class::cast)
      .orElse(null);

    if (verifyAppendOnlyResult == null) {
      fullRefreshReason = String.format("Fail to verify the changes between snapshots %s and %s for base table %s.",
                lastRefreshBaseTableSnapshotId, baseTableSnapshotId, baseTableMetadata.getName());
      return null;
    }
    checkState(verifyAppendOnlyResult.getResultCode() != null);
    switch (verifyAppendOnlyResult.getResultCode()){
      case INVALID_BEGIN_SNAPSHOT:
        fullRefreshReason = String.format("Invalid begin snapshot %s for base table %s. The snapshot might have expired.",
          lastRefreshBaseTableSnapshotId, baseTableMetadata.getName());
        return null;
      case INVALID_END_SNAPSHOT:
        fullRefreshReason = String.format("Invalid end snapshot %s for base table %s.",
          baseTableSnapshotId, baseTableMetadata.getName());
        return null;
      case NOT_ANCESTOR:
        fullRefreshReason = String.format("Snapshot %s is not ancestor of snapshot %s for base table %s. Table might have been rolled back.",
          lastRefreshBaseTableSnapshotId, baseTableSnapshotId, baseTableMetadata.getName());
        return null;
      case NOT_APPEND_ONLY:
        fullRefreshReason = String.format("Changes between snapshots %s and %s on base table %s were not append-only. Cannot do Append Only Incremental Refresh.",
          lastRefreshBaseTableSnapshotId, baseTableSnapshotId, baseTableMetadata.getName());
        //attempt to use Incremental Refresh By Partition Instead of doing a full refresh
        return getIncrementalRefreshByPartitionDecision(lastRefreshBaseTableSnapshotId, baseTableSnapshotId, entityExplorer);
      case APPEND_ONLY:
        checkState(verifyAppendOnlyResult.getSnapshotRanges() != null);
        // 1 union required for 2 ranges.
        // Type of REFLECTION_SNAPSHOT_BASED_INCREMENTAL_MAX_UNIONS is PositiveLongValidator, min value is 1.
        // Note that number of unions required might be less than number of optimize operations on base table. Because
        // there could be adjacent optimize operations, in such case the adjacent ones are combined as one.
        if (verifyAppendOnlyResult.getSnapshotRanges().size() - 1 > optionManager.getOption(REFLECTION_SNAPSHOT_BASED_INCREMENTAL_MAX_UNIONS)) {
          fullRefreshReason = String.format("Number of unions required for append-only ranges between snapshots %s and %s on base table %s exceed system limit (required:%d, limit:%d). Cannot do Append Only Incremental Refresh.",
            lastRefreshBaseTableSnapshotId, baseTableSnapshotId, baseTableMetadata.getName(),
            verifyAppendOnlyResult.getSnapshotRanges().size() - 1,
            optionManager.getOption(REFLECTION_SNAPSHOT_BASED_INCREMENTAL_MAX_UNIONS));
          return null;
        }
        return getAppendOnlySnapshotDiffContext(verifyAppendOnlyResult, entityExplorer);
      default:
        throw new AssertionError("Unsupported VerifyIsAppendOnlyBetweenResult.ResultCode: " + verifyAppendOnlyResult.getResultCode());
    }
  }

  /**
   * Build a SnapshotDiffContext from the verifyAppendOnlyResult
   *
   * @param verifyAppendOnlyResult information about snapshots to use
   * @param catalog the catalog to use
   * @return a SnapshotDiffContext corresponding to verifyAppendOnlyResult
   */
  private SnapshotDiffContext getAppendOnlySnapshotDiffContext(TableMetadataVerifyAppendOnlyResult verifyAppendOnlyResult,
                                                               EntityExplorer catalog) {
    checkState(verifyAppendOnlyResult.getSnapshotRanges() != null &&
      verifyAppendOnlyResult.getSnapshotRanges().size() > 0);
    SnapshotDiffContext snapshotDiffContext = null;
    final Map<String, TableMetadata> cachedTableMetadatas = new HashMap<>();
    cachedTableMetadatas.put(baseTableSnapshotId, baseTableMetadata);
    for (final Pair<String, String> range : verifyAppendOnlyResult.getSnapshotRanges()) {
      final String beginSnapshotId = range.getLeft();
      final String endSnapshotId = range.getRight();
      final TableMetadata beginTableMetadata = getTableMetadataHelper(
        catalog,
        baseTableMetadata.getName(),
        cachedTableMetadatas,
        beginSnapshotId);
      final TableMetadata endTableMetadata = getTableMetadataHelper(
        catalog,
        baseTableMetadata.getName(),
        cachedTableMetadatas,
        endSnapshotId);

      if(beginTableMetadata == null || endTableMetadata == null){
        return null;
      }
      if (snapshotDiffContext == null) {
        snapshotDiffContext = new SnapshotDiffContext(beginTableMetadata, endTableMetadata,
          SnapshotDiffContext.FilterApplyOptions.FILTER_DATA_FILES);
      } else {
        snapshotDiffContext.addSnapshotDiffInterval(beginTableMetadata, endTableMetadata);
      }
    }
    return snapshotDiffContext;
  }

  /**
   * Gets the table Metadata for a Snapshot
   * @param catalog the catalog to use
   * @param namespaceKey the namespace key for the base dataset
   * @param cachedTableMetadatas cached metadatas to use to avoid a call to get table metadata
   * @param snapshotId Snapshot ID we are trying to get the table metadata for
   * @return Table Metadata for snapshotId
   */
  private TableMetadata getTableMetadataHelper(
    final EntityExplorer catalog,
    final NamespaceKey namespaceKey,
    final Map<String, TableMetadata> cachedTableMetadatas,
    final String snapshotId) {

    return Optional.ofNullable((cachedTableMetadatas.get(snapshotId)))
      .orElseGet(() -> {
        try {
          return catalog.getTableSnapshot(namespaceKey,
            new TableVersionContext(TableVersionType.SNAPSHOT_ID, snapshotId)).getDataset();
        } catch (final UserException e) {
          // getTableSnapshot returns a UserException when table is not found.
          fullRefreshReason = String.format("Fail to retrieve snapshot %s for base table %s.",
            snapshotId, baseTableMetadata.getName());
          return null;
        }
      });
  }

  /**
   * Checks if Incremental refresh by partition is possible
   *
   * @return SnapshotDiffContext with information for Incremental Refresh by partition
   * or null if Incremental refresh by partition is  not possible
   */
  private SnapshotDiffContext getIncrementalRefreshByPartitionDecision(final String beginSnapshotId,
                                                                       final String endSnapshotId,
                                                                       final EntityExplorer catalog){
    if(optionManager == null || !optionManager.getOption(INCREMENTAL_REFRESH_BY_PARTITION)){
      //we leave reasonForFullRefresh empty here, we don't want to display reason if the feature is just disabled
      return null;
    }

    //we get the materialization from the previous run of the same reflection
    //the idea is that if the previous run resulted in a failure, which is UNSUPPORTED_PARTITION_EVOLUTION, we fall back to full refresh
    final Materialization lastMaterialization = materializationStore.getPreviousMaterialization(materialization.getReflectionId());
    if (lastMaterialization.getFailure() != null
      && lastMaterialization.getFailure().getMessage() != null
      && lastMaterialization.getFailure().getMessage().contains(UNSUPPORTED_PARTITION_EVOLUTION)) {
      fullRefreshReason += "\nFound unsupported partition evolution in the base dataset. Cannot do Incremental Refresh by Partition.";
      return null;
    }
    final Refresh refresh = materializationStore.getMostRecentRefresh(materialization.getReflectionId());

    final ReflectionManager reflectionManager = reflectionService.getReflectionManager();
    final Table reflectionIcebergTable = reflectionManager.getIcebergTable(reflectionEntry.getId(), refresh.getBasePath());

    final  Map<String, TableMetadata> cachedTableMetadatas = new HashMap<>();
    cachedTableMetadatas.put(baseTableSnapshotId, baseTableMetadata);
    final TableMetadata beginTableMetadata = getTableMetadataHelper(
      catalog,
      baseTableMetadata.getName(),
      cachedTableMetadatas,
      beginSnapshotId);
    final TableMetadata endTableMetadata = getTableMetadataHelper(
      catalog,
      baseTableMetadata.getName(),
      cachedTableMetadatas,
      endSnapshotId);

    if(beginTableMetadata == null || endTableMetadata == null){
      return null;
    }
    final SnapshotDiffContext snapshotDiffContext = new SnapshotDiffContext(beginTableMetadata, endTableMetadata,
      SnapshotDiffContext.FilterApplyOptions.FILTER_DATA_FILES);
    if(!buildCompatiblePartitions(reflectionIcebergTable, strippedPlan, snapshotDiffContext, snapshotDiffContext.getIntervals().get(0).getEndingTableMetadata())){
      fullRefreshReason += "\nThe partitions between the base dataset and the reflection are not compatible. Cannot do Incremental Refresh by Partition.";
      return null;
    }
    fullRefreshReason = "";
    //if we get here everything is fine, and we can proceed with Incremental Refresh by Partition
    snapshotDiffContext.setFilterApplyOptions(SnapshotDiffContext.FilterApplyOptions.FILTER_PARTITIONS);
    return snapshotDiffContext;
  }

  /**
   * Get the reason why we could not perform Snapshot Based Incremental Refresh
   * @return The reason why we could not perform Snapshot Based Incremental Refresh
   * or empty string if we can perform a Snapshot Based Incremental Refresh
   */
  public String getFullRefreshReason() {
    return fullRefreshReason;
  }
}
