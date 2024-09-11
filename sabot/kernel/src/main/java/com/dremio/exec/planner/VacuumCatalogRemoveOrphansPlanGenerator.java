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
package com.dremio.exec.planner;

import static com.dremio.exec.planner.VacuumOutputSchema.getRowType;
import static com.dremio.exec.store.SystemSchemas.CARRY_FORWARD_FILE_PATH_TYPE_SCHEMA;
import static com.dremio.exec.store.SystemSchemas.FILE_PATH;
import static com.dremio.exec.store.SystemSchemas.ICEBERG_SNAPSHOTS_SCAN_SCHEMA;
import static com.dremio.exec.store.SystemSchemas.METADATA_FILE_PATH;
import static com.dremio.exec.store.SystemSchemas.METADATA_PATH_SCAN_SCHEMA;
import static com.dremio.exec.store.iceberg.SnapshotsScanOptions.Mode.ALL_SNAPSHOTS;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.VacuumOptions;
import com.dremio.exec.planner.cost.iceberg.IcebergCostEstimates;
import com.dremio.exec.planner.physical.DistributionTrait;
import com.dremio.exec.planner.physical.HashToRandomExchangePrel;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.SystemSchemas;
import com.dremio.exec.store.iceberg.IcebergLocationFinderPrel;
import com.dremio.exec.store.iceberg.NessieCommitsScanPrel;
import com.dremio.exec.store.iceberg.SnapshotsScanOptions;
import com.dremio.exec.store.iceberg.SnapshotsScanOptions.Mode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.iceberg.TableProperties;

/** Expand plans for VACUUM CATALOG REMOVE ORPHANS flow. */
public class VacuumCatalogRemoveOrphansPlanGenerator extends VacuumTableRemoveOrphansPlanGenerator {
  private final VacuumOptions vacuumCatalogOptions;
  private final String fsScheme;

  public VacuumCatalogRemoveOrphansPlanGenerator(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      VacuumOptions vacuumOptions,
      StoragePluginId storagePluginId,
      IcebergCostEstimates icebergCostEstimates,
      String user,
      String fsScheme,
      String schemeVariate) {
    super(
        cluster,
        traitSet,
        Collections.emptyList(),
        icebergCostEstimates,
        vacuumOptions,
        storagePluginId,
        storagePluginId,
        user,
        null,
        null);
    this.vacuumCatalogOptions = vacuumOptions;
    this.fsScheme = fsScheme;
    this.schemeVariate = schemeVariate;
  }

  @Override
  protected Prel createMetadataPathScanPlan() {
    SnapshotsScanOptions snapshotsScanOptions =
        new SnapshotsScanOptions(
            ALL_SNAPSHOTS,
            vacuumCatalogOptions.getOlderThanInMillis(),
            vacuumCatalogOptions.getRetainLast());

    return new NessieCommitsScanPrel(
        cluster,
        traitSet,
        METADATA_PATH_SCAN_SCHEMA,
        snapshotsScanOptions,
        user,
        storagePluginId,
        icebergCostEstimates.getSnapshotsCount(),
        1,
        fsScheme,
        getSchemeVariate());
  }

  @Override
  protected Prel getPartitionStatsScanPrel(Prel snapshotsScanPlan) {
    DistributionTrait distributionTrait =
        getHashDistributionTraitForFields(
            snapshotsScanPlan.getRowType(), ImmutableList.of(METADATA_FILE_PATH, FILE_PATH));
    RelTraitSet traitSet =
        cluster.getPlanner().emptyTraitSet().plus(Prel.PHYSICAL).plus(distributionTrait);
    HashToRandomExchangePrel hashToRandomExchangePrel =
        new HashToRandomExchangePrel(
            cluster, traitSet, snapshotsScanPlan, distributionTrait.getFields());
    return super.getPartitionStatsScanPrel(hashToRandomExchangePrel);
  }

  @Override
  protected Prel createLiveSnapshotsProducerPlan() {
    SnapshotsScanOptions snapshotsScanOptions =
        new SnapshotsScanOptions(
            Mode.LIVE_SNAPSHOTS,
            vacuumCatalogOptions.getOlderThanInMillis(),
            vacuumCatalogOptions.getRetainLast());
    BatchSchema schema = ICEBERG_SNAPSHOTS_SCAN_SCHEMA.merge(CARRY_FORWARD_FILE_PATH_TYPE_SCHEMA);
    return new NessieCommitsScanPrel(
        cluster,
        traitSet,
        schema,
        snapshotsScanOptions,
        user,
        storagePluginId,
        icebergCostEstimates.getSnapshotsCount(),
        1,
        fsScheme,
        getSchemeVariate());
  }

  @Override
  protected long recentFileSelectionCutOff() {
    long gracePeriodTimestamp = System.currentTimeMillis() - vacuumOptions.getGracePeriodInMillis();
    return Math.min(vacuumOptions.getOlderThanInMillis(), gracePeriodTimestamp);
  }

  @Override
  protected Prel locationProviderPrel() {
    Prel metadataJsonProducerPrel = createMetadataPathScanPlan();
    // Use parallelism with LocationFinder operator.
    // In the case of Vacuum catalog, it can have much metadata from different branches and tables.
    DistributionTrait distributionTrait =
        getHashDistributionTraitForFields(
            metadataJsonProducerPrel.getRowType(), ImmutableList.of(METADATA_FILE_PATH));
    RelTraitSet traitSet =
        cluster.getPlanner().emptyTraitSet().plus(Prel.PHYSICAL).plus(distributionTrait);
    HashToRandomExchangePrel hashToRandomExchangePrel =
        new HashToRandomExchangePrel(
            cluster, traitSet, metadataJsonProducerPrel, distributionTrait.getFields());
    Prel locationFinder =
        new IcebergLocationFinderPrel(
            storagePluginId,
            cluster,
            traitSet,
            hashToRandomExchangePrel,
            getRowType(SystemSchemas.TABLE_LOCATION_SCHEMA, cluster.getTypeFactory()),
            mq -> mq.getRowCount(metadataJsonProducerPrel),
            metadataJsonProducerPrel.getEstimatedSize(),
            user,
            ImmutableMap.of(TableProperties.GC_ENABLED, Boolean.FALSE.toString()),
            continueOnError());
    Prel dedupLocation = reduceDuplicateFilePaths(locationFinder);
    return projectPath(dedupLocation);
  }

  @Override
  protected boolean continueOnError() {
    return true;
  }
}
