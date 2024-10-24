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

import static com.dremio.exec.ExecConstants.ENABLE_VACUUM_CATALOG_BRIDGE_OPERATOR;
import static com.dremio.exec.planner.VacuumOutputSchema.getRowType;
import static com.dremio.exec.store.SystemSchemas.CARRY_FORWARD_FILE_PATH_TYPE_SCHEMA;
import static com.dremio.exec.store.SystemSchemas.FILE_PATH;
import static com.dremio.exec.store.SystemSchemas.ICEBERG_SNAPSHOTS_SCAN_SCHEMA;
import static com.dremio.exec.store.SystemSchemas.METADATA_FILE_PATH;
import static com.dremio.exec.store.SystemSchemas.METADATA_PATH_SCAN_SCHEMA;
import static com.dremio.exec.store.iceberg.SnapshotsScanOptions.Mode.ALL_SNAPSHOTS;
import static org.apache.iceberg.DremioTableProperties.NESSIE_GC_ENABLED;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.VacuumOptions;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.cost.iceberg.IcebergCostEstimates;
import com.dremio.exec.planner.physical.BridgeExchangePrel;
import com.dremio.exec.planner.physical.BridgeReaderPrel;
import com.dremio.exec.planner.physical.DistributionTrait;
import com.dremio.exec.planner.physical.FilterPrel;
import com.dremio.exec.planner.physical.HashToRandomExchangePrel;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.ProjectPrel;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.SystemSchemas;
import com.dremio.exec.store.iceberg.IcebergLocationFinderPrel;
import com.dremio.exec.store.iceberg.NessieCommitsScanPrel;
import com.dremio.exec.store.iceberg.SnapshotsScanOptions;
import com.dremio.exec.store.iceberg.SnapshotsScanOptions.Mode;
import com.dremio.options.OptionManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.Pair;

/** Expand plans for VACUUM CATALOG REMOVE ORPHANS flow. */
public class VacuumCatalogRemoveOrphansPlanGenerator extends VacuumTableRemoveOrphansPlanGenerator {
  private final VacuumOptions vacuumCatalogOptions;
  private final String fsScheme;
  private final OptionManager optionManager;
  private BridgeExchangePrel bridgePrel;
  private String bridgeId;

  public VacuumCatalogRemoveOrphansPlanGenerator(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      VacuumOptions vacuumOptions,
      StoragePluginId storagePluginId,
      IcebergCostEstimates icebergCostEstimates,
      String user,
      String fsScheme,
      String schemeVariate,
      OptimizerRulesContext context) {
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
    this.optionManager = ((QueryContext) context).getOptions();
  }

  private Prel createMetadataPathScanPlan() {
    if (vacuumCatalogBridgeOperatorEnabled()) {
      createBridgeNessieScanIfNotExists();

      final RexBuilder rexBuilder = cluster.getRexBuilder();
      final ProjectPrel projectPrel = projectMetadaPathFromNessieScan(rexBuilder);
      return filterNonNull(rexBuilder, projectPrel, METADATA_FILE_PATH);
    }

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
    if (vacuumCatalogBridgeOperatorEnabled()) {
      createBridgeNessieScanIfNotExists();
      return new BridgeReaderPrel(
          bridgePrel.getCluster(), bridgePrel.getTraitSet(), bridgePrel.getRowType(), 1d, bridgeId);
    }

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
            ImmutableMap.of(NESSIE_GC_ENABLED, Boolean.FALSE.toString()),
            continueOnError());
    Prel dedupLocation = reduceDuplicateFilePaths(locationFinder);
    return projectPath(dedupLocation);
  }

  @Override
  protected boolean continueOnError() {
    return true;
  }

  /**
   * To allow changes in the order of initialization for VACUUM TABLE plan, this method is used to
   * initialize the bridgePrel. The single scan plan will always have the nessie scan on the branch
   * with IcebergLocationFinder TF (the all file listing branch). The BridgeReader will be on the
   * ManifestScan branch of the plan (live files listing branch).
   */
  private void createBridgeNessieScanIfNotExists() {
    if (bridgePrel != null) {
      return;
    }

    SnapshotsScanOptions snapshotsScanOptions =
        new SnapshotsScanOptions(
            Mode.LIVE_SNAPSHOTS,
            vacuumCatalogOptions.getOlderThanInMillis(),
            vacuumCatalogOptions.getRetainLast());
    BatchSchema schema = ICEBERG_SNAPSHOTS_SCAN_SCHEMA.merge(CARRY_FORWARD_FILE_PATH_TYPE_SCHEMA);
    final NessieCommitsScanPrel scanPrel =
        new NessieCommitsScanPrel(
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
    bridgeId = UUID.randomUUID().toString();
    bridgePrel =
        new BridgeExchangePrel(scanPrel.getCluster(), scanPrel.getTraitSet(), scanPrel, bridgeId);
  }

  private boolean vacuumCatalogBridgeOperatorEnabled() {
    return optionManager.getOption(ENABLE_VACUUM_CATALOG_BRIDGE_OPERATOR);
  }

  private ProjectPrel projectMetadaPathFromNessieScan(RexBuilder rexBuilder) {
    List<RexNode> projectExpressions = new ArrayList<>();
    final List<String> projectFields =
        METADATA_PATH_SCAN_SCHEMA.getFields().stream()
            .map(field -> field.getName())
            .collect(Collectors.toList());
    projectFields.forEach(
        field -> {
          final Pair<Integer, RelDataTypeField> fieldWithIndex =
              MoreRelOptUtil.findFieldWithIndex(bridgePrel.getRowType().getFieldList(), field);
          final RexInputRef inputRef =
              rexBuilder.makeInputRef(fieldWithIndex.right.getType(), fieldWithIndex.left);
          projectExpressions.add(inputRef);
        });
    RelDataType rowType =
        RexUtil.createStructType(
            rexBuilder.getTypeFactory(),
            projectExpressions,
            projectFields,
            SqlValidatorUtil.F_SUGGESTER);

    return ProjectPrel.create(
        bridgePrel.getCluster(), bridgePrel.getTraitSet(), bridgePrel, projectExpressions, rowType);
  }

  private Prel filterNonNull(RexBuilder rexBuilder, Prel input, String colName) {
    RelDataTypeField filterCol = input.getRowType().getField(colName, false, false);
    RexNode removeNullEntries =
        rexBuilder.makeCall(
            SqlStdOperatorTable.IS_NOT_NULL,
            rexBuilder.makeInputRef(filterCol.getType(), filterCol.getIndex()));
    return FilterPrel.create(input.getCluster(), input.getTraitSet(), input, removeNullEntries);
  }
}
