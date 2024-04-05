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

import static com.dremio.exec.store.SystemSchemas.FILE_PATH;
import static com.dremio.exec.store.SystemSchemas.FILE_TYPE;
import static com.dremio.exec.store.SystemSchemas.RECORDS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CASE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.EQUALS;
import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;
import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.VacuumOptions;
import com.dremio.exec.planner.cost.iceberg.IcebergCostEstimates;
import com.dremio.exec.planner.physical.DistributionTrait;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.ProjectPrel;
import com.dremio.exec.planner.physical.StreamAggPrel;
import com.dremio.exec.planner.physical.UnionExchangePrel;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.iceberg.IcebergFileType;
import com.dremio.exec.store.iceberg.IcebergOrphanFileDeletePrel;
import com.dremio.exec.store.iceberg.SnapshotsScanOptions;
import com.dremio.service.namespace.PartitionChunkMetadata;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.ImmutableBitSet;

/** Expand plans for VACUUM TABLE EXPIRE SNAPSHOTS flow. */
public class VacuumTableExpireSnapshotsPlanGenerator extends VacuumPlanGenerator {
  private final String tableLocation; // This table location should have path scheme info.

  public VacuumTableExpireSnapshotsPlanGenerator(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<PartitionChunkMetadata> splits,
      IcebergCostEstimates icebergCostEstimates,
      VacuumOptions vacuumOptions,
      StoragePluginId internalStoragePlugin,
      StoragePluginId storagePluginId,
      String user,
      String tableLocation) {
    super(
        cluster,
        traitSet,
        splits,
        icebergCostEstimates,
        vacuumOptions,
        internalStoragePlugin,
        storagePluginId,
        user);
    this.tableLocation = tableLocation;
  }

  @Override
  protected boolean enableCarryForwardOnPartitionStats() {
    return false;
  }

  /*
   *                            UnionExchangePrel
   *                                     │
   *                                     │
   *                        IcebergOrphanFileDeleteTF
   *                                     │
   *                                     │
   *                       Filter (live.filePath = null)
   *                                     │
   *                                     │
   *              HashJoin (expired.filePath=live.filePath (LEFT))
   *                                  │    │
   *            ┌─────────────────────┘    └──────────┐
   *            │                                     │
   * Project (filepath, filetype)        Project (filepath, filetype)
   *            │                                     │
   *            │                                     │
   *            │                        HashAgg(filepath [deduplicate])
   *            │                                     │
   *            │                                     │
   * Project                             Project
   * [ (filepath,    filetype)          [ (filepath,    filetype)
   *       │             │                    │             │
   * (datafilepath, filecontent) ]      (datafilepath, filecontent) ]
   *            │                                     │
   *            │                                     │
   * IcebergManifestScanTF               IcebergManifestScanTF
   *            │                                     │
   *            │                                     │
   * IcebergManifestListScanTF           IcebergManifestListScanTF
   *            │                                     │
   *            │                                     │
   * PartitionStatsScanTF                PartitionStatsScanTF
   *            │                                     │
   *            │                                     │
   * ExpireSnapshotScan                  ExpiredSnapshotScan
   * (Expired snapshot ids)              (Live snapshot ids)
   */

  @Override
  public Prel buildPlan() {
    try {
      if (icebergCostEstimates.getSnapshotsCount() == 1
          || vacuumOptions.getRetainLast() >= icebergCostEstimates.getSnapshotsCount()) {
        return outputZerosPlan();
      }
      Prel expiredSnapshotFilesPlan =
          filePathAndTypeScanPlan(snapshotsScanPlan(SnapshotsScanOptions.Mode.EXPIRED_SNAPSHOTS));
      Prel liveSnapshotsFilesPlan =
          deDupFilePathAndTypeScanPlan(snapshotsScanPlan(SnapshotsScanOptions.Mode.LIVE_SNAPSHOTS));
      Prel orphanFilesPlan = orphanFilesPlan(expiredSnapshotFilesPlan, liveSnapshotsFilesPlan);
      Prel deleteOrphanFilesPlan = deleteOrphanFilesPlan(orphanFilesPlan);
      return outputSummaryPlan(deleteOrphanFilesPlan);
    } catch (InvalidRelException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected Prel deleteOrphanFilesPlan(Prel input) {
    BatchSchema outSchema =
        BatchSchema.newBuilder()
            .addField(Field.nullable(FILE_PATH, Types.MinorType.VARCHAR.getType()))
            .addField(Field.nullable(FILE_TYPE, Types.MinorType.VARCHAR.getType()))
            .addField(Field.nullable(RECORDS, Types.MinorType.BIGINT.getType()))
            .setSelectionVectorMode(BatchSchema.SelectionVectorMode.NONE)
            .build();

    // We do overestimate instead of underestimate. 1) Use file counts from ALL snapshot; 2)
    // consider every snapshot has partition stats files.
    return new IcebergOrphanFileDeletePrel(
        storagePluginId,
        input.getCluster(),
        input.getTraitSet(),
        outSchema,
        input,
        icebergCostEstimates.getEstimatedRows(),
        user,
        tableLocation);
  }

  @Override
  protected Prel outputSummaryPlan(Prel input) throws InvalidRelException {
    RelOptCluster cluster = input.getCluster();
    RelDataTypeFactory typeFactory = cluster.getTypeFactory();

    // Use single thread to collect deleted orphan files.
    input =
        new UnionExchangePrel(
            input.getCluster(), input.getTraitSet().plus(DistributionTrait.SINGLETON), input);

    // Projected conditions
    RexNode dataFileCondition = buildCaseCall(input, IcebergFileType.DATA);
    RexNode positionDeleteCondition = buildCaseCall(input, IcebergFileType.POSITION_DELETES);
    RexNode equalityDeleteCondition = buildCaseCall(input, IcebergFileType.EQUALITY_DELETES);
    RexNode manifestCondition = buildCaseCall(input, IcebergFileType.MANIFEST);
    RexNode manifestListCondition = buildCaseCall(input, IcebergFileType.MANIFEST_LIST);
    RexNode partitionStatsCondition = buildCaseCall(input, IcebergFileType.PARTITION_STATS);

    // Projected deleted data files
    RelDataType nullableBigInt =
        typeFactory.createTypeWithNullability(typeFactory.createSqlType(BIGINT), true);
    List<RexNode> projectExpression =
        ImmutableList.of(
            dataFileCondition,
            positionDeleteCondition,
            equalityDeleteCondition,
            manifestCondition,
            manifestListCondition,
            partitionStatsCondition);

    List<String> summaryCols =
        VacuumOutputSchema.EXPIRE_SNAPSHOTS_OUTPUT_SCHEMA.getFields().stream()
            .map(Field::getName)
            .collect(Collectors.toList());

    RelDataTypeFactory.FieldInfoBuilder fieldInfoBuilder = typeFactory.builder();
    summaryCols.forEach(c -> fieldInfoBuilder.add(c, nullableBigInt));
    RelDataType projectedRowType = fieldInfoBuilder.build();

    ProjectPrel project =
        ProjectPrel.create(cluster, traitSet, input, projectExpression, projectedRowType);

    // Aggregated summary
    List<AggregateCall> aggs =
        summaryCols.stream()
            .map(c -> buildAggregateCall(project, projectedRowType, c))
            .collect(Collectors.toList());
    Prel agg =
        StreamAggPrel.create(
            cluster,
            project.getTraitSet(),
            project,
            ImmutableBitSet.of(),
            Collections.EMPTY_LIST,
            aggs,
            null);

    // Project: return 0 as row count in case there is no Agg record (i.e., no orphan files to
    // delete)
    List<RexNode> projectExprs =
        summaryCols.stream().map(c -> notNullProjectExpr(agg, c)).collect(Collectors.toList());
    RelDataType projectRowType =
        RexUtil.createStructType(
            agg.getCluster().getTypeFactory(), projectExprs, summaryCols, null);
    return ProjectPrel.create(cluster, agg.getTraitSet(), agg, projectExprs, projectRowType);
  }

  private RexNode buildCaseCall(Prel orphanFileDeleteRel, IcebergFileType icebergFileType) {
    RexBuilder rexBuilder = cluster.getRexBuilder();
    RelDataTypeFactory typeFactory = cluster.getTypeFactory();
    Function<String, RexNode> makeLiteral =
        i -> rexBuilder.makeLiteral(i, typeFactory.createSqlType(VARCHAR), false);
    RelDataType nullableBigInt =
        typeFactory.createTypeWithNullability(typeFactory.createSqlType(BIGINT), true);

    RelDataTypeField orphanFileTypeField =
        orphanFileDeleteRel.getRowType().getField(FILE_TYPE, false, false);
    RexInputRef orphanFileTypeIn =
        rexBuilder.makeInputRef(orphanFileTypeField.getType(), orphanFileTypeField.getIndex());
    RelDataTypeField recordsField =
        orphanFileDeleteRel.getRowType().getField(RECORDS, false, false);
    RexNode recordsIn =
        rexBuilder.makeCast(
            nullableBigInt,
            rexBuilder.makeInputRef(recordsField.getType(), recordsField.getIndex()));

    RexNode equalsCall =
        rexBuilder.makeCall(EQUALS, orphanFileTypeIn, makeLiteral.apply(icebergFileType.name()));
    return rexBuilder.makeCall(
        CASE, equalsCall, recordsIn, rexBuilder.makeZeroLiteral(nullableBigInt));
  }
}
