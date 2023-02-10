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

import static com.dremio.exec.planner.OptimizeOutputSchema.NEW_DATA_FILES_COUNT;
import static com.dremio.exec.planner.OptimizeOutputSchema.REWRITTEN_DATA_FILE_COUNT;
import static com.dremio.exec.store.RecordWriter.OPERATION_TYPE_COLUMN;
import static com.dremio.exec.store.RecordWriter.RECORDS_COLUMN;
import static com.dremio.exec.store.SystemSchemas.DATAFILE_PATH;
import static com.dremio.exec.store.SystemSchemas.ICEBERG_METADATA;
import static com.dremio.exec.store.iceberg.IcebergUtils.getCurrentPartitionSpec;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CASE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.EQUALS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SUM;
import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;

import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.physical.config.ImmutableManifestScanFilters;
import com.dremio.exec.physical.config.ManifestScanFilters;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.ProjectPrel;
import com.dremio.exec.planner.physical.StreamAggPrel;
import com.dremio.exec.planner.sql.handlers.query.OptimizeOptions;
import com.dremio.exec.store.OperationType;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.iceberg.IcebergScanPlanBuilder;
import com.dremio.exec.store.iceberg.model.ImmutableManifestScanOptions;
import com.dremio.exec.store.iceberg.model.ManifestScanOptions;
import com.dremio.exec.util.ColumnUtils;
import com.dremio.exec.util.LongRange;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/***
 * Expand plans for OPTIMIZE TABLE
 */
public class OptimizePlanGenerator extends TableManagementPlanGenerator {

  private final IcebergScanPlanBuilder planBuilder;
  private final Long minInputFiles;

  public OptimizePlanGenerator(RelOptTable table, RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
                               TableMetadata tableMetadata, CreateTableEntry createTableEntry, OptimizerRulesContext context, OptimizeOptions optimizeOptions) {
    super(table, cluster, traitSet, input, tableMetadata, createTableEntry, context);
    ManifestScanFilters manifestScanFilters =  new ImmutableManifestScanFilters.Builder()
      .setSkipDataFileSizeRange(new LongRange(optimizeOptions.getMinFileSizeBytes(), optimizeOptions.getMaxFileSizeBytes()))
      .setMinPartitionSpecId(getCurrentPartitionSpec(tableMetadata.getDatasetConfig().getPhysicalDataset()).specId()).build();
    this.planBuilder = new IcebergScanPlanBuilder (
      cluster,
      traitSet,
      table,
      tableMetadata,
      null,
      context,
      manifestScanFilters
    );
    this.minInputFiles = optimizeOptions.getMinInputFiles();
  }

  /*
  *
                              ┌──────────────────────┐
                              │IcebergWriterCommitter│
                              └─────────▲────────────┘
                              ┌─────────┴────────────┐
                              │      Union           │
                              └─────────▲────────────┘
               ┌────────────────────────┴────────────────────┐
  ┌────────────┴───────────────────┐             ┌───────────┴────────────┐
  │       TableFunctionPrel        │             │    WriterPrel          │
  │ (DELETED_DATA_FILES_METADATA)  │             │                        │
  └────────────▲───────────────────┘             └──────────▲─────────────┘
               │                                 ┌──────────┴─────────────┐
               │                                 │ TableFunctionPrel      │
               │                                 │ (DATA_FILE_SCAN)       │
               │                                 └──────────▲─────────────┘
 ┌─────────────┴──────────────────┐              ┌──────────┴─────────────┐
 │  IcebergManifestScanPrel       │              │ IcebergManifestScanPrel│
 │    (With Predicates)           │              │  (With Predicates)     │
 └──────────────▲─────────────────┘              └───────────▲────────────┘
 ┌──────────────┴─────────────────┐              ┌───────────┴────────────┐
 │   IcebergManifestListPrel      │              │IcebergManifestListPrel │
 └────────────────────────────────┘              └────────────────────────┘
  * */
  public Prel getPlan() {
    try {
      return getOutputSummaryPlan(getDataWriterPlan(planBuilder.build(), deleteDataFilePlan()));
    } catch (InvalidRelException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Scan the manifests to return the deleted data files.
   *
   *        Project
   *          |
   *          |
   *  IcebergManifestScanPrel
   *          |
   *          |
   * IcebergManifestListPrel
   */
  private Prel deleteDataFilePlan() {
    ManifestScanOptions manifestScanOptions =  new ImmutableManifestScanOptions.Builder()
      .setIncludesSplitGen(false)
      .setIncludesIcebergMetadata(true)
      .build();
    RelNode manifestScan = planBuilder.buildManifestRel(manifestScanOptions);
    RexBuilder rexBuilder = cluster.getRexBuilder();

    final List<String> projectFields = ImmutableList.of(ColumnUtils.ROW_COUNT_COLUMN_NAME,ColumnUtils.FILE_PATH_COLUMN_NAME, ICEBERG_METADATA);

    Pair<Integer, RelDataTypeField> datafilePathCol = MoreRelOptUtil.findFieldWithIndex(manifestScan.getRowType().getFieldList(), DATAFILE_PATH);
    Pair<Integer, RelDataTypeField> icebergMetadataCol = MoreRelOptUtil.findFieldWithIndex(manifestScan.getRowType().getFieldList(), ICEBERG_METADATA);
    Preconditions.checkNotNull(datafilePathCol, "ManifestScan should always have datafilePath with rowType.");
    Preconditions.checkNotNull(icebergMetadataCol, "ManifestScan should always have icebergMetadata with rowType.");

    final List<RexNode> projectExpressions = ImmutableList.of(rexBuilder.makeBigintLiteral(BigDecimal.ONE),
      rexBuilder.makeInputRef(datafilePathCol.right.getType(), datafilePathCol.left),
      rexBuilder.makeInputRef(icebergMetadataCol.right.getType(), icebergMetadataCol.left));

    RelDataType newRowType = RexUtil.createStructType(rexBuilder.getTypeFactory(), projectExpressions, projectFields, SqlValidatorUtil.F_SUGGESTER);

    return ProjectPrel.create(manifestScan.getCluster(), manifestScan.getTraitSet(), manifestScan, projectExpressions, newRowType);
  }

  /**
   * rewritten_data_files_count=[CASE(=(OperationType, 0), 1, null)], new_data_files_count=[CASE(=(OperationType, 1), 1, null)
   */
  private Prel getOutputSummaryPlan(Prel writerPrel) throws InvalidRelException {
    //Initializations and literal for projected conditions.
    RexBuilder rexBuilder = cluster.getRexBuilder();
    RelOptCluster cluster = writerPrel.getCluster();
    RelTraitSet traitSet = writerPrel.getTraitSet();
    RelDataTypeFactory typeFactory = cluster.getTypeFactory();
    RelDataType nullableBigInt = typeFactory.createTypeWithNullability(typeFactory.createSqlType(BIGINT), true);

    Function<Integer, RexNode> makeLiteral = i -> rexBuilder.makeLiteral(i, typeFactory.createSqlType(INTEGER), false);
    RelDataTypeField opTypeField = writerPrel.getRowType().getField(OPERATION_TYPE_COLUMN, false, false);
    RexInputRef opTypeIn = rexBuilder.makeInputRef(opTypeField.getType(), opTypeField.getIndex());
    RelDataTypeField recordsField = writerPrel.getRowType().getField(RECORDS_COLUMN, false, false);
    RexNode recordsIn = rexBuilder.makeCast(nullableBigInt, rexBuilder.makeInputRef(recordsField.getType(), recordsField.getIndex()));

    // Projected conditions
    RexNode deletedFileOp = rexBuilder.makeCall(EQUALS, opTypeIn, makeLiteral.apply(OperationType.DELETE_DATAFILE.value));
    RexNode newFileOp = rexBuilder.makeCall(EQUALS, opTypeIn, makeLiteral.apply(OperationType.ADD_DATAFILE.value));
    RexNode flagRewrittenFile = rexBuilder.makeCall(CASE, deletedFileOp, recordsIn, rexBuilder.makeZeroLiteral(nullableBigInt));
    RexNode flagNewFile = rexBuilder.makeCall(CASE, newFileOp, recordsIn, rexBuilder.makeZeroLiteral(nullableBigInt));

    // Projected new/written data files
    List<RexNode> projectExpression = ImmutableList.of(flagRewrittenFile, flagNewFile);
    RelDataType projectedRowType = typeFactory.builder().add(REWRITTEN_DATA_FILE_COUNT, nullableBigInt).add(NEW_DATA_FILES_COUNT, nullableBigInt).build();
    ProjectPrel project = ProjectPrel.create(cluster, traitSet, writerPrel, projectExpression, projectedRowType);

    // Aggregated summary
    AggregateCall totalRewrittenFiles = sum(project, projectedRowType, REWRITTEN_DATA_FILE_COUNT);
    AggregateCall totalNewFiles = sum(project, projectedRowType, NEW_DATA_FILES_COUNT);
    StreamAggPrel aggregatedCounts = StreamAggPrel.create(cluster, traitSet, project, ImmutableBitSet.of(),
      Collections.EMPTY_LIST, ImmutableList.of(totalRewrittenFiles, totalNewFiles), null);

    return aggregatedCounts;
  }

  private AggregateCall sum(Prel relNode, RelDataType projectRowType, String fieldName) {
    RelDataTypeField recordsField = projectRowType.getField(fieldName, false, false);
    return AggregateCall.create(
      SUM,
      false,
      false,
      ImmutableList.of(recordsField.getIndex()),
      -1,
      RelCollations.EMPTY,
      1,
      relNode,
      null,
      recordsField.getName());
  }
}
