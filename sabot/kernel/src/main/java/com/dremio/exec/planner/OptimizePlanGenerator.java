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
import static com.dremio.exec.planner.OptimizeOutputSchema.OPTIMIZE_OUTPUT_SUMMARY;
import static com.dremio.exec.planner.OptimizeOutputSchema.REWRITTEN_DATA_FILE_COUNT;
import static com.dremio.exec.planner.OptimizeOutputSchema.REWRITTEN_DELETE_FILE_COUNT;
import static com.dremio.exec.store.RecordWriter.OPERATION_TYPE_COLUMN;
import static com.dremio.exec.store.RecordWriter.RECORDS_COLUMN;
import static com.dremio.exec.store.SystemSchemas.DATAFILE_PATH;
import static com.dremio.exec.store.SystemSchemas.DELETE_FILE_PATH;
import static com.dremio.exec.store.SystemSchemas.FILE_SIZE;
import static com.dremio.exec.store.SystemSchemas.ICEBERG_METADATA;
import static com.dremio.exec.store.SystemSchemas.IMPLICIT_SEQUENCE_NUMBER;
import static com.dremio.exec.store.SystemSchemas.PARTITION_SPEC_ID;
import static com.dremio.exec.store.SystemSchemas.POS;
import static com.dremio.exec.store.iceberg.IcebergUtils.getCurrentPartitionSpec;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CASE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.EQUALS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SUM;
import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.PartitionSpec;

import com.dremio.common.JSONOptions;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.physical.config.ImmutableManifestScanFilters;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.physical.config.TableFunctionContext;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.planner.logical.partition.PruneFilterCondition;
import com.dremio.exec.planner.physical.DistributionTrait;
import com.dremio.exec.planner.physical.FilterPrel;
import com.dremio.exec.planner.physical.HashAggPrel;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.ProjectPrel;
import com.dremio.exec.planner.physical.StreamAggPrel;
import com.dremio.exec.planner.physical.TableFunctionPrel;
import com.dremio.exec.planner.physical.TableFunctionUtil;
import com.dremio.exec.planner.physical.UnionAllPrel;
import com.dremio.exec.planner.physical.ValuesPrel;
import com.dremio.exec.planner.sql.CalciteArrowHelper;
import com.dremio.exec.planner.sql.handlers.query.OptimizeOptions;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.OperationType;
import com.dremio.exec.store.RecordWriter;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.iceberg.IcebergScanPlanBuilder;
import com.dremio.exec.store.iceberg.OptimizeManifestsTableFunctionContext;
import com.dremio.exec.store.iceberg.model.ImmutableManifestScanOptions;
import com.dremio.exec.store.iceberg.model.ManifestScanOptions;
import com.dremio.exec.util.ColumnUtils;
import com.dremio.exec.util.LongRange;
import com.dremio.service.namespace.dataset.proto.ScanStats;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/***
 * Expand plans for OPTIMIZE TABLE
 */
public class
OptimizePlanGenerator extends TableManagementPlanGenerator {
  private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final List<String> deleteFilesMetadataInputCols = ImmutableList.of(ColumnUtils.ROW_COUNT_COLUMN_NAME, ColumnUtils.FILE_PATH_COLUMN_NAME, ICEBERG_METADATA);;

  private final IcebergScanPlanBuilder planBuilder;
  private final OptimizeOptions optimizeOptions;
  private final Integer icebergCurrentPartitionSpecId;

  public OptimizePlanGenerator(RelOptTable table,
                               RelOptCluster cluster,
                               RelTraitSet traitSet,
                               RelNode input,
                               TableMetadata tableMetadata,
                               CreateTableEntry createTableEntry,
                               OptimizerRulesContext context,
                               OptimizeOptions optimizeOptions,
                               PruneFilterCondition partitionFilter) {
    super(table, cluster, traitSet, input, tableMetadata, createTableEntry, context);
    PartitionSpec currentPartitionSpec = getCurrentPartitionSpec(tableMetadata.getDatasetConfig().getPhysicalDataset());
    validatePruneCondition(partitionFilter);
    if (!isPartitionExpressionRequired(partitionFilter, currentPartitionSpec)) {
      partitionFilter = new PruneFilterCondition(partitionFilter.getPartitionRange(), null, null);
    }
    this.icebergCurrentPartitionSpecId = currentPartitionSpec != null ? currentPartitionSpec.specId() : 0;
    int minSpecId = icebergCurrentPartitionSpecId;
    /*
    * In case of filter, it should not use all the data files for compaction.
    * It filtered out and applies the target file size range.
    * If filter is not there, it compacts all the old data files irrespective of the target file size.
    * */
    if (partitionFilter != null && (partitionFilter.getPartitionRange() != null || partitionFilter.getPartitionExpression() != null)) {
      minSpecId = 0;
    }
    ScanStats deleteStats = tableMetadata.getDatasetConfig().getPhysicalDataset().getIcebergMetadata().getDeleteManifestStats();
    ImmutableManifestScanFilters.Builder manifestScanFiltersBuilder =
      (deleteStats != null && deleteStats.getRecordCount() > 0) ? new ImmutableManifestScanFilters.Builder()
        : new ImmutableManifestScanFilters.Builder()
        .setSkipDataFileSizeRange(new LongRange(optimizeOptions.getMinFileSizeBytes(), optimizeOptions.getMaxFileSizeBytes()))
        .setMinPartitionSpecId(minSpecId);
    this.planBuilder = new IcebergScanPlanBuilder (
      cluster,
      traitSet,
      table,
      tableMetadata,
      null,
      context,
      manifestScanFiltersBuilder.build(),
      partitionFilter
    );
    this.optimizeOptions = optimizeOptions;
  }

  /**
   * Optimize is only supported on partition columns.
   * It validates if pruneFilterCondition contains any non-partition columns and throws userException.
   * else it returns a list of partition columns from pruneFilterCondition.
   */
  private void validatePruneCondition(PruneFilterCondition pruneFilterCondition) {
    if (pruneFilterCondition != null && pruneFilterCondition.getNonPartitionRange() != null) {
      pruneFilterCondition.getNonPartitionRange().accept(new RexVisitorImpl<Void>(true) {
        @Override
        public Void visitInputRef(RexInputRef inputRef) {
          throw UserException.unsupportedError().message(String.format("OPTIMIZE command is only supported on the partition columns - %s",
            tableMetadata.getReadDefinition().getPartitionColumnsList())).buildSilently();
        }
      });
    }
  }

  /**
   * Use all the applicable data files in the case filter is on transformed partition expression.
   */
  private Boolean isPartitionExpressionRequired(PruneFilterCondition pruneFilterCondition, PartitionSpec partitionSpec) {
    Set<Integer> expressionSourceIds = new HashSet<>();
    if (pruneFilterCondition != null && pruneFilterCondition.getPartitionExpression() != null) {
      pruneFilterCondition.getPartitionExpression().accept(new RexVisitorImpl<Void>(true) {
        @Override
        public Void visitInputRef(RexInputRef inputRef) {
          expressionSourceIds.add(inputRef.getIndex()+1);
          return null;
        }
      });
    }
    for (Integer id: expressionSourceIds) {
      if (!partitionSpec.identitySourceIds().contains(id)) {
        return false;
      }
    }
    return true;
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
  @Override
  public Prel getPlan() {
    try {
      // Optimize manifests only
      if (optimizeOptions.isOptimizeManifestsOnly()) {
        return getOptimizeManifestsOnlyPlan();
      }

      Prel rewritePlan = planBuilder.hasDeleteFiles() ? deleteAwareOptimizePlan()
        : getDataWriterPlan(planBuilder.build(), deleteDataFilePlan());
      if (optimizeOptions.isOptimizeManifestFiles()) { // Optimize data files as well as manifests
        rewritePlan = getOptimizeManifestTableFunctionPrel(rewritePlan, RecordWriter.SCHEMA);
      }
      return getOutputSummaryPlan(rewritePlan);
    } catch (InvalidRelException e) {
      throw new RuntimeException(e);
    }
  }

  /*
  * Plan for OPTIMIZE TABLE operation when table has positional delete files.
  *
  * The left side of the plan is used to mark which files need to be rewritten. It has 2 marked boxes for reuse:
  * Section *A*
  *   Left Branch of section *A* is used to scan DATA manifests and filter these data file objects based on input from
  *   Right Branch which scans DELETE manifests and reads positional delete files. This filtering of data files is done
  *   based on the following conditions:
  *   - File size not in ideal range
  *   - Partition Spec not current
  *   - Data file has Delete File attached to it
  *
  * Section *B*
  *   Used to read DELETE manifests and pass file objects to DELETED_FILES_METADATA table function
  *
  * The right-most branch of the plan is used to write new data files - It takes as input the plan from boxes A and B
  * to mark the input data that needs to be rewritten into ideally sized files.
  *
                                                                               ┌──────────────────────┐
                                                                               │IcebergWriterCommitter│
                                                                               └─────────▲────────────┘
                                                                               ┌─────────┴────────────┐
                                                                               │      Union           │
                                                                               └─────────▲────────────┘
                                         ┌───────────────────────────────────────────────┴──────────────────────────────────────┐
                                         │                                                                                      │
                               ┌─────────┴────────────┐                                                                         │
                               │      Union           │                                                                         │
                               └─────────▲────────────┘                                                                         │
                ┌────────────────────────┴───────────────────────────────────────────────────────┐                              │
   ┌────────────┴───────────────────┐                                               ┌────────────┴──────────────┐    ┌──────────┴───────────┐
   │       TableFunctionPrel        │                                               │       TableFunctionPrel   │    │    WriterPrel        │
   │ (DELETED_FILES_METADATA)       │                                               │ (DELETED_FILES_METADATA)  │    └──────────▲───────────┘
   └────────────▲───────────────────┘                                               └────────────▲──────────────┘               │
 ┌──────────────│─────────────────────────────────────────────────────────────┐  ┌───────────────│───────────────┐   ┌──────────┴───────────┐
 │   ┌──────────┴─────────────┐                                           *A* │  │               │            *B*│   │ TableFunctionPrel    │
 │   │       Filter           │                                               │  │               │               │   │ (DATA_FILE_SCAN)     │
 │   └──────────▲─────────────┘                                               │  │               │               │   └──────────▲───────────┘
 │   ┌──────────┴─────────────┐                                               │  │               │               │              │
 │   │       HashJoin         │──────────────────────────────┐                │  │               │               │   ┌──────────┴───────────┐
 │   └──────────▲─────────────┘                              │                │  │               │               │   │ TableFunctionPrel    │
 │              │                                 ┌──────────┴─────────────┐  │  │               │               │   │(IcebergDeleteFileAgg)│
 │              │                                 │    HashAggPrel         │  │  │               │               │   └──────────▲───────────┘
 │              │                                 └──────────▲─────────────┘  │  │               │               │              │
 │              │                                 ┌──────────┴─────────────┐  │  │               │               │   ┌──────────┴───────────┐
 │              │                                 │ TableFunctionPrel      │  │  │               │               │   │       HashJoin       │
 │              │                                 │ (DATA_FILE_SCAN)       │  │  │               │               │   └──────────▲───────────┘
 │              │                                 └──────────▲─────────────┘  │  │               │               │              │
 │┌─────────────┴──────────────────┐              ┌──────────┴─────────────┐  │  │    ┌──────────┴─────────────┐ │              │
 ││  IcebergManifestScanPrel       │              │ IcebergManifestScanPrel│  │  │    │ IcebergManifestScanPrel│ │        ┌─────┴──────┐
 ││    DATA                        │              │  DELETE                │  │  │    │  DELETE                │ │  ┌─────┴────┐  ┌────┴─────┐
 │└──────────────▲─────────────────┘              └───────────▲────────────┘  │  │    └───────────▲────────────┘ │  │          │  │          │
 │┌──────────────┴─────────────────┐              ┌───────────┴────────────┐  │  │    ┌───────────┴────────────┐ │  │    *A*   │  │   *B*    │
 ││   IcebergManifestListPrel      │              │IcebergManifestListPrel │  │  │    │IcebergManifestListPrel │ │  │          │  │          │
 │└────────────────────────────────┘              └────────────────────────┘  │  │    └────────────────────────┘ │  └──────────┘  └──────────┘
 └────────────────────────────────────────────────────────────────────────────┘  └───────────────────────────────┘
 */
  private Prel deleteAwareOptimizePlan() throws InvalidRelException {
    return getDataWriterPlan(planBuilder.buildDataScanWithSplitGen(
        planBuilder.buildDataAndDeleteFileJoinAndAggregate(
          buildRemoveSideDataFilePlan(), buildRemoveSideDeleteFilePlan())),
      manifestWriterPlan -> {
        try {
          return getMetadataWriterPlan(deleteDataFilePlan(), removeDeleteFilePlan(), manifestWriterPlan);
        } catch (InvalidRelException e) {
          throw new RuntimeException(e);
        }
      });
  }

  private Prel getOptimizeManifestsOnlyPlan() {
    RelTraitSet manifestTraitSet = traitSet.plus(DistributionTrait.SINGLETON).plus(Prel.PHYSICAL);

    // OptimizeManifestTableFunction forwards the input to the next operator.
    // Hence, the values provided here will be supplied to the output in happy case.
    ObjectNode successMessage = OBJECT_MAPPER.createObjectNode();
    successMessage.set(OPTIMIZE_OUTPUT_SUMMARY, new TextNode("Optimize table successful"));
    RelDataType rowType = OptimizeOutputSchema.getRelDataType(cluster.getTypeFactory(), true);
    ValuesPrel valuesPrel = new ValuesPrel(cluster, manifestTraitSet, rowType, new JSONOptions(successMessage), 1d);

    return getOptimizeManifestTableFunctionPrel(valuesPrel, CalciteArrowHelper.fromCalciteRowTypeJson(rowType));
  }

  private Prel getOptimizeManifestTableFunctionPrel(Prel input, BatchSchema outputSchema) {
    TableFunctionContext functionContext = new OptimizeManifestsTableFunctionContext(tableMetadata, outputSchema,
      createTableEntry.getIcebergTableProps());

    TableFunctionConfig functionConfig = new TableFunctionConfig(
      TableFunctionConfig.FunctionType.ICEBERG_OPTIMIZE_MANIFESTS, true, functionContext);
    return new TableFunctionPrel(cluster, traitSet, table, input, tableMetadata, functionConfig, input.getRowType());
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
    ManifestScanOptions manifestScanOptions = new ImmutableManifestScanOptions.Builder()
      .setIncludesSplitGen(false)
      .setIncludesIcebergMetadata(true)
      .build();

    RelNode output = planBuilder.hasDeleteFiles() ? buildRemoveSideDataFilePlan()
      : planBuilder.buildManifestRel(manifestScanOptions);
    RexBuilder rexBuilder = cluster.getRexBuilder();

    Pair<Integer, RelDataTypeField> datafilePathCol = MoreRelOptUtil.findFieldWithIndex(output.getRowType().getFieldList(), DATAFILE_PATH);
    Pair<Integer, RelDataTypeField> icebergMetadataCol = MoreRelOptUtil.findFieldWithIndex(output.getRowType().getFieldList(), ICEBERG_METADATA);

    final List<RexNode> projectExpressions = ImmutableList.of(rexBuilder.makeBigintLiteral(BigDecimal.ONE),
      rexBuilder.makeInputRef(datafilePathCol.right.getType(), datafilePathCol.left),
      rexBuilder.makeInputRef(icebergMetadataCol.right.getType(), icebergMetadataCol.left));

    RelDataType newRowType = RexUtil.createStructType(rexBuilder.getTypeFactory(), projectExpressions, deleteFilesMetadataInputCols, SqlValidatorUtil.F_SUGGESTER);

    return ProjectPrel.create(output.getCluster(), output.getTraitSet(), output, projectExpressions, newRowType);
  }

  /**
   * Scan the manifests to return the purged delete files.
   * Plan same as {@link #deleteDataFilePlan} with manifest scan operator reading DELETE manifests instead of DATA.
   */
  private RelNode removeDeleteFilePlan() {
    RexBuilder rexBuilder = cluster.getRexBuilder();
    RelNode output = buildRemoveSideDeleteFilePlan();
    Pair<Integer, RelDataTypeField> outputFilePathCol = MoreRelOptUtil.findFieldWithIndex(output.getRowType().getFieldList(), DATAFILE_PATH);
    Pair<Integer, RelDataTypeField> outputIcebergMetadataCol = MoreRelOptUtil.findFieldWithIndex(output.getRowType().getFieldList(), ICEBERG_METADATA);

    final List<RexNode> outputExpressions = ImmutableList.of(rexBuilder.makeBigintLiteral(BigDecimal.ONE),
      rexBuilder.makeInputRef(outputFilePathCol.right.getType(), outputFilePathCol.left),
      rexBuilder.makeInputRef(outputIcebergMetadataCol.right.getType(), outputIcebergMetadataCol.left));

    RelDataType outputRowType = RexUtil.createStructType(rexBuilder.getTypeFactory(), outputExpressions, deleteFilesMetadataInputCols, SqlValidatorUtil.F_SUGGESTER);

    return ProjectPrel.create(output.getCluster(), output.getTraitSet(), output, outputExpressions, outputRowType);
  }

  private Prel getMetadataWriterPlan(RelNode dataFileAggrPlan, RelNode deleteFileAggrPlan, RelNode manifestWriterPlan) throws InvalidRelException {
    // Insert a table function that'll pass the path through and set the OperationType
    TableFunctionPrel deletedDataFilesTableFunctionPrel = getDeleteFilesMetadataTableFunctionPrel(dataFileAggrPlan,
      getProjectedColumns(), TableFunctionUtil.getDeletedFilesMetadataTableFunctionContext(
        OperationType.DELETE_DATAFILE, RecordWriter.SCHEMA, getProjectedColumns(), true));
    TableFunctionPrel deletedDeleteFilesTableFunctionPrel = getDeleteFilesMetadataTableFunctionPrel(deleteFileAggrPlan,
      getProjectedColumns(), TableFunctionUtil.getDeletedFilesMetadataTableFunctionContext(
        OperationType.DELETE_DELETEFILE, RecordWriter.SCHEMA, getProjectedColumns(), true));

    RelNode deletedDataAndDeleteFilesTableFunction = new UnionAllPrel(cluster,
      deleteFileAggrPlan.getTraitSet(),
      ImmutableList.of(deletedDataFilesTableFunctionPrel, deletedDeleteFilesTableFunctionPrel),
      true);

    final RelTraitSet traits = traitSet.plus(DistributionTrait.SINGLETON).plus(Prel.PHYSICAL);

    // Union the updating of the deleted data's metadata with the rest
    return getUnionPrel(traits, manifestWriterPlan, deletedDataAndDeleteFilesTableFunction);
  }

  /**
   * @param deleteFileScan DataFileScan table function Prel created by scanning positional delete files
   * @return HashAggregate of input on File path with COUNT aggregation on delete positions
   */
  public static Prel aggregateDeleteFiles(RelNode deleteFileScan) {
    RelDataTypeField filePathField = Preconditions.checkNotNull(deleteFileScan.getRowType()
      .getField(DELETE_FILE_PATH, false, false));
    RelDataTypeField implicitSequenceNumberField = Preconditions.checkNotNull(deleteFileScan.getRowType()
      .getField(IMPLICIT_SEQUENCE_NUMBER, false, false));

    AggregateCall aggPosCount = AggregateCall.create(
      SqlStdOperatorTable.COUNT,
      false,
      false,
      Collections.emptyList(),
      -1,
      RelCollations.EMPTY,
      1,
      deleteFileScan,
      deleteFileScan.getCluster().getTypeFactory().createSqlType(SqlTypeName.BIGINT),
      POS
    );
    AggregateCall aggSeqNumberMax = AggregateCall.create(
      SqlStdOperatorTable.MAX,
      false,
      ImmutableList.of(implicitSequenceNumberField.getIndex()),
      -1,
      implicitSequenceNumberField.getType(),
      IMPLICIT_SEQUENCE_NUMBER
    );

    ImmutableBitSet groupSet = ImmutableBitSet.of(filePathField.getIndex());
    try {
      return HashAggPrel.create(
        deleteFileScan.getCluster(),
        deleteFileScan.getTraitSet(),
        deleteFileScan,
        groupSet,
        ImmutableList.of(groupSet),
        ImmutableList.of(aggPosCount, aggSeqNumberMax),
        null
      );
    } catch (InvalidRelException e) {
      throw new RuntimeException("Failed to create HashAggPrel during delete file scan.", e);
    }
  }

  /**
   * @param input Manifest Scan (DATA), joined with Delete file reads - Files that have deletes linked to them
   *              will have a non-null POS column.
   * @return filter input for data files that need to be rewritten by applying the following conditions
   * <ul>
   * <li> File size not in ideal range </li>
   * <li> Partition spec not matching current partition </li>
   * <li> Has delete file(s) attached </li>
   * </ul>
   */
  private RelNode subOptimalDataFilesFilter(RelNode input) {
    RexBuilder rexBuilder = cluster.getRexBuilder();

    Pair<Integer, RelDataTypeField> dataFileSizeCol = MoreRelOptUtil.findFieldWithIndex(input.getRowType().getFieldList(), FILE_SIZE);
    Pair<Integer, RelDataTypeField> dataPartitionSpecIdCol = MoreRelOptUtil.findFieldWithIndex(input.getRowType().getFieldList(), PARTITION_SPEC_ID);
    Pair<Integer, RelDataTypeField> deleteDataFilePosCol  = MoreRelOptUtil.findFieldWithIndex(input.getRowType().getFieldList(), POS);

    RexNode posCondition = rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, rexBuilder.makeInputRef(deleteDataFilePosCol.right.getType(), deleteDataFilePosCol.left));
    RexNode partitionSpecIdCondition = rexBuilder.makeCall(SqlStdOperatorTable.NOT_EQUALS,
      rexBuilder.makeInputRef(dataPartitionSpecIdCol.right.getType(), dataPartitionSpecIdCol.left),
      rexBuilder.makeLiteral(icebergCurrentPartitionSpecId, dataPartitionSpecIdCol.right.getType()));
    RexNode minFileSizeCondition = rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN,
      rexBuilder.makeInputRef(dataFileSizeCol.right.getType(), dataFileSizeCol.left),
      rexBuilder.makeLiteral(optimizeOptions.getMinFileSizeBytes(), dataFileSizeCol.right.getType()));
    RexNode maxFileSizeCondition = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN,
      rexBuilder.makeInputRef(dataFileSizeCol.right.getType(), dataFileSizeCol.left),
      rexBuilder.makeLiteral(optimizeOptions.getMaxFileSizeBytes(), dataFileSizeCol.right.getType()));

    RexNode filterCondition = rexBuilder.makeCall(SqlStdOperatorTable.OR, ImmutableList.of(posCondition, partitionSpecIdCondition,
      minFileSizeCondition, maxFileSizeCondition));
    return new FilterPrel(cluster, input.getTraitSet(), input, RexUtil.flatten(rexBuilder, filterCondition));
  }

  /**
   * [*A*] from {@link #deleteAwareOptimizePlan}
   */
  private RelNode buildRemoveSideDataFilePlan(){
    return subOptimalDataFilesFilter(planBuilder.buildDataManifestScanWithDeleteJoin(
      aggregateDeleteFiles(planBuilder.buildDeleteFileScan(context))));
  }

  /**
   * [*B*] from {@link #deleteAwareOptimizePlan}
   */
  private RelNode buildRemoveSideDeleteFilePlan(){
    return planBuilder.buildManifestRel(new ImmutableManifestScanOptions.Builder().setIncludesSplitGen(false)
      .setIncludesIcebergMetadata(true).setManifestContent(ManifestContent.DELETES).build(), false);
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
    RexNode removedDeleteFileOp = rexBuilder.makeCall(EQUALS, opTypeIn, makeLiteral.apply(OperationType.DELETE_DELETEFILE.value));
    RexNode newFileOp = rexBuilder.makeCall(EQUALS, opTypeIn, makeLiteral.apply(OperationType.ADD_DATAFILE.value));
    RexNode flagRewrittenFile = rexBuilder.makeCall(CASE, deletedFileOp, recordsIn, rexBuilder.makeZeroLiteral(nullableBigInt));
    RexNode flagRewrittenDeleteFile = rexBuilder.makeCall(CASE, removedDeleteFileOp, recordsIn, rexBuilder.makeZeroLiteral(nullableBigInt));
    RexNode flagNewFile = rexBuilder.makeCall(CASE, newFileOp, recordsIn, rexBuilder.makeZeroLiteral(nullableBigInt));

    // Projected new/written data files
    List<RexNode> projectExpression = ImmutableList.of(flagRewrittenFile, flagRewrittenDeleteFile, flagNewFile);
    RelDataType projectedRowType = typeFactory.builder()
      .add(REWRITTEN_DATA_FILE_COUNT, nullableBigInt)
      .add(REWRITTEN_DELETE_FILE_COUNT, nullableBigInt)
      .add(NEW_DATA_FILES_COUNT, nullableBigInt).build();
    ProjectPrel project = ProjectPrel.create(cluster, traitSet, writerPrel, projectExpression, projectedRowType);

    // Aggregated summary
    AggregateCall totalRewrittenFiles = sum(project, projectedRowType, REWRITTEN_DATA_FILE_COUNT);
    AggregateCall totalRewrittenDeleteFiles = sum(project, projectedRowType, REWRITTEN_DELETE_FILE_COUNT);
    AggregateCall totalNewFiles = sum(project, projectedRowType, NEW_DATA_FILES_COUNT);
    StreamAggPrel aggregatedCounts = StreamAggPrel.create(cluster, traitSet, project, ImmutableBitSet.of(),
      Collections.EMPTY_LIST, ImmutableList.of(totalRewrittenFiles, totalRewrittenDeleteFiles, totalNewFiles), null);

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
