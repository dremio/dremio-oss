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
package com.dremio.exec.planner.physical.visitor;

import static com.dremio.exec.ExecConstants.ADAPTIVE_HASH;
import static com.dremio.exec.ExecConstants.DATA_SCAN_PARALLELISM;
import static com.dremio.exec.ExecConstants.ENABLE_ICEBERG_COMBINE_SMALL_FILES_FOR_PARTITIONED_TABLE_WRITES;
import static com.dremio.exec.ExecConstants.FORCE_USE_MOR_VECTOR_SORTER_FOR_PARTITION_TABLES_WITH_UNDEFINED_SORT_ORDER;
import static com.dremio.exec.store.RecordWriter.OPERATION_TYPE_COLUMN;
import static com.dremio.exec.store.RecordWriter.RECORDS_COLUMN;
import static com.dremio.exec.store.iceberg.IcebergUtils.hasNonIdentityPartitionColumns;
import static com.dremio.exec.store.iceberg.IcebergUtils.isIdentityPartitionColumn;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.physical.base.CombineSmallFileOptions;
import com.dremio.exec.physical.base.TableFormatWriterOptions.TableFormatOperation;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.physical.config.TableFunctionContext;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.planner.physical.AdaptiveHashExchangePrel;
import com.dremio.exec.planner.physical.BridgeExchangePrel;
import com.dremio.exec.planner.physical.BridgeReaderPrel;
import com.dremio.exec.planner.physical.DistributionTrait;
import com.dremio.exec.planner.physical.DistributionTrait.DistributionType;
import com.dremio.exec.planner.physical.DistributionTraitDef;
import com.dremio.exec.planner.physical.FilterPrel;
import com.dremio.exec.planner.physical.HashPrelUtil;
import com.dremio.exec.planner.physical.HashToRandomExchangePrel;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.exec.planner.physical.ProjectAllowDupPrel;
import com.dremio.exec.planner.physical.ProjectPrel;
import com.dremio.exec.planner.physical.SingleMergeExchangePrel;
import com.dremio.exec.planner.physical.SortPrel;
import com.dremio.exec.planner.physical.TableFunctionPrel;
import com.dremio.exec.planner.physical.TableFunctionUtil;
import com.dremio.exec.planner.physical.UnionAllPrel;
import com.dremio.exec.planner.physical.WindowPrel;
import com.dremio.exec.planner.physical.WriterPrel;
import com.dremio.exec.planner.sql.CalciteArrowHelper;
import com.dremio.exec.planner.sql.Checker;
import com.dremio.exec.planner.sql.DynamicReturnType;
import com.dremio.exec.planner.sql.SqlFunctionImpl;
import com.dremio.exec.planner.sql.parser.DmlUtils;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.OperationType;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.RecordWriter;
import com.dremio.exec.store.SystemSchemas;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.exec.store.iceberg.IcebergSerDe;
import com.dremio.exec.store.iceberg.IcebergSplitGenPrel;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.exec.store.iceberg.SchemaConverter;
import com.dremio.exec.util.ColumnUtils;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.namespace.file.proto.IcebergFileConfig;
import com.dremio.service.namespace.file.proto.ParquetFileConfig;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Insert additional operators before writing to impose various types of operations including: -
 * Create a hashed value to use for sorting when doing DISTRIBUTE BY - Sorting the data (on
 * DISTRIBUTE BY as well as the local sort field(s)) - Create a change detection field to create
 * separate files for partitions.
 */
public class WriterUpdater extends BasePrelVisitor<Prel, Void, RuntimeException> {

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(WriterUpdater.class);

  private static final WriterUpdater INSTANCE = new WriterUpdater();

  private static final String nonPartitionTableDummyPartitionValue =
      "p0$DREMIO_nonPartitionTableValue";
  private static String notSmallFileDummyPartitionValue =
      "p0$DREMIO_notSmallFilePartitionTableValue";

  private WriterUpdater() {}

  public static Prel update(Prel prel) {
    return prel.accept(INSTANCE, null);
  }

  private Prel renameAsNecessary(
      RelDataType expectedRowType, Prel initialInput, TableFormatOperation icebergWriterOperation) {
    boolean typesAndNamesExactMatch =
        RelOptUtil.areRowTypesEqual(initialInput.getRowType(), expectedRowType, true);
    boolean compatibleTypes;
    if (icebergWriterOperation == TableFormatOperation.INSERT) {
      compatibleTypes =
          MoreRelOptUtil.areRowTypesCompatibleForInsert(
              initialInput.getRowType(), expectedRowType, false, true);
    } else {
      compatibleTypes =
          MoreRelOptUtil.areRowTypesCompatible(
              initialInput.getRowType(), expectedRowType, false, true);
    }

    // schemas match exactly, or no chance of matching
    // we don't need to do any transformation or there is no use of transformation
    if (typesAndNamesExactMatch || !compatibleTypes) {
      return initialInput;
    }

    final RexBuilder rexBuilder = initialInput.getCluster().getRexBuilder();
    final List<RexNode> castExps =
        RexUtil.generateCastExpressions(rexBuilder, expectedRowType, initialInput.getRowType());

    return ProjectPrel.create(
        initialInput.getCluster(),
        initialInput.getTraitSet(),
        initialInput,
        castExps,
        expectedRowType);
  }

  @Override
  public Prel visitWriter(WriterPrel initialPrel, Void value) throws RuntimeException {
    final CreateTableEntry tableEntry = initialPrel.getCreateTableEntry();
    final WriterOptions options = tableEntry.getOptions();
    final Prel initialInput = ((Prel) initialPrel.getInput()).accept(this, null);

    Prel input =
        renameAsNecessary(
            initialPrel.getExpectedInboundRowType(),
            initialInput,
            options.getTableFormatOptions().getOperation());
    final WriterPrel prel =
        initialPrel.copy(initialPrel.getTraitSet(), ImmutableList.<RelNode>of(input));

    // update the writer by applying distribution/partition transformation/sorting
    Prel updatedWritePrel =
        updateWriter(prel, input, tableEntry, true, prel.getExpectedInboundRowType());

    // combine small files if needed
    return combineSmallFiles(updatedWritePrel, input, tableEntry, prel.getExpectedInboundRowType());
  }

  private Prel updateWriter(
      Prel prel,
      Prel input,
      CreateTableEntry tableEntry,
      boolean addSort,
      RelDataType expectedWriterInboundRowType) {
    WriterOptions options = tableEntry.getOptions();
    if (options.hasDistributions()) {
      // we need to add a new hash value field
      // TODO: make this happen in tandem with the distribution hashing as opposed to separate).
      DistributionTrait distribution = input.getTraitSet().getTrait(DistributionTraitDef.INSTANCE);
      if (distribution.getType() != DistributionType.HASH_DISTRIBUTED
          && distribution.getType() != DistributionType.ADAPTIVE_HASH_DISTRIBUTED) {
        throw UserException.planError()
            .message("Tried to plan a distribution writer but distribution was incorrect.")
            .build(logger);
      }

      if (distribution.getFields().size() != options.getDistributionColumns().size()) {
        // TODO: add check
      }

      Prel project =
          HashPrelUtil.addHashProject(distribution.getFields(), input, options.getRingCount());

      // last column is the hash modulo expression. this column will be added to sort keys and
      // changeDirection detection columns
      List<Integer> hashFieldIndex = ImmutableList.of(project.getRowType().getFieldCount() - 1);

      return updateWriterWithPartition(
          prel, project, hashFieldIndex, tableEntry, addSort, expectedWriterInboundRowType);
    } else if (options.hasPartitions()) {
      return updateWriterWithPartition(prel, input, ImmutableList.of(), tableEntry, addSort, null);
    } else if (options.hasSort()) {
      // no partitions or distributions.
      // insert a sort on sort fields.

      // For Merge-On-Read DML DELETE operations, data-rows do not exist.
      // Thus, no need to adhere to iceberg sortOrder.
      // Positional Deletes will be sorted by 'file_path' and 'pos' in other parts of the plan.
      if (DmlUtils.isMergeOnReadDeleteOperation(options)) {
        return prel;
      }

      final RelCollation collation =
          getCollation(
              prel.getTraitSet(), getFieldIndices(options.getSortColumns(), input.getRowType()));
      final Prel sort =
          SortPrel.create(
              input.getCluster(), input.getTraitSet().plus(collation), input, collation);
      return new WriterPrel(
          prel.getCluster(), prel.getTraitSet(), sort, tableEntry, expectedWriterInboundRowType);
    } else {
      return prel;
    }
  }

  private Prel addSort(
      Prel prel,
      Prel input,
      WriterOptions options,
      List<Integer> additionalFieldIndices,
      List<String> partitionColumns,
      RelDataType inputRowType) {
    List<Integer> sortKeys = new ArrayList<>();

    sortKeys.addAll(additionalFieldIndices);

    // sort by partitions.
    final Set<Integer> sortedKeys = Sets.newHashSet();
    if (options.hasPartitions()) {
      List<Integer> partitionKeys = getFieldIndices(partitionColumns, inputRowType);
      sortKeys.addAll(partitionKeys);
      sortedKeys.addAll(partitionKeys);
    }

    // then sort by sort keys, if available.
    if (options.hasSort()) {
      List<Integer> sortRequestKeys = getFieldIndices(options.getSortColumns(), inputRowType);
      for (Integer key : sortRequestKeys) {
        if (sortedKeys.contains(key)) {
          logger.warn(
              "Rejecting sort key {} since it is already included in partition clause.", key);
          continue;
        }
        sortKeys.add(key);
      }
    }

    addSystemColumnsToSortForMergeOnReadDmlIfNeeded(prel, options, inputRowType, sortKeys);

    final RelCollation collation = getCollation(prel.getTraitSet(), sortKeys);
    return SortPrel.create(
        input.getCluster(), input.getTraitSet().plus(collation), input, collation);
  }

  /**
   * Add System Columns to Sort-Order if required by operation. (Merge-On-Read DML Only).
   *
   * <p>Partitioned iceberg tables (with no iceberg sort-order) undergoing a Merge-On-Read DML will
   * add 'file_path' and 'pos' columns to the sort list (EXCEPT FOR Operation: Merge_InsertOnly
   * operation)
   *
   * <p>Sorting by System Columns (file_path, pos) is an iceberg-spec requirement. Handling the sort
   * here allows us to skip the VectorSorter requirement during the writer phase.
   *
   * <p>Note: Only Applies to Merge-On-Read DML Operations on Partitioned Iceberg Tables with no
   * defined Iceberg-Sort-Order
   *
   * @param options writer options
   * @param inputRowType input table schema
   * @param sortKeys columns to sort
   */
  private void addSystemColumnsToSortForMergeOnReadDmlIfNeeded(
      Prel prel, WriterOptions options, RelDataType inputRowType, List<Integer> sortKeys) {

    // Check 0: ensure flag for
    // 'Use VectorSorter on Partitioned Tables with undefined Sort Order' is OFF
    PlannerSettings plannerSettings = PrelUtil.getPlannerSettings(prel.getCluster());
    if (plannerSettings
        .getOptions()
        .getOption(FORCE_USE_MOR_VECTOR_SORTER_FOR_PARTITION_TABLES_WITH_UNDEFINED_SORT_ORDER)) {
      return;
    }

    // Check 1: Operation is Merge-On-Read DML. Return otherwise.
    // Check 2: Table is Partitioned. Return otherwise.
    // Check 3: table should not have a defined sort-order. Return if one exists.
    if (!DmlUtils.isMergeOnReadDmlOperation(options)
        || !options.hasPartitions()
        || options.hasSort()) {
      return;
    }

    // Final Check: system columns must exist.
    // This ensures INSERT_ONLY merge operations are excluded.
    // INSERT_ONLY merge Dml will not have system columns in the input schema.
    Set<String> fieldNames = new HashSet<>(inputRowType.getFieldNames());
    // Check if system columns exist (ensuring INSERT_ONLY merge operations are excluded).
    if (!fieldNames.contains(ColumnUtils.FILE_PATH_COLUMN_NAME)
        || !fieldNames.contains(ColumnUtils.ROW_INDEX_COLUMN_NAME)) {
      return;
    }

    sortKeys.add(inputRowType.getField(ColumnUtils.FILE_PATH_COLUMN_NAME, false, false).getIndex());
    sortKeys.add(inputRowType.getField(ColumnUtils.ROW_INDEX_COLUMN_NAME, false, false).getIndex());
  }

  private Prel updateWriterWithPartition(
      Prel prel,
      Prel input,
      List<Integer> additionalFieldIndices,
      CreateTableEntry tableEntry,
      boolean addSort,
      RelDataType expectedWriterRowType) {
    WriterOptions options = tableEntry.getOptions();
    RelDataType inputRowType = input.getRowType();
    List<String> partitionColumns = options.getPartitionColumns();
    PartitionSpec partitionSpec =
        Optional.ofNullable(
                options.getTableFormatOptions().getIcebergSpecificOptions().getIcebergTableProps())
            .map(IcebergTableProps::getDeserializedPartitionSpec)
            .orElse(null);
    if (options.getTableFormatOptions().isTableFormatWriter()
        && partitionSpec != null
        && hasNonIdentityPartitionColumns(options.getPartitionSpec())) {
      partitionColumns = new ArrayList<>();
      input =
          getTableFunctionOnPartitionColumns(options, input, prel, partitionColumns, partitionSpec);
      inputRowType = input.getRowType();
    }

    if (addSort) {
      input = addSort(prel, input, options, additionalFieldIndices, partitionColumns, inputRowType);
    }

    List<Integer> fieldIndices = new ArrayList<>();
    fieldIndices.addAll(additionalFieldIndices);
    fieldIndices.addAll(getFieldIndices(partitionColumns, inputRowType));

    // we need to sort by the partitions.
    final Prel changeDetectionPrel = addChangeDetectionProject(input, fieldIndices);
    return new WriterPrel(
        prel.getCluster(),
        prel.getTraitSet(),
        changeDetectionPrel,
        tableEntry,
        expectedWriterRowType != null ? expectedWriterRowType : inputRowType);
  }

  /**
   * Combine small files from first-round writer.
   *
   * <p>Union (combined Small Files + Pass-through Files) |
   * |------------------------------------------------- | | Combined Small Files Pass-through Files
   * | | BridgeExchange------------------------------------ | Window (count files by partition
   * values) | Sort( sort files by partition values) | Project (add Partition Values column) |
   * Original Writer Output
   */
  private Prel combineSmallFiles(
      Prel writerPrel,
      Prel writerInputPrel,
      CreateTableEntry tableEntry,
      RelDataType expectedWriterInboundRowType) {
    WriterOptions options = tableEntry.getOptions();
    PlannerSettings plannerSettings = PrelUtil.getPlannerSettings(writerPrel.getCluster());
    boolean combineSmallFilesEnabledForPartitionedTable =
        plannerSettings
            .getOptions()
            .getOption(ENABLE_ICEBERG_COMBINE_SMALL_FILES_FOR_PARTITIONED_TABLE_WRITES);
    if (!options.getTableFormatOptions().isTableFormatWriter()
        || options.getCombineSmallFileOptions() == null
        ||
        // Writing to partitioned tables does not generate small files since the Sort before Writer
        // will guarantee all rows with
        // same partition values will be written together.
        // After the Sort is removed from writer plan (DX-58885), we need turn on small-file
        // combination for partitioned tables too
        (options.hasPartitions() && !combineSmallFilesEnabledForPartitionedTable)) {
      return writerPrel;
    }

    try {
      Prel prel = (Prel) writerPrel.copy(writerPrel.getTraitSet(), writerPrel.getInputs());

      RexBuilder rexBuilder = prel.getCluster().getRexBuilder();
      RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
      RelDataTypeField filePathField =
          prel.getRowType().getField(RecordWriter.PATH_COLUMN, false, false);
      RelDataTypeField fileSizeField =
          prel.getRowType().getField(RecordWriter.FILESIZE_COLUMN, false, false);
      final String partitionValuesFieldName = "p0$partitionValuesColumn";

      // add a projected column containing partition values
      prel =
          addPartitionValueField(
              rexBuilder,
              typeFactory,
              prel,
              options.hasPartitions(),
              fileSizeField,
              partitionValuesFieldName,
              options.getCombineSmallFileOptions());

      // sort data files by partition values, it is a prep for Windowing,
      // this also avoids expensive Sort on record level for partitioned tables
      RelDataTypeField partitionValuesField =
          prel.getRowType().getField(partitionValuesFieldName, false, false);
      final RelCollation collation =
          getCollation(prel.getTraitSet(), ImmutableList.of(partitionValuesField.getIndex()));
      prel =
          SortPrel.create(prel.getCluster(), prel.getTraitSet().plus(collation), prel, collation);

      // add SingleMergeExchange to merge local sort result
      prel =
          new SingleMergeExchangePrel(
              prel.getCluster(),
              prel.getTraitSet().plus(Prel.PHYSICAL).plus(DistributionTrait.SINGLETON),
              prel,
              ((SortPrel) prel).getCollation());

      // add Windowing to count files per partition value
      // The count is used to filter out the single small data file which wont go to the
      // second-round writer
      final String partitionFileCountFieldName = "w0$partitionFileCount";
      prel =
          addWindowingToCountDataFilesPerPartitionValue(
              rexBuilder, typeFactory, prel, partitionFileCountFieldName, partitionValuesField);

      String bridgeId = UUID.randomUUID().toString();
      // add bridge exchange so that data files output from first-round writing would be used by
      // both small-file combination side plan and pass-though side plan
      prel = new BridgeExchangePrel(prel.getCluster(), prel.getTraitSet(), prel, bridgeId);

      // construct small file combining side plan
      RelDataTypeField partitionFileCountField =
          prel.getRowType().getField(partitionFileCountFieldName, false, false);
      Prel smallFilesSidePrel =
          getSmallFilesSidePrel(
              writerInputPrel,
              rexBuilder,
              typeFactory,
              prel,
              filePathField,
              fileSizeField,
              partitionFileCountField,
              partitionValuesField,
              tableEntry,
              expectedWriterInboundRowType);

      // construct pass-through side plan
      Prel passThroughFilesPrel =
          getPassThroughFilesPrel(
              bridgeId,
              rexBuilder,
              typeFactory,
              prel,
              partitionFileCountField,
              partitionValuesField);

      // add RR exchanges for UnionAll inputs if necessary
      // UnionAllPrule will add RR on its inputs.
      // The plan in WriterUpdater is hand-crafted and is not optimized by UnionAllPrule.
      // We need to copy some logic from UnionAllPrule
      List<RelNode> unionAllInputs =
          PrelUtil.convertUnionAllInputs(plannerSettings, smallFilesSidePrel, passThroughFilesPrel);

      // union combined small files with pass-through files
      return new UnionAllPrel(
          smallFilesSidePrel.getCluster(), smallFilesSidePrel.getTraitSet(), unionAllInputs, false);
    } catch (InvalidRelException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Add a projected column containing partition values. a. for non-small files, set dummy partition
   * value "p0$DREMIO_notSmallFilePartitionTableValue", regardless partitioned table or not. files
   * with this partition value wont go into the second round writing b. for small files in
   * non-partitioned table, set dummy partition value "p0$DREMIO_nonPartitionTableValue" c. for
   * small files in partitioned table, set real partition value getting from first-round writer
   * output column RecordWriter.PARTITION_VALUE_COLUMN
   */
  private Prel addPartitionValueField(
      RexBuilder rexBuilder,
      RelDataTypeFactory typeFactory,
      Prel prel,
      boolean hasPartitions,
      RelDataTypeField fileSizeField,
      String partitionValuesFieldName,
      CombineSmallFileOptions combineSmallFileOptions) {
    // Add projected column for file counting per partition value
    List<RexNode> ProjectExprs =
        prel.getRowType().getFieldList().stream()
            .map(field -> rexBuilder.makeInputRef(field.getType(), field.getIndex()))
            .collect(Collectors.toList());

    RexNode notSmallFileDummyPartitionValueExpr =
        rexBuilder.makeLiteral(notSmallFileDummyPartitionValue);
    List<String> projectNames =
        prel.getRowType().getFieldList().stream()
            .map(field -> field.getName())
            .collect(Collectors.toList());
    projectNames.add(partitionValuesFieldName);

    RexNode notSmallFileCondition =
        rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
            rexBuilder.makeInputRef(fileSizeField.getType(), fileSizeField.getIndex()),
            rexBuilder.makeLiteral(
                combineSmallFileOptions.getSmallFileSize(),
                typeFactory.createSqlType(SqlTypeName.BIGINT)));

    RelDataTypeField operationTypeField =
        prel.getRowType().getField(RecordWriter.OPERATION_TYPE_COLUMN, false, false);
    RexNode addedDataFileOnlyCondition =
        rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(operationTypeField.getType(), operationTypeField.getIndex()),
            rexBuilder.makeLiteral(
                OperationType.ADD_DATAFILE.value, typeFactory.createSqlType(SqlTypeName.INTEGER)));

    RelDataType nonNullableVarchar = typeFactory.createSqlType(SqlTypeName.VARCHAR);
    RelDataType nullableVarchar = typeFactory.createTypeWithNullability(nonNullableVarchar, true);

    RexNode partitionValuesFieldExpr;
    if (!hasPartitions) {
      // Non-partitioned tables,  use the same dummy value since we want to count all files
      partitionValuesFieldExpr =
          rexBuilder.makeCall(
              SqlStdOperatorTable.CASE,
              addedDataFileOnlyCondition,
              rexBuilder.makeLiteral(nonPartitionTableDummyPartitionValue),
              rexBuilder.makeNullLiteral(nullableVarchar));
    } else {
      // Partitioned tables, get the partition value from first-round writer output field
      // RecordWriter.PARTITION_VALUE_COLUMN
      RelDataTypeField partitionValueField =
          prel.getRowType().getField(RecordWriter.PARTITION_VALUE_COLUMN, false, false);
      partitionValuesFieldExpr =
          rexBuilder.makeCall(
              SqlStdOperatorTable.CASE,
              addedDataFileOnlyCondition,
              rexBuilder.makeInputRef(
                  partitionValueField.getType(), partitionValueField.getIndex()),
              rexBuilder.makeLiteral(nonPartitionTableDummyPartitionValue));
    }
    partitionValuesFieldExpr =
        rexBuilder.makeCall(
            SqlStdOperatorTable.CASE,
            notSmallFileCondition,
            notSmallFileDummyPartitionValueExpr,
            partitionValuesFieldExpr);
    ProjectExprs.add(partitionValuesFieldExpr);

    RelDataType fileCountFieldRowType =
        RexUtil.createStructType(
            prel.getCluster().getTypeFactory(), ProjectExprs, projectNames, null);

    return ProjectPrel.create(
        prel.getCluster(), prel.getTraitSet(), prel, ProjectExprs, fileCountFieldRowType);
  }

  /**
   * Add Windowing to count files per partition value. The count is used to filter out single small
   * data file which wont go to the second-round writer
   */
  private Prel addWindowingToCountDataFilesPerPartitionValue(
      RexBuilder rexBuilder,
      RelDataTypeFactory typeFactory,
      Prel prel,
      String partitionFileCountColumnName,
      RelDataTypeField partitionValuesColumn) {
    // add Windowing to count files per partition value
    Window.RexWinAggCall countOnPartitionColumnCall =
        new Window.RexWinAggCall(
            SqlStdOperatorTable.COUNT,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            ImmutableList.of(rexBuilder.makeInputRef(prel, partitionValuesColumn.getIndex())),
            0,
            false);

    Window.Group group =
        new Window.Group(
            // for partitioned table, group by partition values, for non-partitioned table, group by
            // operation type so that we could get the count of added files
            ImmutableBitSet.of(partitionValuesColumn.getIndex()),
            false,
            RexWindowBound.create(SqlWindow.createUnboundedPreceding(SqlParserPos.ZERO), null),
            RexWindowBound.create(SqlWindow.createUnboundedFollowing(SqlParserPos.ZERO), null),
            RelCollations.EMPTY,
            ImmutableList.of(countOnPartitionColumnCall));
    List<String> names = new ArrayList<>();
    List<RelDataType> types = new ArrayList<>();
    names.addAll(prel.getRowType().getFieldNames());
    names.add(partitionFileCountColumnName);
    types.addAll(
        prel.getRowType().getFieldList().stream()
            .map(RelDataTypeField::getType)
            .collect(Collectors.toList()));
    types.add(typeFactory.createSqlType(SqlTypeName.BIGINT));

    return WindowPrel.create(
        prel.getCluster(),
        prel.getTraitSet(),
        prel,
        ImmutableList.of(),
        typeFactory.createStructType(types, names),
        group,
        null);
  }

  /***
   * Create the small file conditions:
   *    1. they are added data files
   *    2. there are two or more small file per partition value
   *    3. the files are not small files
   */
  private RexNode getSmallFilesConditions(
      Prel prel,
      RexBuilder rexBuilder,
      RelDataTypeFactory typeFactory,
      RelDataTypeField partitionFileCountField,
      RelDataTypeField partitionValuesField) {
    // only care added data files
    RelDataTypeField operationTypeField =
        prel.getRowType().getField(RecordWriter.OPERATION_TYPE_COLUMN, false, false);
    RexNode addedDataFileOnlyCondition =
        rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(operationTypeField.getType(), operationTypeField.getIndex()),
            rexBuilder.makeLiteral(
                OperationType.ADD_DATAFILE.value, typeFactory.createSqlType(SqlTypeName.INTEGER)));

    // there are two or more small file per partition value
    RexNode multipleSmallFileCondition =
        rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            rexBuilder.makeInputRef(
                partitionFileCountField.getType(), partitionFileCountField.getIndex()),
            rexBuilder.makeLiteral(1, typeFactory.createSqlType(SqlTypeName.BIGINT)));

    // excludes non-small files
    RexNode notSmallFilesCondition =
        rexBuilder.makeCall(
            SqlStdOperatorTable.NOT_EQUALS,
            rexBuilder.makeInputRef(
                partitionValuesField.getType(), partitionValuesField.getIndex()),
            rexBuilder.makeLiteral(notSmallFileDummyPartitionValue));

    return rexBuilder.makeCall(
        SqlStdOperatorTable.AND,
        addedDataFileOnlyCondition,
        notSmallFilesCondition,
        multipleSmallFileCondition);
  }

  /**
   * Add small file filter 1. there are two or more small file per partition value 2. the files are
   * not small files
   */
  private Prel addSmallFilesFilterPrel(
      RexBuilder rexBuilder,
      RelDataTypeFactory typeFactory,
      Prel prel,
      RelDataTypeField partitionFileCountField,
      RelDataTypeField partitionValuesField) {
    RexNode smallFilesConditions =
        getSmallFilesConditions(
            prel, rexBuilder, typeFactory, partitionFileCountField, partitionValuesField);

    return FilterPrel.create(prel.getCluster(), prel.getTraitSet(), prel, smallFilesConditions);
  }

  /**
   * Read rows from small files
   *
   * <p>DataFileScan | IcebergSplitGen | Project ( from RecordWriter.SCHEMA to IcebergSplitGen
   * required input schema) | Small Data File Records
   */
  private Prel readRowsFromSmallFiles(
      RexBuilder rexBuilder,
      RelDataTypeFactory typeFactory,
      Prel prel,
      RelDataTypeField filePathField,
      RelDataTypeField fileSizeField,
      CreateTableEntry tableEntry,
      Function<RelMetadataQuery, Double> estimateRowCountFn) {
    WriterOptions options = tableEntry.getOptions();

    // map RecordWriter.SCHEMA to IcebergSplitGen required input schema
    List<String> projectNames =
        ImmutableList.of(
            SystemSchemas.DATAFILE_PATH, SystemSchemas.FILE_SIZE, SystemSchemas.PARTITION_INFO);

    List<RexNode> projectExprs = new ArrayList<>();
    projectExprs.add(rexBuilder.makeInputRef(filePathField.getType(), filePathField.getIndex()));
    projectExprs.add(rexBuilder.makeInputRef(fileSizeField.getType(), fileSizeField.getIndex()));
    // make null values for SystemSchemas.PARTITION_INFO
    RelDataType nonNullableVarbinary = typeFactory.createSqlType(SqlTypeName.VARBINARY);
    RelDataType nullableVarbinary =
        typeFactory.createTypeWithNullability(nonNullableVarbinary, true);
    projectExprs.add(rexBuilder.makeNullLiteral(nullableVarbinary));

    RelDataType projectRowType =
        RexUtil.createStructType(
            prel.getCluster().getTypeFactory(), projectExprs, projectNames, null);

    prel =
        ProjectPrel.create(
            prel.getCluster(), prel.getTraitSet(), prel, projectExprs, projectRowType);

    // Split Gen
    IcebergTableProps tableProps =
        options.getTableFormatOptions().getIcebergSpecificOptions().getIcebergTableProps();

    prel =
        new IcebergSplitGenPrel(
            prel.getCluster(),
            prel.getTraitSet(),
            prel.getTable(),
            prel,
            RecordReader.SPLIT_GEN_AND_COL_IDS_SCAN_SCHEMA,
            ImmutableList.of(tableEntry.getDatasetPath().getPathComponents()),
            tableEntry.getPlugin().getId(),
            options.getExtendedProperty());

    // table scan phase
    final FileConfig config =
        new IcebergFileConfig()
            .setParquetDataFormat(new ParquetFileConfig())
            .asFileConfig()
            .setLocation(tableEntry.getLocation())
            .setCtime(1L)
            .setOwner(tableEntry.getUserName());
    boolean limitDataScanParallelism =
        PrelUtil.getPlannerSettings(prel.getCluster())
            .getOptions()
            .getOption(DATA_SCAN_PARALLELISM);
    TableFunctionContext dataScanContext =
        new TableFunctionContext(
            config,
            tableProps.getPersistedFullSchema(),
            tableProps.getPersistedFullSchema(),
            ImmutableList.of(tableEntry.getDatasetPath().getPathComponents()),
            null,
            null,
            tableEntry.getPlugin().getId(),
            null,
            getColumns(tableProps.getPersistedFullSchema()),
            ImmutableList.of(),
            options.getExtendedProperty(),
            false,
            false,
            false,
            null,
            getColIdMap(tableProps.getPersistedFullSchema(), tableProps.getIcebergSchema()));
    TableFunctionConfig tableFunctionConfig =
        TableFunctionUtil.getDataFileScanTableFunctionConfig(
            dataScanContext, limitDataScanParallelism, 1);

    return new TableFunctionPrel(
        prel.getCluster(),
        prel.getTraitSet(),
        prel.getTable(),
        prel,
        null,
        tableFunctionConfig,
        getRowType(tableProps.getPersistedFullSchema(), prel.getCluster()),
        estimateRowCountFn,
        null,
        ImmutableList.of(),
        tableEntry.getUserName());
  }

  /**
   * Assign file size bucket to files in unpartitioned table. 1. get the running total of file size
   * 2. convert file size running total into bucket number bucket_number =
   * currentFileSizeRunningTotal / bucketSize
   */
  private Prel assignFileSizeBucketToUnpartitionedTable(
      RexBuilder rexBuilder,
      RelDataTypeFactory typeFactory,
      Prel prel,
      String fileSizeBucketFieldName,
      RelDataTypeField filePathField,
      RelDataTypeField fileSizeField,
      Long targetFileSize) {
    // add Windowing to get running total of file sizes
    Window.RexWinAggCall countOnPartitionColumnCall =
        new Window.RexWinAggCall(
            SqlStdOperatorTable.SUM,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            ImmutableList.of(rexBuilder.makeInputRef(prel, fileSizeField.getIndex())),
            0,
            false);

    Window.Group group =
        new Window.Group(
            ImmutableBitSet.of(),
            false,
            RexWindowBound.create(SqlWindow.createUnboundedPreceding(SqlParserPos.ZERO), null),
            RexWindowBound.create(SqlWindow.createCurrentRow(SqlParserPos.ZERO), null),
            RelCollations.of(filePathField.getIndex()),
            ImmutableList.of(countOnPartitionColumnCall));

    String fileSizeRunningTotalFieldName = "w0$fileSizeRunningTotalFieldName";
    List<String> names = new ArrayList<>();
    List<RelDataType> types = new ArrayList<>();
    names.addAll(prel.getRowType().getFieldNames());
    names.add(fileSizeRunningTotalFieldName);
    types.addAll(
        prel.getRowType().getFieldList().stream()
            .map(RelDataTypeField::getType)
            .collect(Collectors.toList()));
    types.add(typeFactory.createSqlType(SqlTypeName.BIGINT));

    Prel fileSizeRunningTotalPrel =
        WindowPrel.create(
            prel.getCluster(),
            prel.getTraitSet(),
            prel,
            ImmutableList.of(),
            typeFactory.createStructType(types, names),
            group,
            null);

    // convert file size running total into bucket number
    // bucket_number = currentFileSizeRunningTotal / targetFileSize
    List<RexNode> fileSizeBucketProjectExprs =
        prel.getRowType().getFieldList().stream()
            .map(field -> rexBuilder.makeInputRef(field.getType(), field.getIndex()))
            .collect(Collectors.toList());

    List<String> fileSizeBucketProjectNames =
        prel.getRowType().getFieldList().stream()
            .map(field -> field.getName())
            .collect(Collectors.toList());
    fileSizeBucketProjectNames.add(fileSizeBucketFieldName);

    final SqlFunction division =
        new SqlFunction(
            "/",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.explicit(SqlTypeName.BIGINT),
            null,
            OperandTypes.NUMERIC_NUMERIC,
            SqlFunctionCategory.NUMERIC);
    RelDataTypeField fileSizeRunningTotalField =
        fileSizeRunningTotalPrel.getRowType().getField(fileSizeRunningTotalFieldName, false, false);
    RexNode fileSizeBucketFieldExpr =
        rexBuilder.makeCall(
            division,
            rexBuilder.makeInputRef(
                fileSizeRunningTotalField.getType(), fileSizeRunningTotalField.getIndex()),
            rexBuilder.makeLiteral(Long.toString(targetFileSize)));
    fileSizeBucketProjectExprs.add(fileSizeBucketFieldExpr);

    RelDataType fileSizeBucketFieldRowType =
        RexUtil.createStructType(
            prel.getCluster().getTypeFactory(),
            fileSizeBucketProjectExprs,
            fileSizeBucketProjectNames,
            null);

    return ProjectPrel.create(
        fileSizeRunningTotalPrel.getCluster(),
        fileSizeRunningTotalPrel.getTraitSet(),
        fileSizeRunningTotalPrel,
        fileSizeBucketProjectExprs,
        fileSizeBucketFieldRowType);
  }

  /**
   * WriterPrel (second-round writing to combine small files) | Read From Small Files | Hash
   * Exchange on (partitioned table: partition value, non-partitioned table: bucket# | Assign
   * bucket# for non-partitioned tables (only for non-partitioned tables) | Filter (keep non-single
   * small files only)
   */
  private Prel getSmallFilesSidePrel(
      Prel originalWriterInputPrel,
      RexBuilder rexBuilder,
      RelDataTypeFactory typeFactory,
      Prel prel,
      RelDataTypeField filePathField,
      RelDataTypeField fileSizeField,
      RelDataTypeField partitionFileCountField,
      RelDataTypeField partitionValuesField,
      CreateTableEntry tableEntry,
      RelDataType expectedWriterInboundRowType) {

    CombineSmallFileOptions combineSmallFileOptions =
        tableEntry.getOptions().getCombineSmallFileOptions();
    Long targetFileSize = combineSmallFileOptions.getTargetFileSize();
    if (targetFileSize == null) {
      targetFileSize =
          PrelUtil.getPlannerSettings(prel.getCluster())
              .getOptions()
              .getOption(ExecConstants.TARGET_COMBINED_SMALL_PARQUET_BLOCK_SIZE_VALIDATOR);
    }

    // add filters to keep non-single small files only
    Prel smallFilesSidePrel =
        addSmallFilesFilterPrel(
            rexBuilder, typeFactory, prel, partitionFileCountField, partitionValuesField);

    // Add hash exchange to distribute the load if applicable
    if (!combineSmallFileOptions.getIsSingleWriter()) {
      // assign file size bucket number to files in unpartitioned table.
      // thus, files with same bucket number will be hash distributed to the same writer
      final String fileSizeBucketFieldName = "p0$fileSizeBucketFieldName";
      if (!tableEntry.getOptions().hasPartitions()) {
        smallFilesSidePrel =
            assignFileSizeBucketToUnpartitionedTable(
                rexBuilder,
                typeFactory,
                smallFilesSidePrel,
                fileSizeBucketFieldName,
                filePathField,
                fileSizeField,
                targetFileSize);
      }

      // hash distribute files on
      //   * partition value column for partitioned tables
      //            Since the files are already ordered, hash distribution from the single sender(by
      // the MergeExchange before bridge)
      //            would guarantee files belonging to the same partition values pack together on
      // the
      // receiver side
      //            thus, we dont need Sort in the second-round read/write rows
      //   * file size bucket column for non-partitioned tables
      List<String> hashDistributionFields =
          ImmutableList.of(
              tableEntry.getOptions().hasPartitions()
                  ? partitionValuesField.getName()
                  : fileSizeBucketFieldName);
      smallFilesSidePrel = getHashToRandomExchangePrel(smallFilesSidePrel, hashDistributionFields);
    }

    // read rows from small files
    Function<RelMetadataQuery, Double> estimateRowCountFn =
        rm -> rm.getRowCount(originalWriterInputPrel);
    smallFilesSidePrel =
        readRowsFromSmallFiles(
            rexBuilder,
            typeFactory,
            smallFilesSidePrel,
            filePathField,
            fileSizeField,
            tableEntry,
            estimateRowCountFn);

    // add second-round Writer
    CreateTableEntry tableEntryWithSmallFileCombinationWriter =
        tableEntry.cloneWithFields(
            tableEntry
                .getOptions()
                .withCombineSmallFileOptions(
                    CombineSmallFileOptions.builder()
                        .setSmallFileSize(
                            tableEntry.getOptions().getCombineSmallFileOptions().getSmallFileSize())
                        .setTargetFileSize(targetFileSize)
                        .setIsSmallFileWriter(true)
                        .build()));

    Prel updatedWriter =
        updateWriter(
            prel,
            smallFilesSidePrel,
            tableEntryWithSmallFileCombinationWriter,
            false,
            expectedWriterInboundRowType);
    if (updatedWriter == prel) {
      return new WriterPrel(
          prel.getCluster(),
          prel.getTraitSet(),
          smallFilesSidePrel,
          tableEntryWithSmallFileCombinationWriter,
          expectedWriterInboundRowType);
    }
    return updatedWriter;
  }

  /***
   * Identified small files will be combined to bigger files on second round writing.
   * The original files are marked as Orphan files with OperationType.ORPHAN_DATAFILE
   * ManifestWriter will do the orphan file deletion for files with OperationType.ORPHAN_DATAFILE
   */
  private Prel markOrphanFiles(
      RexBuilder rexBuilder,
      RelDataTypeFactory typeFactory,
      Prel prel,
      RelDataTypeField partitionValuesField,
      RelDataTypeField partitionFileCountField) {

    List<RexNode> projectWriterColumnsExprs = new ArrayList<>();
    RexNode smallFilesConditions =
        getSmallFilesConditions(
            prel, rexBuilder, typeFactory, partitionFileCountField, partitionValuesField);
    for (RelDataTypeField field : prel.getRowType().getFieldList()) {
      RexNode original = rexBuilder.makeInputRef(field.getType(), field.getIndex());
      if (field.getName().equals(OPERATION_TYPE_COLUMN)) {
        RexNode orphanFileOperationType =
            rexBuilder.makeLiteral(
                OperationType.ORPHAN_DATAFILE.value,
                typeFactory.createSqlType(SqlTypeName.INTEGER));
        RexNode operationTypeFieldExpr =
            rexBuilder.makeCall(
                SqlStdOperatorTable.CASE, smallFilesConditions, orphanFileOperationType, original);
        projectWriterColumnsExprs.add(operationTypeFieldExpr);
      } else if (field.getName().equals(RECORDS_COLUMN)) {
        // mark records as 0 for small files  so that it wont be counted in ManifestFileRecordWriter
        RelDataType bigInt = typeFactory.createSqlType(SqlTypeName.BIGINT);
        RelDataType nullableBigInt = typeFactory.createTypeWithNullability(bigInt, true);
        RexNode zeroRecordExpr = rexBuilder.makeZeroLiteral(nullableBigInt);
        RexNode zeroRecordForSmallFilesExpr =
            rexBuilder.makeCall(
                SqlStdOperatorTable.CASE, smallFilesConditions, zeroRecordExpr, original);
        projectWriterColumnsExprs.add(zeroRecordForSmallFilesExpr);
      } else {
        projectWriterColumnsExprs.add(original);
      }
    }

    return ProjectPrel.create(
        prel.getCluster(), prel.getTraitSet(), prel, projectWriterColumnsExprs, prel.getRowType());
  }

  /**
   * Those files out from first-round writer will pass through the second round writer. if 1. not
   * small files 2. there is only one such small file per partition value
   *
   * <p>For small files to be combined, mark them as orphan files to delete
   */
  private Prel getPassThroughFilesPrel(
      String bridgeId,
      RexBuilder rexBuilder,
      RelDataTypeFactory typeFactory,
      Prel prel,
      RelDataTypeField partitionFileCountField,
      RelDataTypeField partitionValuesField) {
    String partitionFileCountFieldName = partitionFileCountField.getName();
    String partitionValuesFieldName = partitionValuesField.getName();

    Prel receiverPrel =
        new BridgeReaderPrel(
            prel.getCluster(), prel.getTraitSet(), prel.getRowType(), 1d, bridgeId);

    // mark small files to be combined as orphan files to delete
    Prel passThroughPrel =
        markOrphanFiles(
            rexBuilder, typeFactory, receiverPrel, partitionValuesField, partitionFileCountField);

    // remove partitionValuesFieldName and partitionFileCountFieldName to align with
    // RecordWriter.SCHEMA
    List<RexNode> projectWriterColumnsExprs =
        passThroughPrel.getRowType().getFieldList().stream()
            .filter(
                field ->
                    !field.getName().equals(partitionFileCountFieldName)
                        && !field.getName().equals(partitionValuesFieldName))
            .map(field -> rexBuilder.makeInputRef(field.getType(), field.getIndex()))
            .collect(Collectors.toList());
    List<String> projectWriterColumnsNames =
        passThroughPrel.getRowType().getFieldList().stream()
            .filter(
                field ->
                    !field.getName().equals(partitionFileCountFieldName)
                        && !field.getName().equals(partitionValuesFieldName))
            .map(field -> field.getName())
            .collect(Collectors.toList());
    RelDataType projectWriterColumnsRowType =
        RexUtil.createStructType(
            passThroughPrel.getCluster().getTypeFactory(),
            projectWriterColumnsExprs,
            projectWriterColumnsNames,
            null);

    return ProjectPrel.create(
        passThroughPrel.getCluster(),
        passThroughPrel.getTraitSet(),
        passThroughPrel,
        projectWriterColumnsExprs,
        projectWriterColumnsRowType);
  }

  public static RelCollation getCollation(RelTraitSet set, List<Integer> keys) {
    return set.canonize(
        RelCollations.of(
            FluentIterable.from(keys)
                .transform(
                    new Function<Integer, RelFieldCollation>() {
                      @Override
                      public RelFieldCollation apply(Integer input) {
                        return new RelFieldCollation(input);
                      }
                    })
                .toList()));
  }

  public static List<Integer> getFieldIndices(
      final List<String> columns, final RelDataType inputRowType) {
    return FluentIterable.from(columns)
        .transform(
            new Function<String, Integer>() {
              @Override
              public Integer apply(String input) {
                return Preconditions.checkNotNull(
                        inputRowType.getField(input, false, false),
                        String.format(
                            "Partition column '%s' could not be resolved in the table's column lists",
                            input))
                    .getIndex();
              }
            })
        .toList();
  }

  /**
   * A PrelVisitor which will insert a project under Writer.
   *
   * <p>For CTAS : create table t1 partition by (con_A) select * from T1; A Project with Item expr
   * will be inserted, in addition to *. We need insert another Project to remove this additional
   * expression.
   *
   * <p>In addition, to make execution's implementation easier, a special field is added to Project
   * : PARTITION_COLUMN_IDENTIFIER = newPartitionValue(Partition_colA) ||
   * newPartitionValue(Partition_colB) || ... || newPartitionValue(Partition_colN).
   */
  /**
   * Given a list of index keys, generate an additional project that detects change in the index
   * keys.
   *
   * @return
   * @throws RuntimeException
   */
  public Prel addChangeDetectionProject(final Prel input, final List<Integer> changeKeys)
      throws RuntimeException {

    // No partition columns.
    if (changeKeys.isEmpty()) {
      return input;
    }

    final RelDataType childRowType = input.getRowType();
    final RelOptCluster cluster = input.getCluster();
    final List<RexNode> exprs = new ArrayList<>();
    final List<String> fieldNames = new ArrayList<>();

    for (final RelDataTypeField field : childRowType.getFieldList()) {
      exprs.add(RexInputRef.of(field.getIndex(), childRowType));
      fieldNames.add(field.getName());
    }

    // find list of partition columns.
    final List<RexNode> partitionColumnExprs =
        Lists.newArrayListWithExpectedSize(changeKeys.size());
    final List<RelDataTypeField> fields = childRowType.getFieldList();
    for (Integer changeKey : changeKeys) {
      RelDataTypeField field = fields.get(changeKey);
      if (field == null) {
        throw UserException.validationError()
            .message("Partition column %s is not in the SELECT list of CTAS!", changeKey)
            .build(logger);
      }

      partitionColumnExprs.add(RexInputRef.of(field.getIndex(), childRowType));
    }

    // Add partition column comparator to Project's field name list.
    fieldNames.add(WriterPrel.PARTITION_COMPARATOR_FIELD);

    final RexNode partionColComp =
        createPartitionColComparator(cluster.getRexBuilder(), partitionColumnExprs);
    exprs.add(partionColComp);
    final RelDataType rowTypeWithPCComp =
        RexUtil.createStructType(cluster.getTypeFactory(), exprs, fieldNames);

    final ProjectPrel projectUnderWriter =
        ProjectAllowDupPrel.create(
            cluster,
            cluster.getPlanner().emptyTraitSet().plus(Prel.PHYSICAL),
            input,
            exprs,
            rowTypeWithPCComp);
    return projectUnderWriter;
  }

  private static RexNode createPartitionColComparator(
      final RexBuilder rexBuilder, List<RexNode> inputs) {

    final SqlFunction op =
        SqlFunctionImpl.create(
            WriterPrel.PARTITION_COMPARATOR_FUNC, DynamicReturnType.INSTANCE, Checker.of(1));

    final List<RexNode> compFuncs = Lists.newArrayListWithExpectedSize(inputs.size());

    for (final RexNode input : inputs) {
      compFuncs.add(rexBuilder.makeCall(op, ImmutableList.of(input)));
    }

    return composeDisjunction(rexBuilder, compFuncs);
  }

  private static RexNode composeDisjunction(final RexBuilder rexBuilder, List<RexNode> compFuncs) {
    final SqlFunction booleanOrFunc =
        SqlFunctionImpl.create("orNoShortCircuit", DynamicReturnType.INSTANCE, Checker.of(2));
    RexNode node = compFuncs.remove(0);
    while (!compFuncs.isEmpty()) {
      node = rexBuilder.makeCall(booleanOrFunc, node, compFuncs.remove(0));
    }
    return node;
  }

  @Override
  public Prel visitPrel(Prel prel, Void value) throws RuntimeException {

    List<RelNode> newInputs = new ArrayList<>();
    for (Prel input : prel) {
      newInputs.add(input.accept(this, null));
    }

    return (Prel) prel.copy(prel.getTraitSet(), newInputs);
  }

  RelDataType getRowType(BatchSchema newSchema, RelOptCluster relOptCluster) {
    final RelDataTypeFactory factory = relOptCluster.getTypeFactory();
    final RelDataTypeFactory.FieldInfoBuilder builder =
        new RelDataTypeFactory.FieldInfoBuilder(factory);
    for (Field field : newSchema) {
      builder.add(
          field.getName(),
          CalciteArrowHelper.wrap(CompleteType.fromField(field)).toCalciteType(factory, true));
    }
    return builder.build();
  }

  BatchSchema getNewBatchSchema(
      BatchSchema batchSchema,
      PartitionSpec partitionSpec,
      List<String> partitionColumns,
      RelDataType inputRowType) {
    List<Field> fields = new ArrayList<>();
    Schema schema = partitionSpec.schema();

    SchemaConverter schemaConverter = SchemaConverter.getBuilder().build();
    for (PartitionField partitionField : partitionSpec.fields()) {
      if (isIdentityPartitionColumn(partitionField)) {
        partitionColumns.add(partitionField.name());
        continue;
      }
      Types.NestedField field = schema.findField(partitionField.sourceId());
      Type resultType = partitionField.transform().getResultType(field.type());
      CompleteType completeType = schemaConverter.fromIcebergType(resultType);
      Field newField = completeType.toField(IcebergUtils.getPartitionFieldName(partitionField));
      fields.add(newField);
      partitionColumns.add(newField.getName());
    }
    if (inputRowType.getFieldList().stream()
        .map(RelDataTypeField::getName)
        .anyMatch(ColumnUtils.COPY_HISTORY_COLUMN_NAME::equals)) {
      fields.add(
          Field.nullablePrimitive(
              ColumnUtils.COPY_HISTORY_COLUMN_NAME, ArrowType.PrimitiveType.Utf8.INSTANCE));
    }
    return batchSchema.cloneWithFields(fields);
  }

  List<SchemaPath> getColumns(BatchSchema newbatchSchema) {
    List<SchemaPath> schemaPathList = new ArrayList<>();
    for (Field field : newbatchSchema.getFields()) {
      schemaPathList.add(SchemaPath.getSimplePath(field.getName()));
    }
    return schemaPathList;
  }

  Map<String, Integer> getColIdMap(BatchSchema batchSchema, String icebergSchemaString) {
    if (icebergSchemaString != null) {
      Schema icebergSchema = IcebergSerDe.deserializedJsonAsSchema(icebergSchemaString);
      return IcebergUtils.getIcebergColumnNameToIDMap(icebergSchema);
    }

    Map<String, Integer> colIdMap = new HashMap<>();
    for (int i = 0; i < batchSchema.getFields().size(); i++) {
      Field field = batchSchema.getColumn(i);
      colIdMap.put(field.getName(), i);
    }
    return colIdMap;
  }

  private Prel getTableFunctionOnPartitionColumns(
      WriterOptions options,
      Prel input,
      Prel prel,
      List<String> partitionColumns,
      PartitionSpec partitionSpec) {
    IcebergTableProps tableProps =
        options.getTableFormatOptions().getIcebergSpecificOptions().getIcebergTableProps();
    BatchSchema newBatchSchema =
        getNewBatchSchema(
            tableProps.getPersistedFullSchema(),
            partitionSpec,
            partitionColumns,
            input.getRowType());
    RelDataType rowType = getRowType(newBatchSchema, prel.getCluster());

    List<SchemaPath> schemaPathList = getColumns(newBatchSchema);
    TableFunctionConfig icebergTransformTableFunctionConfig =
        TableFunctionUtil.getIcebergPartitionTransformTableFunctionConfig(
            tableProps, newBatchSchema, schemaPathList);

    TableFunctionPrel transformTableFunctionPrel =
        new TableFunctionPrel(
            prel.getCluster(),
            prel.getTraitSet(),
            prel.getTable(),
            input,
            null,
            icebergTransformTableFunctionConfig,
            rowType,
            rm -> rm.getRowCount(input));

    // add HashToRandomExchange to distribute values after partition transformation
    return getHashToRandomExchangePrel(transformTableFunctionPrel, partitionColumns);
  }

  private Prel getHashToRandomExchangePrel(Prel input, List<String> partitionColumns) {
    DistributionTrait distributionTrait =
        WriterOptions.hashDistributedOn(partitionColumns, input.getRowType());
    RelTraitSet relTraitSet =
        input.getCluster().getPlanner().emptyTraitSet().plus(Prel.PHYSICAL).plus(distributionTrait);
    boolean supportAdaptiveHash =
        PrelUtil.getPlannerSettings(input.getCluster()).getOptions().getOption(ADAPTIVE_HASH);
    if (supportAdaptiveHash) {
      return new AdaptiveHashExchangePrel(
          input.getCluster(), relTraitSet, input, distributionTrait.getFields());
    } else {
      return new HashToRandomExchangePrel(
          input.getCluster(), relTraitSet, input, distributionTrait.getFields());
    }
  }
}
