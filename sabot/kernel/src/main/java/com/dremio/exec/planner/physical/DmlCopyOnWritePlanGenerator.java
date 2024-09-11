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
package com.dremio.exec.planner.physical;

import static com.dremio.exec.ExecConstants.ENABLE_DML_DISPLAY_RESULT_ONLY;
import static com.dremio.exec.util.ColumnUtils.isSystemColumn;
import static org.apache.calcite.rel.core.TableModify.Operation.DELETE;
import static org.apache.calcite.rel.core.TableModify.Operation.MERGE;
import static org.apache.calcite.rel.core.TableModify.Operation.UPDATE;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.physical.config.ManifestScanFilters;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.planner.sql.parser.DmlUtils;
import com.dremio.exec.store.OperationType;
import com.dremio.exec.store.RecordWriter;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.iceberg.IcebergScanPlanBuilder;
import com.dremio.exec.util.ColumnUtils;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;

/***
 *
 * Generate copy-on-write based DML query plan. Support Delete, Update and Merge operations.
 *
 *                                Writer
 *             ____________________ |__________________________
 *            |                     |                          |
 *          Delete                Update                     Merge
 *    (non-deleted data)       (updated  data)       (updated data + new data)
 *             |                    |                          |
 *             |____________________|__________________________|
 *                                  |
 *                                 Join
 *                        ON __FilePath == __FilePath
 *                        AND __RowIndex == __RowIndex
 *                                  |
 *            ______________________|_________________________
 *            |                                               |
 *            |                                               |
 *        Full Data from                                  DMLed data
 *        impacted data files
 */
public class DmlCopyOnWritePlanGenerator extends DmlPlanGeneratorBase {

  public DmlCopyOnWritePlanGenerator(
      RelOptTable table,
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      TableMetadata tableMetadata,
      CreateTableEntry createTableEntry,
      TableModify.Operation operation,
      List<String> updateColumnList,
      boolean hasSource,
      OptimizerRulesContext context) {
    super(
        table,
        cluster,
        traitSet,
        input,
        tableMetadata,
        createTableEntry,
        operation,
        updateColumnList,
        hasSource,
        context);
    this.insertColumnCount =
        DmlUtils.calculateInsertColumnCount(
            inputColumnCount, 0, DmlUtils.SYSTEM_COLUMN_COUNT, updateColumnCount);
    validateOperationType(operation, updateColumnList);
  }

  /**
   * Copy-on-Write Case...
   *
   * @return Copy On Write Insert (source) Column Count found in 'input' RelNode.
   */
  private int calculateInsertColumnCount() {
    return inputColumnCount - DmlUtils.SYSTEM_COLUMN_COUNT - updateColumnCount;
  }

  @Override
  public Prel getPlan() {
    try {
      Prel dataFileAggPlan;
      Prel writerInputPlan;

      if (mergeType == MergeType.INSERT_ONLY) {
        Prel insertOnlyMergeInputDataPlan =
            getInsertOnlyMergeInputDataPlan(
                input.getRowType().getFieldCount() - DmlUtils.SYSTEM_COLUMN_COUNT, input);
        dataFileAggPlan = getDataFileAggPlan(insertOnlyMergeInputDataPlan);
        writerInputPlan = getInsertOnlyMergePlan(insertOnlyMergeInputDataPlan);
      } else {
        dataFileAggPlan = getDataFileAggPlan(input);
        writerInputPlan =
            getDmlQueryResultFromCopyOnWriteJoinPlan(getCopyOnWriteJoinPlan(dataFileAggPlan));
      }

      boolean displayResultOnly =
          PrelUtil.getPlannerSettings(cluster)
              .getOptions()
              .getOption(ENABLE_DML_DISPLAY_RESULT_ONLY);
      if (displayResultOnly) {
        return writerInputPlan;
      }

      return getRowCountPlan(getDataWriterPlan(writerInputPlan, dataFileAggPlan));
    } catch (Exception ex) {
      throw UserException.planError(ex).buildSilently();
    }
  }

  /**
   * Copy-On-Write Version. Builds the project names for insert-only DML. Note that this instance
   * method does not use the 'projectFields' parameter. Instead, field names are provided by 'table'
   * data fields, (i.e, the expected output col names)
   *
   * @param projectFields NOT USED for Copy-On-Write Version
   * @return the list of output column names intended for SQL projection.
   */
  @Override
  public List<String> getInsertOnlyProjectNames(List<RelDataTypeField> projectFields) {
    return table.getRowType().getFieldList().stream()
        .map(RelDataTypeField::getName)
        .filter(name -> !isSystemColumn(name))
        .collect(Collectors.toList());
  }

  /***
   *
   *    HashJoinPrel ----------------------------------------------|
   *    LEFT OUTER ON __FilePath == __FilePath                     |
   *      AND __RowIndex == __RowIndex                         HashToRandomExchangePrel
   *        |                                                      |
   *        |                                                      |
   *    HashToRandomExchangePrel                               (query results input with __FilePath)
   *        |
   *        |
   *    (input from getCopyOnWriteDataFileScanPlan)
   */
  private Prel getCopyOnWriteJoinPlan(Prel dataFileAggrPlan) {
    // left side: full Data from impacted data files
    RelNode leftDataFileScan = getCopyOnWriteDataFileScanPlan(dataFileAggrPlan);

    DistributionTrait leftDistributionTrait =
        getHashDistributionTraitForFields(
            leftDataFileScan.getRowType(),
            ImmutableList.of(ColumnUtils.FILE_PATH_COLUMN_NAME, ColumnUtils.ROW_INDEX_COLUMN_NAME));
    RelTraitSet leftTraitSet =
        cluster.getPlanner().emptyTraitSet().plus(Prel.PHYSICAL).plus(leftDistributionTrait);
    HashToRandomExchangePrel leftDataFileScanHashExchange =
        new HashToRandomExchangePrel(
            cluster, leftTraitSet, leftDataFileScan, leftDistributionTrait.getFields());

    // right side: DMLed data
    DistributionTrait rightDistributionTrait =
        getHashDistributionTraitForFields(
            input.getRowType(),
            ImmutableList.of(ColumnUtils.FILE_PATH_COLUMN_NAME, ColumnUtils.ROW_INDEX_COLUMN_NAME));
    RelTraitSet rightTraitSet =
        cluster.getPlanner().emptyTraitSet().plus(Prel.PHYSICAL).plus(rightDistributionTrait);
    HashToRandomExchangePrel rightInputHashExchange =
        new HashToRandomExchangePrel(
            cluster, rightTraitSet, input, rightDistributionTrait.getFields());

    // hash join on __FilePath == __FilePath AND __RowIndex == __RowIndex
    RelDataTypeField leftFilePathField =
        leftDataFileScanHashExchange
            .getRowType()
            .getField(ColumnUtils.FILE_PATH_COLUMN_NAME, false, false);
    RelDataTypeField leftRowIndexField =
        leftDataFileScanHashExchange
            .getRowType()
            .getField(ColumnUtils.ROW_INDEX_COLUMN_NAME, false, false);
    RelDataTypeField rightFilePathField =
        rightInputHashExchange
            .getRowType()
            .getField(ColumnUtils.FILE_PATH_COLUMN_NAME, false, false);
    RelDataTypeField rightRowIndexField =
        rightInputHashExchange
            .getRowType()
            .getField(ColumnUtils.ROW_INDEX_COLUMN_NAME, false, false);

    int leftFieldCount = leftDataFileScanHashExchange.getRowType().getFieldCount();
    RexBuilder rexBuilder = cluster.getRexBuilder();
    RexNode joinCondition =
        rexBuilder.makeCall(
            SqlStdOperatorTable.AND,
            rexBuilder.makeCall(
                SqlStdOperatorTable.EQUALS,
                rexBuilder.makeInputRef(leftFilePathField.getType(), leftFilePathField.getIndex()),
                rexBuilder.makeInputRef(
                    rightFilePathField.getType(), leftFieldCount + rightFilePathField.getIndex())),
            rexBuilder.makeCall(
                SqlStdOperatorTable.EQUALS,
                rexBuilder.makeInputRef(leftRowIndexField.getType(), leftRowIndexField.getIndex()),
                rexBuilder.makeInputRef(
                    rightRowIndexField.getType(), leftFieldCount + rightRowIndexField.getIndex())));

    return HashJoinPrel.create(
        leftDataFileScanHashExchange.getCluster(),
        leftDataFileScanHashExchange.getTraitSet(),
        leftDataFileScanHashExchange,
        rightInputHashExchange,
        joinCondition,
        null,
        getCopyOnWriteJoinType(),
        true);
  }

  private JoinRelType getCopyOnWriteJoinType() {
    if (operation == MERGE) {
      return JoinRelType.FULL;
    }

    return JoinRelType.LEFT;
  }

  /***
   *    ProjectPrel (userCol1, ..., userColN, file_path, row_index)
   *        |
   *        |
   *    FilterPrel (Delete only, right.__RowIndex IS NULL)
   *        |
   *        |
   *    JoinPrel
   *        |
   *        |
   *
   *      Input data layout from join
   *      Left Original Data Area(userCol1,...,userColN, left_file_path, left_row_index) | Inserted Data Area (userCol1,...,userColN) | right_file_path, right_row_index | Updated Data Area (updatedCol1,...,updateColM)
   *
   *     Output data layout
   *     userCol1, ..., userColN, file_path, row_index
   *
   *     Input -> Output mapping rule:
   *         if left_row_index column is null (i.e., the row is inserted),
   *                use inserted value
   *         else if right_row_index column is null (i.e., the value is not updated),
   *                use original value
   *        else (i.e., the value is updated)
   *                use updated value.
   */
  private Prel getDmlQueryResultFromCopyOnWriteJoinPlan(Prel joinPlan) {
    Prel currentPrel = joinPlan;
    RexBuilder rexBuilder = cluster.getRexBuilder();
    int leftFieldCount = joinPlan.getInput(0).getRowType().getFieldCount();
    RelDataTypeField leftRowIndexField =
        joinPlan.getInput(0).getRowType().getField(ColumnUtils.ROW_INDEX_COLUMN_NAME, false, false);
    RelDataTypeField rightRowIndexField =
        joinPlan.getInput(1).getRowType().getField(ColumnUtils.ROW_INDEX_COLUMN_NAME, false, false);

    // detect dups
    if ((operation == MERGE
            && (mergeType == MergeType.UPDATE_ONLY || mergeType == MergeType.UPDATE_INSERT))
        || ((operation == UPDATE || operation == DELETE) && hasSource)) {
      // Adds a table function to look for multiple matching rows, currently happens for
      // 1. MERGE with UPDATEs
      // 2. UPDATE with source
      // 3. DELETE with source
      // Throws an exception when a row to update matches multiple times.
      currentPrel = new IcebergDmlMergeDuplicateCheckPrel(currentPrel, table, tableMetadata);
    }

    // Add Delete filter: FilterPrel (right.__RowIndex IS NULL)
    if (operation == DELETE) {
      currentPrel =
          addColumnIsNullFilter(
              currentPrel,
              rightRowIndexField.getType(),
              leftFieldCount + rightRowIndexField.getIndex());
    }

    // project (tablecol1, ..., tablecolN)
    List<RelDataTypeField> projectFields = getFieldsWithoutSystemColumns(joinPlan.getInput(0));
    List<String> projectNames =
        projectFields.stream().map(RelDataTypeField::getName).collect(Collectors.toList());
    //  the condition for checking if right_row_index column is null
    RexNode rightRowIndexNullCheck =
        rexBuilder.makeCall(
            SqlStdOperatorTable.IS_NULL,
            rexBuilder.makeInputRef(
                rightRowIndexField.getType(), leftFieldCount + rightRowIndexField.getIndex()));

    //  the condition for checking if left_row_index column is null
    RexNode leftRowIndexFieldNullCheck =
        rexBuilder.makeCall(
            SqlStdOperatorTable.IS_NULL,
            rexBuilder.makeInputRef(leftRowIndexField.getType(), leftRowIndexField.getIndex()));

    List<RexNode> projectExprs = new ArrayList<>();
    for (RelDataTypeField field : projectFields) {
      RexNode projectExpr =
          getColumnProjectExpr(
              rexBuilder,
              field,
              Optional.of(leftFieldCount),
              field.getIndex(),
              leftRowIndexFieldNullCheck,
              rightRowIndexNullCheck);
      projectExprs.add(projectExpr);
    }

    RelDataType projectRowType =
        RexUtil.createStructType(
            joinPlan.getCluster().getTypeFactory(), projectExprs, projectNames, null);

    return ProjectPrel.create(
        currentPrel.getCluster(),
        currentPrel.getTraitSet(),
        currentPrel,
        projectExprs,
        projectRowType);
  }

  /**
   * Acquires the update column of the Copy On Write Join Plan.
   *
   * @return Expression which calculates index of the correct update-"Copy_On_Write"-output col
   */
  @Override
  protected RexNode getUpdateRexNodeExpr(
      RexBuilder rexBuilder,
      RelDataTypeField field,
      int updateColumnIndex,
      Optional<Integer> leftFieldCount,
      int insertedFieldCount,
      RexNode rowIndexNullCondition,
      RexNode rightRowIndexNullCondition) {

    return rexBuilder.makeCall(
        SqlStdOperatorTable.CASE,
        rightRowIndexNullCondition,
        rexBuilder.makeInputRef(field.getType(), field.getIndex()),
        rexBuilder.makeInputRef(
            field.getType(),
            leftFieldCount.get()
                + insertedFieldCount
                + DmlUtils.SYSTEM_COLUMN_COUNT
                + updateColumnIndex));
  }

  /**
   * calculates the index of the Reference field to be used as an insert column.
   *
   * <p>
   *
   * @param field Rel from 'input' of focus during parsing.
   * @param leftFieldCount total count of fields on left side of join
   * @return index reference to desired 'insert' field within 'input'. This field will be contained
   *     in a CASE call, which is used for projection Prel
   */
  @Override
  protected RexNode getMergeWithInsertExpr(
      RexBuilder rexBuilder,
      RelDataTypeField field,
      Optional<Integer> leftFieldCount,
      RexNode RowIndexFieldNullCheck,
      RexNode updateExprForInsert) {

    int index = leftFieldCount.get() + field.getIndex();
    RexNode insertColRef = rexBuilder.makeInputRef(field.getType(), index);
    return rexBuilder.makeCall(
        SqlStdOperatorTable.CASE, RowIndexFieldNullCheck, insertColRef, updateExprForInsert);
  }

  /**
   * @return the field list without system columns, filtered by target table fields
   */
  private List<RelDataTypeField> getFieldsWithoutSystemColumns(RelNode input) {
    return input.getRowType().getFieldList().stream()
        .filter(
            f ->
                table.getRowType().getField(f.getName(), false, false) != null
                    && !isSystemColumn(f.getName()))
        .collect(Collectors.toList());
  }

  /***
   *
   *    TableFunctionPrel(DATA_FILE_SCAN)
   *        |
   *        |
   *    HashToRandomExchangePrel hash(splitsIdentity)
   *        |
   *        |
   *    HashJoinPrel ----------------------------------------------|
   *    INNER ON splitsIdentity.path == __FilePath                 |
   *        |                                                      |
   *        |                                                  BroadcastExchangePrel
   *        |                                                      |
   *    TableFunctionPrel(SPLIT_GEN_MANIFEST_SCAN)             dataFileListInput
   *        |                                                  from createDataFileAggPrel()
   *        |
   *    HashToRandomExchangePrel hash(splitsIdentity)
   *        |
   *        |
   *    IcebergManifestListPrel
   */
  private RelNode getCopyOnWriteDataFileScanPlan(RelNode dataFileListInput) {
    List<SchemaPath> allColumns =
        table.getRowType().getFieldNames().stream()
            .map(SchemaPath::getSimplePath)
            .collect(Collectors.toList());
    IcebergScanPlanBuilder builder =
        new IcebergScanPlanBuilder(
            cluster,
            traitSet,
            table,
            tableMetadata,
            allColumns,
            context,
            ManifestScanFilters.empty(),
            null);

    return builder.buildWithDmlDataFileFiltering(dataFileListInput);
  }

  @Override
  protected RexNode getDeleteExpr(RexBuilder rexBuilder, RelDataTypeField field, int index) {
    return rexBuilder.makeInputRef(field.getType(), index);
  }

  /**
   * Copy On Write version of updateInsertExpr is identical to super's updateExpr. Re-use.
   *
   * @return
   */
  @Override
  protected RexNode getUpdateInsertExpr(
      RexBuilder rexBuilder,
      RelDataTypeField field,
      Optional<Integer> leftFieldCount,
      Integer insertedFieldCount,
      RexNode rowIndexNullCondition,
      RexNode rightRowIndexNullCondition) {

    return super.getUpdateExpr(
        rexBuilder,
        field,
        leftFieldCount,
        insertedFieldCount,
        rowIndexNullCondition,
        rightRowIndexNullCondition);
  }

  /**
   * HashAggPrel groupby(__FilePath) | | HashToRandomExchangePrel | | (query results input with
   * __FilePath)
   */
  private Prel getDataFileAggPlan(RelNode aggInput) throws InvalidRelException {
    RelDataTypeField filePathField =
        Preconditions.checkNotNull(
            aggInput.getRowType().getField(ColumnUtils.FILE_PATH_COLUMN_NAME, false, false));

    // hash exchange on __FilePath
    DistributionTrait.DistributionField distributionField =
        new DistributionTrait.DistributionField(filePathField.getIndex());
    DistributionTrait distributionTrait =
        new DistributionTrait(
            DistributionTrait.DistributionType.HASH_DISTRIBUTED,
            ImmutableList.of(distributionField));
    RelTraitSet relTraitSet =
        cluster.getPlanner().emptyTraitSet().plus(Prel.PHYSICAL).plus(distributionTrait);
    HashToRandomExchangePrel filePathHashExchPrel =
        new HashToRandomExchangePrel(cluster, relTraitSet, aggInput, distributionTrait.getFields());

    // hash agg groupby(__FilePath)
    ImmutableBitSet groupSet = ImmutableBitSet.of(filePathField.getIndex());

    // get dmled row count
    AggregateCall aggRowCount =
        AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            false,
            Collections.emptyList(),
            -1,
            RelCollations.EMPTY,
            1,
            filePathHashExchPrel,
            cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT),
            ColumnUtils.ROW_COUNT_COLUMN_NAME);

    return HashAggPrel.create(
        cluster,
        filePathHashExchPrel.getTraitSet(),
        filePathHashExchPrel,
        groupSet,
        ImmutableList.of(groupSet),
        ImmutableList.of(aggRowCount),
        null);
  }

  /***
   *    ProjectPrel (RecordWriter.RECORDS)
   *        |
   *        |
   *    HashAggPrel (sum(RecordWriter.RECORDS))
   *        |
   *        |
   *        |
   *    FilterPrel (OperationType = OperationType.DELETE_DATAFILE)
   *        |
   *        |
   *
   */
  private Prel getRowCountPlan(Prel writerPrel) throws InvalidRelException {
    RexBuilder rexBuilder = cluster.getRexBuilder();

    // Filter:
    // The OPERATION_TYPE column in rows coming from writer committer could be:
    // 1. OperationType.ADD_DATAFILE  ---- the file we created from anti-join results
    // 2. OperationType.DELETE_DATAFILE  -- the files touched by DML operations
    // we only keep rows from DMLed files (i.e.,  with OperationType.DELETE_DATAFILE) since the
    // final rowcount will be DMLed rows
    RelDataTypeField operationTypeField =
        writerPrel.getRowType().getField(RecordWriter.OPERATION_TYPE.getName(), false, false);
    RexNode deleteDataFileLiteral =
        rexBuilder.makeLiteral(
            OperationType.DELETE_DATAFILE.value,
            cluster.getTypeFactory().createSqlType(SqlTypeName.INTEGER),
            true);

    RexNode filterCondition =
        rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(writerPrel, operationTypeField.getIndex()),
            deleteDataFileLiteral);

    FilterPrel filterPrel =
        FilterPrel.create(
            writerPrel.getCluster(), writerPrel.getTraitSet(), writerPrel, filterCondition);

    // Agg: get the total row count for DMLed results
    RelDataTypeField recordsField =
        writerPrel.getRowType().getField(RecordWriter.RECORDS.getName(), false, false);
    AggregateCall aggRowCount =
        AggregateCall.create(
            SqlStdOperatorTable.SUM,
            false,
            false,
            ImmutableList.of(recordsField.getIndex()),
            -1,
            RelCollations.EMPTY,
            0,
            filterPrel,
            null,
            RecordWriter.RECORDS.getName());

    StreamAggPrel rowCountAgg =
        StreamAggPrel.create(
            filterPrel.getCluster(),
            filterPrel.getTraitSet(),
            filterPrel,
            ImmutableBitSet.of(),
            ImmutableList.of(),
            ImmutableList.of(aggRowCount),
            null);

    // Project: return 0 as row count in case there is no Agg record (i.e., no DMLed results)
    recordsField = rowCountAgg.getRowType().getField(RecordWriter.RECORDS.getName(), false, false);
    List<String> projectNames = ImmutableList.of(recordsField.getName());
    RexNode zeroLiteral =
        rexBuilder.makeLiteral(
            0, rowCountAgg.getCluster().getTypeFactory().createSqlType(SqlTypeName.INTEGER), true);
    // check if the count of row count records is 0 (i.e., records column is null)
    RexNode rowCountRecordExistsCheckCondition =
        rexBuilder.makeCall(
            SqlStdOperatorTable.IS_NULL,
            rexBuilder.makeInputRef(recordsField.getType(), recordsField.getIndex()));
    // case when the count of row count records is 0, return 0, else return aggregated row count
    RexNode projectExpr =
        rexBuilder.makeCall(
            SqlStdOperatorTable.CASE,
            rowCountRecordExistsCheckCondition,
            zeroLiteral,
            rexBuilder.makeInputRef(recordsField.getType(), recordsField.getIndex()));
    List<RexNode> projectExprs = ImmutableList.of(projectExpr);

    RelDataType projectRowType =
        RexUtil.createStructType(
            rowCountAgg.getCluster().getTypeFactory(), projectExprs, projectNames, null);

    return ProjectPrel.create(
        rowCountAgg.getCluster(),
        rowCountAgg.getTraitSet(),
        rowCountAgg,
        projectExprs,
        projectRowType);
  }
}
