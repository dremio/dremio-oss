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

import static com.dremio.exec.util.ColumnUtils.isSystemColumn;
import static org.apache.calcite.rel.core.TableModify.Operation.DELETE;
import static org.apache.calcite.rel.core.TableModify.Operation.MERGE;
import static org.apache.calcite.rel.core.TableModify.Operation.UPDATE;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.planner.TableManagementPlanGenerator;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.planner.sql.parser.DmlUtils;
import com.dremio.exec.store.OperationType;
import com.dremio.exec.store.RecordWriter;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.util.ColumnUtils;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
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
 * Generate Merge_on_Read based DML query plan. Support Delete, Update and Merge operations.
 *
 *                                       Writer
 *             ____________________________ |___________________________________
 *            |                             |                                  |
 *          Delete                    Update/Merge                    'Insert_Only' Merge
 *    (Deleted Data Only)     (Deleted Data & Updated Data)             (New Data only)
 *             |                            |                                  |
 *             |____________________________|__________________________________|
 *                                          |
 *                                     Projection
 *                                          |
 *                                     DMLed data
 */
public class DmlPositionalMergeOnReadPlanGenerator extends DmlPlanGeneratorBase {

  private final boolean isTablePartitioned;

  private final List<String> partitionedColumns;

  /**
   * Constructor for Merge On Read Physical Plan Generator. <br>
   * The input node field order is different from copy-on-write Plan... <br>
   * {@link TableManagementPlanGenerator#input} 'input' contains all the columns extracted from sql
   * validation. This includes the following columns (fields) in this respective order <br>
   * a) Target Table Columns ------> Table Columns <br>
   * b) System Columns ----/ <br>
   * c) Insert Source Columns ------> Insert Columns <br>
   * d) Update Source Columns ------> Update Columns <br>
   * Depending on the Dml Type, some columns will be omitted during sql validation phase.
   */
  public DmlPositionalMergeOnReadPlanGenerator(
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

    partitionedColumns = createTableEntry.getOptions().getPartitionColumns();
    this.isTablePartitioned = createTableEntry.getOptions().hasPartitions();

    Set<String> updateColumns =
        updateColumnList == null ? Collections.emptySet() : new HashSet<>(updateColumnList);

    this.outdatedTargetColumnNames =
        DmlUtils.getOutdatedTargetColumns(updateColumns, table, partitionedColumns);

    // total number of source columns (used for insert cases)
    insertColumnCount =
        DmlUtils.calculateInsertColumnCount(
            inputColumnCount,
            outdatedTargetColumnNames.size(),
            tableColumnCount,
            updateColumnCount);

    validateOperationType(operation, updateColumnList);
  }

  @Override
  public Prel getPlan() {
    try {
      Prel writerInputPlan;
      if (mergeType == MergeType.INSERT_ONLY) {
        HashToRandomExchangePrel inputDataFileScanHashExchange =
            buildHashToRandomExchangePrel(input);
        RelNode inputRel =
            (inputDataFileScanHashExchange == null) ? input : inputDataFileScanHashExchange;
        Prel insertOnlyMergeInputDataPlan = getInsertOnlyMergeInputDataPlan(0, inputRel);
        writerInputPlan = getInsertOnlyMergePlan(insertOnlyMergeInputDataPlan);
      } else {
        boolean dupsCheck = isDuplicateCheckTableFunctionNecessary();
        Prel duplicateCheckPrel = null;

        if (dupsCheck) {
          duplicateCheckPrel = new IcebergDmlMergeDuplicateCheckPrel(input, table, tableMetadata);
        }

        if (mustUseRowSplitter()) {
          writerInputPlan =
              duplicateCheckPrel == null
                  ? prepareForSplitter(input)
                  : prepareForSplitter(duplicateCheckPrel);
        } else {
          writerInputPlan =
              duplicateCheckPrel == null
                  ? getMergeOnReadProjectionPlanPrel(input)
                  : getMergeOnReadProjectionPlanPrel(duplicateCheckPrel);
        }
      }

      return getMergeOnReadRowCountPlan(getMergeOnReadDataWriterPlan(writerInputPlan));
    } catch (Exception ex) {
      throw UserException.planError(ex).buildSilently();
    }
  }

  private boolean mustUseRowSplitter() {
    return (isTablePartitioned
        && partitionedColumns.stream().anyMatch(updateColumnsWithIndex.keySet()::contains)
        && (operation == UPDATE
            || mergeType == MergeType.UPDATE_ONLY
            || mergeType == MergeType.UPDATE_INSERT));
  }

  private <T extends RelNode> Prel prepareForSplitter(T input) {
    return new IcebergMergeOnReadRowSplitterPrel(
        input,
        table,
        tableMetadata,
        tableColumnCount,
        insertColumnCount,
        updateColumnsWithIndex,
        outdatedTargetColumnNames,
        partitionedColumns);
  }

  /**
   * Merge-On-Read Version. Builds the project names for insert-only DML. Project names are built by
   * streaming the projectField list. 'projectfield' list names tend to be proj Expressions (i.e.
   * $f0... $f1... etc)
   *
   * @param projectFields the insert_only output RelNodes sent to SQL projection
   * @return the list of output column names intended for SQL projection.
   */
  @Override
  public List<String> getInsertOnlyProjectNames(List<RelDataTypeField> projectFields) {
    return table.getRowType().getFieldNames().stream()
        .filter(f -> !isSystemColumn(f))
        .collect(Collectors.toList());
  }

  @Override
  protected RexNode getDeleteExpr(RexBuilder rexBuilder, RelDataTypeField field, int index) {
    if (isTablePartitioned) {
      if ((null != partitionedColumns && partitionedColumns.contains(field.getName()))
          || isSystemColumn(field.getName())) {
        return rexBuilder.makeInputRef(field.getType(), index);
      } else {
        return rexBuilder.makeNullLiteral(field.getType());
      }
    } else {
      return rexBuilder.makeInputRef(field.getType(), index);
    }
  }

  /**
   * Acquires the update column. Merge-On-Read Version.
   *
   * <p>Index location of desired update column Reference = total expected cols - total outdated
   * target cols + total insert cols + update idx of field.
   *
   * <p>Why we increment parsedOutdatedTargetColumns: Logical Planning has trimmed unnecessary
   * (outdated) target columns from our 'input' relNode. These 'outdated' are indirectly reference
   * for each occurence of an update col... <br>
   * ...In other words, for every update col, there is an 'outdated' target col that's been removed
   * from the 'input'. Thus, For every field referenced in the update col index, we pass an index
   * where an outdated target col once existed... In order to remain in syc with index references
   * while parsing, we tally each occurrence as we parse through an outdatedTargetCol.
   *
   * <p>Note: Merge On Read Update Expr only requires half of the parameters.
   *
   * @return the index of the desired update column RelNode.
   */
  @Override
  protected RexNode getUpdateRexNodeExpr(
      RexBuilder rexBuilder,
      RelDataTypeField field,
      int updateColumnIndex,
      Optional<Integer> leftFieldCount,
      int insertedFieldCount,
      RexNode rowIndexNullCondition,
      RexNode rightRowIndexNullcondition) {

    if (outdatedTargetColumnNames.contains(field.getName())) {
      parsedOutdatedTargetColumns++;
    }

    return rexBuilder.makeInputRef(
        field.getType(),
        tableColumnCount
            - outdatedTargetColumnNames.size()
            + insertColumnCount
            + updateColumnIndex);
  }

  /**
   * Merge On Read Version. Acquires an update col reference (if exists), otherwise, get target col
   * reference. The output Col Ref is passed to {@link #getMergeWithInsertExpr} in order to
   * determine official node selected for output.
   *
   * @param rexBuilder Calcite's RexNode Builder
   * @param field the Rex field at reference
   * @param leftFieldCount starting index of input
   * @param insertedFieldCount started index of insert cols
   * @param rowIndexNullCondition null condition Case statement. Used to select correct input ref
   * @param rightRowIndexNullCondition null condition case statement for input Sys cols on right
   *     side of join. NOT USED for Merge On Read Version.
   * @return the update col reference, or the target col reference,
   */
  @Override
  protected RexNode getUpdateInsertExpr(
      RexBuilder rexBuilder,
      RelDataTypeField field,
      Optional<Integer> leftFieldCount,
      Integer insertedFieldCount,
      RexNode rowIndexNullCondition,
      RexNode rightRowIndexNullCondition) {

    Integer updateColumnIndex = updateColumnsWithIndex.get(field.getName());

    // if no update col, then field is either insert col or target col
    if (updateColumnIndex == null) {

      // It's a Target Col. Grab it.
      int targetColIndex = field.getIndex() - parsedOutdatedTargetColumns;
      return rexBuilder.makeInputRef(field.getType(), targetColIndex);

    } else {

      // It's an Update Col. Grab it.
      return getUpdateRexNodeExpr(
          rexBuilder,
          field,
          updateColumnIndex,
          leftFieldCount,
          insertedFieldCount,
          rowIndexNullCondition,
          rightRowIndexNullCondition);
    }
  }

  /***
   *
   *   Projection Prel (Project Columns are dependent on {@link TableModify.Operation}.
   *         |          For Details, view {@link #getColumnProjectExpr})
   *         |
   *         |
   *   'input' Columns
   */
  private <T extends RelNode> Prel getMergeOnReadProjectionPlanPrel(T input) {
    HashToRandomExchangePrel inputDataFileScanHashExchange = buildHashToRandomExchangePrel(input);

    RelNode finalInput =
        (inputDataFileScanHashExchange == null) ? input : inputDataFileScanHashExchange;

    RelDataTypeField rowIndexField =
        finalInput.getRowType().getField(ColumnUtils.ROW_INDEX_COLUMN_NAME, false, false);

    RexBuilder rexBuilder = cluster.getRexBuilder();
    RexNode rowIndexFieldNullCheck =
        rexBuilder.makeCall(
            SqlStdOperatorTable.IS_NULL,
            rexBuilder.makeInputRef(rowIndexField.getType(), rowIndexField.getIndex()));

    List<RelDataTypeField> projectFields;

    if (this.operation == DELETE && !isTablePartitioned) {
      projectFields =
          table.getRowType().getFieldList().stream()
              .filter(f -> isSystemColumn(f.getName()))
              .collect(Collectors.toList());
    } else {
      projectFields = table.getRowType().getFieldList();
    }

    List<String> projectNames =
        projectFields.stream().map(RelDataTypeField::getName).collect(Collectors.toList());

    List<RexNode> projectExprs = new ArrayList<>();
    int index = 0;
    for (RelDataTypeField field : projectFields) {
      RexNode projectExpr =
          getColumnProjectExpr(
              rexBuilder, field, Optional.empty(), index, rowIndexFieldNullCheck, null);
      projectExprs.add(projectExpr);
      index++;
    }

    RelDataType projectRowType =
        RexUtil.createStructType(
            finalInput.getCluster().getTypeFactory(), projectExprs, projectNames, null);

    return ProjectPrel.create(
        finalInput.getCluster(),
        finalInput.getTraitSet(),
        finalInput,
        projectExprs,
        projectRowType);
  }

  private boolean isDuplicateCheckTableFunctionNecessary() {
    // detect dups
    return ((operation == MERGE
            && (mergeType == MergeType.UPDATE_ONLY || mergeType == MergeType.UPDATE_INSERT))
        || ((operation == UPDATE || operation == DELETE) && hasSource));
    // Adds a table function to look for multiple matching rows, currently happens for
    // 1. MERGE with UPDATEs
    // 2. UPDATE with source
    // 3. DELETE with source
    // Throws an exception when a row to update matches multiple times.
  }

  /**
   * Merge-On-Read only.
   *
   * <p>add HashToRandomExchange if table is <b>not</b> partitioned. This output will be used to
   * generate the projections for Merge On Real DML Planning.
   *
   * @return a HashToRandomExchange varient of the input Relnode.
   */
  private <T extends RelNode> HashToRandomExchangePrel buildHashToRandomExchangePrel(T input) {
    if (!isTablePartitioned) {
      DistributionTrait inputDistributionTrait =
          getHashDistributionTraitForFields(
              input.getRowType(),
              ImmutableList.of(
                  ColumnUtils.FILE_PATH_COLUMN_NAME, ColumnUtils.ROW_INDEX_COLUMN_NAME));
      RelTraitSet inputTraitSet =
          cluster.getPlanner().emptyTraitSet().plus(Prel.PHYSICAL).plus(inputDistributionTrait);
      return new HashToRandomExchangePrel(
          cluster, inputTraitSet, input, inputDistributionTrait.getFields());
    }
    return null;
  }

  /**
   * Determine official nodes passed update_insert output. This method is responsible for
   * determining the correct node for WHEN NOT MATCHED case. The WHEN MATCHED case was determined
   * from previous method ({@link #getUpdateInsertExpr}).
   *
   * <p>The WHEN NOT MATCHED option will always return the insert column. Insert statements never
   * reference the target columns.
   *
   * <p>The WHEN NOT MATCHED case will either be a target col or an insert col. If insert name
   * found, then is insert col. Otherwise, target col. <br>
   *
   * @param rexBuilder Calcite's RexNode Builder
   * @param field the Rex field of reference
   * @param fieldCount NOT USED in merge on read impl
   * @param RowIndexFieldNullCheck null condition on Sys Col Case statement. Used to select correct
   *     input ref.
   * @param updateExprForInsert The node Selected for WHEN MATCHED case
   * @return the (WHEN NOT MATCHED || WHEN MATCHED) as a CASE package.
   */
  @Override
  protected RexNode getMergeWithInsertExpr(
      RexBuilder rexBuilder,
      RelDataTypeField field,
      Optional<Integer> fieldCount,
      RexNode RowIndexFieldNullCheck,
      RexNode updateExprForInsert) {

    // return early if Sys Col. CASE builder not needed.
    if (field.getName().equals(ColumnUtils.FILE_PATH_COLUMN_NAME)
        || field.getName().equals(ColumnUtils.ROW_INDEX_COLUMN_NAME)) {
      return rexBuilder.makeInputRef(
          field.getType(), field.getIndex() - outdatedTargetColumnNames.size());
    }

    // is an insert Column. Grab index for it
    int index = tableColumnCount + field.getIndex() - outdatedTargetColumnNames.size();

    RexNode insertOrTargetColRef = rexBuilder.makeInputRef(field.getType(), index);

    return rexBuilder.makeCall(
        SqlStdOperatorTable.CASE,
        RowIndexFieldNullCheck,
        insertOrTargetColRef,
        updateExprForInsert);
  }

  private Prel getMergeOnReadRowCountPlan(Prel writerPrel) throws InvalidRelException {
    RexBuilder rexBuilder = cluster.getRexBuilder();

    // Filter:
    // The OPERATION_TYPE column in rows coming from writer committer could be:
    // 1. OperationType.ADD_DATAFILE  ---- the data files written
    // 2. OperationType.ADD_DELETEFILE --- the delete files added written
    // rowCount should be...
    //    For Delete:               ADD_DELETEFILE
    //    For Update:               Tracking Either Works
    //    For Merge-update_only:    Tracking Either Works
    //    For Merge-update_insert:  ADD_DATAFILE... Because new datafile values are guaranteed,
    // while deletes are not
    // ^TODO: this assumption will fail once merge_deletes are supported.
    //  For Merge-insert_only:    ADD_DATAFILE
    // we only keep rows from DMLed files (i.e.,  with OperationType.DELETE_DATAFILE) since the
    // final rowcount will be DMLed rows

    // TODO: Add this code once OperationType Class has ADD_DELETEFILE. (DX-86508 & DX-85892)
    RelDataTypeField operationTypeField =
        writerPrel.getRowType().getField(RecordWriter.OPERATION_TYPE.getName(), false, false);

    RexNode literal;
    if (operation == TableModify.Operation.DELETE) {
      literal =
          rexBuilder.makeLiteral(
              OperationType.ADD_DELETEFILE.value,
              cluster.getTypeFactory().createSqlType(SqlTypeName.INTEGER),
              true);
    } else {
      literal =
          rexBuilder.makeLiteral(
              OperationType.ADD_DATAFILE.value,
              cluster.getTypeFactory().createSqlType(SqlTypeName.INTEGER),
              true);
    }

    RexNode filterCondition =
        rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(writerPrel, operationTypeField.getIndex()),
            literal);

    FilterPrel filterPrel =
        FilterPrel.create(
            writerPrel.getCluster(), writerPrel.getTraitSet(), writerPrel, filterCondition);

    RelDataTypeField recordsField =
        writerPrel.getRowType().getField(RecordWriter.RECORDS.getName(), false, false);

    List<AggregateCall> aggregateCalls = new ArrayList<>();
    aggregateCalls.add(
        AggregateCall.create(
            SqlStdOperatorTable.SUM,
            false,
            false,
            ImmutableList.of(recordsField.getIndex()),
            -1,
            0,
            filterPrel,
            null,
            RecordWriter.RECORDS.getName()));

    StreamAggPrel rowCountAgg =
        StreamAggPrel.create(
            filterPrel.getCluster(),
            filterPrel.getTraitSet(),
            filterPrel,
            ImmutableBitSet.of(),
            ImmutableList.of(),
            aggregateCalls,
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
