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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.apache.commons.collections.CollectionUtils;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.physical.config.TableFunctionContext;
import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.store.OperationType;
import com.dremio.exec.store.RecordWriter;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.iceberg.IcebergScanPlanBuilder;
import com.dremio.exec.util.ColumnUtils;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/***
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
public class DmlPlanGenerator {

  private static final int SYSTEM_COLUMN_COUNT = 2;
  private final RelOptTable table;
  private final RelOptCluster cluster;
  private final RelTraitSet traitSet;
  private final RelNode input;
  private final TableMetadata tableMetadata;
  private final CreateTableEntry createTableEntry;
  private final TableModify.Operation operation;
  private final OptimizerRulesContext context;
  // update column names along with its index
  private final Map<String, Integer> updateColumnsWithIndex = new HashMap<>();

  public enum MergeType {
    UPDATE_ONLY,
    UPDATE_INSERT,
    INSERT_ONLY,
    INVALID
  }
  private MergeType mergeType = MergeType.INVALID;

  public DmlPlanGenerator(RelOptTable table, RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
                          TableMetadata tableMetadata, CreateTableEntry createTableEntry,
                          TableModify.Operation operation, List<String> updateColumnList,
                          OptimizerRulesContext context) {
    this.table = Preconditions.checkNotNull(table);
    this.cluster = cluster;
    this.traitSet = traitSet;
    this.input = input;
    this.tableMetadata = Preconditions.checkNotNull(tableMetadata, "TableMetadata cannot be null.");
    this.createTableEntry = Preconditions.checkNotNull(createTableEntry, "CreateTableEntry cannot be null.");
    this.operation = Preconditions.checkNotNull(operation, "DML operation cannot be null.");
    this.context = Preconditions.checkNotNull(context, "Context cannot be null.");

    validateOperation(operation, updateColumnList);
  }

  private void validateOperation(TableModify.Operation operation, List<String> updateColumnList) {
    // validate updateColumnList
    if (operation == TableModify.Operation.UPDATE || operation == TableModify.Operation.MERGE) {
      if (CollectionUtils.isNotEmpty(updateColumnList)) {
        for (int i = 0; i < updateColumnList.size(); i++) {
          this.updateColumnsWithIndex.put(updateColumnList.get(i), i);
        }
      }
    }

    // set and validate merge type
    if (operation == MERGE) {
      int inputColumnCount = input.getRowType().getFieldCount();
      int updateColumnCount = updateColumnsWithIndex.size();
      if (updateColumnCount == 0) {
        mergeType = MergeType.INSERT_ONLY;
      }  else if (inputColumnCount > SYSTEM_COLUMN_COUNT + updateColumnCount) {
        mergeType = MergeType.UPDATE_INSERT;
      } else {
        mergeType = MergeType.UPDATE_ONLY;
      }
    }
  }

  public Prel getPlan() {
    try {
      Prel dataFileAggPlan;
      Prel writerInputPlan;

      if (mergeType == MergeType.INSERT_ONLY) {
        Prel insertOnlyMergeInputDataPlan = getInsertOnlyMergeInputDataPlan();
        dataFileAggPlan = getDataFileAggPlan(insertOnlyMergeInputDataPlan);
        writerInputPlan = getInsertOnlyMergePlan(insertOnlyMergeInputDataPlan);
      } else {
        dataFileAggPlan = getDataFileAggPlan(input);
        writerInputPlan = getDmlQueryResultFromCopyOnWriteJoinPlan(getCopyOnWriteJoinPlan(dataFileAggPlan));
      }

      return getRowCountPlan(getDataWriterPlan(writerInputPlan, dataFileAggPlan));
    } catch (Exception ex) {
      throw UserException.planError(ex).buildSilently();
    }
  }

  /**
   * Utility function to apply IS_NULL(col) filter for the given input node
   */
  private Prel addColumnIsNullFilter(RelNode inputNode, RelDataType fieldType,  int fieldIndex) {
    RexBuilder rexBuilder = cluster.getRexBuilder();

    RexNode filterCondition = rexBuilder.makeCall(
      SqlStdOperatorTable.IS_NULL,
      rexBuilder.makeInputRef(fieldType, fieldIndex));

    return FilterPrel.create(
      inputNode.getCluster(),
      inputNode.getTraitSet(),
      inputNode,
      filterCondition);
  }

  /**
   *  Extract "When Not Matched" rows from input
   *    Input data layout from Calcite's
   *    Inserted Data Area (Exp(userCol1),...,Exp(userColN)), System Columns(filePath, rowIndex)
   */
  private Prel getInsertOnlyMergeInputDataPlan() {
    // filter out matched rows since we are doing "When Not Matched"
    RelDataTypeField filePathField = input.getRowType()
      .getField(ColumnUtils.FILE_PATH_COLUMN_NAME, false, false);

    return addColumnIsNullFilter(input, filePathField.getType(),
      input.getRowType().getFieldCount() - SYSTEM_COLUMN_COUNT);
  }

  /**
   *  InsertOnlyMergePlan is essentially a wrapper on insertOnlyMergeInputDataPlan.
   * The inserted data columns names from Calcite is in expression format (e.g., $0)
   * We need map the names to user column names so that the downstream plan can consume
   */
  private Prel getInsertOnlyMergePlan(Prel insertOnlyMergeInputDataPlan) {
    RexBuilder rexBuilder = cluster.getRexBuilder();

    // project (tablecol1, ..., tablecolN)
    List<RelDataTypeField> projectFields = insertOnlyMergeInputDataPlan.getRowType().getFieldList().stream()
      .filter(f -> !isSystemColumn(f.getName()))
      .collect(Collectors.toList());
    List<String> projectNames = table.getRowType().getFieldList()
      .stream()
      .filter(f -> !isSystemColumn(f.getName()))
      .map(f -> f.getName()).collect(Collectors.toList());

    List<RexNode> projectExprs = new ArrayList<>();
    for (RelDataTypeField field : projectFields) {
      RexNode projectExpr = rexBuilder.makeInputRef(field.getType(), field.getIndex());
      projectExprs.add(projectExpr);
    }

    RelDataType projectRowType = RexUtil.createStructType(insertOnlyMergeInputDataPlan.getCluster().getTypeFactory(), projectExprs,
      projectNames, null);

    return ProjectPrel.create(
      insertOnlyMergeInputDataPlan.getCluster(),
      insertOnlyMergeInputDataPlan.getTraitSet(),
      insertOnlyMergeInputDataPlan,
      projectExprs,
      projectRowType);
  }

  /**
   *    WriterCommitterPrel
   *        |
   *        |
   *    UnionAllPrel ---------------------------------------------|
   *        |                                                     |
   *        |                                                     |
   *    WriterPrel                                            TableFunctionPrel (DELETED_DATA_FILES_METADATA)
   *        |                                                 this converts a path into required IcebergMetadata blob
   *        |                                                     |
   *    (input from copyOnWriteResultsPlan)                       (deleted data files list from dataFileAggrPlan)
   */
  private Prel getDataWriterPlan(RelNode copyOnWriteResultsPlan, final RelNode dataFileAggrPlan) {
    return WriterPrule.createWriter(
      copyOnWriteResultsPlan,
      copyOnWriteResultsPlan.getRowType(),
      tableMetadata.getDatasetConfig(), createTableEntry,
      manifestWriterPlan -> {
        try {
          return getMetadataWriterPlan(dataFileAggrPlan, manifestWriterPlan);
        } catch (InvalidRelException e) {
          throw new RuntimeException(e);
        }
      });
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
    // we only keep rows from DMLed files (i.e.,  with OperationType.DELETE_DATAFILE) since the final rowcount will be DMLed rows
    RelDataTypeField operationTypeField  = writerPrel.getRowType()
      .getField(RecordWriter.OPERATION_TYPE.getName(), false, false);
    RexNode deleteDataFileLiteral = rexBuilder.makeLiteral(OperationType.DELETE_DATAFILE.value, cluster.getTypeFactory().createSqlType(SqlTypeName.INTEGER), true);

    RexNode filterCondition = rexBuilder.makeCall(
      SqlStdOperatorTable.EQUALS,
      rexBuilder.makeInputRef(writerPrel, operationTypeField.getIndex()),
      deleteDataFileLiteral);

    FilterPrel filterPrel = FilterPrel.create(
      writerPrel.getCluster(),
      writerPrel.getTraitSet(),
      writerPrel,
      filterCondition);

    // Agg: get the total row count for DMLed results
    RelDataTypeField recordsField  = writerPrel.getRowType()
      .getField(RecordWriter.RECORDS.getName(), false, false);
    AggregateCall aggRowCount = AggregateCall.create(
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

    StreamAggPrel rowCountAgg = StreamAggPrel.create(
      filterPrel.getCluster(),
      filterPrel.getTraitSet(),
      filterPrel,
      ImmutableBitSet.of(),
      ImmutableList.of(),
      ImmutableList.of(aggRowCount),
      null);

    // Project: return 0 as row count in case there is no Agg record (i.e., no DMLed results)
    recordsField  = rowCountAgg.getRowType()
      .getField(RecordWriter.RECORDS.getName(), false, false);
    List<String> projectNames = ImmutableList.of(recordsField.getName());
    RexNode zeroLiteral = rexBuilder.makeLiteral(0, rowCountAgg.getCluster().getTypeFactory().createSqlType(SqlTypeName.INTEGER), true);
    // check if the count of row count records is 0 (i.e., records column is null)
    RexNode rowCountRecordExistsCheckCondition = rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL,
      rexBuilder.makeInputRef(recordsField.getType(), recordsField.getIndex()));
    // case when the count of row count records is 0, return 0, else return aggregated row count
    RexNode projectExpr = rexBuilder.makeCall(SqlStdOperatorTable.CASE,
      rowCountRecordExistsCheckCondition, zeroLiteral,
      rexBuilder.makeInputRef(recordsField.getType(), recordsField.getIndex()));
    List<RexNode> projectExprs = ImmutableList.of(projectExpr);

    RelDataType projectRowType = RexUtil.createStructType(rowCountAgg.getCluster().getTypeFactory(), projectExprs,
      projectNames, null);

    return ProjectPrel.create(
      rowCountAgg.getCluster(),
      rowCountAgg.getTraitSet(),
      rowCountAgg,
      projectExprs,
      projectRowType);
  }

  private Prel getMetadataWriterPlan(RelNode dataFileAggrPlan, RelNode manifestWriterPlan) throws InvalidRelException {
    ImmutableList<SchemaPath> projectedCols = RecordWriter.SCHEMA.getFields().stream()
      .map(f -> SchemaPath.getSimplePath(f.getName()))
      .collect(ImmutableList.toImmutableList());

    // Insert a table function that'll pass the path through and set the OperationType
    TableFunctionPrel deletedDataFilesTableFunctionPrel = new TableFunctionPrel(
      dataFileAggrPlan.getCluster(),
      dataFileAggrPlan.getTraitSet(),
      table,
      dataFileAggrPlan,
      tableMetadata,
      new TableFunctionConfig(
        TableFunctionConfig.FunctionType.DELETED_DATA_FILES_METADATA,
        true,
        new TableFunctionContext(RecordWriter.SCHEMA, projectedCols, true)),
      ScanRelBase.getRowTypeFromProjectedColumns(projectedCols,
        RecordWriter.SCHEMA, dataFileAggrPlan.getCluster()));

    final RelTraitSet traits = traitSet.plus(DistributionTrait.SINGLETON).plus(Prel.PHYSICAL);

    // Union the updating of the deleted data's metadata with the rest
    return new UnionAllPrel(cluster,
      traits,
      ImmutableList.of(manifestWriterPlan,
        new UnionExchangePrel(cluster, traits,
          deletedDataFilesTableFunctionPrel)),
      false);
  }

  /**
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

    DistributionTrait leftDistributionTrait = getHashDistributionTraitForFields(leftDataFileScan.getRowType(),
      ImmutableList.of(ColumnUtils.FILE_PATH_COLUMN_NAME, ColumnUtils.ROW_INDEX_COLUMN_NAME));
    RelTraitSet leftTraitSet = cluster.getPlanner().emptyTraitSet().plus(Prel.PHYSICAL)
      .plus(leftDistributionTrait);
    HashToRandomExchangePrel leftDataFileScanHashExchange = new HashToRandomExchangePrel(cluster, leftTraitSet,
      leftDataFileScan, leftDistributionTrait.getFields());

    // right side: DMLed data
    DistributionTrait rightDistributionTrait = getHashDistributionTraitForFields(input.getRowType(),
      ImmutableList.of(ColumnUtils.FILE_PATH_COLUMN_NAME, ColumnUtils.ROW_INDEX_COLUMN_NAME));
    RelTraitSet rightTraitSet = cluster.getPlanner().emptyTraitSet().plus(Prel.PHYSICAL)
      .plus(rightDistributionTrait);
    HashToRandomExchangePrel rightInputHashExchange = new HashToRandomExchangePrel(cluster, rightTraitSet,
      input, rightDistributionTrait.getFields());

    // hash join on __FilePath == __FilePath AND __RowIndex == __RowIndex
    RelDataTypeField leftFilePathField = leftDataFileScanHashExchange.getRowType()
      .getField(ColumnUtils.FILE_PATH_COLUMN_NAME, false, false);
    RelDataTypeField leftRowIndexField = leftDataFileScanHashExchange.getRowType()
      .getField(ColumnUtils.ROW_INDEX_COLUMN_NAME, false, false);
    RelDataTypeField rightFilePathField = rightInputHashExchange.getRowType()
      .getField(ColumnUtils.FILE_PATH_COLUMN_NAME, false, false);
    RelDataTypeField rightRowIndexField = rightInputHashExchange.getRowType()
      .getField(ColumnUtils.ROW_INDEX_COLUMN_NAME, false, false);

    int leftFieldCount = leftDataFileScanHashExchange.getRowType().getFieldCount();
    RexBuilder rexBuilder = cluster.getRexBuilder();
    RexNode joinCondition = rexBuilder.makeCall(
      SqlStdOperatorTable.AND,
      rexBuilder.makeCall(
        SqlStdOperatorTable.EQUALS,
        rexBuilder.makeInputRef(leftFilePathField.getType(), leftFilePathField.getIndex()),
        rexBuilder.makeInputRef(rightFilePathField.getType(), leftFieldCount + rightFilePathField.getIndex())),
      rexBuilder.makeCall(
        SqlStdOperatorTable.EQUALS,
        rexBuilder.makeInputRef(leftRowIndexField.getType(), leftRowIndexField.getIndex()),
        rexBuilder.makeInputRef(rightRowIndexField.getType(), leftFieldCount + rightRowIndexField.getIndex())));

    return HashJoinPrel.create(
      leftDataFileScanHashExchange.getCluster(),
      leftDataFileScanHashExchange.getTraitSet(),
      leftDataFileScanHashExchange,
      rightInputHashExchange,
      joinCondition,
      null,
      getCopyOnWriteJoinType());
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
    RelDataTypeField leftRowIndexField = joinPlan.getInput(0).getRowType()
      .getField(ColumnUtils.ROW_INDEX_COLUMN_NAME, false, false);
    RelDataTypeField rightRowIndexField = joinPlan.getInput(1).getRowType()
      .getField(ColumnUtils.ROW_INDEX_COLUMN_NAME, false, false);

    // Add Delete filter: FilterPrel (right.__RowIndex IS NULL)
    if (operation == DELETE) {
      currentPrel = addColumnIsNullFilter(joinPlan, rightRowIndexField.getType(),
        leftFieldCount + rightRowIndexField.getIndex());
    }

    if (operation == MERGE && (mergeType == MergeType.UPDATE_ONLY || mergeType == MergeType.UPDATE_INSERT)) {
      // Adds a table function to look for multiple matching rows, currently only happening for MERGE with
      // UPDATEs. Throws an exception when a row to update matches multiple times.
      currentPrel = new IcebergDmlMergeDuplicateCheckPrel(currentPrel, table, tableMetadata);
    }

    // project (tablecol1, ..., tablecolN)
    List<RelDataTypeField> projectFields = getFieldsWithoutSystemColumns(joinPlan.getInput(0));
    List<String> projectNames = projectFields.stream().map(RelDataTypeField::getName).collect(Collectors.toList());
    //  the condition for checking if right_row_index column is null
    RexNode rightRowIndexNullCheck = rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL,
      rexBuilder.makeInputRef(rightRowIndexField.getType(), leftFieldCount + rightRowIndexField.getIndex()));

    //  the condition for checking if left_row_index column is null
    RexNode leftRowIndexFieldNullCheck = rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL,
      rexBuilder.makeInputRef(leftRowIndexField.getType(), leftRowIndexField.getIndex()));

    List<RexNode> projectExprs = new ArrayList<>();
    for (RelDataTypeField field : projectFields) {
      RexNode projectExpr = getCopyOnWriteJoinColumnProjectExpr(
        rexBuilder, field, leftFieldCount, leftRowIndexFieldNullCheck, rightRowIndexNullCheck);
      projectExprs.add(projectExpr);
    }

    RelDataType projectRowType = RexUtil.createStructType(joinPlan.getCluster().getTypeFactory(), projectExprs,
      projectNames, null);

    return ProjectPrel.create(
      currentPrel.getCluster(),
      currentPrel.getTraitSet(),
      currentPrel,
      projectExprs,
      projectRowType);
  }

  /**
   *  if right_row_index column is null (i.e., the value is not updated),
   *       use the column value from left side (original value)
   *   else (i.e., the value is updated)
   *       use the right side updated value
   *
   */
  private RexNode getUpdateExpr(RexBuilder rexBuilder, RelDataTypeField field, int leftFieldCount, int insertedFieldCount, RexNode rightRowIndexNullcondition) {
    Integer updateColumnIndex = updateColumnsWithIndex.get(field.getName());

    // the value is not updated, use the column value from left side (original value)
    if (updateColumnIndex == null) {
      return rexBuilder.makeInputRef(field.getType(), field.getIndex());
    }

    // value is updated, use the right side updated value.
    return rexBuilder.makeCall(SqlStdOperatorTable.CASE, rightRowIndexNullcondition,
      rexBuilder.makeInputRef(field.getType(), field.getIndex()),
      rexBuilder.makeInputRef(field.getType(), leftFieldCount + insertedFieldCount + SYSTEM_COLUMN_COUNT + updateColumnIndex));
  }

  /**
   *   if left_row_index column is null (i.e., the row is inserted),
   *       use inserted value
   *   else
   *       use update expression:
   *                        if right_row_index column is null (i.e., the value is not updated),
   *                                use original value
   *                        else (i.e., the value is updated)
   *                                use updated value
   *
   */
  private RexNode getMergeWithInsertExpr(RexBuilder rexBuilder, RelDataTypeField field, int leftFieldCount,
                                         RexNode leftRowIndexFieldNullCheck, RexNode updateExpr) {
    return rexBuilder.makeCall(SqlStdOperatorTable.CASE, leftRowIndexFieldNullCheck,
      rexBuilder.makeInputRef(field.getType(), leftFieldCount + field.getIndex()),
      updateExpr);
  }

  private RexNode getCopyOnWriteJoinColumnProjectExpr(
    RexBuilder rexBuilder, RelDataTypeField field, int leftFieldCount,
    RexNode leftRowIndexNullCheck, RexNode rightRowIndexNullCheck) {
    switch (operation) {
      case DELETE:
        return rexBuilder.makeInputRef(field.getType(), field.getIndex());
      case UPDATE:
        return getUpdateExpr(rexBuilder, field, leftFieldCount, 0, rightRowIndexNullCheck);
      case MERGE:
        switch(mergeType) {
          case UPDATE_ONLY:
            return getUpdateExpr(rexBuilder, field, leftFieldCount, 0, rightRowIndexNullCheck);
          case UPDATE_INSERT:
            RexNode updateExprWithInsertedColumns = getUpdateExpr(rexBuilder, field, leftFieldCount,
              leftFieldCount - SYSTEM_COLUMN_COUNT, rightRowIndexNullCheck);
            return getMergeWithInsertExpr(rexBuilder, field, leftFieldCount, leftRowIndexNullCheck, updateExprWithInsertedColumns);
          default:
            throw new UnsupportedOperationException(String.format("Unrecoverable Error: Invalid type: %s", mergeType));
        }
      default:
        throw new UnsupportedOperationException(String.format("Unrecoverable Error: Invalid type: %s", operation));
    }
  }

  /**
   * @return the field list without system columns, filtered by target table fields
   */
  private List<RelDataTypeField> getFieldsWithoutSystemColumns(RelNode input) {
    return input.getRowType().getFieldList().stream()
      .filter(f -> table.getRowType().getField(f.getName(), false, false) != null && !isSystemColumn(f.getName()))
      .collect(Collectors.toList());
  }

  private DistributionTrait getHashDistributionTraitForFields(RelDataType rowType, List<String> columnNames) {
    ImmutableList<DistributionTrait.DistributionField> fields = columnNames.stream()
      .map(n -> new DistributionTrait.DistributionField(
        Preconditions.checkNotNull(rowType.getField(n, false, false)).getIndex()))
      .collect(ImmutableList.toImmutableList());
    return new DistributionTrait(DistributionTrait.DistributionType.HASH_DISTRIBUTED, fields);
  }

  /**
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
    List<SchemaPath> allColumns = table.getRowType().getFieldNames().stream()
        .map(SchemaPath::getSimplePath).collect(Collectors.toList());
    IcebergScanPlanBuilder builder = new IcebergScanPlanBuilder(
        cluster,
        traitSet,
        table,
        tableMetadata,
        allColumns,
        context);

    return builder.buildWithDmlDataFileFiltering(dataFileListInput);
  }

  /**
   *    HashAggPrel groupby(__FilePath)
   *        |
   *        |
   *    HashToRandomExchangePrel
   *        |
   *        |
   *    (query results input with __FilePath)
   */
  private Prel getDataFileAggPlan(RelNode aggInput) throws InvalidRelException {
    RelDataTypeField filePathField = Preconditions.checkNotNull(aggInput.getRowType()
      .getField(ColumnUtils.FILE_PATH_COLUMN_NAME, false, false));

    // hash exchange on __FilePath
    DistributionTrait.DistributionField distributionField =
      new DistributionTrait.DistributionField(filePathField.getIndex());
    DistributionTrait distributionTrait = new DistributionTrait(DistributionTrait.DistributionType.HASH_DISTRIBUTED,
      ImmutableList.of(distributionField));
    RelTraitSet relTraitSet = cluster.getPlanner().emptyTraitSet().plus(Prel.PHYSICAL).plus(distributionTrait);
    HashToRandomExchangePrel filePathHashExchPrel = new HashToRandomExchangePrel(cluster, relTraitSet,
      aggInput, distributionTrait.getFields());

    // hash agg groupby(__FilePath)
    ImmutableBitSet groupSet = ImmutableBitSet.of(filePathField.getIndex());

    // get dmled row count
    AggregateCall aggRowCount = AggregateCall.create(
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
}
