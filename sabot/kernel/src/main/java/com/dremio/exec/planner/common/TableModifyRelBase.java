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
package com.dremio.exec.planner.common;

import static com.dremio.exec.ExecConstants.ENABLE_DML_DISPLAY_RESULT_ONLY;
import static com.dremio.exec.util.ColumnUtils.FILE_PATH_COLUMN_NAME;
import static com.dremio.exec.util.ColumnUtils.ROW_INDEX_COLUMN_NAME;

import com.dremio.exec.catalog.DremioPrepareTable;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.exec.planner.sql.parser.DmlUtils;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare.CatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactory.Builder;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.iceberg.RowLevelOperationMode;

/**
 * Base class of relational expression that modifies a table in Dremio.
 *
 * <p>It is similar to TableScan, but represents a request to modify a table rather than read from
 * it. It takes one child which produces the modified rows. Those rows are: for DELETE, the old
 * values; for UPDATE, all old values plus updated new values; for MERGE, all old values plus
 * updated new values and new values.
 */
public class TableModifyRelBase extends TableModify {

  private final CreateTableEntry createTableEntry;
  private final RelDataType expectedInputRowType;

  // Workaround for a Calcite issue:
  // the constructor of TableModify has a bug expecting updateColumnList as null in non-update
  // cases,
  // which breaks the update clause in Merge statement.
  // CALCITE-3921 fixed this issue. https://dremio.atlassian.net/browse/DX-46901
  private final List<String> mergeUpdateColumnList;

  // If the TableModify operation has a source
  private final boolean hasSource;

  private final RowLevelOperationMode dmlWriteMode;

  protected TableModifyRelBase(
      Convention convention,
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelOptTable table,
      CatalogReader catalogReader,
      RelNode input,
      Operation operation,
      List<String> updateColumnList,
      List<RexNode> sourceExpressionList,
      boolean flattened,
      CreateTableEntry createTableEntry,
      List<String> mergeUpdateColumnList,
      boolean hasSource,
      RowLevelOperationMode dmlWriteMode) {
    super(
        cluster,
        traitSet,
        table,
        catalogReader,
        input,
        operation,
        updateColumnList,
        sourceExpressionList,
        flattened);
    assert getConvention() == convention;
    this.dmlWriteMode = dmlWriteMode;
    this.rowType = evaluateOutputRowType();
    this.mergeUpdateColumnList = mergeUpdateColumnList;
    this.hasSource = hasSource;
    this.createTableEntry = createTableEntry;
    this.expectedInputRowType =
        evaluateExpectedInputRowType(updateColumnList, mergeUpdateColumnList);
  }

  /**
   * Evaluate the expected input row type using the data types from the table.
   *
   * <p>If COPY_ON_WRITE: `input`'s structure is as following: <br>
   * - INSERT columns (for MERGE with INSERT) <br>
   * - SYSTEM columns <br>
   * - UPDATE columns (for MERGE and UPDATE, only for the updated columns)
   *
   * <p>If MERGE_ON_READ: `input`'s structure is as following: <br>
   * - Original TARGET columns (Except for Delete) <br>
   * - SYSTEM columns <br>
   * - INSERT Source Columns (MERGE_INSERT & MERGE_INSERT_UPDATE Only) <br>
   * - UPDATE columns (for MERGE and UPDATE, only for the updated columns) Otherwise, we could have
   * `
   *
   * <p>table`'s structure is as following: <br>
   * - TABLE's columns <br>
   * - SYSTEM columns
   */
  private RelDataType evaluateExpectedInputRowType(
      List<String> updateColumnList, List<String> mergeUpdateColumnList) {
    final RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
    final RelDataType tableRowType = table.getRowType();
    final int systemColumnCount =
        tableRowType.getFieldCount() - table.unwrap(DremioTable.class).getExtendedColumnOffset();
    final RelDataType inputRowType = input.getRowType();
    final RelDataTypeFactory.Builder expectedInputRowTypeBuilder = typeFactory.builder();

    final List<String> updateColumns =
        isDelete()
            ? Collections.emptyList()
            : (isUpdate() ? updateColumnList : mergeUpdateColumnList);
    if (dmlWriteMode == RowLevelOperationMode.MERGE_ON_READ) {
      evaluateMergeOnReadExpectedInputType(
          tableRowType, inputRowType, updateColumns, expectedInputRowTypeBuilder);
    } else {
      evaluateCopyOnWriteExpectedInputType(
          tableRowType,
          systemColumnCount,
          inputRowType,
          updateColumns,
          expectedInputRowTypeBuilder);
    }

    RelDataType expectedInputRowType = expectedInputRowTypeBuilder.build();

    if (isFlattened()) {
      expectedInputRowType = SqlTypeUtil.flattenRecordType(typeFactory, expectedInputRowType, null);
    }

    return expectedInputRowType;
  }

  /** Handles the input cols for Dml Merge On Read Case */
  private void evaluateMergeOnReadExpectedInputType(
      RelDataType tableRowType,
      RelDataType inputRowType,
      List<String> updateColumns,
      RelDataTypeFactory.Builder expectedInputRowTypeBuilder) {

    int totalTargetColCount = tableRowType.getFieldCount() - DmlUtils.SYSTEM_COLUMN_COUNT;

    DremioPrepareTable dremioPrepareTable = (DremioPrepareTable) this.getTable();
    List<String> partitionColumns =
        dremioPrepareTable
            .getTable()
            .getDatasetConfig()
            .getReadDefinition()
            .getPartitionColumnsList();

    // if Delete, only get systemCols
    if (isDelete()) {

      if (null != partitionColumns && !partitionColumns.isEmpty()) {
        getExpectedDataColumnsForMergeOnReadDelete(inputRowType, expectedInputRowTypeBuilder);
      }
      getExpectedMergeOnReadSystemCols(tableRowType, expectedInputRowTypeBuilder);

    } else {
      // if update or merge, acquire target cols (if present), sys cols ...
      // ... source insert cols (if present), update cols (if present)
      final int targetAndSystemColumnCount = tableRowType.getFieldCount();

      Set<String> outdatedTargetColumnNames =
          DmlUtils.getOutdatedTargetColumns(new HashSet<>(updateColumns), table, partitionColumns);

      // Get target (original) cols, if present.
      // Insert-Only Case does not contain any original target cols.
      getExpectedMergeOnReadTargetCols(
          tableRowType,
          outdatedTargetColumnNames,
          totalTargetColCount,
          expectedInputRowTypeBuilder);

      // Get system cols
      getExpectedMergeOnReadSystemCols(tableRowType, expectedInputRowTypeBuilder);

      // Get Insert Columns, if present.
      getExpectedMergeOnReadInsertCols(
          targetAndSystemColumnCount,
          outdatedTargetColumnNames.size(),
          inputRowType,
          updateColumns,
          expectedInputRowTypeBuilder);

      // Get Update Columns, if present.
      getExpectedMergeOnReadUpdateCols(
          inputRowType, tableRowType, updateColumns, expectedInputRowTypeBuilder);
    }
  }

  private void getExpectedDataColumnsForMergeOnReadDelete(
      RelDataType inputRowType, Builder expectedInputRowTypeBuilder) {
    for (int i = 0; i < inputRowType.getFieldCount() - DmlUtils.SYSTEM_COLUMN_COUNT; i++) {
      RelDataTypeField field = inputRowType.getFieldList().get(i);
      expectedInputRowTypeBuilder.add(field);
    }
  }

  private void getMergeOnReadDeletePartitionColumns(
      RelDataType inputRowType,
      Builder expectedInputRowTypeBuilder,
      List<String> partitionColumns) {
    if (inputRowType.getFieldCount() > DmlUtils.SYSTEM_COLUMN_COUNT) {
      assert partitionColumns.size() == inputRowType.getFieldCount() - DmlUtils.SYSTEM_COLUMN_COUNT;
      for (int i = 0; i < inputRowType.getFieldCount() - DmlUtils.SYSTEM_COLUMN_COUNT; i++) {
        RelDataTypeField field = inputRowType.getFieldList().get(i);
        if (partitionColumns.contains(field.getName())) {
          expectedInputRowTypeBuilder.add(field);
        }
      }
    }
  }

  /**
   * if input cols > table cols + update cols? then there's insert cols... These will appear after
   * the system cols. <br>
   * Note: target columns may be excluded if outdated. This is for DML performance/memory benefits
   */
  private void getExpectedMergeOnReadTargetCols(
      RelDataType tableRowType,
      Set<String> outdatedTargetColumnNames,
      int totalTargetColCount,
      RelDataTypeFactory.Builder expectedInputRowTypeBuilder) {

    for (int i = 0; i < totalTargetColCount; i++) {
      if (outdatedTargetColumnNames.contains(tableRowType.getFieldList().get(i).getName())) {
        continue;
      }
      expectedInputRowTypeBuilder.add(table.getRowType().getFieldList().get(i));
    }
  }

  /**
   * System cols are placed within tableRowType (target table) System Cols appear at end of the
   * tableRowType (target Table).
   */
  private void getExpectedMergeOnReadSystemCols(
      RelDataType tableRowType, RelDataTypeFactory.Builder expectedInputRowTypeBuilder) {

    // Get System Cols
    expectedInputRowTypeBuilder.add(tableRowType.getField(FILE_PATH_COLUMN_NAME, false, false));
    expectedInputRowTypeBuilder.add(tableRowType.getField(ROW_INDEX_COLUMN_NAME, false, false));
  }

  /** get insert cols, if present */
  private void getExpectedMergeOnReadInsertCols(
      int targetAndSystemColumnCount,
      int totalOutdatedTargetColCount,
      RelDataType inputRowType,
      List<String> updateColumns,
      RelDataTypeFactory.Builder expectedInputRowTypeBuilder) {

    // if insert call exists, get total number of source columns.
    // (source column count is same as the total target cols (sys cols not included))
    int insertColCount =
        inputRowType.getFieldCount()
            + totalOutdatedTargetColCount
            - table.getRowType().getFieldList().size()
            - updateColumns.size();

    for (int i = 0; i < insertColCount; i++) {
      expectedInputRowTypeBuilder.add(table.getRowType().getFieldList().get(i));
    }
  }

  /** get update cols, if present */
  private void getExpectedMergeOnReadUpdateCols(
      RelDataType inputRowType,
      RelDataType tableRowType,
      List<String> updateColumns,
      RelDataTypeFactory.Builder expectedInputRowTypeBuilder) {

    for (int i = 0; i < updateColumns.size(); i++) {
      expectedInputRowTypeBuilder.add(
          inputRowType
              .getFieldList()
              .get(inputRowType.getFieldCount() - updateColumns.size() + i)
              .getName(),
          tableRowType.getField(updateColumns.get(i), true, false).getType());
    }
  }

  /** Handles the input cols for Dml Merge On Read Case */
  private void evaluateCopyOnWriteExpectedInputType(
      RelDataType tableRowType,
      int systemColumnCount,
      RelDataType inputRowType,
      List<String> updateColumns,
      RelDataTypeFactory.Builder expectedInputRowTypeBuilder) {
    final int insertColumnCount =
        inputRowType.getFieldCount() - systemColumnCount - updateColumns.size();
    // Check if there are INSERT columns (for MERGE). For UPDATE and DELETE, insertColumnCount is
    // always 0.
    for (int i = 0; i < insertColumnCount; i++) {
      // If there are INSERT columns, all the target table columns will be present.
      expectedInputRowTypeBuilder.add(table.getRowType().getFieldList().get(i));
    }

    // Add the types for the system columns.
    final List<RelDataTypeField> sourceRowTypeFields = inputRowType.getFieldList();
    for (int i = 0; i < systemColumnCount; i++) {
      expectedInputRowTypeBuilder.add(sourceRowTypeFields.get(i + insertColumnCount));
    }

    // Add the UPDATE columns, if present.
    for (int i = 0; i < inputRowType.getFieldCount() - insertColumnCount - systemColumnCount; i++) {
      expectedInputRowTypeBuilder.add(
          sourceRowTypeFields.get(i + insertColumnCount + systemColumnCount).getName(),
          tableRowType.getField(updateColumns.get(i), true, false).getType());
    }
  }

  private RelDataType evaluateOutputRowType() {

    boolean displayResultOnly =
        PrelUtil.getPlannerSettings(getCluster())
            .getOptions()
            .getOption(ENABLE_DML_DISPLAY_RESULT_ONLY);
    if (displayResultOnly) {
      RelDataTypeFactory.FieldInfoBuilder outputFieldBuilder =
          getCluster().getTypeFactory().builder();
      outputFieldBuilder.addAll(
          table.getRowType().getFieldList().stream()
              .filter(
                  f ->
                      !f.getName().equalsIgnoreCase(FILE_PATH_COLUMN_NAME)
                          && !f.getName().equalsIgnoreCase(ROW_INDEX_COLUMN_NAME))
              .collect(Collectors.toList()));

      return outputFieldBuilder.build();
    }

    return DmlUtils.evaluateOutputRowType(getInput(), getCluster(), getOperation());
  }

  public CreateTableEntry getCreateTableEntry() {
    return this.createTableEntry;
  }

  public RelDataType getExpectedInputRowType() {
    return this.expectedInputRowType;
  }

  public List<String> getMergeUpdateColumnList() {
    return mergeUpdateColumnList;
  }

  public boolean hasSource() {
    return hasSource;
  }

  public RowLevelOperationMode getDmlWriteMode() {
    return dmlWriteMode;
  }
}
