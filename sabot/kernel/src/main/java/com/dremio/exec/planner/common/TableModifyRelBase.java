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

  // Trims unnecessary target columns for Merge-On-Read DML.
  private final Set<String> outdatedTargetColumns;

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
      Set<String> outdatedTargetColumns,
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
    this.outdatedTargetColumns = outdatedTargetColumns;
    this.rowType = evaluateOutputRowType();
    this.expectedInputRowType =
        evaluateExpectedInputRowType(
            updateColumnList, mergeUpdateColumnList, outdatedTargetColumns);
    this.createTableEntry = createTableEntry;
    this.mergeUpdateColumnList = mergeUpdateColumnList;
    this.hasSource = hasSource;
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
      List<String> updateColumnList,
      List<String> mergeUpdateColumnList,
      Set<String> outdatedTargetColumns) {
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
          tableRowType,
          systemColumnCount,
          inputRowType,
          updateColumns,
          expectedInputRowTypeBuilder,
          outdatedTargetColumns);
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

  /**
   * Handles the input cols for Dml Merge On Read Case //TODO: Merge on Read input type shall be
   * optimized before release-ready. Expected input type should follow
   */
  private void evaluateMergeOnReadExpectedInputType(
      RelDataType tableRowType,
      int systemColumnCount,
      RelDataType inputRowType,
      List<String> updateColumns,
      RelDataTypeFactory.Builder expectedInputRowTypeBuilder,
      Set<String> mergeOpDuplicateColumns) {

    // if Delete, only get systemCols
    if (isDelete()) {

      // System cols are placed within tableRowType (target table)
      // System Cols appear at the end of the target Table. System Cols Index = total target cols -
      // system cols.
      final int sysColStart = tableRowType.getFieldCount() - systemColumnCount;
      for (int i = sysColStart; i < tableRowType.getFieldCount(); i++) {
        expectedInputRowTypeBuilder.add(tableRowType.getFieldList().get(i));
      }
      // if update or merge, acquire target cols, sys cols, source insert cols if present, update
      // columns if present
    } else {
      final int targetAndSystemColumnCount = tableRowType.getFieldCount();

      // Get Target Columns & System Columns. These fields must always exist for merge / update
      // If update operation, all column names in the updateColumn list can discard the target
      Set<String> outdatedTargetColumns =
          (isUpdate()) ? new HashSet<>(updateColumns) : mergeOpDuplicateColumns;

      for (int i = 0; i < targetAndSystemColumnCount; i++) {
        if (outdatedTargetColumns.contains(table.getRowType().getFieldList().get(i).getName())) {
          continue;
        }
        expectedInputRowTypeBuilder.add(table.getRowType().getFieldList().get(i));
      }

      // Get Insert Columns, if present.
      // is input cols > table cols + update cols? then there's insert cols... These will appear
      // after the system cols.
      // need the number of insert cols (total - (target cols + sys cls) - updatecols.
      int insertColCount =
          inputRowType.getFieldCount()
              + outdatedTargetColumns.size()
              - table.getRowType().getFieldList().size()
              - updateColumns.size();
      for (int i = 0; i < insertColCount; i++) {
        expectedInputRowTypeBuilder.add(
            inputRowType
                .getFieldList()
                .get(i + targetAndSystemColumnCount - outdatedTargetColumns.size()));
      }

      // Get Update Columns, if present
      for (int i = 0; i < updateColumns.size(); i++) {
        expectedInputRowTypeBuilder.add(
            (inputRowType
                .getFieldList()
                .get(inputRowType.getFieldCount() - updateColumns.size() + i)));
      }
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

  public Set<String> getOutdatedTargetColumns() {
    return outdatedTargetColumns;
  }

  public RowLevelOperationMode getDmlWriteMode() {
    return dmlWriteMode;
  }
}
