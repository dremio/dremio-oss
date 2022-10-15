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

import java.util.Collections;
import java.util.List;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeUtil;

import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.planner.sql.parser.DmlUtils;

/**
 * Base class of relational expression that modifies a table in Dremio.
 *
 * It is similar to TableScan, but represents a request to modify a table rather than read from it.
 * It takes one child which produces the modified rows. Those rows are:
 *  for DELETE, the old values;
 *  for UPDATE, all old values plus updated new values;
 *  for MERGE, all old values plus updated new values and new values.
 */
public class TableModifyRelBase extends TableModify {

  private final CreateTableEntry createTableEntry;
  private final RelDataType expectedInputRowType;

  // Workaround for a Calcite issue:
  // the constructor of TableModify has a bug expecting updateColumnList as null in non-update cases,
  // which breaks the update clause in Merge statement.
  // CALCITE-3921 fixed this issue. https://dremio.atlassian.net/browse/DX-46901
  private final List<String> mergeUpdateColumnList;

  // If the TableModify operation has a source
  private final boolean hasSource;

  protected TableModifyRelBase(Convention convention,
                               RelOptCluster cluster,
                               RelTraitSet traitSet,
                               RelOptTable table,
                               Prepare.CatalogReader catalogReader,
                               RelNode input,
                               Operation operation,
                               List<String> updateColumnList,
                               List<RexNode> sourceExpressionList,
                               boolean flattened,
                               CreateTableEntry createTableEntry,
                               List<String> mergeUpdateColumnList,
                               boolean hasSource) {
    super(cluster, traitSet, table, catalogReader, input, operation, updateColumnList, sourceExpressionList, flattened);
    assert getConvention() == convention;

    this.rowType = evaluateOutputRowType();
    this.expectedInputRowType = evaluateExpectedInputRowType(updateColumnList, mergeUpdateColumnList);
    this.createTableEntry = createTableEntry;
    this.mergeUpdateColumnList = mergeUpdateColumnList;
    this.hasSource = hasSource;
  }

  /**
   * Evaluate the expected input row type using the data types from the table.
   *
   * `input`'s structure is as following:
   *  - INSERT columns (for MERGE with INSERT)
   *  - SYSTEM columns
   *  - UPDATE columns (for MERGE and UPDATE, only for the updated columns)
   *
   * `table`'s structure is as following:
   *  - TABLE's columns
   *  - SYSTEM columns
   */
  private RelDataType evaluateExpectedInputRowType(List<String> updateColumnList, List<String> mergeUpdateColumnList) {
    final RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
    final RelDataType tableRowType = table.getRowType();
    final int systemColumnCount = tableRowType.getFieldCount() - table.unwrap(DremioTable.class).getExtendedColumnOffset();
    final RelDataType inputRowType = input.getRowType();
    final RelDataTypeFactory.Builder expectedInputRowTypeBuilder = typeFactory.builder();

    final List<String> updateColumns = isDelete()
      ? Collections.emptyList()
      : (isUpdate() ? updateColumnList : mergeUpdateColumnList);
    final int insertColumnCount = inputRowType.getFieldCount() - systemColumnCount - updateColumns.size();
    // Check if there are INSERT columns (for MERGE). For UPDATE and DELETE, insertColumnCount is always 0.
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
      expectedInputRowTypeBuilder.add(sourceRowTypeFields.get(i + insertColumnCount + systemColumnCount).getName(),
        tableRowType.getField(updateColumns.get(i), true, false).getType());
    }

    RelDataType expectedInputRowType = expectedInputRowTypeBuilder.build();

    if (isFlattened()) {
      expectedInputRowType = SqlTypeUtil.flattenRecordType(typeFactory, expectedInputRowType, null);
    }

    return expectedInputRowType;
  }

  private RelDataType evaluateOutputRowType() {
    return DmlUtils.evaluateOutputRowType(getCluster(), getOperation());
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
}
