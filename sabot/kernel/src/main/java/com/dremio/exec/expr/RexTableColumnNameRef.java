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
package com.dremio.exec.expr;

import com.dremio.common.utils.PathUtils;
import com.dremio.exec.planner.physical.visitor.QueryProfileProcessor;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexTableInputRef;

/**
 * Variable which wraps a tableInputRef and stores the table.columnName in its digest.
 *
 * <p>This object is used by {@link QueryProfileProcessor} to show the entire columnName of the
 * referenced InputRef
 *
 * <p>Note that this kind of {@link RexNode} is an auxiliary data structure with a very specific
 * purpose and should not be used in relational expressions.
 */
public class RexTableColumnNameRef extends RexInputRef {

  private final RexTableInputRef tableInputRef;

  public RexTableColumnNameRef(RexTableInputRef tableRef) {
    super(tableRef.getIndex(), tableRef.getType());
    this.tableInputRef = tableRef;
    RelOptTable table = tableRef.getTableRef().getTable();
    this.digest =
        PathUtils.constructFullPath(table.getQualifiedName())
            + "."
            + table.getRowType().getFieldList().get(index).getName();
  }

  // ~ Methods ----------------------------------------------------------------

  @Override
  public boolean equals(Object obj) {
    return this == obj
        || obj instanceof RexTableInputRef
            && tableInputRef.equals(((RexTableInputRef) obj))
            && index == ((RexTableInputRef) obj).getIndex();
  }

  @Override
  public String toString() {
    return digest;
  }

  @Override
  public int hashCode() {
    return digest.hashCode();
  }
}
