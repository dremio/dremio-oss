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
package com.dremio.exec.planner.logical;

import com.dremio.exec.planner.common.TableModifyRelBase;
import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare.CatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.iceberg.RowLevelOperationMode;

/**
 * Relational expression that modifies a table in Dremio.
 *
 * <p>It is similar to TableScan, but represents a request to modify a table rather than read from
 * it. It takes one child which produces the modified rows. Those rows are: for DELETE, the old
 * values; for UPDATE, all old values plus updated new values; for MERGE, all old values plus
 * updated new values and new values.
 */
public class TableModifyRel extends TableModifyRelBase implements Rel {

  protected TableModifyRel(
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
        LOGICAL,
        cluster,
        traitSet,
        table,
        catalogReader,
        input,
        operation,
        updateColumnList,
        sourceExpressionList,
        flattened,
        createTableEntry,
        mergeUpdateColumnList,
        hasSource,
        outdatedTargetColumns,
        dmlWriteMode);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new TableModifyRel(
        getCluster(),
        traitSet,
        getTable(),
        getCatalogReader(),
        sole(inputs),
        getOperation(),
        getUpdateColumnList(),
        getSourceExpressionList(),
        isFlattened(),
        getCreateTableEntry(),
        getMergeUpdateColumnList(),
        hasSource(),
        getOutdatedTargetColumns(),
        getDmlWriteMode());
  }
}
