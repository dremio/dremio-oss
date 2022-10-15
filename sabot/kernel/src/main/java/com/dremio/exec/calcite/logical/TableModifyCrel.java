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
package com.dremio.exec.calcite.logical;

import java.util.List;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rex.RexNode;

import com.dremio.exec.planner.common.TableModifyRelBase;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.google.common.base.Preconditions;

/**
 * Crel level relational expression that modifies a table.
 *
 * It is similar to TableScan, but represents a request to modify a table rather than read from it.
 * It takes one child which produces the modified rows. Those rows are:
 *  for DELETE, the old values;
 *  for UPDATE, all old values plus updated new values;
 *  for MERGE, all old values plus updated new values and new values.
 */
public class TableModifyCrel extends TableModifyRelBase {

  public TableModifyCrel(RelOptCluster cluster,
                         RelTraitSet traitSet,
                         RelOptTable table,
                         Prepare.CatalogReader schema,
                         RelNode input,
                         TableModify.Operation operation,
                         List<String> updateColumnList,
                         List<RexNode> sourceExpressionList,
                         boolean flattened,
                         CreateTableEntry createTableEntry,
                         List<String> mergeUpdateColumnList,
                         boolean hasSource) {
    super(Convention.NONE, cluster, traitSet, table, schema, input, operation, updateColumnList,
      sourceExpressionList, flattened, createTableEntry, mergeUpdateColumnList, hasSource);

    Preconditions.checkArgument(operation != Operation.INSERT, "Insert is not supported in TableModifyCrel");
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new TableModifyCrel(getCluster(), traitSet, getTable(), getCatalogReader(), sole(inputs), getOperation(),
      getUpdateColumnList(), getSourceExpressionList(), isFlattened(), getCreateTableEntry(), getMergeUpdateColumnList(), hasSource());
  }

  public static TableModifyCrel create(RelOptTable table,
                                       Prepare.CatalogReader schema,
                                       RelNode input,
                                       TableModify.Operation operation,
                                       List<String> updateColumnList,
                                       List<RexNode> sourceExpressionList,
                                       boolean flattened,
                                       List<String> mergeUpdateColumnList,
                                       boolean hasSource) {
    final RelOptCluster cluster = input.getCluster();
    final RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE);
    return new TableModifyCrel(cluster, traitSet, table, schema, input,
      operation, updateColumnList, sourceExpressionList, flattened, null, mergeUpdateColumnList, hasSource);
  }

  public TableModifyCrel createWith(CreateTableEntry createTableEntry) {
    return new TableModifyCrel(getCluster(), getTraitSet(), getTable(), getCatalogReader(), sole(getInputs()),
      getOperation(), getUpdateColumnList(), getSourceExpressionList(), isFlattened(), createTableEntry, getMergeUpdateColumnList(), hasSource());
  }

  public TableModifyCrel createWith(RelNode input) {
    return new TableModifyCrel(getCluster(), getTraitSet(), getTable(), getCatalogReader(), input,
      getOperation(), getUpdateColumnList(), getSourceExpressionList(), isFlattened(), getCreateTableEntry(), getMergeUpdateColumnList(), hasSource());
  }
}
