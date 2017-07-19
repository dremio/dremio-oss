/*
 * Copyright (C) 2017 Dremio Corporation
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

import java.util.List;

import com.dremio.common.logical.data.LogicalOperator;
import com.dremio.common.logical.data.Writer;
import com.dremio.exec.planner.common.WriterRelBase;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;

public class WriterRel extends WriterRelBase implements Rel {

  private final boolean isSingleWriter;

  public WriterRel(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, CreateTableEntry createTableEntry,
      final boolean isSingleWriter) {
    super(LOGICAL, cluster, traitSet, input, createTableEntry);
    setRowType();
    this.isSingleWriter = isSingleWriter;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new WriterRel(getCluster(), traitSet, sole(inputs), getCreateTableEntry(), isSingleWriter());
  }

  @Override
  public LogicalOperator implement(LogicalPlanImplementor implementor) {
    LogicalOperator childOp = implementor.visitChild(this, 0, getInput());
    return Writer
        .builder()
        .setInput(childOp)
        .setCreateTableEntry(getCreateTableEntry())
        .build();
  }

  public boolean isSingleWriter() {
    return isSingleWriter;
  }
}
