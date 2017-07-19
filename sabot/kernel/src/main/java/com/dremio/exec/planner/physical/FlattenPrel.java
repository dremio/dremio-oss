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
package com.dremio.exec.planner.physical;

import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.config.FlattenPOP;
import com.dremio.exec.planner.logical.ParseContext;
import com.dremio.exec.planner.logical.RexToExpr;
import com.dremio.exec.record.BatchSchema;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rex.RexNode;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class FlattenPrel extends SinglePrel implements Prel {

  RexNode toFlatten;

  public FlattenPrel(RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode toFlatten) {
    super(cluster, traits, child);
    this.toFlatten = toFlatten;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new FlattenPrel(getCluster(), traitSet, sole(inputs), toFlatten);
  }

  @Override
  public Iterator<Prel> iterator() {
    return PrelUtil.iter(getInput());
  }

  @Override public double estimateRowCount(RelMetadataQuery mq) {
    // We expect for flattens output to be expanding. Use a constant to expand the data.
    return mq.getRowCount(input) * PrelUtil.getPlannerSettings(getCluster().getPlanner()).getFlattenExpansionAmount();
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    Prel child = (Prel) this.getInput();

    PhysicalOperator childPOP = child.getPhysicalOperator(creator);
    FlattenPOP f = new FlattenPOP(childPOP, (SchemaPath) getFlattenExpression(new ParseContext(PrelUtil.getSettings(getCluster()))));
    return creator.addMetadata(this, f);
  }

  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw).item("flattenField", this.toFlatten);
  }

  @Override
  public BatchSchema.SelectionVectorMode getEncoding() {
    return BatchSchema.SelectionVectorMode.NONE;
  }

  protected LogicalExpression getFlattenExpression(ParseContext context){
    return RexToExpr.toExpr(context, getInput(), toFlatten);
  }
}
