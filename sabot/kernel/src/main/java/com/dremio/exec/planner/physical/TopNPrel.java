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

import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.config.TopN;
import com.dremio.exec.planner.cost.DremioCost;
import com.dremio.exec.planner.cost.DremioCost.Factory;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators.LongValidator;
import com.dremio.options.TypeValidators.PositiveLongValidator;
import java.io.IOException;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

@Options
public class TopNPrel extends SinglePrel {

  public static final LongValidator RESERVE =
      new PositiveLongValidator("planner.op.topn.reserve_bytes", Long.MAX_VALUE, DEFAULT_RESERVE);
  public static final LongValidator LIMIT =
      new PositiveLongValidator("planner.op.topn.limit_bytes", Long.MAX_VALUE, DEFAULT_LIMIT);

  protected int limit;
  protected final RelCollation collation;

  public TopNPrel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode child,
      int limit,
      RelCollation collation) {
    super(cluster, traitSet, child);
    this.limit = limit;
    this.collation = collation;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new TopNPrel(getCluster(), traitSet, sole(inputs), this.limit, this.collation);
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    Prel child = (Prel) this.getInput();

    PhysicalOperator childPOP = child.getPhysicalOperator(creator);

    return new TopN(
        creator.props(
            this,
            null,
            childPOP.getProps().getSchema().clone(SelectionVectorMode.NONE),
            RESERVE,
            LIMIT),
        childPOP,
        limit,
        PrelUtil.getOrdering(this.collation, getInput().getRowType()),
        false);
  }

  /**
   * Cost of doing Top-N is proportional to M log N where M is the total number of input rows and N
   * is the limit for Top-N. This makes Top-N preferable to Sort since cost of full Sort is
   * proportional to M log M .
   */
  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery relMetadataQuery) {
    if (PrelUtil.getSettings(getCluster()).useDefaultCosting()) {
      // We use multiplier 0.05 for TopN operator, and 0.1 for Sort, to make TopN a preferred
      // choice.
      return super.computeSelfCost(planner, relMetadataQuery).multiplyBy(0.05);
    }
    RelNode child = this.getInput();
    double inputRows = relMetadataQuery.getRowCount(child);
    int numSortFields = this.collation.getFieldCollations().size();
    double cpuCost =
        DremioCost.COMPARE_CPU_COST * numSortFields * inputRows * (Math.log(limit) / Math.log(2));
    double diskIOCost =
        0; // assume in-memory for now until we enforce operator-level memory constraints
    Factory costFactory = (Factory) planner.getCostFactory();
    return costFactory.makeCost(inputRows, cpuCost, diskIOCost, 0);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw).item("limit", limit);
  }

  @Override
  public SelectionVectorMode[] getSupportedEncodings() {
    return SelectionVectorMode.NONE_AND_TWO;
  }

  @Override
  public SelectionVectorMode getEncoding() {
    return SelectionVectorMode.NONE;
  }
}
