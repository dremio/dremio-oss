/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import java.io.IOException;
import java.util.Iterator;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

import com.dremio.exec.ExecConstants;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.config.ExternalSort;
import com.dremio.exec.planner.common.SortRelBase;
import com.dremio.exec.planner.cost.DremioCost;
import com.dremio.exec.planner.cost.DremioCost.Factory;
import com.dremio.exec.planner.physical.visitor.PrelVisitor;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators.BooleanValidator;
import com.dremio.options.TypeValidators.DoubleValidator;
import com.dremio.options.TypeValidators.LongValidator;
import com.dremio.options.TypeValidators.PositiveLongValidator;
import com.dremio.options.TypeValidators.RangeDoubleValidator;

@Options
public class SortPrel extends SortRelBase implements Prel {

  public static final LongValidator RESERVE = new PositiveLongValidator("planner.op.sort.reserve_bytes", Long.MAX_VALUE, 20_000_000);
  public static final LongValidator LIMIT = new PositiveLongValidator("planner.op.sort.limit_bytes", Long.MAX_VALUE, DEFAULT_LIMIT);
  public static final DoubleValidator FACTOR = new RangeDoubleValidator("planner.op.sort.factor", 0.0, 1000.0, 1.0d);
  public static final BooleanValidator BOUNDED = new BooleanValidator("planner.op.sort.bounded", true);

  /** Creates a SortRel with offset and fetch. */
  private SortPrel(RelOptCluster cluster, RelTraitSet traits, RelNode input, RelCollation collation, RexNode offset, RexNode fetch) {
    super(cluster, traits, input, collation, offset, fetch);
  }

  public static SortPrel create(RelOptCluster cluster, RelTraitSet traits, RelNode input, RelCollation collation) {
    return create(cluster, traits, input, collation, null, null);
  }

  public static SortPrel create(RelOptCluster cluster, RelTraitSet traits, RelNode input, RelCollation collation, RexNode offset, RexNode fetch) {
    final RelTraitSet adjustedTraits = adjustTraits(traits, collation);
    // TODO: should distribution be adjusted too to match input?
    return new SortPrel(cluster, adjustedTraits, input, collation, offset, fetch);
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery relMetadataQuery) {
    if(PrelUtil.getSettings(getCluster()).useDefaultCosting()) {
      //We use multiplier 0.05 for TopN operator, and 0.1 for Sort, to make TopN a preferred choice.
      return super.computeSelfCost(planner).multiplyBy(.1);
    }

    RelNode child = this.getInput();
    double inputRows = relMetadataQuery.getRowCount(child);
    // int  rowWidth = child.getRowType().getPrecision();
    int numSortFields = this.collation.getFieldCollations().size();
    double cpuCost = DremioCost.COMPARE_CPU_COST * numSortFields * inputRows * (Math.log(inputRows)/Math.log(2));
    double diskIOCost = 0; // assume in-memory for now until we enforce operator-level memory constraints

    // TODO: use rowWidth instead of avgFieldWidth * numFields
    // avgFieldWidth * numFields * inputRows
    double numFields = this.getRowType().getFieldCount();
    long fieldWidth = PrelUtil.getPlannerSettings(planner).getOptions()
      .getOption(ExecConstants.AVERAGE_FIELD_WIDTH_KEY).getNumVal();

    double memCost = fieldWidth * numFields * inputRows;

    Factory costFactory = (Factory) planner.getCostFactory();
    return costFactory.makeCost(inputRows, cpuCost, diskIOCost, 0, memCost);
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    Prel child = (Prel) this.getInput();

    PhysicalOperator childPOP = child.getPhysicalOperator(creator);
    return new ExternalSort(
        creator.props(this, null, childPOP.getProps().getSchema(), RESERVE, LIMIT)
          .cloneWithBound(creator.getOptionManager().getOption(BOUNDED))
          .cloneWithMemoryFactor(creator.getOptionManager().getOption(FACTOR))
          .cloneWithMemoryExpensive(true)
        ,
        childPOP,
        PrelUtil.getOrdering(this.collation, getInput().getRowType()),
        false
        );
  }

  @Override
  public SortPrel copy(
      RelTraitSet traitSet,
      RelNode newInput,
      RelCollation newCollation,
      RexNode offset,
      RexNode fetch) {
    return SortPrel.create(getCluster(), traitSet, newInput, newCollation);
  }

  @Override
  public Iterator<Prel> iterator() {
    return PrelUtil.iter(getInput());
  }

  @Override
  public <T, X, E extends Throwable> T accept(PrelVisitor<T, X, E> logicalVisitor, X value) throws E {
    return logicalVisitor.visitPrel(this, value);
  }

  @Override
  public SelectionVectorMode[] getSupportedEncodings() {
    return SelectionVectorMode.NONE_AND_TWO;
  }

  @Override
  public SelectionVectorMode getEncoding() {
    return SelectionVectorMode.NONE;
  }

  @Override
  public boolean needsFinalColumnReordering() {
    return true;
  }

}
