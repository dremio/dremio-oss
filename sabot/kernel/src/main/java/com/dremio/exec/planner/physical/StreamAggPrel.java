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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.ImmutableBitSet;

import com.dremio.common.logical.data.NamedExpression;
import com.dremio.exec.expr.ExpressionTreeMaterializer;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.config.StreamingAggregate;
import com.dremio.exec.planner.cost.DremioCost;
import com.dremio.exec.planner.cost.DremioCost.Factory;
import com.dremio.exec.planner.physical.visitor.PrelVisitor;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.util.AssertionUtil;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators.LongValidator;
import com.dremio.options.TypeValidators.PositiveLongValidator;

@Options
public class StreamAggPrel extends AggPrelBase implements Prel{

  public static final LongValidator RESERVE = new PositiveLongValidator("planner.op.streamingagg.reserve_bytes", Long.MAX_VALUE, DEFAULT_RESERVE);
  public static final LongValidator LIMIT = new PositiveLongValidator("planner.op.streamingagg.limit_bytes", Long.MAX_VALUE, DEFAULT_LIMIT);

  private StreamAggPrel(RelOptCluster cluster,
                       RelTraitSet traits,
                       RelNode child,
                       boolean indicator,
                       ImmutableBitSet groupSet,
                       List<ImmutableBitSet> groupSets,
                       List<AggregateCall> aggCalls,
                       OperatorPhase phase) throws InvalidRelException {
    super(cluster, traits, child, indicator, groupSet, groupSets, aggCalls, phase);
  }

  public static StreamAggPrel create(RelOptCluster cluster,
                       RelTraitSet traits,
                       RelNode child,
                       boolean indicator,
                       ImmutableBitSet groupSet,
                       List<ImmutableBitSet> groupSets,
                       List<AggregateCall> aggCalls,
                       OperatorPhase phase) throws InvalidRelException {
    final RelTraitSet adjustedTraits = AggPrelBase.adjustTraits(traits, child, groupSet)
        .replaceIf(RelCollationTraitDef.INSTANCE, () -> {
          // Validate input collation which should match groups
          if (AssertionUtil.isAssertionsEnabled()) {
            validateCollation(cluster, child, groupSet);
          }

          return collation(groupSet);
        });

    return new StreamAggPrel(cluster, adjustedTraits, child, indicator, groupSet, groupSets, aggCalls, phase);
  }

  /**
   * Creates collation based on streaming agg groupset
   *
   * @param groupSet
   * @return
   */
  public static RelCollation collation(ImmutableBitSet groupSet) {
    // The collation of a streaming agg is based on its groups
    List<RelFieldCollation> fields = IntStream.range(0, groupSet.cardinality())
        .mapToObj(RelFieldCollation::new)
        .collect(Collectors.toList());
    return RelCollations.of(fields);
  }

  public static void validateCollation(RelOptCluster cluster, RelNode child, ImmutableBitSet groupSet) {
    if (groupSet.isEmpty()) {
      // If no groups, no collation is required
      return;
    }

    final RelCollation requiredCollation = RelCollations.of(
        StreamSupport.stream(groupSet.spliterator(), false).map(RelFieldCollation::new).collect(Collectors.toList()));

    final RelMetadataQuery mq = cluster.getMetadataQuery();
    final List<RelCollation> collations = mq.collations(child);

    for(RelCollation collation: collations) {
      if (collation.satisfies(requiredCollation)) {
        return;
      }
    }

    throw new AssertionError("child collations [" + collations + "] does not match expected collation [" + requiredCollation + "]");
  }

  @Override
  public Aggregate copy(RelTraitSet traitSet, RelNode input, boolean indicator, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
    try {
      return StreamAggPrel.create(getCluster(), traitSet, input, indicator, groupSet, groupSets, aggCalls, this.getOperatorPhase());
    } catch (InvalidRelException e) {
      throw new AssertionError(e);
    }
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    if(PrelUtil.getSettings(getCluster()).useDefaultCosting()) {
      return super.computeSelfCost(planner).multiplyBy(.1);
    }
    RelNode child = this.getInput();
    double inputRows = mq.getRowCount(child);

    int numGroupByFields = this.getGroupCount();
    int numAggrFields = this.aggCalls.size();
    double cpuCost = DremioCost.COMPARE_CPU_COST * numGroupByFields * inputRows;
    // add cpu cost for computing the aggregate functions
    cpuCost += DremioCost.FUNC_CPU_COST * numAggrFields * inputRows;
    Factory costFactory = (Factory)planner.getCostFactory();
    return costFactory.makeCost(inputRows, cpuCost, 0 /* disk i/o cost */, 0 /* network cost */);
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {

    final Prel child = (Prel) this.getInput();
    final PhysicalOperator childPop = child.getPhysicalOperator(creator);

    final BatchSchema childSchema = childPop.getProps().getSchema();
    List<NamedExpression> exprs = new ArrayList<>();
    exprs.addAll(keys);
    exprs.addAll(aggExprs);
    BatchSchema schema = ExpressionTreeMaterializer.materializeFields(exprs, childSchema, creator.getFunctionLookupContext())
        .setSelectionVectorMode(SelectionVectorMode.NONE)
        .build();
    return new StreamingAggregate(
        creator.props(this, null, schema, RESERVE, LIMIT),
        childPop,
        keys,
        aggExprs,
        1.0f);
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
    return SelectionVectorMode.DEFAULT;
  }

  @Override
  public SelectionVectorMode getEncoding() {
    return SelectionVectorMode.NONE;
  }
}
