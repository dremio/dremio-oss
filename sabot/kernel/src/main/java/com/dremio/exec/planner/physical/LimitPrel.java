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
import java.util.Iterator;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.config.Limit;
import com.dremio.exec.planner.common.LimitRelBase;
import com.dremio.exec.planner.physical.visitor.PrelVisitor;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators.LongValidator;
import com.dremio.options.TypeValidators.PositiveLongValidator;

@Options
public class LimitPrel extends LimitRelBase implements Prel {

  public static final LongValidator RESERVE = new PositiveLongValidator("planner.op.limit.reserve_bytes", Long.MAX_VALUE, DEFAULT_RESERVE);
  public static final LongValidator LIMIT = new PositiveLongValidator("planner.op.limit.limit_bytes", Long.MAX_VALUE, DEFAULT_LIMIT);


  public LimitPrel(RelOptCluster cluster, RelTraitSet traitSet, RelNode child, RexNode offset, RexNode fetch) {
    super(cluster, traitSet, child, offset, fetch);
  }

  public LimitPrel(RelOptCluster cluster, RelTraitSet traitSet, RelNode child, RexNode offset, RexNode fetch, boolean pushDown) {
    super(cluster, traitSet, child, offset, fetch, pushDown);
  }

  @Override
  public Sort copy(RelTraitSet traitSet, RelNode newInput, RelCollation newCollation, RexNode offset, RexNode fetch) {
    return new LimitPrel(getCluster(), traitSet, newInput, offset, fetch, isPushDown());
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    Prel child = (Prel) this.getInput();

    PhysicalOperator childPOP = child.getPhysicalOperator(creator);

    int first = getFirst();
    Integer last = getLast();

    return new Limit(
        creator.props(this, null, childPOP.getProps().getSchema(), RESERVE, LIMIT),
        childPOP,
        first,
        last
        );
  }

  private int getFirst(){
    // First offset to include into results (inclusive). Null implies it is starting from offset 0
    return offset != null ? Math.max(0, RexLiteral.intValue(offset)) : 0;
  }

  private Integer getLast(){
    // Last offset to stop including into results (exclusive), translating fetch row counts into an offset.
    // Null value implies including entire remaining result set from first offset
    return fetch != null ? Math.max(0, RexLiteral.intValue(fetch)) + getFirst() : null;
  }

  public boolean isTrivial(){
    Integer last = getLast();
    if(last != null && last <= PrelUtil.getPlannerSettings(getCluster()).getSliceTarget() ){
      return true;
    } else {
      return false;
    }
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

  @Override
  public boolean needsFinalColumnReordering() {
    return true;
  }

}
