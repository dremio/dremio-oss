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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;

import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.config.Filter;
import com.dremio.exec.planner.common.FilterRelBase;
import com.dremio.exec.planner.logical.ParseContext;
import com.dremio.exec.planner.physical.visitor.PrelVisitor;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators.LongValidator;
import com.dremio.options.TypeValidators.PositiveLongValidator;

@Options
public class FilterPrel extends FilterRelBase implements Prel {

  public static final LongValidator RESERVE = new PositiveLongValidator("planner.op.filter.reserve_bytes", Long.MAX_VALUE, DEFAULT_RESERVE);
  public static final LongValidator LIMIT = new PositiveLongValidator("planner.op.filter.limit_bytes", Long.MAX_VALUE, DEFAULT_LIMIT);

  protected FilterPrel(RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode condition) {
    super(Prel.PHYSICAL, cluster, traits, child, condition);
  }

  @Override
  public org.apache.calcite.rel.core.Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
    return new FilterPrel(getCluster(), traitSet, input, condition);
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {

    Prel child = (Prel) this.getInput();

    PhysicalOperator childPOP = child.getPhysicalOperator(creator);

    return new Filter(
        creator.props(this, null, childPOP.getProps().getSchema().clone(SelectionVectorMode.TWO_BYTE), RESERVE, LIMIT),
        childPOP,
        getFilterExpression(new ParseContext(PrelUtil.getSettings(getCluster()))),
        1.0f
        );
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

  /**
   * FilterPrel adds an SV2 (TWO_BYTE mode SelectionVector).
   */
  @Override
  public SelectionVectorMode getEncoding() {
    return SelectionVectorMode.TWO_BYTE;
  }

  @Override
  public boolean needsFinalColumnReordering() {
    return true;
  }

}
