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
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.config.RoundRobinExchange;
import com.dremio.exec.planner.cost.DremioCost;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators.LongValidator;
import com.dremio.options.TypeValidators.PositiveLongValidator;

@Options
public class RoundRobinExchangePrel extends ExchangePrel {

  public static final LongValidator RECEIVER_RESERVE = new PositiveLongValidator("planner.op.receiver.roundrobin.reserve_bytes", Long.MAX_VALUE, DEFAULT_RESERVE);
  public static final LongValidator RECEIVER_LIMIT = new PositiveLongValidator("planner.op.receiver.roundrobin.limit_bytes", Long.MAX_VALUE, DEFAULT_LIMIT);
  public static final LongValidator SENDER_RESERVE = new PositiveLongValidator("planner.op.sender.roundrobin.reserve_bytes", Long.MAX_VALUE, DEFAULT_RESERVE);
  public static final LongValidator SENDER_LIMIT = new PositiveLongValidator("planner.op.sender.roundrobin.limit_bytes", Long.MAX_VALUE, DEFAULT_LIMIT);

  public RoundRobinExchangePrel(RelOptCluster cluster, RelTraitSet traitSet, RelNode input) {
    super(cluster, traitSet, input);
    assert input.getConvention() == Prel.PHYSICAL;
  }

  /**
   * Sends a copy of each batch to one node (same as the data size)
   */
  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    if(PrelUtil.getSettings(getCluster()).useDefaultCosting()) {
      return super.computeSelfCost(planner).multiplyBy(.1);
    }

    RelNode child = this.getInput();
    final double inputRows = mq.getRowCount(child);

    final int  rowWidth = child.getRowType().getFieldCount() * DremioCost.AVG_FIELD_WIDTH;
    final double cpuCost = DremioCost.SVR_CPU_COST * inputRows;
    final double networkCost = DremioCost.BYTE_NETWORK_COST * inputRows * rowWidth;

    return new DremioCost(inputRows, cpuCost, 0, networkCost);
  }

  @Override
  public SelectionVectorMode getEncoding() {
    return SelectionVectorMode.NONE;
  }

  @Override
  public SelectionVectorMode[] getSupportedEncodings() {
    return SelectionVectorMode.DEFAULT;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new RoundRobinExchangePrel(getCluster(), traitSet, sole(inputs));
  }

  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    Prel child = (Prel) this.getInput();

    PhysicalOperator childPOP = child.getPhysicalOperator(creator);

    final OpProps props = creator.props(this, null, childPOP.getProps().getSchema());
    final int senderOperatorId = OpProps.buildOperatorId(childPOP.getProps().getMajorFragmentId(), 0);
    final OpProps senderProps = creator.props(senderOperatorId, this, null, props.getSchema(), SENDER_RESERVE, SENDER_LIMIT, props.getCost() * 0.01);
    final OpProps receiverProps = creator.props(this, null, props.getSchema(), RECEIVER_RESERVE, RECEIVER_LIMIT, props.getCost() * 0.01);

    return new RoundRobinExchange(
        props,
        senderProps,
        receiverProps,
        props.getSchema(),
        childPOP);
  }

}
