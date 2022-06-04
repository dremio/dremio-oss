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
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.config.BridgeExchange;
import com.dremio.exec.planner.cost.DremioCost;
import com.dremio.exec.record.BatchSchema;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators;

@Options
public class BridgeExchangePrel extends ExchangePrel {
  public static final TypeValidators.LongValidator RECEIVER_RESERVE = new TypeValidators.PositiveLongValidator("planner.op.receiver.bridge.reserve_bytes", Long.MAX_VALUE, DEFAULT_RESERVE);
  public static final TypeValidators.LongValidator RECEIVER_LIMIT = new TypeValidators.PositiveLongValidator("planner.op.receiver.bridge.limit_bytes", Long.MAX_VALUE, DEFAULT_LIMIT);
  public static final TypeValidators.LongValidator SENDER_RESERVE = new TypeValidators.PositiveLongValidator("planner.op.sender.bridge.reserve_bytes", Long.MAX_VALUE, DEFAULT_RESERVE);
  public static final TypeValidators.LongValidator SENDER_LIMIT = new TypeValidators.PositiveLongValidator("planner.op.sender.bridge.limit_bytes", Long.MAX_VALUE, DEFAULT_LIMIT);
  private final String bridgeSetId;

  public BridgeExchangePrel(RelOptCluster cluster, RelTraitSet traits, RelNode child, String bridgeSetId) {
    super(cluster, traits, child);
    this.bridgeSetId = bridgeSetId;
  }

  /**
   *
   */
  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    RelNode child = this.getInput();
    final double rowCount = mq.getRowCount(this);
    final DremioCost.Factory costFactory = (DremioCost.Factory)planner.getCostFactory();

    double adjustmentFactor = PrelUtil.getPlannerSettings(getCluster()).getCseCostAdjustmentFactor();
    if (adjustmentFactor == Double.MAX_VALUE) {
      return costFactory.makeHugeCost();
    }

    final int  rowWidth = child.getRowType().getFieldCount() * DremioCost.AVG_FIELD_WIDTH;
    final double cpuCost = DremioCost.PROJECT_SIMPLE_CPU_COST * rowCount * rowWidth;
    final double ioCost = DremioCost.BYTE_DISK_WRITE_COST * rowCount * rowWidth;

    return costFactory.makeCost(rowCount, cpuCost, ioCost, 0).multiplyBy(adjustmentFactor);
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    Prel child = (Prel) this.getInput();

    PhysicalOperator childPOP = child.getPhysicalOperator(creator);

    final OpProps props = creator.props(this, null, childPOP.getProps().getSchema());
    final int senderOperatorId = OpProps.buildOperatorId(childPOP.getProps().getMajorFragmentId(), 0);
    final OpProps senderProps = creator.props(senderOperatorId, this, null, props.getSchema(), SENDER_RESERVE, SENDER_LIMIT, props.getCost() * 0.01);
    final OpProps receiverProps = creator.props(this, null, props.getSchema(), RECEIVER_RESERVE, RECEIVER_LIMIT, props.getCost() * 0.01);

    return new BridgeExchange(
      props,
      senderProps,
      receiverProps,
      props.getSchema(),
      childPOP,
      creator.getOptionManager(),
      getBridgeSetId());
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new BridgeExchangePrel(getCluster(), traitSet, sole(inputs), getBridgeSetId());
  }

  @Override
  public BatchSchema.SelectionVectorMode getEncoding() {
    return BatchSchema.SelectionVectorMode.NONE;
  }

  public String getBridgeSetId() {
    return bridgeSetId;
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
      .item("bridgeSetId", bridgeSetId);
  }
}
