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

import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.cost.DremioCost;
import com.dremio.exec.planner.cost.DremioCost.Factory;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators.LongValidator;
import com.dremio.options.TypeValidators.PositiveLongValidator;

@Options
public class OrderedPartitionExchangePrel extends ExchangePrel {

  public static final LongValidator SENDER_RESERVE = new PositiveLongValidator("planner.op.orderedpartition.sender.reserve_bytes", Long.MAX_VALUE, DEFAULT_RESERVE);
  public static final LongValidator SENDER_LIMIT = new PositiveLongValidator("planner.op.orderedpartition.sender.limit_bytes", Long.MAX_VALUE, DEFAULT_LIMIT);
  public static final LongValidator RECEIVER_RESERVE = new PositiveLongValidator("planner.op.orderedpartition.receiver.reserve_bytes", Long.MAX_VALUE, DEFAULT_RESERVE);
  public static final LongValidator RECEIVER_LIMIT = new PositiveLongValidator("planner.op.orderedpartition.receiver.limit_bytes", Long.MAX_VALUE, DEFAULT_LIMIT);

  public OrderedPartitionExchangePrel(RelOptCluster cluster, RelTraitSet traitSet, RelNode input) {
    super(cluster, traitSet, input);
    assert input.getConvention() == Prel.PHYSICAL;
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    if (PrelUtil.getSettings(getCluster()).useDefaultCosting()) {
      return super.computeSelfCost(planner, mq).multiplyBy(.1);
    }
    RelNode child = this.getInput();
    double inputRows = mq.getRowCount(child);

    int  rowWidth = child.getRowType().getFieldCount() * DremioCost.AVG_FIELD_WIDTH;

    double rangePartitionCpuCost = DremioCost.RANGE_PARTITION_CPU_COST * inputRows;
    double svrCpuCost = DremioCost.SVR_CPU_COST * inputRows;
    double networkCost = DremioCost.BYTE_NETWORK_COST * inputRows * rowWidth;
    Factory costFactory = (Factory)planner.getCostFactory();
    return costFactory.makeCost(inputRows, rangePartitionCpuCost + svrCpuCost, 0, networkCost);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new OrderedPartitionExchangePrel(getCluster(), traitSet, sole(inputs));
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    throw new IOException(this.getClass().getSimpleName() + " not supported yet!");
  }

  @Override
  public SelectionVectorMode getEncoding() {
    return SelectionVectorMode.NONE;
  }
}
