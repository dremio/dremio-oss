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

import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.config.HashSenderCalculator;
import com.dremio.exec.physical.config.HashToMergeExchange;
import com.dremio.exec.planner.cost.DremioCost;
import com.dremio.exec.planner.cost.DremioCost.Factory;
import com.dremio.exec.planner.physical.DistributionTrait.DistributionField;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.options.OptionManager;
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
import org.apache.calcite.rel.metadata.RelMetadataQuery;

@Options
public class HashToMergeExchangePrel extends ExchangePrel {

  public static final LongValidator SENDER_RESERVE =
      new PositiveLongValidator(
          "planner.op.hashmerge.sender.reserve_bytes", Long.MAX_VALUE, DEFAULT_RESERVE);
  public static final LongValidator SENDER_LIMIT =
      new PositiveLongValidator(
          "planner.op.hashmerge.sender.limit_bytes", Long.MAX_VALUE, DEFAULT_LIMIT);
  public static final LongValidator RECEIVER_RESERVE =
      new PositiveLongValidator(
          "planner.op.hashmerge.receiver.reserve_bytes", Long.MAX_VALUE, DEFAULT_RESERVE);
  public static final LongValidator RECEIVER_LIMIT =
      new PositiveLongValidator(
          "planner.op.hashmerge.receiver.limit_bytes", Long.MAX_VALUE, DEFAULT_LIMIT);

  private final List<DistributionField> distFields;
  private int numEndPoints = 0;
  private final RelCollation collation;

  public HashToMergeExchangePrel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      List<DistributionField> fields,
      RelCollation collation,
      int numEndPoints) {
    super(cluster, traitSet, input);
    this.distFields = fields;
    this.collation = collation;
    this.numEndPoints = numEndPoints;
    assert input.getConvention() == Prel.PHYSICAL;
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    if (PrelUtil.getSettings(getCluster()).useDefaultCosting()) {
      return super.computeSelfCost(planner, mq).multiplyBy(.1);
    }
    RelNode child = this.getInput();
    double inputRows = mq.getRowCount(child);

    int rowWidth = child.getRowType().getFieldCount() * DremioCost.AVG_FIELD_WIDTH;
    double hashCpuCost = DremioCost.HASH_CPU_COST * inputRows * distFields.size();
    double svrCpuCost = DremioCost.SVR_CPU_COST * inputRows;
    double mergeCpuCost =
        DremioCost.COMPARE_CPU_COST * inputRows * (Math.log(numEndPoints) / Math.log(2));
    double networkCost = DremioCost.BYTE_NETWORK_COST * inputRows * rowWidth;
    Factory costFactory = (Factory) planner.getCostFactory();
    return costFactory.makeCost(inputRows, hashCpuCost + svrCpuCost + mergeCpuCost, 0, networkCost);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new HashToMergeExchangePrel(
        getCluster(), traitSet, sole(inputs), distFields, this.collation, numEndPoints);
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    Prel child = (Prel) this.getInput();

    PhysicalOperator childPOP = child.getPhysicalOperator(creator);

    OptionManager optionManager = creator.getOptionManager();

    final OpProps props = creator.props(this, null, childPOP.getProps().getSchema());
    final int senderOperatorId =
        OpProps.buildOperatorId(childPOP.getProps().getMajorFragmentId(), 0);
    final OpProps senderProps =
        creator.props(
            senderOperatorId,
            this,
            null,
            props.getSchema(),
            SENDER_RESERVE,
            SENDER_LIMIT,
            props.getCost() * 0.5);
    final OpProps receiverProps =
        creator.props(
            this, null, props.getSchema(), RECEIVER_RESERVE, RECEIVER_LIMIT, props.getCost() * 0.5);

    return new HashToMergeExchange(
        props,
        senderProps,
        receiverProps,
        HashSenderCalculator.captureBucketOptions(
            creator.getOptionManager(), SENDER_RESERVE, props.getSchema()),
        props.getSchema(),
        childPOP,
        HashPrelUtil.getHashExpression(this.distFields, getInput().getRowType()),
        PrelUtil.getOrdering(this.collation, getInput().getRowType()),
        optionManager);
  }

  public List<DistributionField> getDistFields() {
    return this.distFields;
  }

  public RelCollation getCollation() {
    return this.collation;
  }

  @Override
  public SelectionVectorMode getEncoding() {
    return SelectionVectorMode.NONE;
  }
}
