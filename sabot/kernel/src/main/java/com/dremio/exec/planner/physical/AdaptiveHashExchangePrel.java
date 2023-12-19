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
import java.util.function.Function;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;

import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.config.HashSenderCalculator;
import com.dremio.exec.physical.config.HashToRandomExchange;
import com.dremio.options.OptionManager;

/**
 * This operator allows adaptive distribution
 * 1. Regular Hash distribution based on distribution columns
 * 2. If data skew is detected, switch to Round-Robin fashion distribution based on the DOP
 */
public class AdaptiveHashExchangePrel extends HashToRandomExchangePrel {

  public AdaptiveHashExchangePrel(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, List<DistributionTrait.DistributionField> fields,
                                  String hashFunctionName, Function<Prel, TableFunctionPrel> tableFunctionCreator, boolean windowPushedDown) {
    super(cluster, traitSet, input, fields, hashFunctionName, tableFunctionCreator, windowPushedDown);
  }

  public AdaptiveHashExchangePrel(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, List<DistributionTrait.DistributionField> fields) {
    super(cluster, traitSet, input, fields);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new AdaptiveHashExchangePrel(getCluster(), traitSet, sole(inputs), fields, hashFunctionName, tableFunctionCreator, windowPushedDown);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs, boolean windowPushedDown) {
    return new AdaptiveHashExchangePrel(getCluster(), traitSet, sole(inputs), fields, hashFunctionName, tableFunctionCreator, windowPushedDown);
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    Prel child = (Prel) this.getInput();

    PhysicalOperator childPOP = child.getPhysicalOperator(creator);

    OptionManager optionManager = creator.getOptionManager();

    final OpProps props = creator.props(this, null, childPOP.getProps().getSchema());
    final int senderOperatorId = OpProps.buildOperatorId(childPOP.getProps().getMajorFragmentId(), 0);
    final OpProps senderProps = creator.props(senderOperatorId, this, null, props.getSchema(), SENDER_RESERVE, SENDER_LIMIT, props.getCost() * 0.5);
    final OpProps receiverProps = creator.props(this, null, props.getSchema(), RECEIVER_RESERVE, RECEIVER_LIMIT, props.getCost() * 0.01);

    return new HashToRandomExchange(
      props,
      senderProps,
      receiverProps,
      HashSenderCalculator.captureBucketOptions(creator.getOptionManager(), SENDER_RESERVE, props.getSchema()),
      props.getSchema(),
      childPOP,
      HashPrelUtil.getHashExpression(this.fields, getInput().getRowType()),
      optionManager,
      true);
  }
}
