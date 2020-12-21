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
package com.dremio.exec.planner.physical.visitor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;

import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.physical.DistributionTrait;
import com.dremio.exec.planner.physical.ExchangePrel;
import com.dremio.exec.planner.physical.HashPrelUtil;
import com.dremio.exec.planner.physical.HashToMergeExchangePrel;
import com.dremio.exec.planner.physical.HashToRandomExchangePrel;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.ProjectPrel;
import com.dremio.options.OptionManager;
import com.google.common.collect.Lists;

/**
 * Inserts proper projects to evaluate the hash expression before the sender and remove it afterwards.
 * */
public class InsertHashProjectVisitor extends BasePrelVisitor<Prel, Void, RuntimeException> {

  public static Prel insertHashProjects(Prel prel, OptionManager options) {
    final boolean isVectorizedPartitionSender = options.getOption(ExecConstants.ENABLE_VECTORIZED_PARTITIONER);
    final boolean muxEnabled = options.getOption(PlannerSettings.MUX_EXCHANGE);

    if (isVectorizedPartitionSender || muxEnabled) {
      return prel.accept(new InsertHashProjectVisitor(), null);
    }

    return prel;
  }

  @Override
  public Prel visitExchange(ExchangePrel prel, Void value) throws RuntimeException {
    Prel child = ((Prel)prel.getInput()).accept(this, null);

    // check if the hash expression has already been added for this particular exchange
    if (!child.getRowType().getFieldNames().contains(HashPrelUtil.HASH_EXPR_NAME)) {
      if (prel instanceof HashToMergeExchangePrel) {
        return visit(prel, ((HashToMergeExchangePrel) prel).getDistFields(), child);
      }
      if (prel instanceof HashToRandomExchangePrel) {
        return visit(prel, ((HashToRandomExchangePrel) prel).getFields(), child);
      }
    }

    return (Prel) prel.copy(prel.getTraitSet(), Collections.singletonList(((RelNode)child)));
  }

  private Prel visit(ExchangePrel hashPrel, List<DistributionTrait.DistributionField> fields, Prel child) {
    final List<String> childFields = child.getRowType().getFieldNames();


    // Insert Project SqlOperatorImpl with new column that will be a hash for HashToRandomExchange fields
    final ProjectPrel addColumnprojectPrel = HashPrelUtil.addHashProject(fields, child, null);
    final Prel newPrel = (Prel) hashPrel.copy(hashPrel.getTraitSet(), Collections.<RelNode>singletonList(addColumnprojectPrel));

    int validRows = newPrel.getRowType().getFieldCount() - 1;
    final List<RelDataTypeField> all = newPrel.getRowType().getFieldList();
    final List<RexNode> keptExprs = new ArrayList<>(validRows);

    final RexBuilder rexBuilder = newPrel.getCluster().getRexBuilder();
    for(int i = 0; i < validRows; i++){
      RexNode rex = rexBuilder.makeInputRef(all.get(i).getType(), i);
      keptExprs.add(rex);
    }

    // remove earlier inserted Project SqlOperatorImpl - since it creates issues down the road in HashJoin
    RelDataType removeRowType = RexUtil.createStructType(newPrel.getCluster().getTypeFactory(), keptExprs, childFields);
    return ProjectPrel.create(newPrel.getCluster(), newPrel.getTraitSet(), newPrel, keptExprs, removeRowType);
  }

  @Override
  public Prel visitPrel(Prel prel, Void value) throws RuntimeException {
    List<RelNode> children = Lists.newArrayList();
    for(Prel child : prel){
      children.add(child.accept(this, null));
    }
    return (Prel) prel.copy(prel.getTraitSet(), children);
  }
}
