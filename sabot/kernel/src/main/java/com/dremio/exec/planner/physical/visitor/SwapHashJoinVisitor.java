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

package com.dremio.exec.planner.physical.visitor;

import java.util.List;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;

import com.dremio.exec.planner.cost.DefaultRelMetadataProvider;
import com.dremio.exec.planner.physical.HashJoinPrel;
import com.dremio.exec.planner.physical.JoinPrel;
import com.dremio.exec.planner.physical.Prel;
import com.google.common.collect.Lists;

/**
 * Visit Prel tree. Find all the HashJoinPrel nodes and set the flag to swap the Left/Right for HashJoinPrel
 * when 1) It's inner join, 2) left rowcount is < (1 + percentage) * right_row_count.
 * The purpose of this visitor is to prevent planner from putting bigger dataset in the RIGHT side,
 * which is not good performance-wise.
 *
 * @see com.dremio.exec.planner.physical.HashJoinPrel
 */

public class SwapHashJoinVisitor extends BasePrelVisitor<Prel, Double, RuntimeException>{

  private static SwapHashJoinVisitor INSTANCE = new SwapHashJoinVisitor();

  public static Prel swapHashJoin(Prel prel, Double marginFactor){
    return prel.accept(INSTANCE, marginFactor);
  }

  private SwapHashJoinVisitor() {

  }

  @Override
  public Prel visitPrel(Prel prel, Double value) throws RuntimeException {
    List<RelNode> children = Lists.newArrayList();
    for(Prel child : prel){
      child = child.accept(this, value);
      children.add(child);
    }

    return (Prel) prel.copy(prel.getTraitSet(), children);
  }

  @Override
  public Prel visitJoin(JoinPrel prel, Double value) throws RuntimeException {
    JoinPrel newJoin = (JoinPrel) visitPrel(prel, value);

    if (prel instanceof HashJoinPrel) {
      // Mark left/right is swapped, when INNER hash join's left row count < ( 1+ margin factor) right row count.
      if ( (newJoin.getLeft().estimateRowCount(DefaultRelMetadataProvider.INSTANCE.getRelMetadataQuery())
          < (1 + value.doubleValue() ) * newJoin.getRight().estimateRowCount(DefaultRelMetadataProvider.INSTANCE.getRelMetadataQuery()) )
          && newJoin.getJoinType() == JoinRelType.INNER) {
        ( (HashJoinPrel) newJoin).setSwapped(true);
      }
    }

    return newJoin;
  }

}
