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
package com.dremio.exec.planner.logical;

import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;

public final class EnhancedFilterJoinExtraction {
  private final RexNode inputFilterConditionPruned;
  private final RexNode inputJoinConditionPruned;
  private final RexNode joinCondition;
  private final RexNode leftPushdownPredicate;
  private final RexNode rightPushdownPredicate;
  private final RexNode remainingFilterCondition;
  private final JoinRelType simplifiedJoinType;

  public EnhancedFilterJoinExtraction(RexNode inputFilterConditionPruned,
    RexNode inputJoinConditionPruned, RexNode joinCondition,
    RexNode leftPushdownPredicate, RexNode rightPushdownPredicate,
    RexNode remainingFilterCondition, JoinRelType simplifiedJoinType) {
    this.inputFilterConditionPruned = inputFilterConditionPruned;
    this.inputJoinConditionPruned = inputJoinConditionPruned;
    this.joinCondition = joinCondition;
    this.leftPushdownPredicate = leftPushdownPredicate;
    this.rightPushdownPredicate = rightPushdownPredicate;
    this.remainingFilterCondition = remainingFilterCondition;
    this.simplifiedJoinType = simplifiedJoinType;
  }

  public RexNode getInputFilterConditionPruned() {
    return inputFilterConditionPruned;
  }

  public RexNode getInputJoinConditionPruned() {
    return inputJoinConditionPruned;
  }

  public RexNode getJoinCondition() {
    return joinCondition;
  }

  public RexNode getLeftPushdownPredicate() {
    return leftPushdownPredicate;
  }

  public RexNode getRightPushdownPredicate() {
    return rightPushdownPredicate;
  }

  public RexNode getRemainingFilterCondition() {
    return remainingFilterCondition;
  }

  public JoinRelType getSimplifiedJoinType() {
    return simplifiedJoinType;
  }

}
