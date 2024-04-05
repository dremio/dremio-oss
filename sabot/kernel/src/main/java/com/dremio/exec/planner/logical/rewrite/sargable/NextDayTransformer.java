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
package com.dremio.exec.planner.logical.rewrite.sargable;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;

/**
 *
 *
 * <pre>
 * NEXT_DAY(col1, 'TU') = '2023-07-18' => col1 >= '2014-07-12' and col1 < '2014-07-19'
 * </pre>
 */
public class NextDayTransformer extends RangeTransformer {
  private final int rhsDayOfWeekDiff;

  NextDayTransformer(
      RelOptCluster relOptCluster,
      StandardForm standardForm,
      SqlOperator sqlOperator,
      boolean isTruncToBegin,
      int rhsDayOfWeekDiff) {
    super(relOptCluster, standardForm, sqlOperator, isTruncToBegin);
    this.rhsDayOfWeekDiff = rhsDayOfWeekDiff;
  }

  @Override
  RexNode getRhsNode() {
    RexNode rhs = super.getRhsNode();
    if (rhsDayOfWeekDiff == 0) {
      return SARGableRexUtils.dateAdd(rhs, -6, rexBuilder);
    }
    return rhs;
  }

  @Override
  public RexDateRange getRange() {
    final RexNode begin;
    final RexNode end;
    if (rhsDayOfWeekDiff == 0) {
      end = super.getRhsNode();
      setRhsEqBegin(true);
    } else {
      end =
          SARGableRexUtils.dateAdd(
              this.getRhsNode(),
              rexBuilder.makeLiteral(
                  String.valueOf(rhsDayOfWeekDiff > 0 ? rhsDayOfWeekDiff : 7 + rhsDayOfWeekDiff)),
              rexBuilder);
      setRhsEqBegin(false);
    }
    begin = SARGableRexUtils.dateAdd(end, rexBuilder.makeLiteral("-6"), rexBuilder);

    final List<RexNode> reducedValues = new ArrayList<>();
    final List<RexNode> constExpNode = new ArrayList<>();
    constExpNode.add(begin);
    constExpNode.add(end);
    RexExecutor rexExecutor = relOptCluster.getPlanner().getExecutor();
    if (rexExecutor != null) {
      rexExecutor.reduce(rexBuilder, constExpNode, reducedValues);
      constExpNode.clear();
      constExpNode.addAll(reducedValues);
      reducedValues.clear();
      rexExecutor.reduce(rexBuilder, constExpNode, reducedValues);

      if (reducedValues.size() == 2
          && reducedValues.get(0) != null
          && reducedValues.get(1) != null) {
        return new RexDateRange(reducedValues.get(0), reducedValues.get(1));
      }
    }

    return new RexDateRange(begin, end);
  }
}
