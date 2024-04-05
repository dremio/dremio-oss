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
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 *
 *
 * <pre>
 * Extract each component of a filter expr with SARGableStandardForm for
 * - LAST_DAY(date_timestamp_expression string) = rhsNode
 * - e.g.:
 * - LAST_DAY(ts) = '2023-01-31' => DATE_TRUNC('MONTH', ts) = '2023-01-01'
 * </pre>
 */
public class LastDayRangeTransformer extends RangeTransformer {

  public LastDayRangeTransformer(
      RelOptCluster relOptCluster,
      StandardForm stdForm,
      SqlOperator sqlOperator,
      boolean isTruncToBegin) {
    super(relOptCluster, stdForm, sqlOperator, isTruncToBegin);
  }

  @Override
  RexNode getLhsParam() {
    return rexBuilder.makeLiteral(TimeUnit.MONTH.toString());
  }

  @Override
  RexNode getRhsNode() {
    RexNode rhs = super.getRhsNode();
    // Cast string date/time literal to date/time type
    if (SqlTypeName.CHAR_TYPES.contains(rhs.getType().getSqlTypeName())) {
      rhs =
          rexBuilder.makeCast(
              rexBuilder.getTypeFactory().createTypeWithNullability(getColumn().getType(), false),
              rhs);
    }
    // Modify rhsNode to the beginning of the month if rhsNode equals to the end of the month
    if (isEqualWithExecutor(
        rhs,
        SARGableRexUtils.dateAdd(
            SARGableRexUtils.dateTrunc(
                TimeUnit.MONTH, SARGableRexUtils.dateAdd(rhs, 1, rexBuilder), rexBuilder),
            -1,
            rexBuilder))) {
      return SARGableRexUtils.dateTrunc(TimeUnit.MONTH, rhs, rexBuilder);
    }
    return rhs;
  }

  @Override
  RexNode getRhsParam() {
    return SARGableRexUtils.toInterval(1L, TimeUnit.MONTH, rexBuilder);
  }

  boolean isEqualWithExecutor(RexNode n1, RexNode n2) {
    final List<RexNode> reducedValues = new ArrayList<>();
    final List<RexNode> constExpNode = new ArrayList<>();
    constExpNode.add(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, n1, n2));
    RexExecutor rexExecutor = relOptCluster.getPlanner().getExecutor();
    if (rexExecutor != null) {
      rexExecutor.reduce(rexBuilder, constExpNode, reducedValues);
      if (reducedValues.get(0) != null) {
        if (reducedValues.get(0) instanceof RexLiteral) {
          RexLiteral boolVal = (RexLiteral) reducedValues.get(0);
          return (Boolean) boolVal.getValue();
        }
      }
    }
    return false;
  }
}
