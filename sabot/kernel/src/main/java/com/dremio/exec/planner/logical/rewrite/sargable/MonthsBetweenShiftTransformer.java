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

import java.math.BigDecimal;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;

/**
 * <pre>
 * Extract each component of a filter expr with SARGableStandardForm for
 * - MONTHS_BETWEEN(date_timestamp_expression1 string, date_timestamp_expression2 string) = rhsNode
 * </pre>
 */
public class MonthsBetweenShiftTransformer extends ShiftTransformer {
  public MonthsBetweenShiftTransformer(RelOptCluster relOptCluster,
                                       StandardForm stdForm,
                                       SqlOperator sqlOperator) {
    super(relOptCluster, stdForm, sqlOperator);
  }

  @Override
  RexNode getRhsNode() {
    return getLhsCall().operands.get(1);
  }

  @Override
  RexNode getRhsParam() {
    RexNode rhs = super.getRhsNode();
    Object obj = ((RexLiteral) rhs).getValue();
    if (obj instanceof BigDecimal) {
      BigDecimal num = (BigDecimal) obj;
      if (isIntegerValue(num)) {
        return SARGableRexUtils.toInterval((RexLiteral) rhs, TimeUnit.MONTH, rexBuilder);
      } else {
        return SARGableRexUtils.toInterval((long) (num.doubleValue() * 30.0), TimeUnit.DAY, rexBuilder);
      }
    }
    return null;
  }

  private boolean isIntegerValue(BigDecimal bd) {
    return bd.scale() <= 0 || bd.signum() == 0 || bd.stripTrailingZeros().scale() <= 0;
  }
}
