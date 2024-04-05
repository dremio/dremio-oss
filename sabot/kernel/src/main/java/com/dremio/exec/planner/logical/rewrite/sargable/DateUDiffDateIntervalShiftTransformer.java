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

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 *
 *
 * <pre>
 * Extract each component of a filter expr with SARGableStandardForm for
 * - DATE_DIFF(date_expression DATE, days INTEGER) = rhsNode
 * - DATE_DIFF(date_expression DATE, time_interval INTERVAL) = rhsNode
 * - DATE_DIFF(timestamp_expression TIMESTAMP, days INTEGER) = rhsNode
 * - DATE_DIFF(timestamp_expression TIMESTAMP, time_interval INTERVAL) = rhsNode
 * - DATE_DIFF(time_expression TIME, time_interval INTERVAL) = rhsNode
 * - DATE_DIFF(date_expression DATE, date_literal DATE) = rhsNode
 * - DATE_DIFF(timestamp_expression TIMESTAMP, timestamp_literal TIMESTAMP) = rhsNode
 * </pre>
 */
public class DateUDiffDateIntervalShiftTransformer extends ShiftTransformer {

  public DateUDiffDateIntervalShiftTransformer(
      RelOptCluster relOptCluster, StandardForm stdForm, SqlOperator sqlOperator) {
    super(relOptCluster, stdForm, sqlOperator);
  }

  @Override
  RexNode getRhsNode() {
    RexNode param = getLhsCall().getOperands().get(1);
    RexNode rhs = super.getRhsNode();
    if (param instanceof RexLiteral) {
      RexLiteral rexLiteral = ((RexLiteral) param);
      if (SqlTypeName.NUMERIC_TYPES.contains(rexLiteral.getTypeName())
          || SqlTypeName.INTERVAL_TYPES.contains(rexLiteral.getTypeName())) {
        // DATE_DIFF(time_expression TIME, time_interval INTERVAL) → TIME -> not supported for
        // DATE_ADD
        // DATE_DIFF(date_expression DATE, time_interval INTERVAL) → TIMESTAMP
        // DATE_DIFF(date_expression DATE, days INTEGER) → DATE
        if (SqlTypeName.CHAR_TYPES.contains(rhs.getType().getSqlTypeName())) {
          return rexBuilder.makeCast(
              rexBuilder
                  .getTypeFactory()
                  .createTypeWithNullability(getLhsCall().operands.get(0).getType(), false),
              rhs);
        }
        return super.getRhsNode();
      } else if (SqlTypeName.CHAR_TYPES.contains(rexLiteral.getTypeName())) {
        // DATE_DIFF(date_expression DATE, date_expression DATE) → INTERVAL DAY
        // DATE_DIFF(timestamp_expression TIMESTAMP, timestamp_expression TIMESTAMP) → INTERVAL DAY
        return rexBuilder.makeCast(
            rexBuilder
                .getTypeFactory()
                .createTypeWithNullability(getLhsCall().operands.get(0).getType(), false),
            param);
      }
    }
    return param;
  }

  @Override
  RexNode getRhsParam() {
    RexNode param = getLhsCall().getOperands().get(1);
    if (param instanceof RexLiteral) {
      RexLiteral rexLiteral = ((RexLiteral) param);
      if (SqlTypeName.NUMERIC_TYPES.contains(rexLiteral.getTypeName())) {
        // DATE_DIFF(date_expression DATE, days INTEGER) → DATE
        return SARGableRexUtils.toInterval((RexLiteral) param, TimeUnit.DAY, rexBuilder);
      } else if (SqlTypeName.INTERVAL_TYPES.contains(rexLiteral.getTypeName())) {
        // DATE_DIFF(time_expression TIME, time_interval INTERVAL) → TIME //not supported for
        // DATE_ADD
        // DATE_DIFF(date_expression DATE, time_interval INTERVAL) → TIMESTAMP
        return param;
      }
    }
    // DATE_DIFF(date_expression DATE, date_expression DATE) → INTERVAL DAY
    // DATE_DIFF(timestamp_expression TIMESTAMP, timestamp_expression TIMESTAMP) → INTERVAL DAY
    return super.getRhsNode();
  }
}
