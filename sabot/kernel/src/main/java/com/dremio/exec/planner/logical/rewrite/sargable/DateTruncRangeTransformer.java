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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * <pre>
 * Extract each component of a filter expr with SARGableStandardForm for DATE_TRUNC
 * DATE_TRUNC(time_unit LITERAL, column DATE|TIMESTAMP) = rhsNode
 * </pre>
 */
public class DateTruncRangeTransformer extends RangeTransformer {
  public DateTruncRangeTransformer(RelOptCluster relOptCluster,
                                   StandardForm stdForm,
                                   SqlOperator sqlOperator) {
    super(relOptCluster, stdForm, sqlOperator);
  }

  @Override
  RexNode getColumn() {
    return getLhsCall().operands.get(1);
  }

  @Override
  RexNode getLhsParam() {
    return getLhsCall().operands.get(0);
  }

  @Override
  RexNode getRhsNode() {
    RexNode rhs = super.getRhsNode();
    // Cast string date/time literal to date/time type
    if (SqlTypeName.CHAR_TYPES.contains(rhs.getType().getSqlTypeName())) {
      return rexBuilder.makeCast(rexBuilder.getTypeFactory().createTypeWithNullability(getReturnType(), false), rhs);
    }
    return rhs;
  }

  @Override
  RelDataType getReturnType() {
    return getColumn().getType();
  }

  @Override
  RexNode getRhsParam() {
    RexNode timeUnit = getLhsParam();
    if (timeUnit instanceof RexLiteral) {
      return SARGableRexUtils.toInterval(1L, (RexLiteral) timeUnit, rexBuilder);
    }
    return timeUnit;
  }
}
