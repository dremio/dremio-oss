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
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimestampString;

/**
 *
 *
 * <pre>
 * Extract each component of a filter expr with SARGableStandardForm for
 * - EXTRACT(time_unit KEYWORD, date_time_expression DATE, TIME, TIMESTAMP)
 * - DATE_PART([field string], source date or timestamp)
 * </pre>
 */
public class ExtractAndDatePartRangeTransformer extends RangeTransformer {

  public ExtractAndDatePartRangeTransformer(
      RelOptCluster relOptCluster, StandardForm stdForm, SqlOperator sqlOperator) {
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
    if (getColumn().getType().getSqlTypeName().equals(SqlTypeName.DATE)) {
      return rexBuilder.makeDateLiteral(
          new DateString(((RexLiteral) rhs).getValue2().toString() + "-01-01"));
    } else {
      return rexBuilder.makeTimestampLiteral(
          new TimestampString(((RexLiteral) rhs).getValue2().toString() + "-01-01 00:00:00"), 0);
    }
  }

  @Override
  RexNode getRhsParam() {
    return SARGableRexUtils.toInterval(1L, TimeUnit.YEAR, rexBuilder);
  }

  @Override // Need to override this because the SqlOperator is not be applied to RHS
  public RexDateRange getRange() {
    final RexNode begin = getRhsNode();
    final RexNode end = SARGableRexUtils.dateAdd(getRhsNode(), getRhsParam(), rexBuilder);

    if (begin == null || end == null) {
      return null;
    }

    return getDateRange(begin, end);
  }
}
