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
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * <pre>
 * Extract each component of a filter expr with SARGableStandardForm for
 * - CAST(expression Any type, data_type Any type) = rhsNode
 * - TO_DATE(numeric_expression int32, int64, float, double) = rhsNode
 * - TO_DATE(string_expression varchar, format varchar) = rhsNode
 * - TO_DATE(string_expression varchar, format varchar, replaceErrorWithNull int32) = rhsNode
 * </pre>
 */
public class CastAndToDateRangeTransformer extends RangeTransformer {

  public CastAndToDateRangeTransformer(RelOptCluster relOptCluster,
                                       StandardForm stdForm,
                                       SqlOperator sqlOperator) {
    super(relOptCluster, stdForm, sqlOperator);
  }

  @Override
  RexNode getLhsParam() {
    return rexBuilder.makeLiteral(TimeUnit.DAY.toString());
  }

  @Override
  RexNode getRhsNode() {
    RexNode rhs = super.getRhsNode();
    if (SqlTypeName.CHAR_TYPES.contains(rhs.getType().getSqlTypeName()) ||
      !getColumn().getType().getSqlTypeName().equals(rhs.getType().getSqlTypeName())) {
      return rexBuilder.makeCast(
        rexBuilder.getTypeFactory().createTypeWithNullability(getColumn().getType(), false), rhs);
    }
    return rhs;
  }

  @Override
  RexNode getRhsParam() {
    return SARGableRexUtils.toInterval(1L, TimeUnit.DAY, rexBuilder);
  }
}
