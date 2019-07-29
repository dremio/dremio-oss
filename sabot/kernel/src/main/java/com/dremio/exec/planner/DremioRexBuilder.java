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
package com.dremio.exec.planner;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeUtil;

public class DremioRexBuilder extends RexBuilder {

  /**
   * Creates a RexBuilder.
   *
   * @param typeFactory Type factory
   */
  public DremioRexBuilder(RelDataTypeFactory typeFactory) {
    super(typeFactory);
  }

  @Override
  public RexNode makeAbstractCast(
    RelDataType type,
    RexNode exp) {
    if (exp.getType().equals(type)) {
      return exp;
    }
    return super.makeAbstractCast(type, exp);
  }

  @Override
  public RexNode makeCast(
    RelDataType type,
    RexNode exp,
    boolean matchNullability) {
    // Special case: bypassing Calcite for interval types
    if (!(exp instanceof RexLiteral)
        && SqlTypeUtil.isExactNumeric(type)
        && SqlTypeUtil.isInterval(exp.getType())) {
      return makeAbstractCast(type, exp);
    }
    RexNode castRexNode = super.makeCast(type, exp, matchNullability);

    // If we have a CAST(A, TYPE) and A is already of the same TYPE (including nullability),
    // then return just A.
    if (castRexNode instanceof RexCall
      && castRexNode.getKind() == SqlKind.CAST
      && castRexNode.getType().equals(((RexCall) castRexNode).getOperands().get(0).getType())) {
      return ((RexCall) castRexNode).getOperands().get(0);
    }

    return castRexNode;
  }
}
