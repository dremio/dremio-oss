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

import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import java.nio.charset.StandardCharsets;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.NlsString;

public class DremioRexBuilder extends RexBuilder {
  public static final DremioRexBuilder INSTANCE =
      new DremioRexBuilder(JavaTypeFactoryImpl.INSTANCE);

  private static final SqlCollation Utf8SqlCollation =
      new SqlCollation("UTF-8$en_US$primary", SqlCollation.Coercibility.IMPLICIT);

  /**
   * Creates a RexBuilder.
   *
   * @param typeFactory Type factory
   */
  public DremioRexBuilder(RelDataTypeFactory typeFactory) {
    super(typeFactory);
  }

  public RexNode makeAbstractCastIgnoreType(RelDataType type, RexNode exp) {
    return super.makeAbstractCast(type, exp);
  }

  @Override
  public RexNode makeAbstractCast(RelDataType type, RexNode exp) {
    if (exp.getType().equals(type)) {
      return exp;
    }
    return super.makeAbstractCast(type, exp);
  }

  @Override
  public RexNode makeCast(RelDataType type, RexNode exp, boolean matchNullability) {
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

  @Override
  protected RexLiteral makeLiteral(Comparable o, RelDataType type, SqlTypeName typeName) {
    // By default calcite encodes strings as Latin-1
    // We don't want to change the default, since that will impact previous users and the baselines
    // So we try to encode as Latin-1 and if there is an exception then we try as UTF-8
    try {
      return super.makeLiteral(o, type, typeName);
    } catch (CalciteException ce) {
      if (typeName != SqlTypeName.CHAR) {
        throw ce;
      }

      NlsString nlsString = (NlsString) o;

      NlsString utf8NlsString = new NlsString(nlsString.getValue(), "UTF-8", Utf8SqlCollation);
      RelDataType utf8RelDataType =
          this.typeFactory.createTypeWithCharsetAndCollation(
              type, StandardCharsets.UTF_8, Utf8SqlCollation);

      return super.makeLiteral(utf8NlsString, utf8RelDataType, typeName);
    }
  }
}
