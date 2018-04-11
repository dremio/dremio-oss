/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.exec.planner.sql;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.LiteralOperandTypeChecker;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql2rel.SqlRexContext;
import org.apache.calcite.sql2rel.SqlRexConvertlet;
import org.apache.calcite.util.NlsString;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class SqlDatePartOperator extends SqlFunction {

  private static final Map<String, TimeUnitRange> VALID_PERIODS = ImmutableMap.<String, TimeUnitRange>builder()
      .put("second",TimeUnitRange.SECOND)
      .put("minute", TimeUnitRange.MINUTE)
      .put("hour", TimeUnitRange.HOUR)
      .put("day", TimeUnitRange.DAY)
      .put("month", TimeUnitRange.MONTH)
      .put("year", TimeUnitRange.YEAR)
      .put("century", TimeUnitRange.CENTURY)
      .put("decade", TimeUnitRange.DECADE)
      .put("dow", TimeUnitRange.DOW)
      .put("doy", TimeUnitRange.DOY)
      .put("epoch", TimeUnitRange.EPOCH)
      .put("millenium", TimeUnitRange.MILLENNIUM)
      .put("quarter", TimeUnitRange.QUARTER)
      .put("week", TimeUnitRange.WEEK).build();

  public static final SqlRexConvertlet CONVERTLET = new DatePartConvertlet();
  public static final SqlDatePartOperator INSTANCE = new SqlDatePartOperator();

  public SqlDatePartOperator() {
    super(
        "DATE_PART",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BIGINT_NULLABLE,
        null,
        OperandTypes.sequence(
            "<PERIOD LITERAL>, <DATE or TIMESTAMP or INTERVAL>",
            new EnumeratedListChecker(VALID_PERIODS.keySet()),
            OperandTypes.or(
                OperandTypes.family(SqlTypeFamily.DATE),
                OperandTypes.family(SqlTypeFamily.TIMESTAMP),
                OperandTypes.family(SqlTypeFamily.DATETIME),
                OperandTypes.family(SqlTypeFamily.DATETIME_INTERVAL),
                OperandTypes.family(SqlTypeFamily.INTERVAL_DAY_TIME),
                OperandTypes.family(SqlTypeFamily.INTERVAL_YEAR_MONTH))
            ),
            SqlFunctionCategory.SYSTEM);
  }

  private static class DatePartConvertlet implements SqlRexConvertlet {

    @Override
    public RexNode convertCall(SqlRexContext cx, SqlCall call) {
      final RexBuilder rexBuilder = cx.getRexBuilder();

      final SqlLiteral literal = (SqlLiteral) call.getOperandList().get(0);
      final String value = ((NlsString)literal.getValue()).getValue();
      TimeUnitRange range = VALID_PERIODS.get(value.toLowerCase());
      Preconditions.checkNotNull(range, "Unhandle range type: %s.", value);
      List<RexNode> exprs = new ArrayList<>();

      exprs.add(rexBuilder.makeFlag(range));
      exprs.add(cx.convertExpression(call.getOperandList().get(1)));

      RelDataTypeFactory typeFactory = cx.getTypeFactory();
      final RelDataType returnType
          = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), exprs.get(1).getType().isNullable());
      return rexBuilder.makeCall(returnType, SqlStdOperatorTable.EXTRACT, exprs);
    }

  }

  public static class EnumeratedListChecker extends LiteralOperandTypeChecker {

    private final Set<String> validStrings;

    public EnumeratedListChecker(Iterable<String> strings) {
      super(false);
      validStrings = ImmutableSet.copyOf(strings);
    }

    @Override
    public boolean checkSingleOperandType(SqlCallBinding callBinding, SqlNode node,
        int iFormalOperand, boolean throwOnFailure) {

      // check that the input is a literal.
      if(!super.checkSingleOperandType(callBinding, node, iFormalOperand, throwOnFailure)) {
        return false;
      }

      final RelDataType type = callBinding.getValidator().deriveType(callBinding.getScope(), node);
      final SqlTypeName typeName = type.getSqlTypeName();

      // Pass type checking for operators if it's of type 'ANY'.
      if (typeName.getFamily() == SqlTypeFamily.ANY) {
        return true;
      }

      if(!(typeName == SqlTypeName.CHAR || typeName == SqlTypeName.VARCHAR)) {
        if(throwOnFailure) {
          throw callBinding.newValidationSignatureError();
        }
        return false;
      }

      final SqlLiteral literal = (SqlLiteral) node;
      final String value = ((NlsString)literal.getValue()).getValue();
      if(validStrings.contains(value.toLowerCase())) {
        return true;
      }

      if(throwOnFailure) {
        throw callBinding.newValidationSignatureError();
        //throw new SqlValidatorException(String.format("DATE_PART function only accepts the following values for a date type: %s.", Joiner.on(", ").join(validStrings)), null);
      }

      return false;
    }
  }




}
