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
package com.dremio.exec.planner.sql;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql2rel.SqlRexContext;
import org.apache.calcite.sql2rel.SqlRexConvertlet;

public class HiveMaskConvertlets {

  public static final class HiveMaskHashConvertlet implements SqlRexConvertlet {
    public static final SqlRexConvertlet INSTANCE = new HiveMaskHashConvertlet();

    @Override
    public RexNode convertCall(SqlRexContext cx, SqlCall call) {
      final RexBuilder rexBuilder = cx.getRexBuilder();
      final RexNode operand0 = cx.convertExpression(call.getOperandList().get(0));
      if (operand0.getType().getFamily() == SqlTypeFamily.CHARACTER) {
        return rexBuilder.makeCall(DremioSqlOperatorTable.HASHSHA256, operand0);
      } else {
        return rexBuilder.makeNullLiteral(operand0.getType());
      }
    }
  }

  private static final int CHAR_COUNT = 4;
  private static final String MASKED_UPPERCASE = "X";
  private static final String MASKED_LOWERCASE = "x";
  private static final String MASKED_DIGIT = "n";
  private static final String MASKED_OTHER = "-1";
  private static final int MASKED_NUMBER = 1;
  private static final int MASKED_DAY = 1;
  private static final int MASKED_MONTH = 0;
  private static final int MASKED_YEAR = 0;

  public static final class HiveMaskConvertlet implements SqlRexConvertlet {
    public static final SqlRexConvertlet INSTANCE = new HiveMaskConvertlet();

    @Override
    public RexNode convertCall(SqlRexContext cx, SqlCall call) {
      final RexBuilder rexBuilder = cx.getRexBuilder();
      final List<SqlNode> operandList = call.getOperandList();
      final List<RexNode> operands =
          new ArrayList<>(
              operandList.stream().map(cx::convertExpression).collect(Collectors.toList()));

      final RexNode operand0 = operands.get(0);

      if ((operand0.getType().getFamily() != SqlTypeFamily.CHARACTER)
          && (operand0.getType().getSqlTypeName() != SqlTypeName.DATE)
          && (!SqlTypeName.INT_TYPES.contains(operand0.getType().getSqlTypeName()))) {
        return rexBuilder.makeNullLiteral(operand0.getType());
      }

      if (operands.size() < 2) {
        operands.add(rexBuilder.makeLiteral(MASKED_UPPERCASE));
      }
      if (operands.size() < 3) {
        operands.add(rexBuilder.makeLiteral(MASKED_LOWERCASE));
      }
      if (operands.size() < 4) {
        operands.add(rexBuilder.makeLiteral(MASKED_DIGIT));
      }
      if (operands.size() < 5) {
        operands.add(rexBuilder.makeLiteral(MASKED_OTHER));
      }
      if (operand0.getType().getFamily() == SqlTypeFamily.CHARACTER) {
        return addModeAndCharCountAndBuild(cx, operands.subList(0, 5), "FULL");
      }
      if (operands.size() < 6) {
        operands.add(
            rexBuilder.makeLiteral(
                MASKED_NUMBER, cx.getTypeFactory().createSqlType(SqlTypeName.INTEGER)));
      }
      if (SqlTypeName.INT_TYPES.contains(operand0.getType().getSqlTypeName())) {
        return addModeAndCharCountAndBuild(cx, operands.subList(0, 6), "FULL");
      }
      if (operands.size() < 7) {
        operands.add(
            rexBuilder.makeLiteral(
                MASKED_DAY, cx.getTypeFactory().createSqlType(SqlTypeName.INTEGER)));
      }
      if (operands.size() < 8) {
        operands.add(
            rexBuilder.makeLiteral(
                MASKED_MONTH, cx.getTypeFactory().createSqlType(SqlTypeName.INTEGER)));
      }
      if (operands.size() < 9) {
        operands.add(
            rexBuilder.makeLiteral(
                MASKED_YEAR, cx.getTypeFactory().createSqlType(SqlTypeName.INTEGER)));
      }
      if (operand0.getType().getFamily() == SqlTypeFamily.DATE) {
        return addModeAndCharCountAndBuild(cx, operands.subList(0, 9), "FULL");
      }
      return rexBuilder.makeNullLiteral(operand0.getType());
    }
  }

  private static RexNode addModeAndCharCountAndBuild(
      SqlRexContext cx, List<RexNode> operands, String mode) {
    RexNode m = cx.getRexBuilder().makeLiteral(mode);
    List<RexNode> ops = new ArrayList<>(operands);
    RexNode charCount =
        cx.getRexBuilder()
            .makeLiteral(-1, cx.getTypeFactory().createSqlType(SqlTypeName.INTEGER), true);
    ops.add(1, m);
    ops.add(2, charCount);
    return cx.getRexBuilder().makeCall(DremioSqlOperatorTable.HIVE_MASK_INTERNAL, ops);
  }

  public static final class HiveMaskFirstLastConvertlet implements SqlRexConvertlet {
    public static final SqlRexConvertlet INSTANCE = new HiveMaskFirstLastConvertlet();

    @Override
    public RexNode convertCall(SqlRexContext cx, SqlCall call) {
      final RexBuilder rexBuilder = cx.getRexBuilder();
      final List<RexNode> operands =
          new ArrayList<>(
              call.getOperandList().stream()
                  .map(cx::convertExpression)
                  .collect(Collectors.toList()));

      final RexNode operand0 = operands.get(0);

      if ((operand0.getType().getFamily() != SqlTypeFamily.CHARACTER)
          && (operand0.getType().getSqlTypeName() != SqlTypeName.DATE)
          && (!SqlTypeName.INT_TYPES.contains(operand0.getType().getSqlTypeName()))) {
        return rexBuilder.makeNullLiteral(operand0.getType());
      }

      if (operands.size() < 2) {
        operands.add(
            rexBuilder.makeLiteral(
                CHAR_COUNT, cx.getTypeFactory().createSqlType(SqlTypeName.INTEGER), true));
      }
      if (operands.size() < 3) {
        operands.add(rexBuilder.makeLiteral(MASKED_UPPERCASE));
      }
      if (operands.size() < 4) {
        operands.add(rexBuilder.makeLiteral(MASKED_LOWERCASE));
      }
      if (operands.size() < 5) {
        operands.add(rexBuilder.makeLiteral(MASKED_DIGIT));
      }
      if (operands.size() < 6) {
        operands.add(rexBuilder.makeLiteral(MASKED_OTHER));
      }
      if (operand0.getType().getFamily() == SqlTypeFamily.CHARACTER) {
        return addModeAndBuild(cx, operands.subList(0, 6), getMode(call.getOperator()));
      }
      if (operands.size() < 7) {
        operands.add(
            rexBuilder.makeLiteral(
                MASKED_NUMBER, cx.getTypeFactory().createSqlType(SqlTypeName.INTEGER)));
      }
      if (SqlTypeName.INT_TYPES.contains(operand0.getType().getSqlTypeName())) {
        return addModeAndBuild(cx, operands.subList(0, 7), getMode(call.getOperator()));
      }
      if (operands.size() < 8) {
        operands.add(
            rexBuilder.makeLiteral(
                MASKED_DAY, cx.getTypeFactory().createSqlType(SqlTypeName.INTEGER)));
      }
      if (operands.size() < 9) {
        operands.add(
            rexBuilder.makeLiteral(
                MASKED_MONTH, cx.getTypeFactory().createSqlType(SqlTypeName.INTEGER)));
      }
      if (operands.size() < 10) {
        operands.add(
            rexBuilder.makeLiteral(
                MASKED_YEAR, cx.getTypeFactory().createSqlType(SqlTypeName.INTEGER)));
      }
      if (operand0.getType().getFamily() == SqlTypeFamily.DATE) {
        return addModeAndBuild(cx, operands.subList(0, 10), getMode(call.getOperator()));
      }
      return rexBuilder.makeNullLiteral(operand0.getType());
    }

    private static RexNode addModeAndBuild(SqlRexContext cx, List<RexNode> operands, String mode) {
      RexNode m = cx.getRexBuilder().makeLiteral(mode);
      List<RexNode> ops = new ArrayList<>(operands);
      ops.add(1, m);
      return cx.getRexBuilder().makeCall(DremioSqlOperatorTable.HIVE_MASK_INTERNAL, ops);
    }
  }

  private static String getMode(SqlOperator op) {
    return op.getName().substring(5).toUpperCase(Locale.ROOT);
  }
}
