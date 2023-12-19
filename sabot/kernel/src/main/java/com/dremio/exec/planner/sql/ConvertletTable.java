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

import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFamily;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql2rel.ReflectiveConvertletTable;
import org.apache.calcite.sql2rel.SqlRexContext;
import org.apache.calcite.sql2rel.SqlRexConvertlet;
import org.apache.calcite.sql2rel.StandardConvertletTable;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.planner.sql.HiveMaskConvertlets.HiveMaskConvertlet;
import com.dremio.exec.planner.sql.HiveMaskConvertlets.HiveMaskFirstLastConvertlet;
import com.dremio.exec.planner.sql.HiveMaskConvertlets.HiveMaskHashConvertlet;
import com.dremio.sabot.exec.context.ContextInformation;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class ConvertletTable extends ReflectiveConvertletTable {
  /*
   * Default convertlet
   *
   * Useful to override Calcite behavior (which might uses Calcite internal
   * constructs not available to Dremio)
   */
  private static final SqlRexConvertlet DEFAULT_CONVERTLET = new SqlRexConvertlet() {
    @Override
    public RexNode convertCall(SqlRexContext cx, SqlCall call) {
      final List<SqlNode> operands = call.getOperandList();
      final List<RexNode> exprs = new LinkedList<>();

      for (SqlNode node: operands) {
        exprs.add(cx.convertExpression(node));
      }

      final RelDataType returnType = cx.getRexBuilder().deriveReturnType(call.getOperator(), exprs);
      return cx.getRexBuilder().makeCall(returnType, call.getOperator(), exprs);
    }
  };

  public ConvertletTable(ContextInformation contextInformation, boolean ieee756DivideBehavior) {
    super();
    addAlias(SqlStdOperatorTable.LOCALTIME, SqlStdOperatorTable.CURRENT_TIME);
    addAlias(SqlStdOperatorTable.LOCALTIMESTAMP, SqlStdOperatorTable.CURRENT_TIMESTAMP);
    addAlias(DremioSqlOperatorTable.NOW, SqlStdOperatorTable.CURRENT_TIMESTAMP);

    if (ieee756DivideBehavior) {
      registerOp(SqlStdOperatorTable.DIVIDE, IEEE754DivideConvertlet.INSTANCE);
    }
    registerOp(SqlStdOperatorTable.TIMESTAMP_DIFF, DEFAULT_CONVERTLET);
    registerOp(SqlStdOperatorTable.EQUALS, EqualityConvertlet.INSTANCE);
    registerOp(SqlStdOperatorTable.NOT_EQUALS, EqualityConvertlet.INSTANCE);
    registerOp(SqlStdOperatorTable.IS_DISTINCT_FROM, EqualityConvertlet.INSTANCE);
    registerOp(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM, EqualityConvertlet.INSTANCE);
    registerOp(DremioSqlOperatorTable.FLATTEN, FlattenConvertlet.INSTANCE);
    registerOp(DremioSqlOperatorTable.DATE_PART, SqlDatePartOperator.CONVERTLET);
    registerOp(SqlStdOperatorTable.MINUS, new SqlRexConvertlet() {
      @Override
      public RexNode convertCall(SqlRexContext cx, SqlCall call) {
        final RexCall e =
            (RexCall) ConvertletTable.this.convertCall(cx, call,
                call.getOperator());
        switch (e.getOperands().get(0).getType().getSqlTypeName()) {
        case DATE:
        case TIME:
        case TIMESTAMP:
          // if DATETIME - INTERVAL, then use special logic
          // Note that although MINUS_DATE special operator is mapped to
          // "-" function, this is also required for JDBC pushdown as special
          // care is taken to make valid SQL depending on dialect when unparsing
          // expression
          if (e.getOperands().size() == 2) {
            switch (e.getOperands().get(1).getType().getSqlTypeName().getFamily()) {
            case INTERVAL_DAY_TIME:
            case INTERVAL_YEAR_MONTH:
              final RexBuilder rexBuilder = cx.getRexBuilder();
              return rexBuilder.makeCall(
                  e.getType(), SqlStdOperatorTable.MINUS_DATE, e.getOperands());
            default:
              break;
            }
          }
          break;
        default:
          break;
        }
        return e;
      }
    });
    // For normalizing TimestampAdd to Datetime_Plus
    registerOp(SqlStdOperatorTable.TIMESTAMP_ADD, new SqlRexConvertlet() {
        @Override
        public RexNode convertCall(SqlRexContext cx, SqlCall call) {
          // TIMESTAMPADD(unit, count, timestamp)
          //  => timestamp + count * INTERVAL '1' UNIT
          final RexBuilder rexBuilder = cx.getRexBuilder();
          final SqlLiteral unitLiteral = call.operand(0);
          final TimeUnit unit = unitLiteral.symbolValue(TimeUnit.class);
          switch (unit) {
            // TODO(DX-11268): Support sub-second intervals with TIMESTAMPADD.
            case MILLISECOND:
            case MICROSECOND:
            case NANOSECOND:
              throw UserException.unsupportedError()
                .message("TIMESTAMPADD function supports the following time units: YEAR, QUARTER, MONTH, WEEK, DAY, HOUR, MINUTE, SECOND")
                .build();
            default:
              break;
          }
          final RexNode timestampNode = cx.convertExpression(call.operand(2));
          final RexNode multiplyNode = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY,
            rexBuilder.makeIntervalLiteral(unit.multiplier,
              new SqlIntervalQualifier(unit, null, unitLiteral.getParserPosition())),
            cx.convertExpression(call.operand(1)));

          return rexBuilder.makeCall(SqlStdOperatorTable.DATETIME_PLUS,
            timestampNode,
            multiplyNode);

        }
    });

    // DX-82919: these convertlets need to be here until catalog code updates the AT syntax to evaluate all functions:
    registerOp(SqlStdOperatorTable.CURRENT_DATE, new ChronoConvertlets.CurrentDateConvertlet(contextInformation));
    registerOp(SqlStdOperatorTable.CURRENT_TIME, new ChronoConvertlets.CurrentTimeConvertlet(contextInformation));
    registerOp(SqlStdOperatorTable.CURRENT_TIMESTAMP, new ChronoConvertlets.CurrentTimeStampConvertlet(contextInformation));

    registerOp(DremioSqlOperatorTable.HIVE_MASK_HASH, HiveMaskHashConvertlet.INSTANCE);
    registerOp(DremioSqlOperatorTable.HIVE_MASK, HiveMaskConvertlet.INSTANCE);
    registerOp(DremioSqlOperatorTable.HIVE_MASK_FIRST_N, HiveMaskFirstLastConvertlet.INSTANCE);
    registerOp(DremioSqlOperatorTable.HIVE_MASK_LAST_N, HiveMaskFirstLastConvertlet.INSTANCE);
    registerOp(DremioSqlOperatorTable.HIVE_MASK_SHOW_FIRST_N, HiveMaskFirstLastConvertlet.INSTANCE);
    registerOp(DremioSqlOperatorTable.HIVE_MASK_SHOW_LAST_N, HiveMaskFirstLastConvertlet.INSTANCE);
    registerOp(DremioSqlOperatorTable.LOG2, Log2Convertlet.INSTANCE);
    registerOp(DremioSqlOperatorTable.SPACE, SpaceConvertlet.INSTANCE);
    registerOp(DremioSqlOperatorTable.SECOND, DatePartFunctionsConvertlet.SECOND_INSTANCE);
    registerOp(DremioSqlOperatorTable.MINUTE, DatePartFunctionsConvertlet.MINUTE_INSTANCE);
    registerOp(DremioSqlOperatorTable.HOUR, DatePartFunctionsConvertlet.HOUR_INSTANCE);
    registerOp(DremioSqlOperatorTable.DAY, DatePartFunctionsConvertlet.DAY_INSTANCE);
    registerOp(DremioSqlOperatorTable.MONTH, DatePartFunctionsConvertlet.MONTH_INSTANCE);
    registerOp(DremioSqlOperatorTable.YEAR, DatePartFunctionsConvertlet.YEAR_INSTANCE);

    // These convertlets support ARRAY Function coercion:
    // This can't be made into a function convertlet, since the type changes.
    registerOp(DremioSqlOperatorTable.ARRAY_APPEND, ArrayFunctionCoercionConvertlets.ArrayAndElement.INSTANCE);
    registerOp(DremioSqlOperatorTable.ARRAY_PREPEND, ArrayFunctionCoercionConvertlets.ElementAndArray.INSTANCE);
    registerOp(DremioSqlOperatorTable.ARRAY_CONTAINS, ArrayFunctionCoercionConvertlets.ArrayAndElement.INSTANCE);
    registerOp(DremioSqlOperatorTable.ARRAY_REMOVE, ArrayFunctionCoercionConvertlets.ArrayAndElement.INSTANCE);
    registerOp(DremioSqlOperatorTable.ARRAY_CONCAT, ArrayFunctionCoercionConvertlets.ArrayAndArray.INSTANCE);
    registerOp(DremioSqlOperatorTable.SET_UNION, ArrayFunctionCoercionConvertlets.ArrayAndArray.INSTANCE);
    registerOp(DremioSqlOperatorTable.ARRAY_INTERSECTION, ArrayFunctionCoercionConvertlets.ArrayAndArray.INSTANCE);
  }

  /*
   * Lookup the hash table to see if we have a custom convertlet for a given
   * operator, if we don't use StandardConvertletTable.
   */
  @Override
  public SqlRexConvertlet get(SqlCall call) {
    SqlRexConvertlet convertlet = super.get(call);
    if (convertlet != null) {
      return convertlet;
    }
    return StandardConvertletTable.INSTANCE.get(call);
  }

  /** Converts a {@link SqlCall} to a {@link RexCall} with a perhaps different
   * operator. */
  private RexNode convertCall(
      SqlRexContext cx,
      SqlCall call,
      SqlOperator op) {
    final List<SqlNode> operands = call.getOperandList();
    final RexBuilder rexBuilder = cx.getRexBuilder();
    final SqlOperandTypeChecker.Consistency consistency =
        op.getOperandTypeChecker() == null
            ? SqlOperandTypeChecker.Consistency.NONE
            : op.getOperandTypeChecker().getConsistency();
    final List<RexNode> exprs =
        convertExpressionList(cx, operands, consistency);
    RelDataType type = rexBuilder.deriveReturnType(op, exprs);
    return rexBuilder.makeCall(type, op, RexUtil.flatten(exprs, op));
  }

  private static List<RexNode> convertExpressionList(SqlRexContext cx,
      List<SqlNode> nodes, SqlOperandTypeChecker.Consistency consistency) {
    final List<RexNode> exprs = Lists.newArrayList();
    for (SqlNode node : nodes) {
      exprs.add(cx.convertExpression(node));
    }
    if (exprs.size() > 1) {
      final RelDataType type =
          consistentType(cx, consistency, RexUtil.types(exprs));
      if (type != null) {
        final List<RexNode> oldExprs = Lists.newArrayList(exprs);
        exprs.clear();
        for (RexNode expr : oldExprs) {
          exprs.add(cx.getRexBuilder().ensureType(type, expr, true));
        }
      }
    }
    return exprs;
  }

  private static RelDataType consistentType(SqlRexContext cx,
      SqlOperandTypeChecker.Consistency consistency, List<RelDataType> types) {
    switch (consistency) {
    case COMPARE:
      final Set<RelDataTypeFamily> families =
          Sets.newHashSet(RexUtil.families(types));
      if (families.size() < 2) {
        // All arguments are of same family. No need for explicit casts.
        return null;
      }
      final List<RelDataType> nonCharacterTypes = Lists.newArrayList();
      for (RelDataType type : types) {
        if (type.getFamily() != SqlTypeFamily.CHARACTER) {
          nonCharacterTypes.add(type);
        }
      }
      if (!nonCharacterTypes.isEmpty()) {
        final int typeCount = types.size();
        types = nonCharacterTypes;
        if (nonCharacterTypes.size() < typeCount) {
          final RelDataTypeFamily family =
              nonCharacterTypes.get(0).getFamily();
          if (family instanceof SqlTypeFamily) {
            // The character arguments might be larger than the numeric
            // argument. Give ourselves some headroom.
            switch ((SqlTypeFamily) family) {
            case INTEGER:
            case NUMERIC:
              nonCharacterTypes.add(
                  cx.getTypeFactory().createSqlType(SqlTypeName.BIGINT));
              break;
            default:
              break;
            }
          }
        }
      }
      // fall through
    case LEAST_RESTRICTIVE:
      return cx.getTypeFactory().leastRestrictive(types);
    default:
      return null;
    }
  }
}
