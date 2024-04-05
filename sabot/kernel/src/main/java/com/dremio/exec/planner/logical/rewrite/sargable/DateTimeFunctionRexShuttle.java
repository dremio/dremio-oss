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
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.fun.SqlDatetimePlusOperator;
import org.apache.calcite.sql.fun.SqlDatetimeSubtractionOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

/** Visitor to walk over the filter condition and rewrite the date/time functions */
public class DateTimeFunctionRexShuttle extends RexShuttle {

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(DateTimeFunctionRexShuttle.class);

  private final RexBuilder rexBuilder;
  private final RelOptCluster relOptCluster;
  private static final int MAX_RECURSIVE = 16;

  public DateTimeFunctionRexShuttle(RelOptCluster relOptCluster) {
    this.relOptCluster = relOptCluster;
    this.rexBuilder = relOptCluster.getRexBuilder();
  }

  @Override
  public RexNode visitCall(RexCall inCall) {
    RexCall call = (RexCall) super.visitCall(inCall);
    return transform(call, 0);
  }

  private RexNode transform(RexCall call, int recursiveLevel) {
    if (recursiveLevel >= MAX_RECURSIVE) {
      logger.info("Maximum recursive call of transform reached: {}.", call);
      return call;
    }
    if (call.operands.size() < 2) {
      return call;
    }

    StandardForm stdForm = StandardForm.build(call);
    if (stdForm == null
        || stdForm.getRhsNode() instanceof RexInputRef
        || (stdForm.getRhsNode() instanceof RexLiteral
            && ((RexLiteral) stdForm.getRhsNode()).getValue2() == null)) {
      return call;
    }

    RexCall lhs = stdForm.getLhsCall();
    RexNode rhs = stdForm.getRhsNode();
    Transformer transformer = null;
    switch (lhs.getOperator().getName().toUpperCase(Locale.ROOT)) {
      case "DATE_ADD":
        transformer =
            new DateAddAndSubShiftTransformer(relOptCluster, stdForm, SARGableRexUtils.DATE_SUB);
        break;
      case "DATE_SUB":
        transformer =
            new DateAddAndSubShiftTransformer(relOptCluster, stdForm, SARGableRexUtils.DATE_ADD);
        break;
      case "DATEDIFF":
        transformer =
            new DateDiffShiftTransformer(relOptCluster, stdForm, SARGableRexUtils.DATE_ADD);
        break;
      case "DATE_DIFF":
        // DATE_DIFF(p1, p2)
        RexNode p1 = lhs.operands.get(0);
        RexNode p2 = lhs.operands.get(1);
        if (p2 instanceof RexLiteral) {
          transformer =
              new DateUDiffDateIntervalShiftTransformer(
                  relOptCluster, stdForm, SARGableRexUtils.DATE_ADD);
        } else if (p2 instanceof RexInputRef
            && p1 instanceof RexLiteral
            && SqlTypeName.INTERVAL_TYPES.contains(rhs.getType().getSqlTypeName())) {
          transformer =
              new DateUDiffDateDateShiftTransformer(
                  relOptCluster, stdForm, SARGableRexUtils.DATE_SUB);
        }
        break;
      case "+":
        if (lhs.op instanceof SqlDatetimePlusOperator) { // TIMESTAMPADD with positive parameter
          transformer =
              new TimestampAddShiftTransformer(
                  relOptCluster, stdForm, SqlStdOperatorTable.MINUS_DATE);
        }
        break;
      case "-":
        if (lhs.op
            instanceof SqlDatetimeSubtractionOperator) { // TIMESTAMPADD with negative parameter
          transformer =
              new TimestampSubShiftTransformer(
                  relOptCluster, stdForm, SqlStdOperatorTable.DATETIME_PLUS);
        }
        break;
      case "DATE_TRUNC":
        transformer =
            new DateTruncRangeTransformer(
                relOptCluster, stdForm, stdForm.getLhsCall().getOperator());
        break;
      case "EXTRACT":
      case "DATE_PART":
        // SARGable for 'YEAR' only
        RexLiteral unitYear = (RexLiteral) lhs.operands.get(0);
        if ("YEAR".equalsIgnoreCase(unitYear.getValue2().toString())) {
          transformer =
              new ExtractAndDatePartRangeTransformer(
                  relOptCluster, stdForm, SARGableRexUtils.DATE_ADD);
        }
        break;
      case "CASE": // "COALESCE":
        transformer = new CaseTransformer(relOptCluster, stdForm);
        break;
      case "AND": // For IN, check further in SARGableRexInTransformer
      case "OR": // For NOT IN
        transformer = new InTransformer(relOptCluster, stdForm);
        break;
      case "LAST_DAY": // LAST_DAY(ts) = '2023-01-31' => DATE_TRUNC('MONTH', ts) = '2023-01-01'
        transformer =
            new LastDayRangeTransformer(relOptCluster, stdForm, SARGableRexUtils.DATE_TRUNC, false);
        break;
      case "NEXT_DAY":
        // NEXT_DAY(ts, 'TU') = '2023-02-07' => DATE_ADD('2023-02-07', -7) <= ts < '2023-02-07'
        // --'2023-02-07' is a Tuesday
        // NEXT_DAY(ts, 'TU') = '2023-02-06' => False --'2023-02-06' !=
        // DATE_ADD(NEXT_DAY('2023-02-06', 'TU'), -7)
        BigDecimal diff =
            minus(
                rexBuilder.makeBigintLiteral(getWeekOfDay(lhs.operands.get(1))),
                extract(TimeUnit.DOW.toString(), rhs));
        if (diff != null) {
          transformer =
              new NextDayTransformer(
                  relOptCluster,
                  stdForm,
                  stdForm.getLhsCall().getOperator(),
                  false,
                  diff.intValue());
        }
        break;
      case "CONVERT_TIMEZONE":
        if (lhs.operands.size() == 3) {
          transformer =
              new ConvertTimezoneShiftTransformer(
                  relOptCluster, stdForm, stdForm.getLhsCall().getOperator());
        }
        break;
      case "MONTHS_BETWEEN":
        if (rhs instanceof RexLiteral) {
          Object obj = ((RexLiteral) rhs).getValue();
          if (obj instanceof BigDecimal) {
            transformer =
                new MonthsBetweenShiftTransformer(
                    relOptCluster, stdForm, SARGableRexUtils.DATE_ADD);
          }
        }
        break;
      case "CAST":
      case "TO_DATE":
        if (lhs.operands.get(0).getType().getSqlTypeName().equals(SqlTypeName.TIMESTAMP)
            && lhs.getType().getSqlTypeName().equals(SqlTypeName.DATE)) {
          transformer =
              new CastAndToDateRangeTransformer(
                  relOptCluster, stdForm, SARGableRexUtils.DATE_TRUNC);
        }
        break;
      default:
        break;
    }

    if (transformer == null) {
      return call;
    }

    RexNode ret = transformer.transform();

    if (ret == null) {
      return call;
    }

    if (!(ret instanceof RexCall)) {
      return ret;
    }

    return transform((RexCall) ret, recursiveLevel + 1);
  }

  private RexNode extract(String keyword, RexNode dateExpr) {
    return rexBuilder.makeCall(SARGableRexUtils.EXTRACT, rexBuilder.makeLiteral(keyword), dateExpr);
  }

  private BigDecimal minus(RexNode n1, RexNode n2) {
    final List<RexNode> reducedValues = new ArrayList<>();
    final List<RexNode> constExpNode = new ArrayList<>();
    constExpNode.add(rexBuilder.makeCall(SqlStdOperatorTable.MINUS, n1, n2));
    RexExecutor rexExecutor = relOptCluster.getPlanner().getExecutor();
    if (rexExecutor != null) {
      rexExecutor.reduce(rexBuilder, constExpNode, reducedValues);
      if (reducedValues.get(0) != null) {
        if (reducedValues.get(0) instanceof RexLiteral) {
          RexLiteral diff = (RexLiteral) reducedValues.get(0);
          return (BigDecimal) diff.getValue();
        }
      }
    }
    return null;
  }

  private BigDecimal getWeekOfDay(RexNode node) {
    if (node instanceof RexLiteral) {
      RexLiteral literal = (RexLiteral) node;
      String keyword = literal.getValue2().toString();
      switch (keyword.toUpperCase(Locale.ROOT)) {
        case "SU":
        case "SUN":
        case "SUNDAY":
          return BigDecimal.valueOf(1);
        case "MO":
        case "MON":
        case "MONDAY":
          return BigDecimal.valueOf(2);
        case "TU":
        case "TUE":
        case "TUESDAY":
          return BigDecimal.valueOf(3);
        case "WE":
        case "WED":
        case "WEDNESDAY":
          return BigDecimal.valueOf(4);
        case "TH":
        case "THU":
        case "THURSDAY":
          return BigDecimal.valueOf(5);
        case "FR":
        case "FRI":
        case "FRIDAY":
          return BigDecimal.valueOf(6);
        case "SA":
        case "SAT":
        case "SATURDAY":
          return BigDecimal.valueOf(7);
        default:
          return null;
      }
    }
    return null;
  }
}
