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

import java.util.Comparator;
import java.util.GregorianCalendar;
import java.util.LinkedList;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

/**
 *
 *
 * <pre>
 * DATE_TRUNC('month', ts) IN ('2023-02-01','2023-03-01','2023-04-01','2023-05-01')
 * =>
 * OR(
 *   AND(>=($0, 2023-02-01 00:00:00), <($0, 2023-03-01 00:00:00)),
 *   AND(>=($0, 2023-03-01 00:00:00), <($0, 2023-04-01 00:00:00)),
 *   AND(>=($0, 2023-04-01 00:00:00), <($0, 2023-05-01 00:00:00)),
 *   AND(>=($0, 2023-05-01 00:00:00), <($0, 2023-06-01 00:00:00))
 * )])
 * => (Merge intervals)
 * AND(>=($0, 2023-02-01 00:00:00), <($0, 2023-06-01 00:00:00))
 * </pre>
 */
public class InTransformer implements Transformer {
  private final RexBuilder rexBuilder;
  private final RexCall lhsCall;

  InTransformer(RelOptCluster relOptCluster, StandardForm standardForm) {
    this.rexBuilder = relOptCluster.getRexBuilder();
    this.lhsCall = standardForm.getLhsCall();
  }

  @Override
  public RexNode transform() {
    List<DateInterval> intervals = new LinkedList<>();
    Boolean isIn = checkDateIntervals(intervals);
    if (isIn != null && intervals.size() > 0) {
      LinkedList<DateInterval> merged = new LinkedList<>();
      intervals.sort(Comparator.comparing(a -> a.begin));
      for (DateInterval interval : intervals) {
        if (merged.isEmpty() || merged.getLast().end < interval.begin) {
          merged.add(interval);
        } else {
          if (merged.getLast().end < interval.end) {
            merged.getLast().end = interval.end;
            merged.getLast().endLiteral = interval.endLiteral;
          }
        }
      }
      if (merged.size() != intervals.size()) {
        RexNode column = getColumn();
        if (column == null) {
          return null;
        }
        if (isIn) {
          List<RexNode> nodes = new LinkedList<>();
          for (DateInterval interval : merged) {
            nodes.add(
                SARGableRexUtils.and(
                    SARGableRexUtils.gte(column, interval.beginLiteral, rexBuilder),
                    SARGableRexUtils.lt(column, interval.endLiteral, rexBuilder),
                    rexBuilder));
          }
          if (nodes.size() == 1) {
            return nodes.get(0);
          }
          return SARGableRexUtils.or(nodes, rexBuilder);
        } else {
          List<RexNode> nodes = new LinkedList<>();
          nodes.add(SARGableRexUtils.lt(column, merged.get(0).beginLiteral, rexBuilder));
          for (int i = 0; i < merged.size() - 1; i++) {
            DateInterval interval = merged.get(i);
            DateInterval intervalNext = merged.get(i + 1);
            nodes.add(
                SARGableRexUtils.and(
                    SARGableRexUtils.gte(column, interval.endLiteral, rexBuilder),
                    SARGableRexUtils.lt(column, intervalNext.beginLiteral, rexBuilder),
                    rexBuilder));
          }
          nodes.add(SARGableRexUtils.gte(column, merged.getLast().endLiteral, rexBuilder));
          if (nodes.size() == 1) {
            return nodes.get(0);
          }
          return SARGableRexUtils.or(nodes, rexBuilder);
        }
      }
    }
    return null;
  }

  RexNode getColumn() {
    return findRexInputRef(lhsCall);
  }

  private RexInputRef findRexInputRef(RexCall call) {
    for (RexNode param : call.operands) {
      if (param instanceof RexInputRef) {
        return (RexInputRef) param;
      } else if (param instanceof RexCall) {
        RexInputRef res = findRexInputRef((RexCall) param);
        if (res != null) {
          return res;
        }
      }
    }
    return null;
  }

  /**
   * Check the list of DateIntervals to merge, if any of the intervals contains a non-RexLiteral
   * begin or end, just return null
   *
   * @return True - IN(...); False - NOT IN(...)
   */
  private Boolean checkDateIntervals(List<DateInterval> output) {
    RexCall call = lhsCall;
    RexNode column = getColumn();
    if (column == null) {
      return null;
    }
    if (call.getKind().equals(SqlKind.OR)) {
      for (int i = 0; i < call.operands.size(); i++) {
        RexNode param = call.operands.get(i);
        if (!(param instanceof RexCall)) {
          continue;
        }
        RexCall paramCall = (RexCall) param;
        if (!paramCall.getKind().equals(SqlKind.AND) || paramCall.operands.size() != 2) {
          break;
        }
        RexCall beginCond = (RexCall) paramCall.operands.get(0);
        RexCall endCond = (RexCall) paramCall.operands.get(1);
        if (!beginCond.operands.get(0).equals(column) || !endCond.operands.get(0).equals(column)) {
          break;
        }
        RexLiteral beginLiteral = (RexLiteral) beginCond.operands.get(1);
        RexLiteral endLiteral = (RexLiteral) endCond.operands.get(1);
        DateInterval interval = (new DateInterval(beginLiteral, endLiteral));
        if (interval.isNotValid()) {
          break;
        }
        output.add(interval);
      }
      return true;
    } else if (call.getKind().equals(SqlKind.AND)) {
      for (int i = 0; i < call.operands.size(); i++) {
        RexNode param = call.operands.get(i);
        if (!(param instanceof RexCall)) {
          continue;
        }
        RexCall paramCall = (RexCall) param;
        if (!paramCall.getKind().equals(SqlKind.OR) || paramCall.operands.size() != 2) {
          break;
        }
        RexCall beginCond = (RexCall) paramCall.operands.get(0);
        RexCall endCond = (RexCall) paramCall.operands.get(1);
        if (!beginCond.operands.get(0).equals(column) || !endCond.operands.get(0).equals(column)) {
          break;
        }
        RexLiteral beginLiteral = (RexLiteral) beginCond.operands.get(1);
        RexLiteral endLiteral = (RexLiteral) endCond.operands.get(1);
        DateInterval interval = (new DateInterval(beginLiteral, endLiteral));
        if (interval.isNotValid()) {
          break;
        }
        output.add(interval);
      }
      return false;
    }
    return null;
  }

  private static class DateInterval {
    private Long begin;
    private Long end;
    private RexLiteral beginLiteral;
    private RexLiteral endLiteral;

    public DateInterval(RexLiteral beginLiteral, RexLiteral endLiteral) {
      this.beginLiteral = beginLiteral;
      this.endLiteral = endLiteral;
      if (beginLiteral.getValue() instanceof GregorianCalendar) {
        begin = ((GregorianCalendar) beginLiteral.getValue()).getTimeInMillis();
      }
      if (endLiteral.getValue() instanceof GregorianCalendar) {
        end = ((GregorianCalendar) endLiteral.getValue()).getTimeInMillis();
      }
    }

    boolean isNotValid() {
      if (begin == null || end == null) {
        return true;
      }
      return begin > end;
    }
  }
}
