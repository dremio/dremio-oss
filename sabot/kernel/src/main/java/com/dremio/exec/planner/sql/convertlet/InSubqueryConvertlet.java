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
package com.dremio.exec.planner.sql.convertlet;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

/** Converts "x IN (1, 2, ...)" to "x=1 OR x=2 OR ...". */
public final class InSubqueryConvertlet extends RexSubqueryConvertlet {
  /**
   * If we don't need the number of expression needed for the threshhold, Then just let it expand
   * out as a decorrelated subquery.
   */
  private final long inSubQueryThreshold;

  public InSubqueryConvertlet(long inSubQueryThreshold) {
    this.inSubQueryThreshold = inSubQueryThreshold;
  }

  @Override
  public boolean matchesSubquery(RexSubQuery rexSubQuery) {
    if (rexSubQuery.getOperator() != SqlStdOperatorTable.IN) {
      return false;
    }

    if (rexSubQuery.operands.size() != 1) {
      return false;
    }

    // If not a values rel, then it's a correlated subquery
    if (!(rexSubQuery.rel instanceof Values)) {
      return false;
    }

    Values values = (Values) rexSubQuery.rel;
    if (values.getRowType().getFieldList().size() != 1) {
      // The values rel needs to be a single column
      return false;
    }

    if (values.getTuples().size() >= inSubQueryThreshold) {
      return false;
    }

    return true;
  }

  @Override
  public RexNode convertSubquery(ConvertletContext cx, RexSubQuery rexSubQuery) {
    RexNode needle = rexSubQuery.operands.get(0);

    Values values = (Values) rexSubQuery.rel;
    List<RexLiteral> haystack =
        values.getTuples().stream().map(tuple -> tuple.get(0)).collect(Collectors.toList());

    RexBuilder rexBuilder = cx.getRexBuilder();
    return RexUtil.composeDisjunction(
        rexBuilder,
        haystack.stream()
            .map(x -> rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, needle, x))
            .collect(Collectors.toList()));
  }
}
