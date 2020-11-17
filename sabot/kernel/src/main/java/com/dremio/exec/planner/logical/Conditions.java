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
package com.dremio.exec.planner.logical;

import static com.dremio.exec.planner.sql.handlers.RexFieldAccessUtils.STRUCTURED_WRAPPER;

import java.util.Set;

import org.apache.calcite.rel.rules.PushProjector;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.PredicateImpl;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import com.google.common.collect.ImmutableSet;

public final class Conditions {

  /**
   * When pushing project past join or filter, should preserve CASE statements.  This is because, if we push down
   * and split up the project with case statement, it could lead to invalid plan.  For example,
   *
   * Project:  case(predicate(X), ITEM(X, 0), Y)
   * Join/Filter
   *
   * Then if we do not preserve CASE but only preserve ITEM, we get the following:
   * Project: case(predicate($1), $2, $3)
   * Join/Filter
   * Project: X, ITEM(X, 0), Y
   *
   * The above plan is wrong, since we do not want ITEM(X, 0) to be evaluated unless predicate(X) is true.
   * For instance, predicate(X) could be IS_LIST(X) and if X is not a list, we shouldn't do ITEM(X,0).
   */
  public static final PushProjector.ExprCondition PRESERVE_ITEM_CASE = new PushProjectorExprCondition();

  private static class PushProjectorExprCondition extends PredicateImpl<RexNode>
    implements PushProjector.ExprCondition {
    @Override
    public boolean test(RexNode expr) {
      if (expr instanceof RexCall) {
        RexCall call = (RexCall)expr;
        return ("item".equals(call.getOperator().getName().toLowerCase())
            || "case".equals(call.getOperator().getName().toLowerCase())
            || STRUCTURED_WRAPPER.getName().equalsIgnoreCase(call.getOperator().getName())
        );
      }
      return false;
    }
  };

  /**
   * Avoid decomposing any expression where we might change the short circuit behavior, similar to
   * the preserve case above, just covers and & or as additional potential short circuit operators.
   */
  public static final PushProjector.ExprCondition SHORT_CIRCUIT_AND_ITEM = new OperatorExprCondition(
      ImmutableSet.<SqlOperator>of(
        SqlStdOperatorTable.CASE,
        SqlStdOperatorTable.ITEM,
        SqlStdOperatorTable.AND,
        SqlStdOperatorTable.OR
        ));

  private static class OperatorExprCondition extends PredicateImpl<RexNode>
    implements PushProjector.ExprCondition {
    private final Set<SqlOperator> operatorSet;

    OperatorExprCondition(Iterable<? extends SqlOperator> operatorSet) {
      this.operatorSet = ImmutableSet.copyOf(operatorSet);
    }

    @Override
    public boolean test(RexNode expr) {
      return expr instanceof RexCall
          && operatorSet.contains(((RexCall) expr).getOperator());
    }
  }
}
