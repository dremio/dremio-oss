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

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;

import com.google.common.collect.Lists;


public class FilterJoinRulesUtil {
  /** Predicate that always returns true for any filter in OUTER join, and only true
   * for EQUAL or IS_NOT_DISTINCT_FROM over RexInputRef in INNER join. With this predicate,
   * the filter expression that return true will be kept in the JOIN OP.
   * Example:  INNER JOIN,   L.C1 = R.C2 and L.C3 + 100 = R.C4 + 100 will be kepted in JOIN.
   *                         L.C5 < R.C6 will be pulled up into Filter above JOIN.
   *           OUTER JOIN,   Keep any filter in JOIN.
  */
  public static final FilterJoinRule.Predicate EQUAL_IS_NOT_DISTINCT_FROM =
    (join, joinType, exp) -> {
      if (joinType != JoinRelType.INNER) {
        return true;  // In OUTER join, we could not pull-up the filter.
                      // All we can do is keep the filter with JOIN, and
                      // then decide whether the filter could be pushed down
                      // into LEFT/RIGHT.
      }

      List<RexNode> tmpLeftKeys = Lists.newArrayList();
      List<RexNode> tmpRightKeys = Lists.newArrayList();
      List<RelDataTypeField> sysFields = Lists.newArrayList();
      List<Integer> filterNulls = Lists.newArrayList();

      try {
        RexNode remaining = RelOptUtil.splitJoinCondition(sysFields, join.getLeft(), join.getRight(),
          exp, tmpLeftKeys, tmpRightKeys, filterNulls, null);

        // if the remaining condition can be pushed down (i.e. a = 1), we should not push the condition above
        ArrayList<RexNode> conditions = new ArrayList<>();
        ArrayList<RexNode> leftConditions = new ArrayList<>();
        ArrayList<RexNode> rightConditions = new ArrayList<>();

        conditions.add(remaining);
        RelOptUtil.classifyFilters(join, conditions, join.getJoinType(), false, true, true, conditions, leftConditions, rightConditions);

        // if the conditions list is empty, that means we were able to push the condition below the join
        return conditions.size() == 0 || remaining.isAlwaysTrue();
      } catch (AssertionError ex) {
        return false;
      }
    };

  private FilterJoinRulesUtil() {
  }
}
