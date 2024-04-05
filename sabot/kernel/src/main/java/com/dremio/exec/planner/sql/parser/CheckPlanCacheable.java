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
package com.dremio.exec.planner.sql.parser;

import com.dremio.exec.ops.QueryContext;
import com.google.common.collect.Lists;
import java.util.List;
import org.apache.calcite.sql.SqlNode;

public class CheckPlanCacheable {
  private QueryContext context;
  private SqlNode node;
  private static List<String> timeOperators = Lists.newArrayList();
  private static List<String> randOperators = Lists.newArrayList();

  static {
    timeOperators.add("NOW");
    timeOperators.add("LOCALTIME");
    timeOperators.add("LOCALTIMESTAMP");
    timeOperators.add("CURRENT_TIMESTAMP");
    timeOperators.add("CURRENT_TIME");
    timeOperators.add("CURRENT_DATE");
    timeOperators.add("UNIX_TIMESTAMP");
    timeOperators.add("TIMEOFDAY");
    randOperators.add("RAND");
    randOperators.add("RANDOM");
  }

  private CheckPlanCacheable(QueryContext context, SqlNode node) {
    this.context = context;
    this.node = node;
  }

  public static CheckPlanCacheable create(QueryContext context, SqlNode node) {
    return new CheckPlanCacheable(context, node);
  }

  public boolean isPlanCacheable() {
    return !checkTimeOperators(node) && !checkRandOperators(node);
  }

  private boolean checkTimeOperators(SqlNode sqlNode) {
    final ExprFinder timeOperatorFinder = new ExprFinder(TIME_FUNCTION_CONDITION);
    sqlNode.accept(timeOperatorFinder);
    return timeOperatorFinder.find();
  }

  private boolean checkRandOperators(SqlNode sqlNode) {
    final ExprFinder randOperatorFinder = new ExprFinder(RAND_FUNCTION_CONDITION);
    sqlNode.accept(randOperatorFinder);
    return randOperatorFinder.find();
  }

  private final SqlNodeCondition TIME_FUNCTION_CONDITION = new FindFunCondition(timeOperators);
  private final SqlNodeCondition RAND_FUNCTION_CONDITION = new FindFunCondition(randOperators);
}
