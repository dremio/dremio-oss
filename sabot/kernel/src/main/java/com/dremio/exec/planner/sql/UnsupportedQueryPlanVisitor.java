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

import static com.dremio.exec.planner.sql.DremioSqlOperatorTable.ARRAY_AGG;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.planner.StatelessRelShuttleImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.sql.type.SqlTypeName;

/** Visitor that checks to see if the query plan is unsupported. */
public final class UnsupportedQueryPlanVisitor extends StatelessRelShuttleImpl {
  private static final UnsupportedQueryPlanVisitor INSTANCE = new UnsupportedQueryPlanVisitor();

  private UnsupportedQueryPlanVisitor() {}

  @Override
  public RelNode visit(LogicalSort sort) {
    checkOrderByArray(sort);
    return super.visit(sort);
  }

  @Override
  public RelNode visit(LogicalAggregate aggregate) {
    checkArrayAggWithRollup(aggregate);
    return super.visit(aggregate);
  }

  public static void checkForUnsupportedQueryPlan(RelNode queryPlan) {
    queryPlan.accept(INSTANCE);
  }

  private static void checkOrderByArray(LogicalSort sort) {
    boolean orderingByArray =
        sort.getSortExps().stream()
            .anyMatch(node -> node.getType().getSqlTypeName() == SqlTypeName.ARRAY);
    if (orderingByArray) {
      throw UserException.planError()
          .message("Sorting by arrays is not supported.")
          .buildSilently();
    }
  }

  private static void checkArrayAggWithRollup(LogicalAggregate aggregate) {
    if (aggregate.getGroupSets().size() > 1) {
      if (aggregate.getAggCallList().stream().anyMatch(x -> ARRAY_AGG.equals(x.getAggregation()))) {
        throw UserException.planError()
            .message("ARRAY_AGG with ROLLUP is currently not supported.")
            .buildSilently();
      }
    }
  }
}
