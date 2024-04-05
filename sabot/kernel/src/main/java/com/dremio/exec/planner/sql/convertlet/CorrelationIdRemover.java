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

import com.dremio.exec.planner.StatelessRelShuttleImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.util.Pair;

public final class CorrelationIdRemover {

  public static Pair<CorrelationId, RelNode> remove(RelNode relNode) {
    RelShuttle shuttle = new RelShuttle();
    RelNode rewrittenRelNode = relNode.accept(shuttle);

    return Pair.of(shuttle.rexShuttle.correlationIdRemoved, rewrittenRelNode);
  }

  public static Pair<CorrelationId, RexNode> remove(RexNode rexNode) {
    RexShuttle shuttle = new RexShuttle();
    RexNode rewrittenRexNode = rexNode.accept(shuttle);

    return Pair.of(shuttle.correlationIdRemoved, rewrittenRexNode);
  }

  private static final class RexShuttle extends org.apache.calcite.rex.RexShuttle {
    private CorrelationId correlationIdRemoved;

    public RexShuttle() {
      correlationIdRemoved = null;
    }

    @Override
    public RexNode visitSubQuery(RexSubQuery subQuery) {
      if (subQuery.correlationId == null) {
        return subQuery;
      }

      RexSubQuery rewrittenSubQuery;
      switch (subQuery.op.getKind()) {
        case IN:
          rewrittenSubQuery = RexSubQuery.in(subQuery.rel, subQuery.operands, null);
          break;

        case EXISTS:
          rewrittenSubQuery = RexSubQuery.exists(subQuery.rel, null);
          break;

        case ARRAY_QUERY_CONSTRUCTOR:
          rewrittenSubQuery = RexSubQuery.array(subQuery.rel, null);
          break;

        default:
          throw new UnsupportedOperationException("Unsupported subquery kind: " + subQuery.op);
      }

      correlationIdRemoved = subQuery.correlationId;
      return rewrittenSubQuery;
    }
  }

  private static final class RelShuttle extends StatelessRelShuttleImpl {
    public final RexShuttle rexShuttle;

    private RelShuttle() {
      rexShuttle = new RexShuttle();
    }

    @Override
    public RelNode visit(LogicalFilter filter) {
      RelNode rewrittenInput = filter.getInput().accept(this);
      RexNode rewrittenCondition = filter.getCondition().accept(rexShuttle);
      if (rexShuttle.correlationIdRemoved == null) {
        return super.visit(filter);
      }

      RelNode rewrittenFilter =
          filter.copy(filter.getTraitSet(), rewrittenInput, rewrittenCondition);
      return rewrittenFilter;
    }
  }
}
