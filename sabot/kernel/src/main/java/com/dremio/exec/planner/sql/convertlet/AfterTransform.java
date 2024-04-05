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

import com.google.common.collect.ImmutableList;
import java.util.function.BiFunction;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;

public final class AfterTransform {
  private final RelNode transformed;
  private final CorrelationId correlationId;

  public AfterTransform(RelNode transformed, CorrelationId correlationId) {
    this.transformed = transformed;
    this.correlationId = correlationId;
  }

  /**
   * Aggregates the transformed array elements back into a single value.
   *
   * @param subqueryCreateFunction the function for aggregating.
   * @return The aggregated value.
   */
  public RexSubQuery aggregate(
      BiFunction<RelNode, CorrelationId, RexSubQuery> subqueryCreateFunction) {
    RexSubQuery rexSubQuery = subqueryCreateFunction.apply(transformed, correlationId);
    return rexSubQuery;
  }

  public RexSubQuery exists() {
    return aggregate(RexSubQuery::exists);
  }

  public RexSubQuery array() {
    return aggregate(RexSubQuery::array);
  }

  public RexSubQuery in(RexNode needle) {
    return aggregate(
        (relNode, correlationId1) ->
            RexSubQuery.in(relNode, ImmutableList.of(needle), correlationId1));
  }
}
