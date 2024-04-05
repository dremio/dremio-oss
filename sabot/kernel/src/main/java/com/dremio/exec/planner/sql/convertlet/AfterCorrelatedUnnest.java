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

import java.util.function.Consumer;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Pair;

public final class AfterCorrelatedUnnest {
  private final RelBuilder relBuilder;
  private final RelNode unnest;
  private final CorrelationId correlationId;

  public AfterCorrelatedUnnest(RelBuilder relBuilder, RelNode unnest, CorrelationId correlationId) {
    this.relBuilder = relBuilder;
    this.unnest = unnest;
    this.correlationId = correlationId;
  }

  /**
   * Applies a transformation on the unnest'd array elements.
   *
   * @param transform the callback for tranformation.
   * @return The next builder state.
   */
  public AfterTransform transform(Consumer<RelBuilder> transform) {
    relBuilder.push(unnest);
    transform.accept(relBuilder);

    RelNode transformed = relBuilder.build();

    // We need to remove all the correlationId from the inner subqueries
    // And move it out to the outermost subquery.
    Pair<CorrelationId, RelNode> removalResult = CorrelationIdRemover.remove(transformed);

    // Use the correlation from the previous stage unless we removed one in this stage.
    CorrelationId rewrittenCorrelationId = correlationId;
    if (removalResult.left != null) {
      rewrittenCorrelationId = removalResult.left;
    }

    return new AfterTransform(removalResult.right, rewrittenCorrelationId);
  }

  public AfterTransform noOp() {
    return transform(
        builder -> {
          /*NO OP*/
        });
  }
}
