/*
 * Copyright (C) 2017 Dremio Corporation
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

import java.math.BigDecimal;

import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.tools.RelBuilderFactory;

public class RelBuilder extends org.apache.calcite.tools.RelBuilder {

  /** Creates a {@link RelBuilderFactory}, a partially-created RelBuilder.
   * Just add a {@link RelOptCluster} and a {@link RelOptSchema} */
  public static RelBuilderFactory proto(final Context context) {
    return new RelBuilderFactory() {
      @Override
      public RelBuilder create(RelOptCluster cluster, RelOptSchema schema) {
        return new RelBuilder(context, cluster, schema);
      }
    };
  }

  protected RelBuilder(Context context, RelOptCluster cluster, RelOptSchema relOptSchema) {
    super(context, cluster, relOptSchema);
  }

  /** Creates a relational expression that reads from an input and throws
   *  all of the rows away.
   */
  @Override
  public RelBuilder empty() {
    final Frame frame = stack.pop();
    final RelNode input;
    // If the rel that we are limiting the output of a rel, we should just add a limit 0 on top.
    // If the rel that we are limiting is a Filter replace it as well since Filter does not
    // change the row type.
    if (!(frame.rel instanceof Filter)) {
      input = frame.rel;
    } else {
      input = frame.rel.getInput(0);
    }
    final RelNode sort = sortFactory.createSort(input, RelCollations.EMPTY,
      frame.rel.getCluster().getRexBuilder().makeExactLiteral(BigDecimal.valueOf(0)),
      frame.rel.getCluster().getRexBuilder().makeExactLiteral(BigDecimal.valueOf(0)));
    push(sort);
    return this;
  }
}
