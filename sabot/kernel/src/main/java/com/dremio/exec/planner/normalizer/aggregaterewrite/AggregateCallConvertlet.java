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
package com.dremio.exec.planner.normalizer.aggregaterewrite;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexNode;

/** Convertlet for rewriting an AggregateCall. */
public abstract class AggregateCallConvertlet {
  protected AggregateCallConvertlet() {}

  public boolean matches(ConvertletContext context) {
    return matches(context.getOldCall());
  }

  protected abstract boolean matches(AggregateCall aggregateCall);

  /**
   * We are going to be rewriting a query plan in this form:
   *
   * <p>Aggregate (oldAggRel + oldCall) Project (inputExprs)
   *
   * <p>To something in this form:
   *
   * <p>Project <- return value will go here Aggregate (newCalls) Project (inputExprs)
   */
  public abstract RexNode convertCall(ConvertletContext context);
}
