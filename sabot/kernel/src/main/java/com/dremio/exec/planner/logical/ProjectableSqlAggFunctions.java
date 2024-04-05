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

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;

/** Helper methods to find if a call or a aggregate rel are projectables */
public final class ProjectableSqlAggFunctions {
  public static final Predicate<AggregateCall> IS_PROJECTABLE_AGGREGATE_CALL =
      new Predicate<AggregateCall>() {
        @Override
        public boolean apply(AggregateCall call) {
          return call.getAggregation() instanceof ProjectableSqlAggFunction;
        }
      };

  /**
   * Check if the aggregate call is projectable
   *
   * @param call the aggregate call
   * @return {@code true} if projectable, {@code false} otherwise
   * @throws NullPointerException if call is {@code null}
   */
  public static final boolean isProjectableAggregateCall(AggregateCall call) {
    return IS_PROJECTABLE_AGGREGATE_CALL.apply(call);
  }

  /**
   * Check if the aggregate rel is projectable
   *
   * @param aggregate the aggregate rel
   * @return {@code true} if projectable, {@code false} otherwise
   * @throws NullPointerException if aggregate is {@code null}
   */
  public static final boolean isProjectableAggregate(Aggregate aggregate) {
    return Iterables.any(aggregate.getAggCallList(), IS_PROJECTABLE_AGGREGATE_CALL);
  }
}
