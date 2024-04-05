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

import static com.dremio.exec.planner.sql.DremioSqlOperatorTable.NDV;

import com.dremio.exec.planner.sql.DremioSqlOperatorTable;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexNode;

/** Converts an NDV expression as HLL Aggregate + HLL_DECODE project. */
public final class NDVConvertlet extends AggregateCallConvertlet {
  public static final NDVConvertlet INSTANCE = new NDVConvertlet();

  private NDVConvertlet() {}

  @Override
  public boolean matches(AggregateCall call) {
    return call.getAggregation() == NDV;
  }

  @Override
  public RexNode convertCall(ConvertletContext convertletContext) {
    AggregateCall oldCall = convertletContext.getOldCall();
    AggregateCall newCall =
        AggregateCallFactory.hll(oldCall.getArgList().get(0), oldCall.filterArg, oldCall.getName());

    RexNode hllRefIndex = convertletContext.addAggregate(newCall);
    return convertletContext
        .getRexBuilder()
        .makeCall(DremioSqlOperatorTable.HLL_DECODE, hllRefIndex);
  }
}
