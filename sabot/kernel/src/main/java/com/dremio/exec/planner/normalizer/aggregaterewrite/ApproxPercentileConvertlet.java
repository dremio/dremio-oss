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

import static com.dremio.exec.planner.sql.DremioSqlOperatorTable.APPROX_PERCENTILE;
import static com.dremio.exec.planner.sql.DremioSqlOperatorTable.TDIGEST_QUANTILE;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;

/**
 * Rewrites APPROX_PERCENTILE to TDIGEST
 *
 * <p>For a query like:
 *
 * <p>SELECT approx_percentile(col, 0.6) FROM table
 *
 * <p>The query plan is:
 *
 * <p>LogicalAggregate(group=[{}], EXPR$0=[APPROX_PERCENTILE($0, $1)])
 * LogicalProject(employee_id=[$0], $f1=[0.6:DECIMAL(2, 1)]) ScanCrel(table=[cp."employee.json"],
 * columns=[`employee_id`, `full_name`, ..., `management_role`], splits=[1])
 *
 * <p>And will be rewritten to:
 *
 * <p>LogicalProject($f0=[TDIGEST_QUANTILE(0.6:DECIMAL(2, 1), $0)]) LogicalAggregate(group=[{}],
 * EXPR$0=[TDIGEST($0, $1)]) LogicalProject(employee_id=[$0], $f1=[0.6:DECIMAL(2, 1)])
 * ScanCrel(table=[cp."employee.json"], columns=[`employee_id`, ..., `management_role`], splits=[1])
 */
public final class ApproxPercentileConvertlet extends AggregateCallConvertlet {
  public static final ApproxPercentileConvertlet INSTANCE = new ApproxPercentileConvertlet();

  private ApproxPercentileConvertlet() {}

  @Override
  public boolean matches(AggregateCall call) {
    return call.getAggregation() == APPROX_PERCENTILE && !call.hasFilter();
  }

  @Override
  public RexNode convertCall(ConvertletContext convertletContext) {
    int columnIndex = convertletContext.getArg(0);
    int percentileIndex = convertletContext.getArg(1);

    RexBuilder rexBuilder = convertletContext.getRexBuilder();

    AggregateCall oldCall = convertletContext.getOldCall();
    AggregateCall tDigestCall =
        AggregateCallFactory.tDigest(
            oldCall.getType(), oldCall.isDistinct(), columnIndex, oldCall.getName());
    RexNode tDigestRef = convertletContext.addAggregate(tDigestCall);

    RexNode percentile =
        ColumnExtractor.extract(convertletContext.getOldAggRel().getInput(), percentileIndex);
    return rexBuilder.makeCall(TDIGEST_QUANTILE, percentile, tDigestRef);
  }
}
