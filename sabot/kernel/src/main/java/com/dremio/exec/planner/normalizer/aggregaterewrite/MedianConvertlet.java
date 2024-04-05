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

import static com.dremio.exec.planner.sql.DremioSqlOperatorTable.MEDIAN;

import java.math.BigDecimal;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

/**
 * The MEDIAN aggregate function can be rewritten as a PERCENTILE_CONT(0.5). For example, the
 * following query:
 *
 * <p>SELECT MEDIAN(age) FROM people
 *
 * <p>can be rewritten as:
 *
 * <p>SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY age) FROM people
 *
 * <p>In general median is a window function that returns the median value of a range of values. It
 * is a specific case of PERCENTILE_CONT, so the following:
 *
 * <p>MEDIAN(median_arg)
 *
 * <p>is equivalent to:
 *
 * <p>PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY median_arg)
 */
public final class MedianConvertlet extends AggregateCallConvertlet {
  public static final MedianConvertlet INSTANCE = new MedianConvertlet();

  private MedianConvertlet() {}

  @Override
  public boolean matches(AggregateCall call) {
    return call.getAggregation() == MEDIAN && !call.hasFilter();
  }

  @Override
  public RexNode convertCall(ConvertletContext convertletContext) {
    RexBuilder rexBuilder = convertletContext.getRexBuilder();

    // Constructing the "0.5" needed for "PERCENTILE_CONT(0.5)"
    RexLiteral pointFiveLiteral = rexBuilder.makeExactLiteral(new BigDecimal(.5));
    int pointFiveLiteralIndex = convertletContext.addExpression(pointFiveLiteral);

    // Constructing the "WITHIN GROUP (ORDER BY median_arg)"
    int medianArgumentIndex = convertletContext.getArg(0);
    final RelCollation withinGroupRelCollation =
        RelCollations.of(new RelFieldCollation(medianArgumentIndex));

    // Construct PERCENTILE_CONT call
    AggregateCall oldCall = convertletContext.getOldCall();
    AggregateCall newCall =
        AggregateCallFactory.percentileCont(
            oldCall.isDistinct(),
            oldCall.isApproximate(),
            pointFiveLiteralIndex,
            withinGroupRelCollation,
            oldCall.getName());

    return convertletContext.addAggregate(newCall);
  }
}
