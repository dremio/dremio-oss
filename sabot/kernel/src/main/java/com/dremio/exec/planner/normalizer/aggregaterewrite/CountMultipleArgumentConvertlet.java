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

import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

/** Rewrites COUNT(A, B) to SUM0(A IS NOT NULL AND B IS NOT NULL ? 1 : 0) */
public final class CountMultipleArgumentConvertlet extends AggregateCallConvertlet {
  public static final CountMultipleArgumentConvertlet INSTANCE =
      new CountMultipleArgumentConvertlet();

  private CountMultipleArgumentConvertlet() {}

  @Override
  public boolean matches(AggregateCall call) {
    return call.getAggregation() == SqlStdOperatorTable.COUNT
        && call.getArgList().size() > 1
        && !call.isDistinct()
        && !call.hasFilter()
        && !call.ignoreNulls();
  }

  @Override
  public RexNode convertCall(ConvertletContext convertletContext) {
    Aggregate originalAggRel = convertletContext.getOldAggRel();
    RelNode input = originalAggRel.getInput();
    List<RexNode> inputRefs = new ArrayList<>();
    AggregateCall oldCall = convertletContext.getOldCall();
    RexBuilder rexBuilder = convertletContext.getRexBuilder();
    for (int arg : oldCall.getArgList()) {
      RexNode inputRef = rexBuilder.makeInputRef(input, arg);
      inputRefs.add(inputRef);
    }

    List<RexNode> isNotNulls =
        inputRefs.stream()
            .map(x -> rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, x))
            .collect(Collectors.toList());

    RexNode isNotNullAnded = RexUtil.composeConjunction(rexBuilder, isNotNulls);
    RexNode caseStatement =
        rexBuilder.makeCall(
            SqlStdOperatorTable.CASE,
            isNotNullAnded,
            rexBuilder.makeBigintLiteral(new BigDecimal(1)),
            rexBuilder.makeBigintLiteral(new BigDecimal(0)));
    int caseIndex = convertletContext.addExpression(caseStatement);
    AggregateCall newAggCall =
        AggregateCall.create(
            SqlStdOperatorTable.SUM0,
            false,
            false,
            false,
            ImmutableList.of(caseIndex),
            oldCall.filterArg,
            oldCall.getCollation(),
            oldCall.getType(),
            oldCall.getName());
    return convertletContext.addAggregate(newAggCall);
  }
}
