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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;

/**
 * We currently do not natively support the following type of query:
 *
 * <p>SELECT SUM(salary) FILTER (WHERE gender = 'F') FROM EMP
 *
 * <p>Luckily that query is just syntax sugar for the following query:
 *
 * <p>SELECT SUM(gender = 'F' ? salary : 0) FROM EMP
 *
 * <p>We can implement that by applying a transformation on the query plan. The original query has
 * the query plan:
 *
 * <p>LogicalAggregate(group=[{}], EXPR$0=[SUM($0) FILTER $1]) LogicalProject(sales=[$0], $f1=[IS
 * TRUE(=($1, 'F'))]) ScanCrel(table=[cp."emp.json"], columns=[`salary`, `gender`], splits=[1])
 *
 * <p>And we can rewrite it to: LogicalAggregate(group=[{}], EXPR$0=[SUM($2)])
 * LogicalProject(salary=[$0], $f1=[IS TRUE(=($1, 'F'))], $f2=[CASE WHEN $1 THEN $0 ELSE 0 END])
 * ScanCrel(table=[cp."emp.json"], columns=[`salary`, `gender`], splits=[1])
 */
public final class AggregateFilterToCaseConvertlet extends AggregateCallConvertlet {
  public static final AggregateFilterToCaseConvertlet INSTANCE =
      new AggregateFilterToCaseConvertlet();

  private AggregateFilterToCaseConvertlet() {}

  @Override
  public boolean matches(AggregateCall call) {
    return call.hasFilter();
  }

  @Override
  public RexNode convertCall(ConvertletContext convertletContext) {
    // Converting the FILTER to a CASE expression
    RexCall caseRexCall =
        createCaseCallFromAggregateFilter(
            convertletContext.getOldCall(),
            convertletContext.getOldAggRel().getInput(),
            convertletContext.getRexBuilder());
    int caseRexCallIndex = convertletContext.addExpression(caseRexCall);

    // Create a new Aggregate Call without the filter,
    // but aggregating on the case statement (that we will add to the project below):
    Aggregate originalAggRel = convertletContext.getOldAggRel();
    AggregateCall oldCall = convertletContext.getOldCall();
    AggregateCall newCall =
        AggregateCall.create(
            oldCall.getAggregation(),
            oldCall.isDistinct(),
            oldCall.isApproximate(),
            ImmutableList.of(caseRexCallIndex),
            /* filterArg */ -1,
            oldCall.getCollation(),
            originalAggRel.getGroupCount(),
            originalAggRel.getInput(),
            oldCall.type,
            oldCall.name);

    return convertletContext.addAggregate(newCall);
  }

  private static RexCall createCaseCallFromAggregateFilter(
      AggregateCall aggregateCall, RelNode originalInput, RexBuilder rexBuilder) {
    // CASE(<filterArg>, <aggregateArg>, <elseValue>)
    final RexNode filterArg = createFilterArgForCaseCall(aggregateCall, originalInput);
    final Pair<RexNode, RelDataType> aggregateArgAndRelDataType =
        createAggregateArgAndRelDataTypeForCaseCall(aggregateCall, originalInput, rexBuilder);
    final RexNode elseValue = rexBuilder.makeNullLiteral(aggregateArgAndRelDataType.right);

    final RexCall caseRexCall =
        (RexCall)
            rexBuilder.makeCall(
                SqlStdOperatorTable.CASE, filterArg, aggregateArgAndRelDataType.left, elseValue);

    return caseRexCall;
  }

  private static RexNode createFilterArgForCaseCall(
      AggregateCall aggregateCall, RelNode originalInput) {
    final int filterArgIndex = aggregateCall.filterArg;
    final RelDataType filterArgRelDataType =
        originalInput.getRowType().getFieldList().get(filterArgIndex).getValue();
    final RexNode filterArg = new RexInputRef(filterArgIndex, filterArgRelDataType);

    return filterArg;
  }

  private static Pair<RexNode, RelDataType> createAggregateArgAndRelDataTypeForCaseCall(
      AggregateCall aggregateCall, RelNode originalInput, RexBuilder rexBuilder) {
    // Some aggregates don't have an aggregate argument (like COUNT):
    final RexNode aggregateArg;
    final RelDataType aggregateArgRelDataType;
    if (aggregateCall.getArgList().isEmpty()) {
      // COUNT(*) FILTER (WHERE gender = 'F')
      // => COUNT(gender = 'F' ? 'dummy' : null)
      aggregateArg = rexBuilder.makeLiteral(false);
      aggregateArgRelDataType = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.ANY);
    } else {
      // AGG(<aggregateArg>) FILTER (WHERE <cond>)
      // => AGG(<cond> ? <aggregateArg> : null)
      final int aggregateArgumentIndex = aggregateCall.getArgList().get(0);
      aggregateArgRelDataType =
          originalInput.getRowType().getFieldList().get(aggregateArgumentIndex).getValue();
      aggregateArg = new RexInputRef(aggregateArgumentIndex, aggregateArgRelDataType);
    }

    return Pair.of(aggregateArg, aggregateArgRelDataType);
  }
}
