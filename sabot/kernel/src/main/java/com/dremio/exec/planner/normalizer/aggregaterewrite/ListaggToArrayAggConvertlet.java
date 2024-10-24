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

import static com.dremio.exec.planner.sql.DremioSqlOperatorTable.ARRAY_TO_STRING;

import com.dremio.exec.planner.sql.DremioSqlOperatorTable;
import com.dremio.exec.planner.types.SqlTypeFactoryImpl;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

/**
 * Rewrites: LISTAGG(expression, delimiter) WITHIN GROUP (ORDER BY expression) to:
 * ARRAY_TO_STRING(ARRAY_AGG(expression) WITHIN GROUP (ORDER BY expression), delimiter)
 */
public final class ListaggToArrayAggConvertlet extends AggregateCallConvertlet {
  public static final ListaggToArrayAggConvertlet INSTANCE = new ListaggToArrayAggConvertlet();

  private ListaggToArrayAggConvertlet() {}

  @Override
  public boolean matches(AggregateCall call) {
    return call.getAggregation() == SqlStdOperatorTable.LISTAGG;
  }

  @Override
  public RexNode convertCall(ConvertletContext convertletContext) {
    Aggregate listAggRel = convertletContext.getOldAggRel();
    RelNode input = listAggRel.getInput();
    AggregateCall listAggCall = convertletContext.getOldCall();

    int newFilter = createNewFilter(convertletContext);
    AggregateCall arrayAggCall = createNewAggregateCall(listAggCall, listAggRel, newFilter);

    RexBuilder rexBuilder = convertletContext.getRexBuilder();

    RexNode arrayAggRefIndex = convertletContext.addAggregate(arrayAggCall);
    RexNode delimiter = extractDelimiter(listAggCall, rexBuilder, input);
    RexNode arrayToStringCall = rexBuilder.makeCall(ARRAY_TO_STRING, arrayAggRefIndex, delimiter);
    RexNode withEmptyArrayHandling =
        handleEmptyArray(rexBuilder, arrayToStringCall, arrayAggRefIndex);
    return rexBuilder.makeCast(listAggCall.getType(), withEmptyArrayHandling, true);
  }

  /**
   * ARRAY_TO_STRING unfortunately turns NULL values into empty strings in the concatenation
   * process:
   *
   * <pre>
   *   ARRAY_TO_STRING(ARRAY[1, NULL, 3])
   *   -> 1,,3
   * </pre>
   *
   * While LISTAGG ignores NULLS
   *
   * <pre>
   *   LISTAGG(ARRAY['1', NULL, '3'], ',')
   *   -> 1,3
   * </pre>
   *
   * To replicate this behavior we can add a `FILTER(WHERE expr IS NOT NULL)` to the rewrite.
   */
  private static int createNewFilter(ConvertletContext convertletContext) {
    Aggregate listAggRel = convertletContext.getOldAggRel();
    RelNode input = listAggRel.getInput();
    AggregateCall listAggCall = convertletContext.getOldCall();

    RexBuilder rexBuilder = convertletContext.getRexBuilder();

    int aggregatedArg = listAggCall.getArgList().get(0);
    RexNode aggregatedArgInputRef = rexBuilder.makeInputRef(input, aggregatedArg);
    RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, aggregatedArgInputRef);
    if (listAggCall.hasFilter()) {
      RexNode originalFilter = rexBuilder.makeInputRef(input, listAggCall.filterArg);
      filter = rexBuilder.makeCall(SqlStdOperatorTable.AND, filter, originalFilter);
    }

    return convertletContext.addExpression(filter);
  }

  private static AggregateCall createNewAggregateCall(
      AggregateCall listAggCall, Aggregate listAggRel, int newFilter) {
    int aggregatedArg = listAggCall.getArgList().get(0);
    RelNode input = listAggRel.getInput();

    RelDataType measureType = input.getRowType().getFieldList().get(aggregatedArg).getType();

    return AggregateCall.create(
        DremioSqlOperatorTable.ARRAY_AGG,
        listAggCall.isDistinct(),
        listAggCall.isApproximate(),
        listAggCall.ignoreNulls(),
        ImmutableList.of(aggregatedArg),
        newFilter,
        listAggCall.getCollation(),
        listAggRel.getGroupCount(),
        listAggRel.getInput(),
        SqlTypeFactoryImpl.INSTANCE.createTypeWithNullability(
            SqlTypeFactoryImpl.INSTANCE.createArrayType(measureType, -1), false),
        listAggCall.name);
  }

  private static RexNode extractDelimiter(
      AggregateCall listAggCall, RexBuilder rexBuilder, RelNode input) {
    RexNode delimiter;
    if (listAggCall.getArgList().size() == 1) {
      delimiter = rexBuilder.makeLiteral("");
    } else {
      if (input instanceof HepRelVertex) {
        input = ((HepRelVertex) input).getCurrentRel();
      }

      Project project = (Project) input;
      int delimiterArg = listAggCall.getArgList().get(1);
      delimiter = project.getProjects().get(delimiterArg);
    }

    return delimiter;
  }

  /**
   * When LISTAGG is presented with an empty group:
   *
   * <pre>
   *   SELECT LISTAGG(Name, ', ') WITHIN GROUP (ORDER BY Name) AS ConcatenatedNames
   *   FROM (
   *     VALUES
   *       (CAST(NULL AS VARCHAR)),
   *       (CAST(NULL AS VARCHAR))
   *  ) AS t(Name);
   * </pre>
   *
   * The result is an `null`, while ARRAY_AGG returns an empty array. We can add a CASE statement in
   * the rewrite logic to handle this.
   */
  private static RexNode handleEmptyArray(
      RexBuilder rexBuilder, RexNode arrayToStringCall, RexNode arrayAggRefIndex) {
    RexNode nullLiteral = rexBuilder.makeNullLiteral(arrayToStringCall.getType());
    RexNode arrayLength =
        rexBuilder.makeCall(DremioSqlOperatorTable.ARRAY_LENGTH, arrayAggRefIndex);
    RexNode isEmpty =
        rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            arrayLength,
            rexBuilder.makeZeroLiteral(arrayLength.getType()));
    return rexBuilder.makeCall(SqlStdOperatorTable.CASE, isEmpty, nullLiteral, arrayToStringCall);
  }
}
