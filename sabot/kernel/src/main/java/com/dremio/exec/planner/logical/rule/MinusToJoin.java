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
package com.dremio.exec.planner.logical.rule;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;

import com.dremio.exec.planner.logical.RelOptHelper;
import com.google.common.collect.ImmutableList;

/**
 * Rule that implements {@link org.apache.calcite.rel.core.Minus} using aggregate, join, filter and project
 * High-level steps:
 * 1. Compute the count of each unique rows of every input
 * 2. Left join the results above one by one
 * 3. After each left join, filter out the rows in which the values of the right input are not NULL
 * 4. After all joins, project only the fields in the original input schema
 */
public class MinusToJoin {
  public static final RelOptRule RULE = new RelOptRule(
      RelOptHelper.any(Minus.class, RelNode.class),
      "MinusToJoin") {

    @Override
    public void onMatch(RelOptRuleCall call) {
      Minus minus = call.rel(0);
      if (minus.all) {
        return; // Not supported without RexWindowBounds.
      }

      RelNode newRel = new MinusToJoin(minus.getCluster().getRexBuilder())
          .rewrite(call::builder, minus);
      call.transformTo(newRel);
    }
  };

  private final RexBuilder rexBuilder;

  public MinusToJoin(RexBuilder rexBuilder) {
    this.rexBuilder = rexBuilder;
  }

  public RelNode rewrite(Supplier<RelBuilder> relBuilderSupplier, Minus minus) {
    // Aggregation that counts the number of each unique row of the input
    Function<RelNode, RelNode> transformToUnique = transformRowToUnique(relBuilderSupplier);

    // Apply the aggregation to the first input (minuend), and add it to the relBuilder
    RelBuilder relBuilder = relBuilderSupplier.get()
        .push(transformToUnique.apply(minus.getInput(0)));

    IntStream.range(1,minus.getInputs().size())
        .forEach(minusInputIndex -> {
          // Previous pushed rel (the aggregation)
          RelNode left = relBuilder.peek();

          relBuilder
              // Apply the aggregation to each subsequent input (subtractors), and add it to the relBuilder
              .push(transformToUnique.apply(minus.getInput(minusInputIndex)))
              // Join two inputs for each field in the original schema
              .join(JoinRelType.LEFT, buildCondition(minus, left, relBuilder.peek(), minusInputIndex))
              // Filter out the rows in which the values of the right input are not NULL
              .filter(buildMinusFilterCondition(minus, minusInputIndex));
        });
    return relBuilder
        // Project only the fields in the original input schema
        .project(buildMask(minus))
        .build();
  }

  private RexNode buildCondition(Minus minus, RelNode left, RelNode right, int minusIndex) {
    RelDataType rowType = minus.getRowType();
    int columnCount = rowType.getFieldCount();
    int columnOffset = (columnCount + 1) * minusIndex; //plus one for unique column for the count

    Stream<RexNode> minusRex =
        IntStream.range(0, columnCount)
          .mapToObj((columnIndex) -> {
            RelDataType lColType = left.getRowType().getFieldList().get(columnIndex).getType();
            RelDataType rColType = right.getRowType().getFieldList().get(columnIndex).getType();

            return rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
                rexBuilder.makeInputRef(lColType,columnIndex),
                rexBuilder.makeInputRef(rColType,columnIndex + columnOffset));
            });

    return (RexUtil.composeConjunction(rexBuilder,
       minusRex.collect(ImmutableList.toImmutableList()), false));
  }

  private RexNode buildMinusFilterCondition(Minus minus, int minusIndex) {
    int columnCount = minus.getRowType().getFieldCount();
    int columnOffset = (columnCount + 1) * minusIndex;
    return rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL,
            rexBuilder.makeInputRef(bigIntType(), columnCount + columnOffset));
  }

  private List<RexNode> buildMask(Minus minus) {
    return IntStream.range(0, minus.getRowType().getFieldCount())
      .mapToObj(index -> rexBuilder.makeInputRef(minus, index))
      .collect(ImmutableList.toImmutableList());
  }

  private Function<RelNode, RelNode> transformRowToUnique(Supplier<RelBuilder> relBuilderSupplier){

    return (relNode) -> {
      RelBuilder relBuilder = relBuilderSupplier.get();
      return relBuilder
          .push(relNode)
          .aggregate(
              relBuilder.groupKey(relBuilder.fields()),
              relBuilder.countStar(null))
          .build();
    };
  }

  private RelDataType bigIntType() {
    return rexBuilder.getTypeFactory().createTypeWithNullability(
        rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BIGINT), true
    );
  }
}
