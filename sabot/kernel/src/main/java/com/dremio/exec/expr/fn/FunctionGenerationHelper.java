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
package com.dremio.exec.expr.fn;

import java.util.ArrayList;
import java.util.List;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.Describer;
import com.dremio.common.expression.FunctionCall;
import com.dremio.common.expression.IfExpression;
import com.dremio.common.expression.IfExpression.IfCondition;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.ValueExpressions.IntExpression;
import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.common.types.Types;
import com.dremio.exec.expr.ClassGenerator.HoldingContainer;
import com.dremio.exec.expr.ClassProducer;
import com.dremio.exec.expr.HoldingContainerExpression;
import com.google.common.collect.Lists;

public class FunctionGenerationHelper {
  public static final String COMPARE_TO_NULLS_HIGH = "compare_to_nulls_high";
  public static final String COMPARE_TO_NULLS_LOW = "compare_to_nulls_low";

  /**
   * Finds ordering comparator ("compare_to...") FunctionHolderExpression with
   * a specified ordering for NULL (and considering NULLS <i>equal</i>).
   * @param  null_high  whether NULL should compare as the lowest value (if
   *                    {@code false}) or the highest value (if {@code true})
   * @param  left  ...
   * @param  right  ...
   * @param  registry  ...
   * @return
   *     FunctionHolderExpression containing the found function implementation
   */
  public static LogicalExpression getOrderingComparator(
      boolean null_high,
      HoldingContainer left,
      HoldingContainer right,
      ClassProducer producer) {
    final String comparator_name =
        null_high ? COMPARE_TO_NULLS_HIGH : COMPARE_TO_NULLS_LOW;

    if (!left.getCompleteType().isComparable() || ! right.getCompleteType().isComparable()){
      throw new UnsupportedOperationException(formatCanNotCompareMsg(left.getCompleteType(), right.getCompleteType()));
    }
    LogicalExpression comparisonFunctionExpression = getFunctionExpression(comparator_name, Types.required(MinorType.INT),
                                 left, right);

    if (!left.getCompleteType().isUnion() && !right.getCompleteType().isUnion()) {
      return producer.materialize(comparisonFunctionExpression, null);
    } else {
      LogicalExpression typeComparisonFunctionExpression = getTypeComparisonFunction(comparisonFunctionExpression, left, right);
      return producer.materialize(typeComparisonFunctionExpression, null);
    }
  }

  /**
   * Finds ordering comparator ("compare_to...") FunctionHolderExpression with
   * a "NULL high" ordering (and considering NULLS <i>equal</i>).
   * @param  left  ...
   * @param  right  ...
   * @param  registry  ...
   * @return FunctionHolderExpression containing the function implementation
   */
  public static LogicalExpression getOrderingComparatorNullsHigh(
      HoldingContainer left,
      HoldingContainer right,
      ClassProducer producer) {
    return getOrderingComparator(true, left, right, producer);
  }

  private static LogicalExpression getFunctionExpression(String name, MajorType returnType, HoldingContainer... args) {
    List<CompleteType> argTypes = new ArrayList<>(args.length);
    List<LogicalExpression> argExpressions = new ArrayList<LogicalExpression>(args.length);
    for(HoldingContainer c : args) {
      argTypes.add(c.getCompleteType());
      argExpressions.add(new HoldingContainerExpression(c));
    }

    return new FunctionCall(name, argExpressions);
  }

  /**
   * Wraps the comparison function in an If-statement which compares the types first, evaluating the comaprison function only
   * if the types are equivialent
   *
   * @param comparisonFunction
   * @param args
   * @return
   */
  private static LogicalExpression getTypeComparisonFunction(LogicalExpression comparisonFunction, HoldingContainer... args) {
    List<LogicalExpression> argExpressions = Lists.newArrayList();
    List<CompleteType> argTypes = Lists.newArrayList();
    for(HoldingContainer c : args) {
      argTypes.add(c.getCompleteType());
      argExpressions.add(new HoldingContainerExpression(c));
    }
    FunctionCall call = new FunctionCall("compareType", argExpressions);

    List<LogicalExpression> newArgs = Lists.newArrayList();
    newArgs.add(call);
    newArgs.add(new IntExpression(0));
    FunctionCall notEqual = new FunctionCall("not_equal", newArgs);

    IfExpression.IfCondition ifCondition = new IfCondition(notEqual, call);
    IfExpression ifExpression = IfExpression.newBuilder().setIfCondition(ifCondition).setElse(comparisonFunction).build();
    return ifExpression;
  }

  private static String formatCanNotCompareMsg(CompleteType left, CompleteType right) {
    StringBuilder sb = new StringBuilder();
    sb.append("Map, List and Union should not be used in group by, order by or in a comparison operator. Dremio does not support compare between ");
    sb.append(Describer.describe(left));
    sb.append(" and ");
    sb.append(Describer.describe(left));
    sb.append(".");

    return sb.toString();
  }

}
