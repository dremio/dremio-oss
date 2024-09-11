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
package com.dremio.common.expression;

import com.dremio.common.expression.visitors.ExprVisitor;
import java.util.Collections;
import java.util.List;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

public class ListAggExpression extends FunctionCall {
  final boolean isDistinct;
  final List<Ordering> orderings;
  final List<LogicalExpression> extraExpressions;
  final CompleteType type;

  public ListAggExpression(
      String name,
      List<LogicalExpression> args,
      boolean isDistinct,
      List<Ordering> orderings,
      List<LogicalExpression> extraExpressions) {
    super(name, args);
    this.isDistinct = isDistinct;
    this.orderings = orderings;
    this.extraExpressions = extraExpressions;
    if ("LISTAGG".equals(name) || "listagg_merge".equals(name)) {
      type = CompleteType.VARCHAR;
    } else if ("local_listagg".equals(name)) {
      type =
          new CompleteType(
              ArrowType.List.INSTANCE,
              Collections.singletonList(Field.nullable("$data$", CompleteType.VARCHAR.getType())));
    } else if ("ARRAY_AGG".equals(name)) {
      // Hack: Ideally the return type should be the List of type args[0]. However, if arg[0] is
      // ArrowLateType,
      // its size cannot be determined by HashAggMemoryEstimator. Given that HashAggMemoryEstimator
      // just estimates
      // the size (e.g. by assuming the size of list as 5), VARBINARY estimate is not too shabby.
      ArrowType elementType =
          args.get(0).getCompleteType().getType() instanceof ArrowLateType
              ? CompleteType.VARBINARY.getType()
              : args.get(0).getCompleteType().getType();
      type =
          new CompleteType(
              ArrowType.List.INSTANCE,
              Collections.singletonList(Field.nullable("$data$", elementType)));
      // ARRAY_AGG Only supports some of the features that LISTAGG supports.
      // Ideally it should be it's own Expression.
      if (isDistinct) {
        throw new UnsupportedOperationException("ARRAY_AGG does not support DISTINCT.");
      }

      if (!orderings.isEmpty()) {
        throw new UnsupportedOperationException("ARRAY_AGG does not support ordering.");
      }

      if (!extraExpressions.isEmpty()) {
        throw new UnsupportedOperationException("ARRAY_AGG does not support extra expressions.");
      }
    } else {
      throw new UnsupportedOperationException("Invalid listagg operator \"" + name + "\"");
    }
  }

  public boolean isDistinct() {
    return isDistinct;
  }

  public List<Ordering> getOrderings() {
    return orderings;
  }

  public List<LogicalExpression> getExtraExpressions() {
    return extraExpressions;
  }

  @Override
  public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {
    return visitor.visitListAggExpression(this, value);
  }

  @Override
  public CompleteType getCompleteType() {
    return type;
  }

  @Override
  public String toString() {
    return "ListAggExpression [functionCall="
        + super.toString()
        + " + isDistinct= + "
        + isDistinct
        + ", ordering="
        + orderings
        + ", extraExpressions="
        + extraExpressions
        + "]";
  }

  public static class Builder {
    private String name;
    private List<LogicalExpression> args;
    private boolean isDistinct;
    private List<Ordering> orderings;
    private List<LogicalExpression> extraExpressions;

    public ListAggExpression.Builder addFunctionCall(LogicalExpression functionCall) {
      this.args = ((FunctionCall) functionCall).args;
      this.name = ((FunctionCall) functionCall).getName();
      return this;
    }

    public ListAggExpression.Builder setIsDistinct(String booleanString) {
      this.isDistinct = "true".equalsIgnoreCase(booleanString);
      return this;
    }

    public ListAggExpression.Builder setExtraExpressions(List<LogicalExpression> extraExpressions) {
      this.extraExpressions = extraExpressions;
      return this;
    }

    public ListAggExpression.Builder setOrderings(List<Ordering> orderings) {
      this.orderings = orderings;
      return this;
    }

    public ListAggExpression build() {
      return new ListAggExpression(name, args, isDistinct, orderings, extraExpressions);
    }
  }

  public static ListAggExpression.Builder newBuilder() {
    return new ListAggExpression.Builder();
  }
}
