/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.exec.store.ischema;

import static com.dremio.exec.expr.fn.impl.RegexpUtil.sqlToRegexLike;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.dremio.exec.store.ischema.ExprNode.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;

@JsonTypeName("info-schema-filter")
public class InfoSchemaFilter {

  private final ExprNode exprRoot;

  @JsonCreator
  public InfoSchemaFilter(@JsonProperty("exprRoot") ExprNode exprRoot) {
    this.exprRoot = exprRoot;
  }

  @JsonProperty("exprRoot")
  public ExprNode getExprRoot() {
    return exprRoot;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    InfoSchemaFilter that = (InfoSchemaFilter) o;
    return Objects.equal(exprRoot, that.exprRoot);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(exprRoot);
  }

  @JsonTypeName("function")
  public static class FunctionExprNode extends ExprNode {
    private final String function;
    private final List<ExprNode> args;

    @JsonCreator
    public FunctionExprNode(@JsonProperty("function") String function, @JsonProperty("args") List<ExprNode> args) {
      super(Type.FUNCTION);
      this.function = function;
      this.args = args;
    }

    public String getFunction() {
      return function;
    }

    public List<ExprNode> getArgs() {
      return args;
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append(function);
      builder.append("(");
      builder.append(Joiner.on(",").join(args));
      builder.append(")");
      return builder.toString();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }
      FunctionExprNode that = (FunctionExprNode) o;
      return Objects.equal(function, that.function) &&
          Objects.equal(args, that.args);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(super.hashCode(), function, args);
    }
  }

  @JsonTypeName("field")
  public static class FieldExprNode extends ExprNode {
    private final String field;

    @JsonCreator
    public FieldExprNode(@JsonProperty("field") String field) {
      super(Type.FIELD);
      this.field = field;
    }

    public String getField() {
      return field;
    }

    @Override
    public String toString() {
      return String.format("Field=%s", field);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }
      FieldExprNode that = (FieldExprNode) o;
      return Objects.equal(field, that.field);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(super.hashCode(), field);
    }
  }

  @JsonTypeName("constant")
  public static class ConstantExprNode extends ExprNode {

    private final String value;

    @JsonCreator
    public ConstantExprNode(@JsonProperty("value") String value) {
      super(Type.CONSTANT);
      this.value = value;
    }

    public String getValue() {
      return value;
    }

    @Override
    public String toString() {
      return String.format("Literal=%s", value);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }
      ConstantExprNode that = (ConstantExprNode) o;
      return Objects.equal(value, that.value);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(super.hashCode(), value);
    }
  }

  public enum Result {
    TRUE,
    FALSE,
    INCONCLUSIVE;
  }

  /**
   * Evaluate the filter for given <COLUMN NAME, VALUE> pairs.
   * @param recordValues
   * @return
   */
  @JsonIgnore
  public Result evaluate(Map<String, String> recordValues) {
    return evaluateHelper(recordValues, getExprRoot());
  }

  private Result evaluateHelper(Map<String, String> recordValues, ExprNode exprNode) {
    if (exprNode.getType() == Type.FUNCTION) {
      return evaluateHelperFunction(recordValues, (FunctionExprNode) exprNode);
    }

    throw new UnsupportedOperationException(
        String.format("Unknown expression type '%s' in InfoSchemaFilter", exprNode.getType()));
  }

  private Result evaluateHelperFunction(Map<String, String> recordValues, FunctionExprNode exprNode) {
    switch(exprNode.function) {
      case "like": {
        FieldExprNode col = (FieldExprNode) exprNode.args.get(0);
        ConstantExprNode pattern = (ConstantExprNode) exprNode.args.get(1);
        ConstantExprNode escape = exprNode.args.size() > 2 ? (ConstantExprNode) exprNode.args.get(2) : null;
        final String fieldValue = recordValues.get(col.field.toString());
        if (fieldValue != null) {
          if (escape == null) {
            return Pattern.matches(sqlToRegexLike(pattern.value), fieldValue) ?
                Result.TRUE : Result.FALSE;
          } else {
            return Pattern.matches(sqlToRegexLike(pattern.value, escape.value), fieldValue) ?
                Result.TRUE : Result.FALSE;
          }
        }

        return Result.INCONCLUSIVE;
      }
      case "equal":
      case "not equal":
      case "notequal":
      case "not_equal": {
        FieldExprNode arg0 = (FieldExprNode) exprNode.args.get(0);
        ConstantExprNode arg1 = (ConstantExprNode) exprNode.args.get(1);

        final String value = recordValues.get(arg0.field.toString());
        if (value != null) {
          if (exprNode.function.equals("equal")) {
            return arg1.value.equals(value) ? Result.TRUE : Result.FALSE;
          } else {
            return arg1.value.equals(value) ? Result.FALSE : Result.TRUE;
          }
        }

        return Result.INCONCLUSIVE;
      }

      case "booleanor": {
        // If at least one arg returns TRUE, then the OR function value is TRUE
        // If all args return FALSE, then OR function value is FALSE
        // For all other cases, return INCONCLUSIVE
        Result result = Result.FALSE;
        for(ExprNode arg : exprNode.args) {
          Result exprResult = evaluateHelper(recordValues, arg);
          if (exprResult == Result.TRUE) {
            return Result.TRUE;
          } else if (exprResult == Result.INCONCLUSIVE) {
            result = Result.INCONCLUSIVE;
          }
        }

        return result;
      }

      case "booleanand": {
        // If at least one arg returns FALSE, then the AND function value is FALSE
        // If at least one arg returns INCONCLUSIVE, then the AND function value is INCONCLUSIVE
        // If all args return TRUE, then the AND function value is TRUE
        for(ExprNode arg : exprNode.args) {
          Result exprResult = evaluateHelper(recordValues, arg);
          if (exprResult != Result.TRUE) {
            return exprResult;
          }
        }

        return Result.TRUE;
      }

      case "in": {
        FieldExprNode col = (FieldExprNode) exprNode.args.get(0);
        List<ExprNode> args = exprNode.args.subList(1, exprNode.args.size());
        final String fieldValue = recordValues.get(col.field.toString());
        if (fieldValue != null) {
          for(ExprNode arg: args) {
            if (fieldValue.equals(((ConstantExprNode) arg).value)) {
              return Result.TRUE;
            }
          }
          return Result.FALSE;
        }

        return Result.INCONCLUSIVE;
      }
    }

    throw new UnsupportedOperationException(
        String.format("Unknown function '%s' in InfoSchemaFilter", exprNode.function));
  }

  @Override
  public String toString() {
    return exprRoot.toString();
  }
}
