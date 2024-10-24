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
package com.dremio.exec.planner.sql.convertlet;

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CAST;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.planner.logical.RelDataTypeEqualityComparer;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.tools.RelBuilder;

public final class ArrayValueConstructorConvertlet extends RexCallConvertlet {
  public static final ArrayValueConstructorConvertlet INSTANCE =
      new ArrayValueConstructorConvertlet();

  private ArrayValueConstructorConvertlet() {}

  @Override
  public boolean matchesCall(RexCall call) {
    return call.getOperator() == ARRAY_VALUE_CONSTRUCTOR;
  }

  @Override
  public RexCall convertCall(ConvertletContext cx, RexCall call) {
    boolean hasNulls =
        call.getOperands().stream()
            .anyMatch(node -> node instanceof RexLiteral && ((RexLiteral) node).isNull());
    if (hasNulls) {
      throw UserException.validationError()
          .message("NULL values are not supported when constructing an ARRAY.")
          .buildSilently();
    }

    boolean allLiterals =
        call.getOperands().stream().allMatch(arrayItem -> arrayItem instanceof RexLiteral);
    return allLiterals ? convertCallLiteralsOnly(cx, call) : convertCallWithNonLiterals(cx, call);
  }

  private static RexCall convertCallLiteralsOnly(ConvertletContext cx, RexCall call) {
    // ARRAY[1, 2, 3] => ARRAY(SELECT item FROM VALUES((1), (2), (3)))
    RexBuilder rexBuilder = cx.getRexBuilder();
    RelBuilder relBuilder = cx.getRelBuilder();

    List<List<RexLiteral>> literalColumn =
        call.getOperands().stream()
            .map(x -> (RexLiteral) x)
            .map(Collections::singletonList)
            .collect(Collectors.toList());

    RelDataType valuesType =
        relBuilder.getTypeFactory().builder().add("col", call.getType().getComponentType()).build();

    RelNode valuesSubquery =
        relBuilder
            .values(literalColumn, valuesType)
            .project(rexBuilder.makeInputRef(call.getType().getComponentType(), 0))
            .build();

    return RexSubQuery.array(valuesSubquery, null);
  }

  private static RexCall convertCallWithNonLiterals(ConvertletContext cx, RexCall call) {
    boolean hasLegacyOnlyElements =
        call.getOperands().stream()
            .anyMatch(
                arrayItem -> {
                  if (!(arrayItem instanceof RexCall)) {
                    return false;
                  }

                  RexCall asCall = (RexCall) arrayItem;
                  if (asCall instanceof RexSubQuery) {
                    return true;
                  }

                  if (asCall.getOperator() instanceof ConvertFromSqlFunction) {
                    return true;
                  }

                  return false;
                });

    // TODO: Once ARRAY_AGG supports ARRAYs we can remove this fork.
    return hasLegacyOnlyElements
        ? convertCallWithNonLiteralsLegacy(cx, call)
        : convertCallWithNonLiteralsProvisional(cx, call);
  }

  private static RexCall convertCallWithNonLiteralsProvisional(ConvertletContext cx, RexCall call) {
    // ARRAY[1 + 1, FOO(2), 3] => ARRAY(
    //    SELECT 1 + 1 FROM VALUES((0))
    //    UNION ALL
    //    SELECT FOO(2) FROM VALUES((0))
    //    UNION ALL
    //    SELECT 3 FROM VALUES((0)))
    RelBuilder relBuilder = cx.getRelBuilder();

    for (RexNode arrayItem : call.getOperands()) {
      IllegalArrayItemDetector detector = new IllegalArrayItemDetector();
      arrayItem.accept(detector);
      if (detector.hasIllegalValue) {
        throw UserException.validationError()
            .message("ARRAY Literal can not have array element: " + arrayItem)
            .buildSilently();
      }

      RelNode relOfArrayItem = nodeToRel(relBuilder, arrayItem);
      relBuilder.push(relOfArrayItem);
    }

    RelNode unionNode = relBuilder.union(true, call.getOperands().size()).build();
    return RexSubQuery.array(unionNode, null);
  }

  private static RelNode nodeToRel(RelBuilder relBuilder, RexNode node) {
    return relBuilder.values(new String[] {"ZERO"}, 0).project(node).build();
  }

  public static RexCall convertCallWithNonLiteralsLegacy(ConvertletContext cx, RexCall call) {
    RexBuilder rexBuilder = cx.getRexBuilder();
    String arrayToString = arrayToString(rexBuilder, call);
    return (RexCall)
        rexBuilder.makeCall(
            new ConvertFromSqlFunction(call.getType()), rexBuilder.makeLiteral(arrayToString));
  }

  private static String arrayToString(RexBuilder rexBuilder, RexCall array) {
    StringBuilder arrayToStringBuilder = new StringBuilder().append('[');

    for (RexNode arrayItem : array.getOperands()) {
      if (arrayItem instanceof RexCall) {
        RexCall arrayItemCall = (RexCall) arrayItem;
        if (arrayItemCall.op.equals(CAST)) {
          RexNode noCastValue = arrayItemCall.getOperands().get(0);

          if (RelDataTypeEqualityComparer.areEquals(
              arrayItem.getType(),
              noCastValue.getType(),
              RelDataTypeEqualityComparer.Options.builder()
                  .withConsiderNullability(false)
                  .withConsiderPrecision(false)
                  .withConsiderScale(false)
                  .build())) {
            // Array Coercion and UDF replacement could have added a no op precision cast:
            arrayItem = noCastValue;
          }
        }
      }

      String arrayItemToString;
      switch (arrayItem.getKind()) {
        case LITERAL:
          RexLiteral literal = (RexLiteral) arrayItem;
          switch (literal.getTypeName()) {
            case CHAR:
              arrayItemToString = "\"" + literal.getValueAs(String.class) + "\"";
              break;

            case NULL:
              arrayItemToString = "null";
              break;

            case TIMESTAMP:
            case DATE:
            case TIME:
            case INTERVAL_DAY_SECOND:
            case INTERVAL_YEAR_MONTH:
              // In the future we can handle some of these using extended JSON
              throw new UnsupportedOperationException("Unable to handle time based literals");

            case DECIMAL:
              switch (literal.getType().getSqlTypeName()) {
                case DOUBLE:
                case FLOAT:
                case DECIMAL:
                  // We are losing precision here, but that is what happens with CONVERT_FROM
                  // When we migrate to ARRAY subquery / array_agg the precision will be preserved.
                  arrayItemToString =
                      Double.toString(((BigDecimal) literal.getValue()).doubleValue());
                  break;

                case TINYINT:
                case SMALLINT:
                case INTEGER:
                case BIGINT:
                  arrayItemToString = literal.getValue().toString();
                  break;

                default:
                  throw new UnsupportedOperationException(
                      "Unknown decimal kind: " + literal.getType().getSqlTypeName());
              }
              break;

            case BOOLEAN:
              arrayItemToString = literal.getValue().toString();
              break;

            default:
              throw new UnsupportedOperationException("Unknown literal type: " + literal.getType());
          }
          break;

        case ARRAY_VALUE_CONSTRUCTOR:
          arrayItemToString = arrayToString(rexBuilder, (RexCall) arrayItem);
          break;

        case ARRAY_QUERY_CONSTRUCTOR:
          RexSubQuery arraySubquery = (RexSubQuery) arrayItem;
          LogicalValues valuesRel = (LogicalValues) arraySubquery.rel;
          List<RexLiteral> items = new ArrayList<>();
          for (List<RexLiteral> tuple : valuesRel.tuples) {
            items.add(tuple.get(0));
          }

          RexNode asValueConstructor = rexBuilder.makeCall(ARRAY_VALUE_CONSTRUCTOR, items);
          arrayItemToString = arrayToString(rexBuilder, (RexCall) asValueConstructor);
          break;

        default:
          // We could have a recursive convert_from call that we need to handle
          if (!(arrayItem instanceof RexCall)) {
            throw new UnsupportedOperationException(
                "Array elements must all be literals. Encountered: " + arrayItem);
          }

          RexCall arrayItemCall = (RexCall) arrayItem;
          if (!(arrayItemCall.op instanceof ConvertFromSqlFunction)) {
            throw new UnsupportedOperationException(
                "Array elements must all be literals. Encountered: " + arrayItem);
          }

          RexLiteral rexLiteral = (RexLiteral) arrayItemCall.getOperands().get(0);
          arrayItemToString = rexLiteral.getValueAs(String.class);
          break;
      }

      arrayToStringBuilder.append(arrayItemToString).append(",");
    }

    String arrayToString =
        arrayToStringBuilder.deleteCharAt(arrayToStringBuilder.length() - 1).append(']').toString();

    return arrayToString;
  }

  private static final class ConvertFromSqlFunction extends SqlFunction {
    public ConvertFromSqlFunction(RelDataType arrayType) {
      super(
          "CONVERT_FROMJSON",
          SqlKind.OTHER,
          ReturnTypes.explicit(arrayType),
          null,
          OperandTypes.CHARACTER,
          SqlFunctionCategory.USER_DEFINED_FUNCTION);
    }
  }

  private static final class IllegalArrayItemDetector extends RexShuttle {
    private boolean hasIllegalValue;

    public IllegalArrayItemDetector() {
      hasIllegalValue = false;
    }

    @Override
    public RexNode visitInputRef(RexInputRef inputRef) {
      hasIllegalValue = true;
      return inputRef;
    }
  }
}
