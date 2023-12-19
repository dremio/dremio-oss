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
package com.dremio.exec.planner.sql;

import java.util.Map;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;

import com.dremio.common.exceptions.UserException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public final class ConvertFromOperators {
  private ConvertFromOperators() {}

  public static final SqlOperator CONVERT_FROM = SqlOperatorBuilder
    .name("CONVERT_FROM")
    .returnType(new SqlReturnTypeInference() {
      @Override
      public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
        SqlCallBinding sqlCallBinding = (SqlCallBinding) opBinding;

        String type;
        SqlLiteral sqlLiteral = (SqlLiteral) sqlCallBinding.operand(1);
        type = ((NlsString) sqlLiteral.getValue()).getValue();
        type = type.toUpperCase();

        SqlOperator operator = (sqlCallBinding.getOperandCount() == 3)
          ? CONVERT_REPLACEUTF8
          : TYPE_TO_OPERATOR_MAP.get(type);

        SqlCallBinding rewrittenCallBinding;
        if (operator == CONVERT_REPLACEUTF8) {
          rewrittenCallBinding = new SqlCallBinding(
            sqlCallBinding.getValidator(),
            sqlCallBinding.getScope(),
            operator.createCall(
              SqlParserPos.ZERO,
              ImmutableList.of(sqlCallBinding.operands().get(0), sqlCallBinding.operands().get(2))));
        } else {
          rewrittenCallBinding = new SqlCallBinding(
            sqlCallBinding.getValidator(),
            sqlCallBinding.getScope(),
            operator.createCall(
              SqlParserPos.ZERO,
              sqlCallBinding.operand(0)));
        }

        return operator.inferReturnType(rewrittenCallBinding);
      }
    })
    .operandTypes(new SqlOperandTypeChecker() {
      @Override
      public boolean checkOperandTypes(SqlCallBinding sqlCallBinding, boolean throwOnFailure) {
        if (sqlCallBinding.operand(1).getKind() != SqlKind.LITERAL) {
          if (throwOnFailure) {
            throw UserException
              .validationError()
              .message("Second argument to CONVERT_FROM must be a literal.")
              .buildSilently();
          }

          return false;
        }

        String type;
        SqlLiteral sqlLiteral = (SqlLiteral) sqlCallBinding.operand(1);
        type = ((NlsString) sqlLiteral.getValue()).getValue();
        type = type.toUpperCase();

        SqlOperator operator;
        if (sqlCallBinding.getOperandCount() != 3) {
          operator = TYPE_TO_OPERATOR_MAP.get(type);
        } else {
          if (!"UTF8".equals(type)) {
            if (throwOnFailure) {
              throw UserException
                .validationError()
                .message("3-argument convert_from only supported for utf8 encoding. Instead, got %s", type)
                .buildSilently();
            }

            return false;
          }

          operator = CONVERT_REPLACEUTF8;
        }

        if (operator == null) {
          if (throwOnFailure) {
            throw UserException
              .validationError()
              .message("Provided type: '" + type + "' is not a known type. The supported list is: " + TYPE_TO_OPERATOR_MAP.keySet())
              .buildSilently();
          }

          return false;
        }

        SqlCallBinding rewrittenCallBinding;
        if (operator == CONVERT_REPLACEUTF8) {
          rewrittenCallBinding = new SqlCallBinding(
            sqlCallBinding.getValidator(),
            sqlCallBinding.getScope(),
            operator.createCall(
              SqlParserPos.ZERO,
              ImmutableList.of(sqlCallBinding.operands().get(0), sqlCallBinding.operands().get(2))));
        } else {
          rewrittenCallBinding = new SqlCallBinding(
            sqlCallBinding.getValidator(),
            sqlCallBinding.getScope(),
            operator.createCall(
              SqlParserPos.ZERO,
              sqlCallBinding.operand(0)));
        }

        return operator.checkOperandTypes(rewrittenCallBinding, throwOnFailure);
      }

      @Override
      public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.between(2, 3);
      }

      @Override
      public String getAllowedSignatures(SqlOperator op, String opName) {
        return null;
      }

      @Override
      public Consistency getConsistency() {
        return null;
      }

      @Override
      public boolean isOptional(int i) {
        return false;
      }
    })
    .build();

  public static final SqlOperator CONVERT_FROMBIGINT = SqlOperatorBuilder
    .name("CONVERT_FROMBIGINT")
    .returnType(SqlTypeName.BIGINT)
    .operandTypes(SqlOperands.BINARY_TYPES)
    .build();

  public static final SqlOperator CONVERT_FROMBIGINT_BE = SqlOperatorBuilder
    .name("CONVERT_FROMBIGINT_BE")
    .returnType(SqlTypeName.BIGINT)
    .operandTypes(SqlOperands.BINARY_TYPES)
    .build();

  public static final SqlOperator CONVERT_FROMBIGINT_HADOOPV = SqlOperatorBuilder
    .name("CONVERT_FROMBIGINT_HADOOPV")
    .returnType(SqlTypeName.BIGINT)
    .operandTypes(SqlOperands.BINARY_TYPES)
    .build();

  public static final SqlOperator CONVERT_FROMBOOLEAN_BYTE = SqlOperatorBuilder
    .name("CONVERT_FROMBOOLEAN_BYTE")
    .returnType(SqlTypeName.BOOLEAN)
    .operandTypes(SqlOperands.BINARY_TYPES)
    .build();

  public static final SqlOperator CONVERT_FROMDATE_EPOCH = SqlOperatorBuilder
    .name("CONVERT_FROMDATE_EPOCH")
    .returnType(SqlTypeName.DATE)
    .operandTypes(SqlOperands.BINARY_TYPES)
    .build();

  public static final SqlOperator CONVERT_FROMDATE_EPOCH_BE = SqlOperatorBuilder
    .name("CONVERT_FROMDATE_EPOCH_BE")
    .returnType(SqlTypeName.DATE)
    .operandTypes(SqlOperands.BINARY_TYPES)
    .build();

  public static final SqlOperator CONVERT_FROMDOUBLE = SqlOperatorBuilder
    .name("CONVERT_FROMDOUBLE")
    .returnType(SqlTypeName.DOUBLE)
    .operandTypes(SqlOperands.BINARY_TYPES)
    .build();

  public static final SqlOperator CONVERT_FROMDOUBLE_BE = SqlOperatorBuilder
    .name("CONVERT_FROMDOUBLE_BE")
    .returnType(SqlTypeName.DOUBLE)
    .operandTypes(SqlOperands.BINARY_TYPES)
    .build();

  public static final SqlOperator CONVERT_FROMFLOAT = SqlOperatorBuilder
    .name("CONVERT_FROMFLOAT")
    .returnType(SqlTypeName.FLOAT)
    .operandTypes(SqlOperands.BINARY_TYPES)
    .build();

  public static final SqlOperator CONVERT_FROMFLOAT_BE = SqlOperatorBuilder
    .name("CONVERT_FROMFLOAT_BE")
    .returnType(SqlTypeName.FLOAT)
    .operandTypes(SqlOperands.BINARY_TYPES)
    .build();

  public static final SqlOperator CONVERT_FROMINT = SqlOperatorBuilder
    .name("CONVERT_FROMINT")
    .returnType(SqlTypeName.INTEGER)
    .operandTypes(SqlOperands.BINARY_TYPES)
    .build();

  public static final SqlOperator CONVERT_FROMINT_BE = SqlOperatorBuilder
    .name("CONVERT_FROMINT_BE")
    .returnType(SqlTypeName.INTEGER)
    .operandTypes(SqlOperands.BINARY_TYPES)
    .build();

  public static final SqlOperator CONVERT_FROMINT_HADOOPV = SqlOperatorBuilder
    .name("CONVERT_FROMINT_HADOOPV")
    .returnType(SqlTypeName.INTEGER)
    .operandTypes(SqlOperands.BINARY_TYPES)
    .build();

  public static final SqlOperator CONVERT_FROMJSON = SqlOperatorBuilder
    .name("CONVERT_FROMJSON")
    .returnType(opBinding -> {
      RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
      SqlTypeName sqlTypeName = getJsonTypeName(opBinding);
      switch (sqlTypeName) {
        case ARRAY:
          return typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.ANY), -1);

        case STRUCTURED:
          // We would need to parse the json return the correct struct type
          return typeFactory.createTypeWithNullability(
            typeFactory.createSqlType(SqlTypeName.ANY),
            true);

        default:
          return typeFactory.createTypeWithNullability(
            typeFactory.createSqlType(sqlTypeName),
            true);
      }})
    .operandTypes(SqlOperand.union(SqlOperands.CHAR_TYPES, SqlOperands.BINARY_TYPES))
    .build();

  private static SqlTypeName getJsonTypeName(SqlOperatorBinding opBinding) {
    if (!opBinding.isOperandLiteral(0, true)) {
      return SqlTypeName.ANY;
    }

    String jsonLiteralValue = opBinding.getOperandLiteralValue(0, String.class);
    if (jsonLiteralValue == null) {
      return SqlTypeName.ANY;
    }

    char firstCharacter = jsonLiteralValue.charAt(0);
    switch (firstCharacter) {
      case 'n':
        return SqlTypeName.NULL;

      case 't':
      case 'f':
        return SqlTypeName.BOOLEAN;

      case '0':
      case '1':
      case '2':
      case '3':
      case '4':
      case '5':
      case '6':
      case '7':
      case '8':
      case '9':
      case '-':
        return SqlTypeName.DOUBLE;

      case '"':
        return SqlTypeName.VARCHAR;

      case '[':
        // We will need to do a full parse the of the JSON if we want to get this right.
        return SqlTypeName.ARRAY;

      case '{':
        // We would need to parse the json return the correct struct type
        return SqlTypeName.STRUCTURED;

      default:
        return SqlTypeName.ANY;
    }
  }

  public static final SqlOperator CONVERT_FROMTIMESTAMP_EPOCH = SqlOperatorBuilder
    .name("CONVERT_FROMTIMESTAMP_EPOCH")
    .returnType(SqlTypeName.TIMESTAMP)
    .operandTypes(SqlOperands.BINARY_TYPES)
    .build();

  public static final SqlOperator CONVERT_FROMTIMESTAMP_EPOCH_BE = SqlOperatorBuilder
    .name("CONVERT_FROMTIMESTAMP_EPOCH_BE")
    .returnType(SqlTypeName.TIMESTAMP)
    .operandTypes(SqlOperands.BINARY_TYPES)
    .build();

  public static final SqlOperator CONVERT_FROMTIMESTAMP_IMPALA = SqlOperatorBuilder
    .name("CONVERT_FROMTIMESTAMP_IMPALA")
    .returnType(SqlTypeName.TIMESTAMP)
    .operandTypes(SqlOperands.BINARY_TYPES)
    .build();

  public static final SqlOperator CONVERT_FROMTIMESTAMP_IMPALA_LOCALTIMEZONE = SqlOperatorBuilder
    .name("CONVERT_FROMTIMESTAMP_IMPALA_LOCALTIMEZONE")
    .returnType(SqlTypeName.TIMESTAMP)
    .operandTypes(SqlOperands.BINARY_TYPES)
    .build();

  public static final SqlOperator CONVERT_FROMTIME_EPOCH = SqlOperatorBuilder
    .name("CONVERT_FROMTIME_EPOCH")
    .returnType(SqlTypeName.TIME)
    .operandTypes(SqlOperands.BINARY_TYPES)
    .build();

  public static final SqlOperator CONVERT_FROMTIME_EPOCH_BE = SqlOperatorBuilder
    .name("CONVERT_FROMTIME_EPOCH_BE")
    .returnType(SqlTypeName.TIME)
    .operandTypes(SqlOperands.BINARY_TYPES)
    .build();

  public static final SqlOperator CONVERT_FROMUTF8 = SqlOperatorBuilder
    .name("CONVERT_FROMUTF8")
    .returnType(SqlTypeName.VARCHAR)
    .operandTypes(SqlOperands.ANY)
    .build();

  public static final SqlOperator CONVERT_REPLACEUTF8 = SqlOperatorBuilder
    .name("CONVERT_REPLACEUTF8")
    .returnType(SqlTypeName.VARCHAR)
    // TODO: this should be an explict cast instead of a union
    .operandTypes(SqlOperands.ANY, SqlOperands.CHAR_TYPES)
    .build();

  private static final Map<String, SqlOperator> TYPE_TO_OPERATOR_MAP = new ImmutableMap.Builder<String, SqlOperator>()
    .put("BIGINT", CONVERT_FROMBIGINT)
    .put("BIGINT_BE", CONVERT_FROMBIGINT_BE)
    .put("BIGINT_HADOOPV", CONVERT_FROMBIGINT_HADOOPV)
    .put("BOOLEAN_BYTE", CONVERT_FROMBOOLEAN_BYTE)
    .put("DATE_EPOCH", CONVERT_FROMDATE_EPOCH)
    .put("DATE_EPOCH_BE", CONVERT_FROMDATE_EPOCH_BE)
    .put("DOUBLE", CONVERT_FROMDOUBLE)
    .put("DOUBLE_BE", CONVERT_FROMDOUBLE_BE)
    .put("FLOAT", CONVERT_FROMFLOAT)
    .put("FLOAT_BE", CONVERT_FROMFLOAT_BE)
    .put("INT", CONVERT_FROMINT)
    .put("INT_BE", CONVERT_FROMINT_BE)
    .put("INT_HADOOPV", CONVERT_FROMINT_HADOOPV)
    .put("JSON", CONVERT_FROMJSON)
    .put("TIMESTAMP_EPOCH", CONVERT_FROMTIMESTAMP_EPOCH)
    .put("TIMESTAMP_EPOCH_BE", CONVERT_FROMTIMESTAMP_EPOCH_BE)
    .put("TIMESTAMP_IMPALA", CONVERT_FROMTIMESTAMP_IMPALA)
    .put("TIMESTAMP_IMPALA_LOCALTIMEZONE", CONVERT_FROMTIMESTAMP_IMPALA_LOCALTIMEZONE)
    .put("TIME_EPOCH", CONVERT_FROMTIME_EPOCH)
    .put("TIME_EPOCH_BE", CONVERT_FROMTIME_EPOCH_BE)
    .put("UTF8", CONVERT_FROMUTF8)
    .build();

  public static SqlOperator convertTypeToOperator(String type) {
    type = type.toUpperCase();
    return TYPE_TO_OPERATOR_MAP.get(type);
  }
}
