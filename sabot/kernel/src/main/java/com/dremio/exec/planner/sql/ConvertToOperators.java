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
import com.google.common.collect.ImmutableMap;

public final class ConvertToOperators {
  private ConvertToOperators() {}

  public static final SqlOperator CONVERT_TO = SqlOperatorBuilder
    .name("CONVERT_TO")
    .returnType(new SqlReturnTypeInference() {
      @Override
      public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
        SqlCallBinding sqlCallBinding = (SqlCallBinding) opBinding;

        String type;
        SqlLiteral sqlLiteral = (SqlLiteral) sqlCallBinding.operand(1);
        type = ((NlsString) sqlLiteral.getValue()).getValue();
        type = type.toUpperCase();

        SqlOperator operator = TYPE_TO_OPERATOR_MAP.get(type);

        SqlCallBinding rewrittenCallBinding = new SqlCallBinding(
          sqlCallBinding.getValidator(),
          sqlCallBinding.getScope(),
          operator.createCall(
            SqlParserPos.ZERO,
            sqlCallBinding.operand(0)));

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
              .message("Second argument to CONVERT_TO must be a literal.")
              .buildSilently();
          }

          return false;
        }

        String type;
        SqlLiteral sqlLiteral = (SqlLiteral) sqlCallBinding.operand(1);
        type = ((NlsString) sqlLiteral.getValue()).getValue();
        type = type.toUpperCase();

        SqlOperator operator = TYPE_TO_OPERATOR_MAP.get(type);
        if (operator == null) {
          if (throwOnFailure) {
            throw UserException
              .validationError()
              .message("Provided type: '" + type + "' is not a known type. The supported list is: " + TYPE_TO_OPERATOR_MAP.keySet())
              .buildSilently();
          }

          return false;
        }

        SqlCallBinding rewrittenCallBinding = new SqlCallBinding(
          sqlCallBinding.getValidator(),
          sqlCallBinding.getScope(),
          operator.createCall(
            SqlParserPos.ZERO,
            sqlCallBinding.operand(0)));

        return operator.checkOperandTypes(rewrittenCallBinding, throwOnFailure);
      }

      @Override
      public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.of(2);
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

  public static final SqlOperator CONVERT_TOBASE64 = SqlOperatorBuilder
    .name("CONVERT_TOBASE64")
    .returnType(SqlTypeName.VARBINARY)
    .operandTypes(SqlOperands.CHAR_TYPES)
    .build();

  public static final SqlOperator CONVERT_TOBIGINT = SqlOperatorBuilder
    .name("CONVERT_TOBIGINT")
    .returnType(SqlTypeName.VARBINARY)
    .operandTypes(SqlOperands.BIGINT)
    .build();

  public static final SqlOperator CONVERT_TOBIGINT_BE = SqlOperatorBuilder
    .name("CONVERT_TOBIGINT_BE")
    .returnType(SqlTypeName.VARBINARY)
    .operandTypes(SqlOperands.BIGINT)
    .build();

  public static final SqlOperator CONVERT_TOBIGINT_HADOOPV = SqlOperatorBuilder
    .name("CONVERT_TOBIGINT_HADOOPV")
    .returnType(SqlTypeName.VARBINARY)
    .operandTypes(SqlOperands.BIGINT)
    .build();

  public static final SqlOperator CONVERT_TOBOOLEAN_BYTE = SqlOperatorBuilder
    .name("CONVERT_TOBOOLEAN_BYTE")
    .returnType(SqlTypeName.VARBINARY)
    .operandTypes(SqlOperands.BOOLEAN)
    .build();

  public static final SqlOperator CONVERT_TOCOMPACTJSON = SqlOperatorBuilder
    .name("CONVERT_TOCOMPACTJSON")
    .returnType(SqlTypeName.ANY)
    .operandTypes(SqlOperands.ANY)
    .build();

  public static final SqlOperator CONVERT_TODATE_EPOCH = SqlOperatorBuilder
    .name("CONVERT_TODATE_EPOCH")
    .returnType(SqlTypeName.VARBINARY)
    .operandTypes(SqlOperands.ANY)
    .build();

  public static final SqlOperator CONVERT_TODATE_EPOCH_BE = SqlOperatorBuilder
    .name("CONVERT_TODATE_EPOCH_BE")
    .returnType(SqlTypeName.VARBINARY)
    .operandTypes(SqlOperands.ANY)
    .build();

  public static final SqlOperator CONVERT_TODOUBLE = SqlOperatorBuilder
    .name("CONVERT_TODOUBLE")
    .returnType(SqlTypeName.VARBINARY)
    .operandTypes(SqlOperands.DOUBLE)
    .build();

  public static final SqlOperator CONVERT_TODOUBLE_BE = SqlOperatorBuilder
    .name("CONVERT_TODOUBLE_BE")
    .returnType(SqlTypeName.VARBINARY)
    .operandTypes(SqlOperands.DOUBLE)
    .build();

  public static final SqlOperator CONVERT_TOEXTENDEDJSON = SqlOperatorBuilder
    .name("CONVERT_TOEXTENDEDJSON")
    .returnType(SqlTypeName.VARBINARY)
    .operandTypes(SqlOperands.ANY)
    .build();

  public static final SqlOperator CONVERT_TOFLOAT = SqlOperatorBuilder
    .name("CONVERT_TOFLOAT")
    .returnType(SqlTypeName.VARBINARY)
    .operandTypes(SqlOperands.FLOAT)
    .build();

  public static final SqlOperator CONVERT_TOFLOAT_BE = SqlOperatorBuilder
    .name("CONVERT_TOFLOAT_BE")
    .returnType(SqlTypeName.VARBINARY)
    .operandTypes(SqlOperands.FLOAT)
    .build();

  public static final SqlOperator CONVERT_TOINT = SqlOperatorBuilder
    .name("CONVERT_TOINT")
    .returnType(SqlTypeName.VARBINARY)
    .operandTypes(SqlOperands.INTEGER)
    .build();

  public static final SqlOperator CONVERT_TOINT_BE = SqlOperatorBuilder
    .name("CONVERT_TOINT_BE")
    .returnType(SqlTypeName.VARBINARY)
    .operandTypes(SqlOperands.INTEGER)
    .build();

  public static final SqlOperator CONVERT_TOINT_HADOOPV = SqlOperatorBuilder
    .name("CONVERT_TOINT_HADOOPV")
    .returnType(SqlTypeName.VARBINARY)
    .operandTypes(SqlOperands.INTEGER)
    .build();

  public static final SqlOperator CONVERT_TOJSON = SqlOperatorBuilder
    .name("CONVERT_TOJSON")
    .returnType(SqlTypeName.VARBINARY)
    .operandTypes(SqlOperands.ANY)
    .build();

  public static final SqlOperator CONVERT_TOSIMPLEJSON = SqlOperatorBuilder
    .name("CONVERT_TOSIMPLEJSON")
    .returnType(SqlTypeName.VARBINARY)
    .operandTypes(SqlOperands.ANY)
    .build();

  public static final SqlOperator CONVERT_TOTIMESTAMP_EPOCH = SqlOperatorBuilder
    .name("CONVERT_TOTIMESTAMP_EPOCH")
    .returnType(SqlTypeName.VARBINARY)
    .operandTypes(SqlOperands.TIMESTAMP)
    .build();

  public static final SqlOperator CONVERT_TOTIMESTAMP_EPOCH_BE = SqlOperatorBuilder
    .name("CONVERT_TOTIMESTAMP_EPOCH_BE")
    .returnType(SqlTypeName.VARBINARY)
    .operandTypes(SqlOperands.TIMESTAMP)
    .build();

  public static final SqlOperator CONVERT_TOTIME_EPOCH = SqlOperatorBuilder
    .name("CONVERT_TOTIME_EPOCH")
    .returnType(SqlTypeName.VARBINARY)
    .operandTypes(SqlOperands.TIME)
    .build();

  public static final SqlOperator CONVERT_TOTIME_EPOCH_BE = SqlOperatorBuilder
    .name("CONVERT_TOTIME_EPOCH_BE")
    .returnType(SqlTypeName.VARBINARY)
    .operandTypes(SqlOperands.TIME)
    .build();

  public static final SqlOperator CONVERT_TOUTF8 = SqlOperatorBuilder
    .name("CONVERT_TOUTF8")
    .returnType(SqlTypeName.VARBINARY)
    // TODO: This should really be done through an explict cast
    .operandTypes(SqlOperands.ANY)
    .build();

  private static final Map<String, SqlOperator> TYPE_TO_OPERATOR_MAP = ImmutableMap.<String, SqlOperator>builder()
    .put("BASE64", CONVERT_TOBASE64)
    .put("BIGINT", CONVERT_TOBIGINT)
    .put("BIGINT_BE", CONVERT_TOBIGINT_BE)
    .put("BIGINT_HADOOPV", CONVERT_TOBIGINT_HADOOPV)
    .put("BOOLEAN_BYTE", CONVERT_TOBOOLEAN_BYTE)
    .put("COMPACTJSON", CONVERT_TOCOMPACTJSON)
    .put("DATE_EPOCH", CONVERT_TODATE_EPOCH)
    .put("DATE_EPOCH_BE", CONVERT_TODATE_EPOCH_BE)
    .put("DOUBLE", CONVERT_TODOUBLE)
    .put("DOUBLE_BE", CONVERT_TODOUBLE_BE)
    .put("EXTENDEDJSON", CONVERT_TOEXTENDEDJSON)
    .put("FLOAT", CONVERT_TOFLOAT)
    .put("FLOAT_BE", CONVERT_TOFLOAT_BE)
    .put("INT", CONVERT_TOINT)
    .put("INT_BE", CONVERT_TOINT_BE)
    .put("INT_HADOOPV", CONVERT_TOINT_HADOOPV)
    .put("JSON", CONVERT_TOJSON)
    .put("SIMPLEJSON", CONVERT_TOSIMPLEJSON)
    .put("TIMESTAMP_EPOCH", CONVERT_TOTIMESTAMP_EPOCH)
    .put("TIMESTAMP_EPOCH_BE", CONVERT_TOTIMESTAMP_EPOCH_BE)
    .put("TIME_EPOCH", CONVERT_TOTIME_EPOCH)
    .put("TIME_EPOCH_BE", CONVERT_TOTIME_EPOCH_BE)
    .put("UTF8", CONVERT_TOUTF8)
    .build();

  public static SqlOperator convertTypeToOperator(String type) {
    type = type.toUpperCase();
    return TYPE_TO_OPERATOR_MAP.get(type);
  }
}
