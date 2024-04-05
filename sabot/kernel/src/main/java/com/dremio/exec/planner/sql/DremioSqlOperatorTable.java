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

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.expr.fn.arrayagg.ArrayAggInternalOperators;
import com.dremio.exec.expr.fn.hll.HyperLogLog;
import com.dremio.exec.expr.fn.impl.MapFunctions;
import com.dremio.exec.expr.fn.listagg.ListAgg;
import com.dremio.exec.expr.fn.tdigest.TDigest;
import com.dremio.exec.planner.sql.parser.SqlContains;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.fun.SqlBaseContextVariable;
import org.apache.calcite.sql.fun.SqlBasicAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;
import org.apache.calcite.util.Optionality;

/**
 * Dremio's Sql Operator Table
 *
 * <p>These operators are used to resolve {@link org.apache.calcite.sql.SqlJdbcFunctionCall} before
 * checking {@link org.apache.calcite.sql.fun.SqlStdOperatorTable}.
 */
public class DremioSqlOperatorTable extends ReflectiveSqlOperatorTable {
  private static DremioSqlOperatorTable instance;

  // ---------------------
  // HyperLogLog Functions
  // ---------------------

  public static final SqlAggFunction HLL = new HyperLogLog.SqlHllAggFunction();
  public static final SqlAggFunction HLL_MERGE = new HyperLogLog.SqlHllMergeAggFunction();
  public static final SqlAggFunction NDV = new HyperLogLog.SqlNdvAggFunction();
  public static final SqlFunction HLL_DECODE = new HyperLogLog.SqlHllDecodeOperator();

  // ---------------------
  // ListAgg Functions
  // ---------------------
  public static final SqlAggFunction LISTAGG_MERGE = new ListAgg.SqlListAggMergeFunction();
  public static final SqlAggFunction LOCAL_LISTAGG = new ListAgg.SqlLocalListAggFunction();

  // ---------------------
  // ARRAY_AGG Functions
  // ---------------------
  public static final SqlAggFunction PHASE1_ARRAY_AGG =
      new ArrayAggInternalOperators.Phase1ArrayAgg();
  public static final SqlAggFunction PHASE2_ARRAY_AGG =
      new ArrayAggInternalOperators.Phase2ArrayAgg();

  // ---------------
  // GEO functions
  // ---------------
  public static final SqlOperator GEO_DISTANCE =
      SqlOperatorBuilder.name("GEO_DISTANCE")
          .returnType(ReturnTypes.DOUBLE)
          .operandTypes(SqlOperands.FLOAT, SqlOperands.FLOAT, SqlOperands.FLOAT, SqlOperands.FLOAT)
          .build();

  public static final SqlOperator GEO_NEARBY =
      SqlOperatorBuilder.name("GEO_NEARBY")
          .returnType(ReturnTypes.BOOLEAN)
          .operandTypes(
              SqlOperands.FLOAT,
              SqlOperands.FLOAT,
              SqlOperands.FLOAT,
              SqlOperands.FLOAT,
              SqlOperands.NUMERIC_TYPES)
          .build();

  public static final SqlOperator GEO_BEYOND =
      SqlOperatorBuilder.name("GEO_BEYOND")
          .returnType(ReturnTypes.BOOLEAN)
          .operandTypes(
              SqlOperands.FLOAT,
              SqlOperands.FLOAT,
              SqlOperands.FLOAT,
              SqlOperands.FLOAT,
              SqlOperands.NUMERIC_TYPES)
          .build();

  // ---------------------
  // STD Library Functions
  // ---------------------

  public static final SqlOperator ROUND =
      SqlOperatorBuilder.name("ROUND")
          .returnType(DremioReturnTypes.NULLABLE_ROUND)
          .operandTypes(OperandTypes.NUMERIC_OPTIONAL_INTEGER)
          .build();

  public static final SqlOperator LOG2 =
      SqlOperatorBuilder.name("LOG2")
          .returnType(ReturnTypes.DOUBLE)
          .operandTypes(SqlOperands.NUMERIC_TYPES)
          .build();

  public static final SqlOperator LOG =
      SqlOperatorBuilder.name("LOG")
          .returnType(ReturnTypes.DOUBLE)
          .operandTypes(SqlOperands.NUMERIC_TYPES, SqlOperands.NUMERIC_TYPES)
          .build();

  public static final SqlOperator TRUNCATE =
      SqlOperatorBuilder.name("TRUNCATE")
          .returnType(DremioReturnTypes.NULLABLE_TRUNCATE)
          .operandTypes(OperandTypes.NUMERIC_OPTIONAL_INTEGER)
          .build();

  public static final SqlOperator TRUNC = SqlOperatorBuilder.alias("TRUNC", TRUNCATE);

  public static final SqlOperator TYPEOF =
      SqlOperatorBuilder.name("TYPEOF")
          .returnType(SqlTypeName.VARCHAR)
          .operandTypes(SqlOperands.ANY)
          .build();

  public static final SqlOperator IDENTITY =
      SqlOperatorBuilder.name("IDENTITY")
          .returnType(ReturnTypes.ARG0)
          .operandTypes(SqlOperands.ANY)
          .build();

  // -----------------------
  // Overriding Default Behavior
  // -----------------------

  // REGEX_SPLIT returns an array of VARCHAR instead of ANY
  public static final SqlOperator REGEXP_SPLIT =
      SqlOperatorBuilder.name("REGEXP_SPLIT")
          .returnType(
              JavaTypeFactoryImpl.INSTANCE.createArrayType(
                  JavaTypeFactoryImpl.INSTANCE.createSqlType(SqlTypeName.VARCHAR), -1))
          .operandTypes(
              SqlOperands.CHAR_TYPES,
              SqlOperands.CHAR_TYPES,
              SqlOperands.CHAR_TYPES,
              SqlOperands.EXACT_TYPES)
          .build();

  // SqlStdOperatorTable.CARDINALITY is overridden here because
  // it supports LIST, MAP as well as STRUCT. In Dremio, we want to
  // allow only LIST and MAP. Not STRUCT.
  public static final SqlOperator CARDINALITY =
      SqlOperatorBuilder.name("CARDINALITY")
          .returnType(ReturnTypes.INTEGER_NULLABLE)
          .operandTypes(SqlOperand.union(SqlOperands.ARRAY, SqlOperands.MAP))
          .build();

  // Concat needs to support taking a variable length argument and honor a nullability check
  public static final SqlOperator CONCAT =
      SqlOperatorBuilder.name("CONCAT")
          .returnType(DremioReturnTypes.VARCHAR_MAX_PRECISION_NULLABLE)
          .operandTypes(SqlOperands.ANY, SqlOperands.ANY_VARIADIC)
          .build();

  // Calcite's REPLACE type inference does not honor the precision
  public static final SqlOperator REPLACE =
      SqlOperatorBuilder.name("REPLACE")
          .returnType(DremioReturnTypes.VARCHAR_MAX_PRECISION_NULLABLE)
          .operandTypes(SqlOperands.ANY, SqlOperands.ANY, SqlOperands.ANY)
          .build();

  // We want to allow for an optional encoding format in the LENGTH function
  public static final SqlOperator LENGTH =
      SqlOperatorBuilder.name("LENGTH")
          .returnType(ReturnTypes.INTEGER_NULLABLE)
          .operandTypes(SqlOperands.ANY, SqlOperands.ANY)
          .build();

  // Trim and Pad functions need to honor nullability
  public static final SqlOperator LPAD =
      SqlOperatorBuilder.name("LPAD")
          .returnType(DremioReturnTypes.VARCHAR_MAX_PRECISION_NULLABLE)
          .operandTypes(SqlOperands.ANY, SqlOperands.ANY, SqlOperands.ANY)
          .build();

  public static final SqlOperator RPAD =
      SqlOperatorBuilder.name("RPAD")
          .returnType(DremioReturnTypes.VARCHAR_MAX_PRECISION_NULLABLE)
          .operandTypes(SqlOperands.ANY, SqlOperands.ANY, SqlOperands.ANY)
          .build();

  public static final SqlOperator LTRIM =
      SqlOperatorBuilder.name("LTRIM")
          .returnType(DremioReturnTypes.VARCHAR_MAX_PRECISION_NULLABLE)
          .operandTypes(SqlOperands.ANY, SqlOperands.ANY_OPTIONAL)
          .build();

  public static final SqlOperator RTRIM =
      SqlOperatorBuilder.name("RTRIM")
          .returnType(DremioReturnTypes.VARCHAR_MAX_PRECISION_NULLABLE)
          .operandTypes(SqlOperands.ANY, SqlOperands.ANY_OPTIONAL)
          .build();

  public static final SqlOperator BTRIM =
      SqlOperatorBuilder.name("BTRIM")
          .returnType(DremioReturnTypes.VARCHAR_MAX_PRECISION_NULLABLE)
          .operandTypes(SqlOperands.ANY, SqlOperands.ANY_OPTIONAL)
          .build();

  public static final SqlOperator TRIM =
      SqlOperatorBuilder.name("TRIM")
          .returnType(DremioReturnTypes.VARCHAR_MAX_PRECISION_NULLABLE)
          .operandTypes(SqlOperands.ANY, SqlOperands.ANY_OPTIONAL)
          .build();

  public static final SqlOperator REPEAT =
      SqlOperatorBuilder.name("REPEAT")
          .returnType(DremioReturnTypes.VARCHAR_MAX_PRECISION_NULLABLE)
          .operandTypes(SqlOperands.VARCHAR, SqlOperands.INTEGER)
          .build();

  public static final SqlOperator SPACE =
      SqlOperatorBuilder.name("SPACE")
          .returnType(DremioReturnTypes.VARCHAR_MAX_PRECISION_NULLABLE)
          .operandTypes(SqlOperands.INTEGER)
          .build();

  public static final SqlOperator SECOND =
      SqlOperatorBuilder.name("SECOND")
          .returnType(ReturnTypes.BIGINT_NULLABLE)
          .operandTypes(
              OperandTypes.or(
                  OperandTypes.family(SqlTypeFamily.DATE),
                  OperandTypes.family(SqlTypeFamily.TIMESTAMP),
                  OperandTypes.family(SqlTypeFamily.DATETIME),
                  OperandTypes.family(SqlTypeFamily.DATETIME_INTERVAL),
                  OperandTypes.family(SqlTypeFamily.INTERVAL_DAY_TIME)))
          .build();

  public static final SqlOperator MINUTE =
      SqlOperatorBuilder.name("MINUTE")
          .returnType(ReturnTypes.BIGINT_NULLABLE)
          .operandTypes(
              OperandTypes.or(
                  OperandTypes.family(SqlTypeFamily.DATE),
                  OperandTypes.family(SqlTypeFamily.TIMESTAMP),
                  OperandTypes.family(SqlTypeFamily.DATETIME),
                  OperandTypes.family(SqlTypeFamily.DATETIME_INTERVAL),
                  OperandTypes.family(SqlTypeFamily.INTERVAL_DAY_TIME)))
          .build();

  public static final SqlOperator HOUR =
      SqlOperatorBuilder.name("HOUR")
          .returnType(ReturnTypes.BIGINT_NULLABLE)
          .operandTypes(
              OperandTypes.or(
                  OperandTypes.family(SqlTypeFamily.DATE),
                  OperandTypes.family(SqlTypeFamily.TIMESTAMP),
                  OperandTypes.family(SqlTypeFamily.DATETIME),
                  OperandTypes.family(SqlTypeFamily.DATETIME_INTERVAL),
                  OperandTypes.family(SqlTypeFamily.INTERVAL_DAY_TIME)))
          .build();

  public static final SqlOperator DAY =
      SqlOperatorBuilder.name("DAY")
          .returnType(ReturnTypes.BIGINT_NULLABLE)
          .operandTypes(
              OperandTypes.or(
                  OperandTypes.family(SqlTypeFamily.DATE),
                  OperandTypes.family(SqlTypeFamily.TIMESTAMP),
                  OperandTypes.family(SqlTypeFamily.DATETIME),
                  OperandTypes.family(SqlTypeFamily.DATETIME_INTERVAL),
                  OperandTypes.family(SqlTypeFamily.INTERVAL_DAY_TIME),
                  OperandTypes.family(SqlTypeFamily.INTERVAL_YEAR_MONTH)))
          .build();

  public static final SqlOperator DAYOFMONTH =
      SqlOperatorBuilder.name("DAYOFMONTH")
          .returnType(ReturnTypes.BIGINT_NULLABLE)
          .operandTypes(
              OperandTypes.or(
                  OperandTypes.family(SqlTypeFamily.DATE),
                  OperandTypes.family(SqlTypeFamily.TIMESTAMP),
                  OperandTypes.family(SqlTypeFamily.DATETIME),
                  OperandTypes.family(SqlTypeFamily.DATETIME_INTERVAL),
                  OperandTypes.family(SqlTypeFamily.INTERVAL_DAY_TIME),
                  OperandTypes.family(SqlTypeFamily.INTERVAL_YEAR_MONTH)))
          .build();

  public static final SqlOperator MONTH =
      SqlOperatorBuilder.name("MONTH")
          .returnType(ReturnTypes.BIGINT_NULLABLE)
          .operandTypes(
              OperandTypes.or(
                  OperandTypes.family(SqlTypeFamily.DATE),
                  OperandTypes.family(SqlTypeFamily.TIMESTAMP),
                  OperandTypes.family(SqlTypeFamily.DATETIME),
                  OperandTypes.family(SqlTypeFamily.DATETIME_INTERVAL),
                  OperandTypes.family(SqlTypeFamily.INTERVAL_DAY_TIME),
                  OperandTypes.family(SqlTypeFamily.INTERVAL_YEAR_MONTH)))
          .build();

  public static final SqlOperator YEAR =
      SqlOperatorBuilder.name("YEAR")
          .returnType(ReturnTypes.BIGINT_NULLABLE)
          .operandTypes(
              OperandTypes.or(
                  OperandTypes.family(SqlTypeFamily.DATE),
                  OperandTypes.family(SqlTypeFamily.TIMESTAMP),
                  OperandTypes.family(SqlTypeFamily.DATETIME),
                  OperandTypes.family(SqlTypeFamily.DATETIME_INTERVAL),
                  OperandTypes.family(SqlTypeFamily.INTERVAL_DAY_TIME),
                  OperandTypes.family(SqlTypeFamily.INTERVAL_YEAR_MONTH)))
          .build();

  // -----------------------
  // Dremio Custom Functions
  // -----------------------
  public static final SqlOperator CONVERT_FROM = ConvertFromOperators.CONVERT_FROM;

  public static final SqlOperator CONVERT_FROMBIGINT = ConvertFromOperators.CONVERT_FROMBIGINT;

  public static final SqlOperator CONVERT_FROMBIGINT_BE =
      ConvertFromOperators.CONVERT_FROMBIGINT_BE;

  public static final SqlOperator CONVERT_FROMBIGINT_HADOOPV =
      ConvertFromOperators.CONVERT_FROMBIGINT_HADOOPV;

  public static final SqlOperator CONVERT_FROMBOOLEAN_BYTE =
      ConvertFromOperators.CONVERT_FROMBOOLEAN_BYTE;

  public static final SqlOperator CONVERT_FROMDATE_EPOCH =
      ConvertFromOperators.CONVERT_FROMDATE_EPOCH;

  public static final SqlOperator CONVERT_FROMDATE_EPOCH_BE =
      ConvertFromOperators.CONVERT_FROMDATE_EPOCH_BE;

  public static final SqlOperator CONVERT_FROMDOUBLE = ConvertFromOperators.CONVERT_FROMDOUBLE;

  public static final SqlOperator CONVERT_FROMDOUBLE_BE =
      ConvertFromOperators.CONVERT_FROMDOUBLE_BE;

  public static final SqlOperator CONVERT_FROMFLOAT = ConvertFromOperators.CONVERT_FROMFLOAT;

  public static final SqlOperator CONVERT_FROMFLOAT_BE = ConvertFromOperators.CONVERT_FROMFLOAT_BE;

  public static final SqlOperator CONVERT_FROMINT = ConvertFromOperators.CONVERT_FROMINT;

  public static final SqlOperator CONVERT_FROMINT_BE = ConvertFromOperators.CONVERT_FROMINT_BE;

  public static final SqlOperator CONVERT_FROMINT_HADOOPV =
      ConvertFromOperators.CONVERT_FROMINT_HADOOPV;

  public static final SqlOperator CONVERT_FROMJSON = ConvertFromOperators.CONVERT_FROMJSON;

  public static final SqlOperator CONVERT_FROMTIMESTAMP_EPOCH =
      ConvertFromOperators.CONVERT_FROMTIMESTAMP_EPOCH;

  public static final SqlOperator CONVERT_FROMTIMESTAMP_EPOCH_BE =
      ConvertFromOperators.CONVERT_FROMTIMESTAMP_EPOCH_BE;

  public static final SqlOperator CONVERT_FROMTIMESTAMP_IMPALA =
      ConvertFromOperators.CONVERT_FROMTIMESTAMP_IMPALA;

  public static final SqlOperator CONVERT_FROMTIMESTAMP_IMPALA_LOCALTIMEZONE =
      ConvertFromOperators.CONVERT_FROMTIMESTAMP_IMPALA_LOCALTIMEZONE;

  public static final SqlOperator CONVERT_FROMTIME_EPOCH =
      ConvertFromOperators.CONVERT_FROMTIME_EPOCH;

  public static final SqlOperator CONVERT_FROMTIME_EPOCH_BE =
      ConvertFromOperators.CONVERT_FROMTIME_EPOCH_BE;

  public static final SqlOperator CONVERT_FROMUTF8 = ConvertFromOperators.CONVERT_FROMUTF8;

  public static final SqlOperator CONVERT_REPLACEUTF8 = ConvertFromOperators.CONVERT_REPLACEUTF8;

  public static final SqlOperator CONVERT_TO = ConvertToOperators.CONVERT_TO;
  public static final SqlOperator CONVERT_TOBASE64 = ConvertToOperators.CONVERT_TOBASE64;
  public static final SqlOperator CONVERT_TOBIGINT = ConvertToOperators.CONVERT_TOBIGINT;
  public static final SqlOperator CONVERT_TOBIGINT_BE = ConvertToOperators.CONVERT_TOBIGINT_BE;
  public static final SqlOperator CONVERT_TOBIGINT_HADOOPV =
      ConvertToOperators.CONVERT_TOBIGINT_HADOOPV;
  public static final SqlOperator CONVERT_TOBOOLEAN_BYTE =
      ConvertToOperators.CONVERT_TOBOOLEAN_BYTE;
  public static final SqlOperator CONVERT_TOCOMPACTJSON = ConvertToOperators.CONVERT_TOCOMPACTJSON;
  public static final SqlOperator CONVERT_TODATE_EPOCH = ConvertToOperators.CONVERT_TODATE_EPOCH;
  public static final SqlOperator CONVERT_TODATE_EPOCH_BE =
      ConvertToOperators.CONVERT_TODATE_EPOCH_BE;
  public static final SqlOperator CONVERT_TODOUBLE = ConvertToOperators.CONVERT_TODOUBLE;
  public static final SqlOperator CONVERT_TODOUBLE_BE = ConvertToOperators.CONVERT_TODOUBLE_BE;
  public static final SqlOperator CONVERT_TOEXTENDEDJSON =
      ConvertToOperators.CONVERT_TOEXTENDEDJSON;
  public static final SqlOperator CONVERT_TOFLOAT = ConvertToOperators.CONVERT_TOFLOAT;
  public static final SqlOperator CONVERT_TOFLOAT_BE = ConvertToOperators.CONVERT_TOFLOAT_BE;
  public static final SqlOperator CONVERT_TOINT = ConvertToOperators.CONVERT_TOINT;
  public static final SqlOperator CONVERT_TOINT_BE = ConvertToOperators.CONVERT_TOINT_BE;
  public static final SqlOperator CONVERT_TOINT_HADOOPV = ConvertToOperators.CONVERT_TOINT_HADOOPV;
  public static final SqlOperator CONVERT_TOJSON = ConvertToOperators.CONVERT_TOJSON;
  public static final SqlOperator CONVERT_TOSIMPLEJSON = ConvertToOperators.CONVERT_TOSIMPLEJSON;
  public static final SqlOperator CONVERT_TOTIMESTAMP_EPOCH =
      ConvertToOperators.CONVERT_TOTIMESTAMP_EPOCH;
  public static final SqlOperator CONVERT_TOTIMESTAMP_EPOCH_BE =
      ConvertToOperators.CONVERT_TOTIMESTAMP_EPOCH_BE;
  public static final SqlOperator CONVERT_TOTIME_EPOCH = ConvertToOperators.CONVERT_TOTIME_EPOCH;
  public static final SqlOperator CONVERT_TOTIME_EPOCH_BE =
      ConvertToOperators.CONVERT_TOTIME_EPOCH_BE;
  public static final SqlOperator CONVERT_TOUTF8 = ConvertToOperators.CONVERT_TOUTF8;

  public static final SqlFunction FLATTEN = new SqlFlattenOperator(0);
  public static final SqlFunction DATE_PART = new SqlDatePartOperator();

  // Function for E()
  public static final SqlOperator E =
      SqlOperatorBuilder.name("E").returnType(SqlTypeName.DOUBLE).noOperands().build();

  public static final SqlFunction HASH =
      new SqlFunction(
          "HASH",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.INTEGER),
          null,
          OperandTypes.ANY,
          SqlFunctionCategory.USER_DEFINED_FUNCTION);

  /**
   * The MEDIAN operator. Takes the median / PERCENTILE_CONT(.5) of a dataset. The argument must be
   * a numeric expression. The return type is a double.
   */
  public static final SqlAggFunction MEDIAN =
      SqlBasicAggFunction.create("MEDIAN", SqlKind.OTHER, ReturnTypes.DOUBLE, OperandTypes.NUMERIC)
          .withFunctionType(SqlFunctionCategory.SYSTEM);

  public static final VarArgSqlOperator CONTAINS_OPERATOR =
      new VarArgSqlOperator("contains", SqlContains.RETURN_TYPE, true);

  public static final SqlFunction COL_LIKE =
      new SqlFunction(
          "COL_LIKE",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.BOOLEAN_NULLABLE,
          null,
          OperandTypes.STRING_STRING,
          SqlFunctionCategory.USER_DEFINED_FUNCTION);

  public static final SqlFunction REGEXP_LIKE =
      new SqlFunction(
          "REGEXP_LIKE",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.BOOLEAN_NULLABLE,
          null,
          OperandTypes.STRING_STRING,
          SqlFunctionCategory.USER_DEFINED_FUNCTION);

  public static final SqlFunction REGEXP_COL_LIKE =
      new SqlFunction(
          "REGEXP_COL_LIKE",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.BOOLEAN_NULLABLE,
          null,
          OperandTypes.STRING_STRING,
          SqlFunctionCategory.USER_DEFINED_FUNCTION);

  // -----------------------------
  // Dremio Hive Masking Functions
  // -----------------------------
  private static final SqlReturnTypeInference HIVE_RETURN_TYPE_INFERENCE =
      opBinding -> {
        RelDataType type = opBinding.getOperandType(0);
        if ((type.getFamily() == SqlTypeFamily.CHARACTER)
            || (type.getSqlTypeName() == SqlTypeName.DATE)
            || (SqlTypeName.INT_TYPES.contains(type.getSqlTypeName()))) {
          return type;
        }

        return opBinding.getTypeFactory().createTypeWithNullability(type, true);
      };

  public static final SqlOperator HIVE_MASK_INTERNAL =
      SqlOperatorBuilder.name("MASK_INTERNAL")
          .returnType(HIVE_RETURN_TYPE_INFERENCE)
          .operandTypes(OperandTypes.ONE_OR_MORE)
          .build();

  public static final SqlOperator HIVE_MASK =
      SqlOperatorBuilder.name("MASK")
          .returnType(HIVE_RETURN_TYPE_INFERENCE)
          .operandTypes(OperandTypes.ONE_OR_MORE)
          .build();

  public static final SqlOperator HIVE_MASK_FIRST_N =
      SqlOperatorBuilder.name("MASK_FIRST_N")
          .returnType(HIVE_RETURN_TYPE_INFERENCE)
          .operandTypes(OperandTypes.ONE_OR_MORE)
          .build();

  public static final SqlOperator HIVE_MASK_LAST_N =
      SqlOperatorBuilder.name("MASK_LAST_N")
          .returnType(HIVE_RETURN_TYPE_INFERENCE)
          .operandTypes(OperandTypes.ONE_OR_MORE)
          .build();
  public static final SqlOperator HIVE_MASK_SHOW_FIRST_N =
      SqlOperatorBuilder.name("MASK_SHOW_FIRST_N")
          .returnType(HIVE_RETURN_TYPE_INFERENCE)
          .operandTypes(OperandTypes.ONE_OR_MORE)
          .build();

  public static final SqlOperator HIVE_MASK_SHOW_LAST_N =
      SqlOperatorBuilder.name("MASK_SHOW_LAST_N")
          .returnType(HIVE_RETURN_TYPE_INFERENCE)
          .operandTypes(OperandTypes.ONE_OR_MORE)
          .build();

  public static final SqlOperator HIVE_MASK_HASH =
      SqlOperatorBuilder.name("MASK_HASH")
          .returnType(
              opBinding -> {
                RelDataType type = opBinding.getOperandType(0);
                if (type.getFamily() == SqlTypeFamily.CHARACTER) {
                  return opBinding.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
                }

                return opBinding.getTypeFactory().createTypeWithNullability(type, true);
              })
          .operandTypes(OperandTypes.ONE_OR_MORE)
          .build();

  // ------------------------
  // Dremio Gandiva Functions
  // ------------------------
  public static final SqlOperator HASHSHA256 =
      SqlOperatorBuilder.name("HASHSHA256").returnType(SqlTypeName.VARCHAR).anyOperands().build();

  public static final SqlOperator BITWISE_AND =
      SqlOperatorBuilder.name("BITWISE_AND")
          .returnType(ReturnTypes.ARG1)
          .operandTypes(OperandTypes.NUMERIC_INTEGER)
          .build();

  // ---------------------
  // Array Functions
  // ---------------------

  /**
   * The "ARRAY_AGG(value) WITHIN GROUP(...) OVER (...)" aggregate function with gathers values into
   * an array.
   */
  public static final SqlAggFunction ARRAY_AGG =
      SqlBasicAggFunction.create(
              "ARRAY_AGG", SqlKind.LISTAGG, DremioReturnTypes.TO_ARRAY, OperandTypes.ANY)
          .withFunctionType(SqlFunctionCategory.SYSTEM)
          .withGroupOrder(Optionality.OPTIONAL)
          .withAllowsNullTreatment(true);

  private static final SqlOperandTypeChecker NUMERIC_ARRAY_TYPE_CHECKER =
      new SqlOperandTypeChecker() {
        @Override
        public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
          RelDataType operandType = callBinding.getOperandType(0);
          if (operandType.getSqlTypeName() != SqlTypeName.ARRAY) {
            if (throwOnFailure) {
              throw UserException.validationError()
                  .message("Expected argument to be an ARRAY.")
                  .build();
            }

            return false;
          }

          if (!SqlTypeName.NUMERIC_TYPES.contains(
              operandType.getComponentType().getSqlTypeName())) {
            if (throwOnFailure) {
              throw UserException.validationError()
                  .message("Expected argument to be an ARRAY of NUMERIC types.")
                  .build();
            }

            return false;
          }

          return true;
        }

        @Override
        public SqlOperandCountRange getOperandCountRange() {
          return SqlOperandCountRanges.of(1);
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
      };

  public static final SqlOperator ARRAY_MIN =
      SqlOperatorBuilder.name("ARRAY_MIN")
          .returnType(DremioReturnTypes.ARG0_ARRAY_ELEMENT)
          .operandTypes(NUMERIC_ARRAY_TYPE_CHECKER)
          .build();

  public static final SqlOperator ARRAY_MAX =
      SqlOperatorBuilder.name("ARRAY_MAX")
          .returnType(DremioReturnTypes.ARG0_ARRAY_ELEMENT)
          .operandTypes(NUMERIC_ARRAY_TYPE_CHECKER)
          .build();

  public static final SqlOperator ARRAY_SUM =
      SqlOperatorBuilder.name("ARRAY_SUM")
          .returnType(DremioReturnTypes.ARG0_ARRAY_ELEMENT)
          .operandTypes(NUMERIC_ARRAY_TYPE_CHECKER)
          .build();

  public static final SqlOperator ARRAY_AVG =
      SqlOperatorBuilder.name("ARRAY_AVG")
          .returnType(DremioReturnTypes.DECIMAL_QUOTIENT_DEFAULT_PRECISION_SCALE)
          .operandTypes(NUMERIC_ARRAY_TYPE_CHECKER)
          .build();

  public static final SqlOperator ARRAY_CONTAINS =
      SqlOperatorBuilder.name("ARRAY_CONTAINS")
          .returnType(ReturnTypes.BOOLEAN_NULLABLE)
          .operandTypes(SqlOperands.ARRAY, SqlOperands.ANY)
          .build();

  public static final SqlOperator ARRAY_REMOVE =
      SqlOperatorBuilder.name("ARRAY_REMOVE")
          .returnType(ReturnTypes.ARG0)
          .operandTypes(SqlOperands.ARRAY, SqlOperands.ANY)
          .build();

  private static SqlOperandTypeChecker arraysOfCoercibleType(
      String functionName, boolean allowVariadic) {
    return new SqlOperandTypeChecker() {
      @Override
      public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
        for (RelDataType operandType : callBinding.collectOperandTypes()) {
          if (operandType.getSqlTypeName() != SqlTypeName.ARRAY) {
            if (throwOnFailure) {
              throw UserException.validationError()
                  .message("'" + functionName + "' expects all operands to be ARRAYs.")
                  .build();
            }

            return false;
          }
        }

        RelDataType leastRestrictiveType =
            callBinding.getTypeFactory().leastRestrictive(callBinding.collectOperandTypes());

        if (leastRestrictiveType == null) {
          if (throwOnFailure) {
            throw UserException.validationError()
                .message("'" + functionName + "' expects all ARRAYs to be the same coercible type.")
                .build();
          }

          return false;
        }

        return true;
      }

      @Override
      public SqlOperandCountRange getOperandCountRange() {
        return allowVariadic ? SqlOperandCountRanges.from(2) : SqlOperandCountRanges.of(2);
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
    };
  }

  private static final SqlReturnTypeInference leastRestrictiveArray =
      new SqlReturnTypeInference() {
        @Override
        public RelDataType inferReturnType(SqlOperatorBinding sqlOperatorBinding) {
          // Calcite has a bug where it doesn't properly take the least restrictive of ARRAY<STRING>
          // but it does do STRING properly
          // Basically the largest precision isn't taken.
          RelDataTypeFactory typeFactory = sqlOperatorBinding.getTypeFactory();
          List<RelDataType> componentTypes =
              sqlOperatorBinding.collectOperandTypes().stream()
                  .map(RelDataType::getComponentType)
                  .collect(Collectors.toList());
          RelDataType leastRestrictiveComponentType = typeFactory.leastRestrictive(componentTypes);

          RelDataType arrayType = typeFactory.createArrayType(leastRestrictiveComponentType, -1);
          boolean anyOperandsNullable =
              sqlOperatorBinding.collectOperandTypes().stream().anyMatch(RelDataType::isNullable);
          RelDataType arrayTypeWithNullability =
              typeFactory.createTypeWithNullability(arrayType, anyOperandsNullable);
          return arrayTypeWithNullability;
        }
      };

  public static final SqlOperator ARRAY_CONCAT =
      SqlOperatorBuilder.name("ARRAY_CONCAT")
          .returnType(leastRestrictiveArray)
          .operandTypes(arraysOfCoercibleType("ARRAY_CONCAT", true))
          .build();

  public static final SqlOperator ARRAY_CAT = SqlOperatorBuilder.alias("ARRAY_CAT", ARRAY_CONCAT);

  public static final SqlOperator ARRAY_DISTINCT =
      SqlOperatorBuilder.name("ARRAY_DISTINCT")
          .returnType(ReturnTypes.ARG0)
          .operandTypes(SqlOperands.ARRAY)
          .build();

  public static final SqlOperator ARRAY_SORT =
      SqlOperatorBuilder.name("ARRAY_SORT")
          .returnType(ReturnTypes.ARG0)
          .operandTypes(SqlOperands.ARRAY)
          .build();

  private static SqlOperandTypeChecker arrayPrependAppendOperandChecker(boolean append) {
    return new SqlOperandTypeChecker() {
      @Override
      public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
        int elementIndex = 0;
        int arrayIndex = 1;
        if (append) {
          int temp = elementIndex;
          elementIndex = arrayIndex;
          arrayIndex = temp;
        }

        RelDataType elementType = callBinding.getOperandType(elementIndex);
        RelDataType arrayType = callBinding.getOperandType(arrayIndex);

        if (arrayType.getSqlTypeName() != SqlTypeName.ARRAY) {
          if (throwOnFailure) {
            throw UserException.validationError()
                .message("'ARRAY_APPEND/PREPEND' expects an ARRAY, but instead got: " + arrayType)
                .buildSilently();
          }

          return false;
        }

        RelDataType leastRestrictiveElementType =
            callBinding
                .getTypeFactory()
                .leastRestrictive(ImmutableList.of(arrayType.getComponentType(), elementType));
        if (leastRestrictiveElementType == null) {
          if (throwOnFailure) {
            throw UserException.validationError()
                .message(
                    "'ARRAY_APPEND/PREPEND' expects 'T' to equal 'E' for ARRAY_APPEND(T item, ARRAY<E> arr).")
                .buildSilently();
          }

          return false;
        }

        return true;
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
    };
  }

  private static SqlReturnTypeInference arrayPrependAppendReturnTypeInference(boolean append) {
    return new SqlReturnTypeInference() {
      @Override
      public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
        // Resulting concat needs to agree with the element and array type:
        RelDataType elementType = opBinding.getOperandType(0);
        RelDataType arrayType = opBinding.getOperandType(1);
        if (append) {
          RelDataType temp = elementType;
          elementType = arrayType;
          arrayType = temp;
        }

        // Use larger precision and nullability
        RelDataType newElementType =
            opBinding
                .getTypeFactory()
                .leastRestrictive(ImmutableList.of(elementType, arrayType.getComponentType()));
        return opBinding
            .getTypeFactory()
            .createTypeWithNullability(
                opBinding.getTypeFactory().createArrayType(newElementType, -1),
                arrayType.isNullable());
      }
    };
  }

  public static final SqlOperator ARRAY_PREPEND =
      SqlOperatorBuilder.name("ARRAY_PREPEND")
          .returnType(arrayPrependAppendReturnTypeInference(false))
          .operandTypes(arrayPrependAppendOperandChecker(false))
          .build();

  public static final SqlOperator ARRAY_APPEND =
      SqlOperatorBuilder.name("ARRAY_APPEND")
          .returnType(arrayPrependAppendReturnTypeInference(true))
          .operandTypes(arrayPrependAppendOperandChecker(true))
          .build();

  public static final SqlOperator ARRAY_REMOVE_AT =
      SqlOperatorBuilder.name("ARRAY_REMOVE_AT")
          .returnType(ReturnTypes.ARG0)
          .operandTypes(SqlOperands.ARRAY, SqlOperands.INTEGER)
          .build();

  public static final SqlOperator ARRAY_SIZE =
      SqlOperatorBuilder.name("ARRAY_SIZE")
          .returnType(ReturnTypes.INTEGER_NULLABLE)
          .operandTypes(SqlOperands.ARRAY)
          .build();

  public static final SqlOperator ARRAY_LENGTH =
      SqlOperatorBuilder.alias("ARRAY_LENGTH", ARRAY_SIZE);

  public static final SqlOperator ARRAY_POSITION =
      SqlOperatorBuilder.name("ARRAY_POSITION")
          .returnType(ReturnTypes.INTEGER_NULLABLE)
          .operandTypes(SqlOperands.ANY, SqlOperands.ARRAY)
          .build();

  private static SqlReturnTypeInference integerArrayReturnTypeInference() {
    return new SqlReturnTypeInference() {
      @Override
      public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
        RelDataTypeFactory typeFactory = opBinding.getTypeFactory();

        RelDataType withoutNullability =
            opBinding
                .getTypeFactory()
                .createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), false);
        RelDataType asArray = opBinding.getTypeFactory().createArrayType(withoutNullability, -1);
        return asArray;
      }
    };
  }

  public static final SqlOperator ARRAY_GENERATE_RANGE =
      SqlOperatorBuilder.name("ARRAY_GENERATE_RANGE")
          .returnType(integerArrayReturnTypeInference())
          .operandTypes(SqlOperands.INTEGER, SqlOperands.INTEGER, SqlOperands.INTEGER_OPTIONAL)
          .build();

  public static final SqlOperator SUBLIST =
      SqlOperatorBuilder.name("SUBLIST")
          .returnType(ReturnTypes.ARG0)
          .operandTypes(SqlOperands.ARRAY, SqlOperands.INTEGER, SqlOperands.INTEGER)
          .build();

  private static SqlReturnTypeInference arrayRemoveNullsTypeInference() {
    return new SqlReturnTypeInference() {
      @Override
      public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
        RelDataType arrayType = opBinding.getOperandType(0);

        RelDataType baseType =
            opBinding.getTypeFactory().createSqlType(arrayType.getComponentType().getSqlTypeName());
        RelDataType withoutNullability =
            opBinding.getTypeFactory().createTypeWithNullability(baseType, false);
        RelDataType asArray = opBinding.getTypeFactory().createArrayType(withoutNullability, -1);

        RelDataType asArrayWithNullability =
            opBinding.getTypeFactory().createTypeWithNullability(asArray, arrayType.isNullable());

        return asArrayWithNullability;
      }
    };
  }

  public static final SqlOperator ARRAY_COMPACT =
      SqlOperatorBuilder.name("ARRAY_COMPACT")
          .returnType(arrayRemoveNullsTypeInference())
          .operandTypes(SqlOperands.ARRAY)
          .build();

  public static final SqlOperator ARRAY_SLICE =
      SqlOperatorBuilder.name("ARRAY_SLICE")
          .returnType(ReturnTypes.ARG0)
          .operandTypes(SqlOperands.ARRAY, SqlOperands.INTEGER, SqlOperands.INTEGER_OPTIONAL)
          .build();

  public static final SqlOperator ARRAY_INSERT =
      SqlOperatorBuilder.name("ARRAY_INSERT")
          .returnType(ReturnTypes.ARG0)
          .operandTypes(
              new SqlOperandTypeChecker() {
                @Override
                public boolean checkOperandTypes(
                    SqlCallBinding callBinding, boolean throwOnFailure) {
                  RelDataType arrayType = callBinding.getOperandType(0);
                  RelDataType indexType = callBinding.getOperandType(1);
                  RelDataType elementType = callBinding.getOperandType(2);

                  if (arrayType.getSqlTypeName() != SqlTypeName.ARRAY) {
                    if (throwOnFailure) {
                      throw UserException.validationError()
                          .message("'ARRAY_INSERT' expects an ARRAY, but instead got: " + arrayType)
                          .build();
                    }
                    return false;
                  }
                  if (elementType.getSqlTypeName() != arrayType.getComponentType().getSqlTypeName()
                      && elementType.getSqlTypeName() != SqlTypeName.NULL) {
                    if (throwOnFailure) {
                      throw UserException.validationError()
                          .message(
                              "'ARRAY_INSERT' expects 'T' to equal 'E' for ARRAY_INSERT(ARRAY<E> arr, Integer index, T item).")
                          .build();
                    }

                    return false;
                  }
                  if (indexType.getSqlTypeName() != SqlTypeName.INTEGER) {
                    if (throwOnFailure) {
                      throw UserException.validationError()
                          .message(
                              "'ARRAY_INSERT' expects an INTEGER, but instead got: " + indexType)
                          .build();
                    }
                    return false;
                  }
                  return true;
                }

                @Override
                public SqlOperandCountRange getOperandCountRange() {
                  return SqlOperandCountRanges.of(3);
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

  public static final SqlOperator ARRAYS_OVERLAP =
      SqlOperatorBuilder.name("ARRAYS_OVERLAP")
          .returnType(ReturnTypes.BOOLEAN_NULLABLE)
          .operandTypes(arraysOfCoercibleType("ARRAYS_OVERLAP", false))
          .build();

  public static final SqlOperator SET_UNION =
      SqlOperatorBuilder.name("SET_UNION")
          .returnType(leastRestrictiveArray)
          .operandTypes(arraysOfCoercibleType("SET_UNION", false))
          .build();

  public static final SqlOperator ARRAY_INTERSECTION =
      SqlOperatorBuilder.name("ARRAY_INTERSECTION")
          .returnType(leastRestrictiveArray)
          .operandTypes(arraysOfCoercibleType("ARRAY_INTERSECTION", false))
          .build();

  public static final SqlOperator ARRAY_TO_STRING =
      SqlOperatorBuilder.name("ARRAY_TO_STRING")
          .returnType(SqlTypeName.VARCHAR)
          .operandTypes(SqlOperands.ARRAY, SqlOperands.VARCHAR)
          .build();

  public static final SqlOperator LIST_TO_DELIMITED_STRING =
      SqlOperatorBuilder.name("LIST_TO_DELIMITED_STRING")
          .returnType(SqlTypeName.VARCHAR)
          .operandTypes(SqlOperands.ANY, SqlOperands.VARCHAR)
          .build();

  public static final SqlOperator ARRAY_FREQUENCY =
      SqlOperatorBuilder.name("ARRAY_FREQUENCY")
          .returnType(
              opBinding -> {
                RelDataType arrayType = opBinding.getOperandType(0);
                RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
                RelDataType mapType =
                    typeFactory.createMapType(
                        arrayType.getComponentType(),
                        JavaTypeFactoryImpl.INSTANCE.createSqlType(SqlTypeName.INTEGER));
                return typeFactory.createTypeWithNullability(mapType, arrayType.isNullable());
              })
          .operandTypes(
              new SqlOperandTypeChecker() {
                @Override
                public boolean checkOperandTypes(
                    SqlCallBinding callBinding, boolean throwOnFailure) {
                  RelDataType operandType = callBinding.getOperandType(0);
                  if (operandType.getSqlTypeName() != SqlTypeName.ARRAY) {
                    if (throwOnFailure) {
                      throw UserException.validationError()
                          .message("'ARRAY_FREQUENCY' expects an ARRAY argument.")
                          .buildSilently();
                    }

                    return false;
                  }
                  RelDataType componentType = operandType.getComponentType();

                  if (!SqlTypeFamily.DATE.contains(componentType)
                      && !SqlTypeFamily.TIME.contains(componentType)
                      && !SqlTypeFamily.TIMESTAMP.contains(componentType)
                      && !SqlTypeFamily.STRING.contains(componentType)
                      && !SqlTypeFamily.NUMERIC.contains(componentType)
                      && !SqlTypeFamily.BOOLEAN.contains(componentType)) {
                    if (throwOnFailure) {
                      throw UserException.validationError()
                          .message("'ARRAY_FREQUENCY' expects an ARRAY of primitive types.")
                          .buildSilently();
                    }

                    return false;
                  }

                  return true;
                }

                @Override
                public SqlOperandCountRange getOperandCountRange() {
                  return SqlOperandCountRanges.of(1);
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

  // ---------------------
  // MAP Functions
  // ---------------------

  public static final SqlOperator LAST_MATCHING_MAP_ENTRY_FOR_KEY =
      SqlOperatorBuilder.name(MapFunctions.LAST_MATCHING_ENTRY_FUNC)
          .returnType(
              new SqlReturnTypeInference() {
                @Override
                public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
                  RelDataType keyType = opBinding.getOperandType(0).getKeyType();
                  RelDataType valueType = opBinding.getOperandType(0).getValueType();

                  // This is just to comply with the item operator
                  // But we should fix last_matching_map_entry to differentiate between key not
                  // found and key found but value is null.
                  RelDataType withNullable =
                      opBinding.getTypeFactory().createTypeWithNullability(valueType, true);
                  return opBinding
                      .getTypeFactory()
                      .createStructType(
                          ImmutableList.of(keyType, withNullable),
                          ImmutableList.of("key", "value"));
                }
              })
          .operandTypes(
              new SqlOperandTypeChecker() {
                @Override
                public boolean checkOperandTypes(
                    SqlCallBinding callBinding, boolean throwOnFailure) {
                  SqlTypeName collectionType = callBinding.getOperandType(0).getSqlTypeName();
                  if (collectionType != SqlTypeName.MAP) {
                    if (!throwOnFailure) {
                      return false;
                    }

                    throw UserException.validationError()
                        .message(
                            "Expected first argument to 'last_matching_entry_func' to be a map, but instead got: "
                                + collectionType)
                        .buildSilently();
                  }

                  SqlTypeName indexType = callBinding.getOperandType(1).getSqlTypeName();
                  SqlTypeName keyType = callBinding.getOperandType(0).getKeyType().getSqlTypeName();
                  if (indexType != keyType) {
                    if (!throwOnFailure) {
                      return false;
                    }

                    throw UserException.validationError()
                        .message(
                            "Expected second argument to 'last_matching_entry_func' to match the key type of the map. "
                                + "Map key type is: "
                                + keyType
                                + "and "
                                + "index type is: "
                                + indexType)
                        .buildSilently();
                  }

                  return true;
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

  // We defer the return type to execution (since we don't know what the return type is without
  // inspecting the parameters).
  public static final SqlOperator KVGEN =
      SqlOperatorBuilder.name("KVGEN")
          .returnType(SqlTypeName.ANY)
          .operandTypes(SqlOperands.ANY)
          .build();
  public static final SqlOperator MAPPIFY = SqlOperatorBuilder.alias("MAPPIFY", KVGEN);

  public static final SqlOperator MAP_KEYS =
      SqlOperatorBuilder.name("MAP_KEYS")
          .returnType(
              JavaTypeFactoryImpl.INSTANCE.createArrayType(
                  JavaTypeFactoryImpl.INSTANCE.createSqlType(SqlTypeName.VARCHAR), -1))
          .operandTypes(SqlOperands.MAP)
          .build();

  public static final SqlOperator MAP_VALUES =
      SqlOperatorBuilder.name("MAP_VALUES")
          .returnType(
              JavaTypeFactoryImpl.INSTANCE.createArrayType(
                  JavaTypeFactoryImpl.INSTANCE.createSqlType(SqlTypeName.ANY), -1))
          .operandTypes(SqlOperands.MAP)
          .build();

  // ---------------------
  // TDigest Functions
  // ---------------------

  public static final SqlAggFunction APPROX_PERCENTILE =
      new TDigest.SqlApproximatePercentileFunction();
  public static final SqlFunction TDIGEST_QUANTILE = new TDigest.SqlTDigestQuantileFunction();

  // ---------------------
  // OVERRIDE Behavior
  // ---------------------

  public static final SqlOperator DATE_TRUNC =
      SqlOperatorBuilder.name("DATE_TRUNC")
          .returnType(
              new SqlReturnTypeInference() {
                @Override
                public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
                  RelDataType operandRelDataType = opBinding.getOperandType(1);
                  SqlTypeName operandType = operandRelDataType.getSqlTypeName();
                  if (!SqlTypeName.CHAR_TYPES.contains(operandType)) {
                    return operandRelDataType;
                  }

                  return opBinding.getTypeFactory().createSqlType(SqlTypeName.DATE);
                }
              })
          .operandTypes(
              SqlOperands.CHAR_TYPES,
              SqlOperand.union(SqlOperands.DATETIME_TYPES, SqlOperands.INTERVAL_TYPES))
          .build();

  public static final SqlOperator NEXT_DAY =
      SqlOperatorBuilder.name("NEXT_DAY")
          .returnType(ReturnTypes.DATE)
          .operandTypes(
              SqlOperand.union(SqlOperands.DATE, SqlOperands.TIMESTAMP), SqlOperands.CHAR_TYPES)
          .build();

  // ---------------------
  // Non Deterministic Functions
  // ---------------------

  public static final SqlOperator RAND =
      SqlOperatorBuilder.name("RAND")
          .returnType(SqlStdOperatorTable.RAND.getReturnTypeInference())
          .operandTypes(SqlStdOperatorTable.RAND.getOperandTypeChecker())
          .withDeterminism(false)
          .build();

  // ---------------------
  // Dynamic Function
  // ---------------------

  public static final SqlOperator NOW =
      SqlOperatorBuilder.alias("NOW", SqlStdOperatorTable.CURRENT_TIMESTAMP);
  public static final SqlOperator STATEMENT_TIMESTAMP =
      SqlOperatorBuilder.alias("STATEMENT_TIMESTAMP", SqlStdOperatorTable.CURRENT_TIMESTAMP);
  public static final SqlOperator TRANSACTION_TIMESTAMP =
      SqlOperatorBuilder.alias("TRANSACTION_TIMESTAMP", SqlStdOperatorTable.CURRENT_TIMESTAMP);
  public static final SqlOperator CURRENT_TIMESTAMP_UTC =
      SqlOperatorBuilder.alias("CURRENT_TIMESTAMP_UTC", SqlStdOperatorTable.CURRENT_TIMESTAMP);
  public static final SqlOperator TIMEOFDAY =
      SqlOperatorBuilder.name("TIMEOFDAY")
          .returnType(SqlTypeName.VARCHAR)
          .noOperands()
          .withDynanism(true)
          .build();

  public static final SqlOperator IS_MEMBER =
      SqlOperatorBuilder.name("IS_MEMBER")
          .returnType(SqlTypeName.BOOLEAN)
          .operandTypes(SqlOperands.STRING_TYPES, SqlOperands.BOOLEAN_OPTIONAL)
          .withDynanism(true)
          .build();

  public static final SqlOperator USER =
      SqlOperatorBuilder.name("USER")
          .returnType(SqlTypeName.VARCHAR)
          .noOperands()
          .withDynanism(true)
          .build();

  public static final SqlOperator SESSION_USER = SqlOperatorBuilder.alias("SESSION_USER", USER);

  public static final SqlOperator SYSTEM_USER = SqlOperatorBuilder.alias("SYSTEM_USER", USER);

  public static final SqlOperator QUERY_USER = SqlOperatorBuilder.alias("QUERY_USER", USER);

  public static final SqlOperator LAST_QUERY_ID =
      new SqlBaseContextVariable(
          "LAST_QUERY_ID",
          ReturnTypes.cascade(ReturnTypes.VARCHAR_2000, SqlTypeTransforms.FORCE_NULLABLE),
          SqlFunctionCategory.SYSTEM) {};

  public static final SqlOperator CURRENT_DATE_UTC =
      SqlOperatorBuilder.alias("CURRENT_DATE_UTC", SqlStdOperatorTable.CURRENT_DATE);

  public static final SqlOperator CURRENT_TIME_UTC =
      SqlOperatorBuilder.alias("CURRENT_TIME_UTC", SqlStdOperatorTable.CURRENT_TIME);

  public static final SqlOperator UNIX_TIMESTAMP =
      SqlOperatorBuilder.name("UNIX_TIMESTAMP")
          .returnType(SqlTypeName.BIGINT)
          .noOperands()
          .withDynanism(true)
          .build();

  private DremioSqlOperatorTable() {}

  /** Returns the standard operator table, creating it if necessary. */
  public static synchronized DremioSqlOperatorTable instance() {
    if (instance == null) {
      // Creates and initializes the standard operator table.
      // Uses two-phase construction, because we can't initialize the
      // table until the constructor of the sub-class has completed.
      instance = new DremioSqlOperatorTable();
      instance.init();
    }
    return instance;
  }
}
