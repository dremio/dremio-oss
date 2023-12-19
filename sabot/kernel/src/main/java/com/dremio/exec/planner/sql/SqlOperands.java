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

import static com.dremio.exec.planner.sql.SqlOperand.optional;
import static com.dremio.exec.planner.sql.SqlOperand.regular;
import static com.dremio.exec.planner.sql.SqlOperand.variadic;

import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Static class to store singletons for commonly used SqlOperand instances.
 */
public final class SqlOperands {
  // Single Types
  public static final SqlOperand ANY = regular(SqlTypeName.ANY);
  public static final SqlOperand ANY_OPTIONAL = optional(ANY);
  public static final SqlOperand ANY_VARIADIC = variadic(ANY);

  public static final SqlOperand BOOLEAN = regular(SqlTypeName.BOOLEAN);
  public static final SqlOperand BOOLEAN_OPTIONAL = optional(BOOLEAN);
  public static final SqlOperand BOOLEAN_VARIADIC = variadic(BOOLEAN);

  public static final SqlOperand TINYINT = regular(SqlTypeName.TINYINT);
  public static final SqlOperand TINYINT_OPTIONAL = optional(TINYINT);
  public static final SqlOperand TINYINT_VARIADIC = variadic(TINYINT);

  public static final SqlOperand SMALLINT = regular(SqlTypeName.SMALLINT);
  public static final SqlOperand SMALLINT_OPTIONAL = optional(SMALLINT);
  public static final SqlOperand SMALLINT_VARIADIC = variadic(SMALLINT);

  public static final SqlOperand INTEGER = regular(SqlTypeName.INTEGER);
  public static final SqlOperand INTEGER_OPTIONAL = optional(INTEGER);
  public static final SqlOperand INTEGER_VARIADIC = variadic(INTEGER);

  public static final SqlOperand BIGINT = regular(SqlTypeName.BIGINT);
  public static final SqlOperand BIGINT_OPTIONAL = optional(BIGINT);
  public static final SqlOperand BIGINT_VARIADIC = variadic(BIGINT);

  public static final SqlOperand DECIMAL = regular(SqlTypeName.DECIMAL);
  public static final SqlOperand DECIMAL_OPTIONAL = optional(DECIMAL);
  public static final SqlOperand DECIMAL_VARIADIC = variadic(DECIMAL);

  public static final SqlOperand FLOAT = regular(SqlTypeName.FLOAT);
  public static final SqlOperand FLOAT_OPTIONAL = optional(FLOAT);
  public static final SqlOperand FLOAT_VARIADIC = variadic(FLOAT);

  public static final SqlOperand REAL = regular(SqlTypeName.REAL);
  public static final SqlOperand REAL_OPTIONAL = optional(REAL);
  public static final SqlOperand REAL_VARIADIC = variadic(REAL);

  public static final SqlOperand DOUBLE = regular(SqlTypeName.DOUBLE);
  public static final SqlOperand DOUBLE_OPTIONAL = optional(DOUBLE);
  public static final SqlOperand DOUBLE_VARIADIC = variadic(DOUBLE);

  public static final SqlOperand CHAR = regular(SqlTypeName.CHAR);
  public static final SqlOperand CHAR_OPTIONAL = optional(CHAR);
  public static final SqlOperand CHAR_VARIADIC = variadic(CHAR);

  public static final SqlOperand VARCHAR = regular(SqlTypeName.VARCHAR);
  public static final SqlOperand VARCHAR_OPTIONAL = optional(VARCHAR);
  public static final SqlOperand VARCHAR_VARIADIC = variadic(VARCHAR);

  public static final SqlOperand BINARY = regular(SqlTypeName.BINARY);
  public static final SqlOperand BINARY_OPTIONAL = optional(BINARY);
  public static final SqlOperand BINARY_VARIADIC = variadic(BINARY);

  public static final SqlOperand VARBINARY = regular(SqlTypeName.VARBINARY);
  public static final SqlOperand VARBINARY_OPTIONAL = optional(VARBINARY);
  public static final SqlOperand VARBINARY_VARIADIC = variadic(VARBINARY);

  public static final SqlOperand DATE = regular(SqlTypeName.DATE);
  public static final SqlOperand DATE_OPTIONAL = optional(DATE);
  public static final SqlOperand DATE_VARIADIC = variadic(DATE);

  public static final SqlOperand TIME = regular(SqlTypeName.TIME);
  public static final SqlOperand TIME_OPTIONAL = optional(TIME);
  public static final SqlOperand TIME_VARIADIC = variadic(TIME);

  public static final SqlOperand TIMESTAMP = regular(SqlTypeName.TIMESTAMP);
  public static final SqlOperand TIMESTAMP_OPTIONAL = optional(TIMESTAMP);
  public static final SqlOperand TIMESTAMP_VARIADIC = variadic(TIMESTAMP);

  public static final SqlOperand INTERVAL_YEAR_MONTH = regular(SqlTypeName.INTERVAL_YEAR_MONTH);
  public static final SqlOperand INTERVAL_YEAR_MONTH_OPTIONAL = optional(INTERVAL_YEAR_MONTH);
  public static final SqlOperand INTERVAL_YEAR_MONTH_VARIADIC = variadic(INTERVAL_YEAR_MONTH);

  public static final SqlOperand INTERVAL_DAY_HOUR = regular(SqlTypeName.INTERVAL_DAY_HOUR);
  public static final SqlOperand INTERVAL_DAY_TIME_OPTIONAL = optional(INTERVAL_DAY_HOUR);
  public static final SqlOperand INTERVAL_DAY_TIME_VARIADIC = variadic(INTERVAL_DAY_HOUR);

  public static final SqlOperand ARRAY = regular(SqlTypeName.ARRAY);
  public static final SqlOperand ARRAY_OPTIONAL = optional(ARRAY);
  public static final SqlOperand ARRAY_VARIADIC = variadic(ARRAY);

  public static final SqlOperand MAP = regular(SqlTypeName.MAP);
  public static final SqlOperand MAP_OPTIONAL = optional(MAP);
  public static final SqlOperand MAP_VARIADIC = variadic(MAP);

  public static final SqlOperand MULTISET = regular(SqlTypeName.MULTISET);
  public static final SqlOperand MULTISET_OPTIONAL = optional(MULTISET);
  public static final SqlOperand MULTISET_VARIADIC = variadic(MULTISET);

  public static final SqlOperand ROW = regular(SqlTypeName.ROW);
  public static final SqlOperand ROW_OPTIONAL = optional(ROW);
  public static final SqlOperand ROW_VARIADIC = variadic(ROW);

  public static final SqlOperand DISTINCT = regular(SqlTypeName.DISTINCT);
  public static final SqlOperand DISTINCT_OPTIONAL = optional(DISTINCT);
  public static final SqlOperand DISTINCT_VARIADIC = variadic(DISTINCT);

  public static final SqlOperand STRUCTURED = regular(SqlTypeName.STRUCTURED);
  public static final SqlOperand STRUCTURED_OPTIONAL = optional(STRUCTURED);
  public static final SqlOperand STRUCTURED_VARIADIC = variadic(STRUCTURED);

  public static final SqlOperand NULL = regular(SqlTypeName.NULL);
  public static final SqlOperand NULL_OPTIONAL = optional(NULL);
  public static final SqlOperand NULL_VARIADIC = variadic(NULL);

  // Group Types
  public static final SqlOperand ALL_TYPES = regular(SqlTypeName.ALL_TYPES);
  public static final SqlOperand ALL_TYPES_OPTIONAL = optional(ALL_TYPES);
  public static final SqlOperand ALL_TYPES_VARIADIC = variadic(ALL_TYPES);

  public static final SqlOperand BOOLEAN_TYPES = regular(SqlTypeName.BOOLEAN_TYPES);
  public static final SqlOperand BOOLEAN_TYPES_OPTIONAL = optional(BOOLEAN_TYPES);
  public static final SqlOperand BOOLEAN_TYPES_VARIADIC = variadic(BOOLEAN_TYPES);

  public static final SqlOperand BINARY_TYPES = regular(SqlTypeName.BINARY_TYPES);
  public static final SqlOperand BINARY_TYPES_OPTIONAL = optional(BINARY_TYPES);
  public static final SqlOperand BINARY_TYPES_VARIADIC = variadic(BINARY_TYPES);

  public static final SqlOperand INT_TYPES = regular(SqlTypeName.INT_TYPES);
  public static final SqlOperand INT_TYPES_OPTIONAL = optional(INT_TYPES);
  public static final SqlOperand INT_TYPES_VARIADIC = variadic(INT_TYPES);

  public static final SqlOperand EXACT_TYPES = regular(SqlTypeName.EXACT_TYPES);
  public static final SqlOperand EXACT_TYPES_OPTIONAL = optional(EXACT_TYPES);
  public static final SqlOperand EXACT_TYPES_VARIADIC = variadic(EXACT_TYPES);

  public static final SqlOperand APPROX_TYPES = regular(SqlTypeName.APPROX_TYPES);
  public static final SqlOperand APPROX_TYPES_OPTIONAL = optional(APPROX_TYPES);
  public static final SqlOperand APPROX_TYPES_VARIADIC = variadic(APPROX_TYPES);

  public static final SqlOperand NUMERIC_TYPES = regular(SqlTypeName.NUMERIC_TYPES);
  public static final SqlOperand NUMERIC_TYPES_OPTIONAL = optional(NUMERIC_TYPES);
  public static final SqlOperand NUMERIC_TYPES_VARIADIC = variadic(NUMERIC_TYPES);

  public static final SqlOperand FRACTIONAL_TYPES = regular(SqlTypeName.FRACTIONAL_TYPES);
  public static final SqlOperand FRACTIONAL_TYPES_OPTIONAL = optional(FRACTIONAL_TYPES);
  public static final SqlOperand FRACTIONAL_TYPES_VARIADIC = variadic(FRACTIONAL_TYPES);

  public static final SqlOperand CHAR_TYPES = regular(SqlTypeName.CHAR_TYPES);
  public static final SqlOperand CHAR_TYPES_OPTIONAL = optional(CHAR_TYPES);
  public static final SqlOperand CHAR_TYPES_VARIADIC = variadic(CHAR_TYPES);

  public static final SqlOperand STRING_TYPES = regular(SqlTypeName.STRING_TYPES);
  public static final SqlOperand STRING_TYPES_OPTIONAL = optional(STRING_TYPES);
  public static final SqlOperand STRING_TYPES_VARIADIC = variadic(STRING_TYPES);

  public static final SqlOperand DATETIME_TYPES = regular(SqlTypeName.DATETIME_TYPES);
  public static final SqlOperand DATETIME_TYPES_OPTIONAL = optional(DATETIME_TYPES);
  public static final SqlOperand DATETIME_TYPES_VARIADIC = variadic(DATETIME_TYPES);

  public static final SqlOperand YEAR_INTERVAL_TYPES = regular(SqlTypeName.YEAR_INTERVAL_TYPES);
  public static final SqlOperand YEAR_INTERVAL_TYPES_OPTIONAL = optional(YEAR_INTERVAL_TYPES);
  public static final SqlOperand YEAR_INTERVAL_TYPES_VARIADIC = variadic(YEAR_INTERVAL_TYPES);

  public static final SqlOperand DAY_INTERVAL_TYPES = regular(SqlTypeName.DAY_INTERVAL_TYPES);
  public static final SqlOperand DAY_INTERVAL_TYPES_OPTIONAL = optional(DAY_INTERVAL_TYPES);
  public static final SqlOperand DAY_INTERVAL_TYPES_VARIADIC = variadic(DAY_INTERVAL_TYPES);

  public static final SqlOperand INTERVAL_TYPES = regular(SqlTypeName.INTERVAL_TYPES);
  public static final SqlOperand INTERVAL_TYPES_OPTIONAL = optional(INTERVAL_TYPES);
  public static final SqlOperand INTERVAL_TYPES_VARIADIC = variadic(INTERVAL_TYPES);

  private SqlOperands() {
    // Utility Class
  }
}
