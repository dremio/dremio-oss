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

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;

import com.dremio.exec.expr.fn.hll.HyperLogLog;
import com.dremio.exec.expr.fn.impl.DremioArgChecker;

/**
 * Dremio's Sql Operator Table
 *
 * These operators are used to resolve {@link org.apache.calcite.sql.SqlJdbcFunctionCall} before
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

  // ---------------
  // GEO functions
  // ---------------
  public static final SqlFunction GEO_DISTANCE = new SqlFunction(
      "GEO_DISTANCE",
      SqlKind.OTHER_FUNCTION,
      ReturnTypes.DOUBLE,
      null,
      new DremioArgChecker(
          true,
          DremioArgChecker.ofFloat("lat1_deg"),
          DremioArgChecker.ofFloat("lon1_deg"),
          DremioArgChecker.ofFloat("lat2_deg"),
          DremioArgChecker.ofFloat("lon2_deg")
      ),
      SqlFunctionCategory.USER_DEFINED_FUNCTION);

  public static final SqlFunction GEO_NEARBY = new SqlFunction(
      "GEO_NEARBY",
      SqlKind.OTHER_FUNCTION,
      ReturnTypes.BOOLEAN,
      null,
      new DremioArgChecker(
          false,
          DremioArgChecker.ofFloat("lat1_deg"),
          DremioArgChecker.ofFloat("lon1_deg"),
          DremioArgChecker.ofFloat("lat2_deg"),
          DremioArgChecker.ofFloat("lon2_deg"),
          DremioArgChecker.ofDouble("distance_meters")
      ),
      SqlFunctionCategory.USER_DEFINED_FUNCTION);
  public static final SqlFunction GEO_BEYOND = new SqlFunction(
      "GEO_BEYOND",
      SqlKind.OTHER_FUNCTION,
      ReturnTypes.BOOLEAN,
      null,
      new DremioArgChecker(
          false,
          DremioArgChecker.ofFloat("lat1_deg"),
          DremioArgChecker.ofFloat("lon1_deg"),
          DremioArgChecker.ofFloat("lat2_deg"),
          DremioArgChecker.ofFloat("lon2_deg"),
          DremioArgChecker.ofDouble("distance_meters")
      ),
      SqlFunctionCategory.USER_DEFINED_FUNCTION);

  // ---------------------
  // STD Library Functions
  // ---------------------

  public static final SqlFunction ROUND =
      new SqlFunction(
          "ROUND",
          SqlKind.OTHER_FUNCTION,
          DremioReturnTypes.NULLABLE_ROUND,
          null,
          OperandTypes.NUMERIC_OPTIONAL_INTEGER,
          SqlFunctionCategory.NUMERIC);

  public static final SqlFunction TRUNCATE =
      new SqlFunction(
          "TRUNCATE",
          SqlKind.OTHER_FUNCTION,
          DremioReturnTypes.NULLABLE_TRUNCATE,
          null,
          OperandTypes.NUMERIC_OPTIONAL_INTEGER,
          SqlFunctionCategory.NUMERIC);


  // -----------------------
  // Dremio Custom Functions
  // -----------------------

  public static final SqlFunction FLATTEN = new SqlFlattenOperator(0);
  public static final SqlFunction DATE_PART = new SqlDatePartOperator();

  // Function for E()
  public static final SqlFunction E_FUNCTION =
      new SqlFunction(new SqlIdentifier("E", SqlParserPos.ZERO), ReturnTypes.DOUBLE,
          null, OperandTypes.NILADIC, null, SqlFunctionCategory.NUMERIC);

  //NOW function
  public static final SqlFunction NOW = new DremioSqlAbstractTimeFunction("NOW", SqlTypeName.TIMESTAMP);


  private DremioSqlOperatorTable() {
  }
  /**
   * Returns the standard operator table, creating it if necessary.
   */
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
