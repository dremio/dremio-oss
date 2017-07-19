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
package com.dremio.exec.planner.sql;

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlReturnTypeInference;

public class SqlAggOperator extends SqlAggFunction {
  // private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SqlAggOperator.class);

  public SqlAggOperator(String name, int argCountMin, int argCountMax, SqlReturnTypeInference sqlReturnTypeInference) {
    super(name,
        new SqlIdentifier(name, SqlParserPos.ZERO),
        SqlKind.OTHER_FUNCTION,
        sqlReturnTypeInference,
        null,
        Checker.getChecker(argCountMin, argCountMax),
        SqlFunctionCategory.USER_DEFINED_FUNCTION);
  }
}
