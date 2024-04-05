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
import com.google.common.annotations.VisibleForTesting;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DremioStringToSqlNode {
  public static final Logger LOGGER = LoggerFactory.getLogger(DremioStringToSqlNode.class);
  private final SqlParser.Config sqlParserConfig;

  public DremioStringToSqlNode(SqlParser.Config sqlParserConfig) {
    this.sqlParserConfig = sqlParserConfig;
  }

  public SqlNode parse(String sqlString) {
    return parseSingleStatementImpl(sqlString, sqlParserConfig, false);
  }

  private static SqlNodeList parseMultipleStatementsImpl(
      String sql, SqlParser.Config parserConfig, boolean isInnerQuery) {
    try {
      SqlParser parser = SqlParser.create(sql, parserConfig);
      return parser.parseStmtList();
    } catch (SqlParseException e) {
      UserException.Builder builder = SqlExceptionHelper.parseError(sql, e);

      if (e.getCause() instanceof StackOverflowError) {
        builder.message(SqlExceptionHelper.PLANNING_STACK_OVERFLOW_ERROR);
      } else if (isInnerQuery) {
        builder.message("Failure parsing a view the query is dependent upon.");
      }

      throw builder.build(LOGGER);
    }
  }

  @VisibleForTesting
  static SqlNode parseSingleStatementImpl(
      String sql, SqlParser.Config parserConfig, boolean isInnerQuery) {
    SqlNodeList list = parseMultipleStatementsImpl(sql, parserConfig, isInnerQuery);
    if (list.size() > 1) {
      UserException.Builder builder = UserException.parseError();
      builder.message(
          "Dremio only supports single statement execution. Unable to execute the given query with %s statements: %s",
          list.size(), sql);
      throw builder.buildSilently();
    }
    SqlNode newNode = list.get(0);
    return newNode;
  }
}
