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
package com.dremio.dac.service.autocomplete.utils;

import com.dremio.exec.planner.sql.SqlExceptionHelper;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;

public abstract class SqlNodeParser {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(SqlNodeParser.class);

  /** Converts sql string into a Calcite Sql Node */
  public abstract SqlNode parseWithException(String sql) throws SqlParseException;

  public SqlNode parse(String sql) {
    try {
      return parseWithException(sql);
    } catch (SqlParseException parseException) {
      throw SqlExceptionHelper.parseError(sql, parseException).build(logger);
    }
  }

  public SqlNode parseWithException(ImmutableList<DremioToken> tokens) throws SqlParseException {
    String sql = SqlQueryUntokenizer.untokenize(tokens);
    return parseWithException(sql);
  }

  public SqlNode parse(ImmutableList<DremioToken> tokens) {
    try {
      return parseWithException(tokens);
    } catch (SqlParseException parseException) {
      throw SqlExceptionHelper.parseError(SqlQueryUntokenizer.untokenize(tokens), parseException)
          .build(logger);
    }
  }
}
