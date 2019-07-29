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

import java.io.IOException;

import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.hadoop.security.AccessControlException;
import org.slf4j.Logger;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.work.foreman.SqlUnsupportedException;

/**
 * Helper class that generates the appropriate user exception
 * from specific parsing exception
 */
public class SqlExceptionHelper {
  public static final String QUERY_PARSING_ERROR = "Failure parsing the query.";
  public static final String INNER_QUERY_PARSING_ERROR = "Failure parsing a view your query is dependent upon.";

  public static final String SQL_QUERY_CONTEXT = "SQL Query";
  public static final String START_LINE_CONTEXT = "startLine";
  public static final String END_LINE_CONTEXT = "endLine";
  public static final String START_COLUMN_CONTEXT = "startColumn";
  public static final String END_COLUMN_CONTEXT = "endColumn";

  public static UserException.Builder validationError(String query, ValidationException ex) {
    Throwable cause = ex;
    if (ex.getCause() != null) {
      // ValidationException generally wraps the "real" cause that we are interested in
      cause = ex.getCause();
    }
    UserException.Builder b = UserException.validationError(cause)
      .addContext(SQL_QUERY_CONTEXT, query);

    // CalciteContextException alters the error message including the start/end positions
    // we need to extract the original error message and add the remaining information as context
    if (cause instanceof CalciteContextException && cause.getCause() != null) {
      CalciteContextException cce = (CalciteContextException) cause;
      b.message(cce.getCause().getMessage())
              .addContext(START_LINE_CONTEXT, cce.getPosLine())
              .addContext(START_COLUMN_CONTEXT, cce.getPosColumn())
              .addContext(END_LINE_CONTEXT, cce.getEndPosLine())
              .addContext(END_COLUMN_CONTEXT, cce.getEndPosColumn());
    }
    return b;
  }

  public static UserException.Builder planError(String query, Exception ex) {
    UserException.Builder b = UserException.planError(ex)
      .addContext(SQL_QUERY_CONTEXT, query);

    // CalciteContextException alters the error message including the start/end positions
    // we need to extract the original error message and add the remaining information as context
    if (ex instanceof CalciteContextException) {
      CalciteContextException cce = (CalciteContextException) ex;
      b.message(cce.getMessage())
              .addContext(START_LINE_CONTEXT, cce.getPosLine())
              .addContext(START_COLUMN_CONTEXT, cce.getPosColumn())
              .addContext(END_LINE_CONTEXT, cce.getEndPosLine())
              .addContext(END_COLUMN_CONTEXT, cce.getEndPosColumn());
    }
    return b;
  }

  private static UserException.Builder addParseContext(UserException.Builder builder, String query, SqlParserPos pos){
    // Calcite convention is to return column and line numbers as 1-based inclusive positions.
    return builder.addContext(SQL_QUERY_CONTEXT, query)
        .addContext(START_LINE_CONTEXT, pos.getLineNum())
        .addContext(START_COLUMN_CONTEXT, pos.getColumnNum())
        .addContext(END_LINE_CONTEXT, pos.getEndLineNum())
        .addContext(END_COLUMN_CONTEXT, pos.getEndColumnNum());
  }

  public static UserException.Builder parseError(String message, String query, SqlParserPos pos) {
    return addParseContext(UserException.parseError().message(message), query, pos);
  }

  public static UserException.Builder parseError(String query, SqlParseException ex) {
    final SqlParserPos pos = ex.getPos();
    if (pos == null) {
      return UserException
          .parseError(ex)
          .addContext(SQL_QUERY_CONTEXT, query);
    } else {
      // Calcite convention is to return column and line numbers as 1-based inclusive positions.
      return addParseContext(UserException.parseError(ex), query, pos);
    }
  }


  public static Exception coerceException(Logger logger, String sql, Exception e, boolean coerceToPlan){
    if(e instanceof UserException){
      return e;
    } else if(e instanceof ValidationException){
      throw validationError(sql, (ValidationException) e).build(logger);
    } else if (e instanceof AccessControlException){
    throw UserException.permissionError(e)
        .addContext(SQL_QUERY_CONTEXT, sql)
        .build(logger);
    } else if (e instanceof SqlUnsupportedException){
    throw UserException.unsupportedError(e)
        .addContext(SQL_QUERY_CONTEXT, sql)
        .build(logger);
    } else if (e instanceof IOException || e instanceof RelConversionException){
      return new QueryInputException("Failure handling SQL.", e);
    } else if (coerceToPlan){
      throw planError(sql, e).build(logger);
    }
    return e;
  }
}
