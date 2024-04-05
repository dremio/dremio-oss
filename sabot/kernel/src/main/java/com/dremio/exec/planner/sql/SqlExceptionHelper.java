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
import com.dremio.exec.work.foreman.SqlUnsupportedException;
import java.io.IOException;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.hadoop.security.AccessControlException;
import org.slf4j.Logger;

/** Helper class that generates the appropriate user exception from specific parsing exception */
public final class SqlExceptionHelper {
  public static final String PLANNING_ERROR = "Error during planning the query.";
  public static final String PLANNING_STACK_OVERFLOW_ERROR =
      "The query planner ran out of internal resources and "
          + "could not produce a query plan. This is a rare event and only expected for extremely complex queries or queries "
          + "that reference a very large number of tables. Please simplify the query. If you believe you've received this "
          + "message in error, contact Dremio Support for more information.";

  public static final String SQL_QUERY_CONTEXT = "SQL Query";
  public static final String START_LINE_CONTEXT = "startLine";
  public static final String END_LINE_CONTEXT = "endLine";
  public static final String START_COLUMN_CONTEXT = "startColumn";
  public static final String END_COLUMN_CONTEXT = "endColumn";

  private SqlExceptionHelper() {}

  public static UserException.Builder validationError(String query, ValidationException ex) {
    Throwable cause = ex;
    if (ex.getCause() != null) {
      // ValidationException generally wraps the "real" cause that we are interested in
      cause = ex.getCause();
    }

    if (cause instanceof StackOverflowError) {
      throw SqlExceptionHelper.planError(query, cause)
          .message(PLANNING_STACK_OVERFLOW_ERROR)
          .buildSilently();
    }

    String message = null;
    SqlParserPos pos = null;

    // CalciteContextException alters the error message including the start/end positions
    // we need to extract the original error message and add the remaining information as context
    if (cause instanceof CalciteContextException) {
      CalciteContextException cce = (CalciteContextException) cause;
      message = cce.getCause().getMessage();
      pos =
          new SqlParserPos(
              cce.getPosLine(), cce.getPosColumn(), cce.getEndPosLine(), cce.getEndPosColumn());
    }

    return sqlError(UserException.validationError(cause), query, message, pos);
  }

  public static UserException.Builder planError(String query, Throwable ex) {
    String message = null;
    SqlParserPos pos = null;

    // CalciteContextException alters the error message including the start/end positions
    // we need to extract the original error message and add the remaining information as context
    if (ex instanceof CalciteContextException) {
      CalciteContextException cce = (CalciteContextException) ex;
      message = cce.getMessage();
      pos =
          new SqlParserPos(
              cce.getPosLine(), cce.getPosColumn(), cce.getEndPosLine(), cce.getEndPosColumn());
    }

    return sqlError(UserException.planError(ex), query, message, pos);
  }

  public static UserException.Builder parseError(String message, String query, SqlParserPos pos) {
    return sqlError(UserException.parseError(), query, message, pos);
  }

  public static UserException.Builder parseError(String query, SqlParseException ex) {
    return sqlError(UserException.parseError(ex), query, ex.getMessage(), ex.getPos());
  }

  public static Exception coerceException(
      Logger logger, String sql, Exception e, boolean coerceToPlan) {
    if (e instanceof UserException) {
      return e;
    } else if (e instanceof ValidationException) {
      throw validationError(sql, (ValidationException) e).build(logger);
    } else if (e instanceof AccessControlException) {
      throw UserException.permissionError(e).addContext(SQL_QUERY_CONTEXT, sql).build(logger);
    } else if (e instanceof SqlUnsupportedException) {
      throw UserException.unsupportedError(e).addContext(SQL_QUERY_CONTEXT, sql).build(logger);
    } else if (e instanceof IOException || e instanceof RelConversionException) {
      return new QueryInputException("Failure handling SQL.", e);
    } else if (coerceToPlan) {
      throw planError(sql, e).build(logger);
    }
    return e;
  }

  private static UserException.Builder sqlError(
      UserException.Builder builder, String query, String message, SqlParserPos pos) {
    if (message != null) {
      if (message.contains("Object") && message.contains("not found")) {
        builder =
            builder.message(message + ". Please check that it exists in the selected context.");
      } else {
        builder = builder.message(message);
      }
    }

    if (pos != null) {
      // Calcite convention is to return column and line numbers as 1-based inclusive positions.
      builder
          .addContext(START_LINE_CONTEXT, pos.getLineNum())
          .addContext(START_COLUMN_CONTEXT, pos.getColumnNum())
          .addContext(END_LINE_CONTEXT, pos.getEndLineNum())
          .addContext(END_COLUMN_CONTEXT, pos.getEndColumnNum());
    }

    if (query != null) {
      builder.addContext(SQL_QUERY_CONTEXT, query);
    }

    return builder;
  }

  public static Exception coerceError(String sql, Error ex) {
    if (ex instanceof StackOverflowError) {
      throw SqlExceptionHelper.planError(sql, ex)
          .message(PLANNING_STACK_OVERFLOW_ERROR)
          .buildSilently();
    }
    throw SqlExceptionHelper.planError(sql, ex).message(PLANNING_ERROR).buildSilently();
  }
}
