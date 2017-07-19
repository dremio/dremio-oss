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

  public static UserException.Builder validationError(String query, ValidationException ex) {
    Throwable cause = ex;
    if (ex.getCause() != null) {
      // ValidationException generally wraps the "real" cause that we are interested in
      cause = ex.getCause();
    }
    UserException.Builder b = UserException.validationError(cause)
      .addContext("Sql Query", query);

    // CalciteContextException alters the error message including the start/end positions
    // we need to extract the original error message and add the remaining information as context
    if (cause instanceof CalciteContextException && cause.getCause() != null) {
      CalciteContextException cce = (CalciteContextException) cause;
      b.message(cce.getCause().getMessage())
              .addContext("startLine", cce.getPosLine())
              .addContext("startColumn", cce.getPosColumn())
              .addContext("endLine", cce.getEndPosLine())
              .addContext("endColumn", cce.getEndPosColumn());
    }
    return b;
  }

  public static UserException.Builder parseError(String query, SqlParseException ex) {
    final SqlParserPos pos = ex.getPos();
    if (pos == null) {
      return UserException
          .parseError(ex)
          .addContext("SQL Query", query);
    } else {
      return UserException
          .parseError(ex)
          .addContext("SQL Query", query)
          .addContext("startLine", pos.getLineNum())
          .addContext("startColumn", pos.getColumnNum())
          .addContext("endLine", pos.getEndLineNum())
          .addContext("endColumn", pos.getEndColumnNum());
    }
  }


  public static Exception coerceException(Logger logger, String sql, Exception e, boolean coerceToPlan){
    if(e instanceof UserException){
      return e;
    } else if(e instanceof ValidationException){
      throw validationError(sql, (ValidationException) e).build(logger);
    } else if (e instanceof AccessControlException){
    throw UserException.permissionError(e)
        .addContext("Sql Query", sql)
        .build(logger);
    } else if (e instanceof SqlUnsupportedException){
    throw UserException.unsupportedError(e)
        .addContext("Sql Query", sql)
        .build(logger);
    } else if (e instanceof IOException || e instanceof RelConversionException){
      return new QueryInputException("Failure handling SQL.", e);
    } else if (coerceToPlan){
      throw UserException.planError(e)
      .addContext("Sql Query", sql)
      .build(logger);
    }
    return e;
  }
}
