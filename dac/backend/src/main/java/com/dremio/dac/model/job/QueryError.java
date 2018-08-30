/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.dac.model.job;

import static com.dremio.exec.planner.sql.SqlExceptionHelper.END_COLUMN_CONTEXT;
import static com.dremio.exec.planner.sql.SqlExceptionHelper.END_LINE_CONTEXT;
import static com.dremio.exec.planner.sql.SqlExceptionHelper.START_COLUMN_CONTEXT;
import static com.dremio.exec.planner.sql.SqlExceptionHelper.START_LINE_CONTEXT;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.dremio.common.exceptions.UserException;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

/**
 * A single query error
 */
public final class QueryError {
  /**
   * A text range
   */
  public static final class Range {
    private final int startLine;
    private final int startColumn;
    private final int endLine;
    private final int endColumn;

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (!(o instanceof QueryError.Range)) {
        return false;
      }

      Range range = (Range) o;
      return startLine == range.startLine &&
        startColumn == range.startColumn &&
        endLine == range.endLine &&
        endColumn == range.endColumn;
    }

    @Override
    public int hashCode() {
      return Objects.hash(startLine, startColumn, endLine, endColumn);
    }

    @JsonCreator
    public Range(
        @JsonProperty("startLine") int startLine,
        @JsonProperty("startColumn") int startColumn,
        @JsonProperty("endLine") int endLine,
        @JsonProperty("endColumn") int endColumn) {
      this.startLine = startLine;
      this.startColumn = startColumn;
      this.endLine = endLine;
      this.endColumn = endColumn;
    }

    public int getStartLine() {
      return startLine;
    }

    public int getStartColumn() {
      return startColumn;
    }

    public int getEndLine() {
      return endLine;
    }

    public int getEndColumn() {
      return endColumn;
    }

    @Override
    public String toString() {
      return "Range [startLine=" + startLine + ", startColumn=" + startColumn + ", endLine=" + endLine + ", endColumn="
          + endColumn + "]";
    }
  }

  private final String message;
  private final QueryError.Range range;

  @JsonCreator
  public QueryError(@JsonProperty("message") String message, @JsonProperty("range") QueryError.Range range) {
    this.message = message;
    this.range = range;
  }

  public String getMessage() {
    return message;
  }

  public QueryError.Range getRange() {
    return range;
  }

  @Override
  public String toString() {
    return "QueryError [message=" + message + ", range=" + range + "]";
  }

  private static QueryError.Range rangeOf(UserException uex) {
    Map<String, String> context = new HashMap<>();
    for(String contextString: uex.getContextStrings()) {
      String[] split = contextString.split(" ", 2);
      if (split.length == 2) {
        context.put(split[0], split[1]);
      } else {
        context.put(contextString, null);
      }
    }

    try {
      final int startLine = Integer.parseInt(context.get(START_LINE_CONTEXT));
      final int startColumn = Integer.parseInt(context.get(START_COLUMN_CONTEXT));
      final int endLine = Integer.parseInt(context.get(END_LINE_CONTEXT));
      final int endColumn = Integer.parseInt(context.get(END_COLUMN_CONTEXT));

      // Providing the UI with the following convention:
      // Ranges are 1-based and inclusive.
      return new Range(
          startLine,
          startColumn,
          endLine ,
          endColumn);

    } catch(NullPointerException | NumberFormatException e) {
      return null;
    }
  }

  public static List<QueryError> of(UserException e) {
    switch(e.getErrorType()) {
    case PARSE:
    case PLAN:
    case VALIDATION:
      return ImmutableList.of(new QueryError(e.getMessage(), rangeOf(e)));

    default:
      return ImmutableList.of();
    }
  }

}
