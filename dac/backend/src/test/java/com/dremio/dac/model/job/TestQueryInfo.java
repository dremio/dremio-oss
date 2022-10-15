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
package com.dremio.dac.model.job;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.Test;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.exceptions.UserRemoteException;
import com.dremio.exec.planner.sql.SqlExceptionHelper;

/**
 * Unit Tests for {@code LocalJobsService}
 */
public class TestQueryInfo {

  @Test
  public void convertExceptionToQueryErrors() {
    SqlParseException parseException = new SqlParseException("test message", new SqlParserPos(7, 42, 13, 57), null, null, null);
    UserException userException = SqlExceptionHelper.parseError("SELECT FOO", parseException)
        .buildSilently();

    List<QueryError> errors = QueryError.of(userException);

    assertEquals(1, errors.size());

    QueryError error = errors.get(0);
    assertEquals("test message", error.getMessage());
    assertEquals(7, error.getRange().getStartLine());
    assertEquals(42, error.getRange().getStartColumn());
    assertEquals(13, error.getRange().getEndLine());
    assertEquals(57, error.getRange().getEndColumn());
  }

  @Test
  public void convertRemoteExceptionToQueryErrors() {
    SqlParseException parseException = new SqlParseException("test message", new SqlParserPos(7, 42, 13, 57), null, null, null);
    UserException userException = SqlExceptionHelper.parseError("SELECT FOO", parseException)
            .buildSilently();

    UserException remoteException = UserRemoteException.create(userException.getOrCreatePBError(false));

    List<QueryError> errors = QueryError.of(remoteException);

    assertEquals(1, errors.size());

    QueryError error = errors.get(0);
    assertEquals("test message", error.getMessage());
    assertEquals(7, error.getRange().getStartLine());
    assertEquals(42, error.getRange().getStartColumn());
    assertEquals(13, error.getRange().getEndLine());
    assertEquals(57, error.getRange().getEndColumn());
  }

  @Test
  public void convertExceptionToQueryErrorsWithPosition() {
    SqlParseException parseException = new SqlParseException("test message 2", new SqlParserPos(7, 42, 7, 42), null, null, null);
    UserException userException = SqlExceptionHelper.parseError("SELECT BAR", parseException)
        .buildSilently();

    List<QueryError> errors = QueryError.of(userException);

    assertEquals(1, errors.size());

    QueryError error = errors.get(0);
    assertEquals("test message 2", error.getMessage());
    assertEquals(7, error.getRange().getStartLine());
    assertEquals(42, error.getRange().getStartColumn());
    assertEquals(7, error.getRange().getEndLine());
    assertEquals(42, error.getRange().getEndColumn());
  }
}
