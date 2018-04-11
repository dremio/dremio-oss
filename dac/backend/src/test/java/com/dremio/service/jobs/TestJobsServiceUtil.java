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
package com.dremio.service.jobs;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.Test;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.planner.sql.SqlExceptionHelper;
import com.dremio.service.job.proto.JobFailureInfo;

/**
 * Unit Tests for {@code JobsServiceUtil}
 */
public class TestJobsServiceUtil {
  @Test
  public void convertExceptionToFailureInfo() {
    // Fake logger to not pollute logs
    org.slf4j.Logger logger = mock(org.slf4j.Logger.class);

    SqlParseException parseException = new SqlParseException("test message", new SqlParserPos(7, 42, 13, 57), null, null, null);
    UserException userException = SqlExceptionHelper.parseError("SELECT FOO", parseException)
        .build(logger);
    String verboseError = userException.getVerboseMessage(false);

    JobFailureInfo jobFailureInfo = JobsServiceUtil.toFailureInfo(verboseError);
    assertEquals(JobFailureInfo.Type.PARSE, jobFailureInfo.getType());

    assertEquals(1, jobFailureInfo.getErrorsList().size());

    JobFailureInfo.Error error = jobFailureInfo.getErrorsList().get(0);
    assertEquals("test message", error.getMessage());
    assertEquals(7, (int) error.getStartLine());
    assertEquals(42, (int) error.getStartColumn());
    assertEquals(13, (int) error.getEndLine());
    assertEquals(57, (int) error.getEndColumn());
  }
}
