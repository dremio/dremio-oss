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
package com.dremio.service.jobs;

import static com.dremio.service.jobs.JobsProtoUtil.toBuf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Collections;

import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.Test;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.ErrorCollectorImpl;
import com.dremio.dac.server.JobsServiceTestUtils;
import com.dremio.exec.planner.sql.SqlExceptionHelper;
import com.dremio.service.job.DownloadSettings;
import com.dremio.service.job.SubmitJobRequest;
import com.dremio.service.job.proto.JobFailureInfo;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.dremio.service.users.SystemUser;

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

  @Test
  public void convertExceptionToFailureInfo1() {
    org.slf4j.Logger logger = mock(org.slf4j.Logger.class);
    String errString = "Failure finding function: grouping(varchar)";
    ErrorCollectorImpl errorCollector = new ErrorCollectorImpl();
    errorCollector.addGeneralError(errString);
    SqlParseException parseException = new SqlParseException(errorCollector.toErrorString(), new SqlParserPos(7, 42, 13, 57), null, null, null);
    UserException userException = SqlExceptionHelper.planError("SELECT FOO", parseException)
      .build(logger);
    String verboseError = userException.getVerboseMessage(false);
    JobFailureInfo jobFailureInfo = JobsServiceUtil.toFailureInfo(verboseError);
    assertEquals(JobFailureInfo.Type.PLAN, jobFailureInfo.getType());
    JobFailureInfo.Error error = jobFailureInfo.getErrorsList().get(0);
    assertEquals(errString, error.getMessage());
  }

  @Test
  public void testToSubmitJobRequest() {

    final JobRequest jobRequest = JobRequest.newBuilder()
      .setSqlQuery(new SqlQuery("select * from sys.version", Collections.emptyList(), SystemUser.SYSTEM_USERNAME))
      .setDatasetPath(new NamespaceKey(new ArrayList<>()))
      .setDatasetVersion(new DatasetVersion("abc"))
      .build();
    final SubmitJobRequest submitJobRequest = JobsServiceTestUtils.toSubmitJobRequest(jobRequest);
    assertEquals(toBuf(new SqlQuery("select * from sys.version", Collections.emptyList(), SystemUser.SYSTEM_USERNAME)),
      submitJobRequest.getSqlQuery());
    assertTrue(jobRequest.getRequestType() == JobRequest.RequestType.DEFAULT &&
      !(submitJobRequest.hasMaterializationSettings() || submitJobRequest.hasDownloadSettings()));
    assertEquals(new DatasetVersion("abc").getVersion(), submitJobRequest.getVersionedDataset().getVersion());
  }

  @Test
  public void testValidateJobRequest() {

    final SubmitJobRequest submitJobRequest = SubmitJobRequest.newBuilder()
      .setDownloadSettings(DownloadSettings.newBuilder()
        .setDownloadId("downloadId")
        .setFilename("fileName")
        .build())
      .setRunInSameThread(true)
      .setQueryType(JobsProtoUtil.toBuf(QueryType.UI_EXPORT))
      .setSqlQuery(toBuf(new SqlQuery("select * from sys.version", Collections.emptyList(), SystemUser.SYSTEM_USERNAME)))
      .build();

    final SubmitJobRequest validJobRequest = LocalJobsService.validateJobRequest(submitJobRequest);

    assertEquals(SystemUser.SYSTEM_USERNAME, validJobRequest.getUsername());
    assertEquals("UNKNOWN", validJobRequest.getVersionedDataset().getPath(0));
    assertEquals("UNKNOWN", validJobRequest.getVersionedDataset().getVersion());
    assertTrue(validJobRequest.hasDownloadSettings());
    assertFalse(validJobRequest.hasMaterializationSettings());
  }
}
