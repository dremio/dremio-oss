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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.ErrorCollectorImpl;
import com.dremio.dac.server.JobsServiceTestUtils;
import com.dremio.exec.planner.sql.SqlExceptionHelper;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.beans.DremioPBError;
import com.dremio.resource.exception.ResourceAllocationException;
import com.dremio.service.job.DownloadSettings;
import com.dremio.service.job.SubmitJobRequest;
import com.dremio.service.job.proto.JobFailureInfo;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.dremio.service.users.SystemUser;
import java.util.ArrayList;
import java.util.Collections;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.calcite.tools.ValidationException;
import org.junit.Test;

/** Unit Tests for {@code JobsServiceUtil} */
public class TestJobsServiceUtil {
  @Test
  public void convertExceptionToFailureInfo() {
    SqlParseException parseException =
        new SqlParseException("test message", new SqlParserPos(7, 42, 13, 57), null, null, null);
    UserException userException =
        SqlExceptionHelper.parseError("SELECT FOO", parseException).buildSilently();
    String verboseError = userException.getVerboseMessage(false);

    JobFailureInfo jobFailureInfo =
        JobsServiceUtil.toFailureInfo(verboseError, UserBitShared.DremioPBError.ErrorType.PARSE);
    assertEquals(JobFailureInfo.Type.PARSE, jobFailureInfo.getType());
    assertEquals(DremioPBError.ErrorType.PARSE, jobFailureInfo.getRootErrorType());

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
    String errString = "Failure finding function: grouping(varchar)";
    ErrorCollectorImpl errorCollector = new ErrorCollectorImpl();
    errorCollector.addGeneralError(errString);
    SqlParseException parseException =
        new SqlParseException(
            errorCollector.toErrorString(), new SqlParserPos(7, 42, 13, 57), null, null, null);
    UserException userException =
        SqlExceptionHelper.planError("SELECT FOO", parseException).buildSilently();
    String verboseError = userException.getVerboseMessage(false);
    JobFailureInfo jobFailureInfo =
        JobsServiceUtil.toFailureInfo(verboseError, UserBitShared.DremioPBError.ErrorType.PLAN);
    assertEquals(DremioPBError.ErrorType.PLAN, jobFailureInfo.getRootErrorType());
    JobFailureInfo.Error error = jobFailureInfo.getErrorsList().get(0);
    assertEquals(errString, error.getMessage());
  }

  @Test
  public void testDefaultRootErrorTypeInFailureInfo() {
    ResourceAllocationException resourceAllocationException =
        new ResourceAllocationException(
            "The engine \\'test engine\\' has been disabled.\n Enable the engine under Project Settings > Engines, and then try again");
    UserException userException =
        UserException.resourceError(resourceAllocationException).buildSilently();
    String verboseError = userException.getVerboseMessage(false);
    JobFailureInfo jobFailureInfo = JobsServiceUtil.toFailureInfo(verboseError, null);

    assertNotNull(jobFailureInfo);
    assertEquals(DremioPBError.ErrorType.SYSTEM, jobFailureInfo.getRootErrorType());
  }

  @Test
  public void testValidationErrorMessageForMissingContext() {
    String genericError = "unable to validate sql node";

    String queryString = "SELECT * FROM trips";
    // index location of "trips" inside the queryString
    int startLineNumber = 1;
    int startColumnNumber = 15;
    int endLineNumber = 1;
    int endColumnNumber = 19;
    String contextExceptionLocation =
        String.format(
            "From line %s, column %s to line %s, column %s",
            startLineNumber, startColumnNumber, endLineNumber, endColumnNumber);

    String actualError = "Object 'trips' not found";

    // create the exception
    SqlValidatorException sqlValidatorException = new SqlValidatorException(actualError, null);
    CalciteContextException calciteContextException =
        new CalciteContextException(
            contextExceptionLocation,
            sqlValidatorException,
            startLineNumber,
            startColumnNumber,
            endLineNumber,
            endColumnNumber);
    ValidationException validationException =
        new ValidationException(genericError, calciteContextException);

    // build the expected exception and convert it to failure info
    UserException userException =
        SqlExceptionHelper.validationError(queryString, validationException).buildSilently();
    String verboseError = userException.getVerboseMessage(false);
    JobFailureInfo jobFailureInfo =
        JobsServiceUtil.toFailureInfo(
            verboseError, UserBitShared.DremioPBError.ErrorType.VALIDATION);
    assertEquals(JobFailureInfo.Type.VALIDATION, jobFailureInfo.getType());
    assertEquals(DremioPBError.ErrorType.VALIDATION, jobFailureInfo.getRootErrorType());
    JobFailureInfo.Error error = jobFailureInfo.getErrorsList().get(0);

    assertEquals(
        actualError + ". Please check that it exists in the selected context.", error.getMessage());
    assertEquals(startLineNumber, (int) error.getStartLine());
    assertEquals(startColumnNumber, (int) error.getStartColumn());
    assertEquals(endLineNumber, (int) error.getEndLine());
    assertEquals(endColumnNumber, (int) error.getEndColumn());
  }

  @Test
  public void testToSubmitJobRequest() {

    final JobRequest jobRequest =
        JobRequest.newBuilder()
            .setSqlQuery(
                new SqlQuery(
                    "select * from sys.version",
                    Collections.emptyList(),
                    SystemUser.SYSTEM_USERNAME))
            .setDatasetPath(new NamespaceKey(new ArrayList<>()))
            .setDatasetVersion(new DatasetVersion("abc"))
            .build();
    final SubmitJobRequest submitJobRequest = JobsServiceTestUtils.toSubmitJobRequest(jobRequest);
    assertEquals(
        toBuf(
            new SqlQuery(
                "select * from sys.version", Collections.emptyList(), SystemUser.SYSTEM_USERNAME)),
        submitJobRequest.getSqlQuery());
    assertTrue(
        jobRequest.getRequestType() == JobRequest.RequestType.DEFAULT
            && !(submitJobRequest.hasMaterializationSettings()
                || submitJobRequest.hasDownloadSettings()));
    assertEquals(
        new DatasetVersion("abc").getVersion(),
        submitJobRequest.getVersionedDataset().getVersion());
  }

  @Test
  public void testValidateJobRequest() {

    final SubmitJobRequest submitJobRequest =
        SubmitJobRequest.newBuilder()
            .setDownloadSettings(
                DownloadSettings.newBuilder()
                    .setDownloadId("downloadId")
                    .setFilename("fileName")
                    .build())
            .setRunInSameThread(true)
            .setQueryType(JobsProtoUtil.toBuf(QueryType.UI_EXPORT))
            .setSqlQuery(
                toBuf(
                    new SqlQuery(
                        "select * from sys.version",
                        Collections.emptyList(),
                        SystemUser.SYSTEM_USERNAME)))
            .build();

    final SubmitJobRequest validJobRequest = LocalJobsService.validateJobRequest(submitJobRequest);

    assertEquals(SystemUser.SYSTEM_USERNAME, validJobRequest.getUsername());
    assertEquals("UNKNOWN", validJobRequest.getVersionedDataset().getPath(0));
    assertEquals("UNKNOWN", validJobRequest.getVersionedDataset().getVersion());
    assertTrue(validJobRequest.hasDownloadSettings());
    assertFalse(validJobRequest.hasMaterializationSettings());
  }
}
