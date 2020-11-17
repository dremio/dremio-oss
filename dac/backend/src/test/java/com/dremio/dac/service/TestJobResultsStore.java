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
package com.dremio.dac.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import com.dremio.dac.daemon.DACDaemonModule;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.model.job.JobDataFragmentWrapper;
import com.dremio.dac.model.sources.SourceUI;
import com.dremio.dac.model.sources.UIMetadataPolicy;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.util.JSONUtil;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.NASConf;
import com.dremio.service.job.JobDetails;
import com.dremio.service.job.JobDetailsRequest;
import com.dremio.service.job.JobSummary;
import com.dremio.service.job.JobSummaryRequest;
import com.dremio.service.job.proto.JobAttempt;
import com.dremio.service.job.proto.JobFailureInfo;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobInfo;
import com.dremio.service.job.proto.JobProtobuf;
import com.dremio.service.job.proto.JobResult;
import com.dremio.service.job.proto.JobState;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.JobDataClientUtils;
import com.dremio.service.jobs.JobDataFragment;
import com.dremio.service.jobs.JobNotFoundException;
import com.dremio.service.jobs.JobRequest;
import com.dremio.service.jobs.JobResultsStore;
import com.dremio.service.jobs.JobsProtoUtil;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.LocalJobsService;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.dremio.service.users.SystemUser;
import com.dremio.test.UserExceptionMatcher;
import com.google.common.io.Files;

/**
 * Tests for job results store.
 */
public class TestJobResultsStore extends BaseTestServer {

  @Rule
  public final ExpectedException exception = ExpectedException.none();

  @Rule
  public final TemporaryFolder tmpDir = new TemporaryFolder();

  private BufferAllocator allocator;

  private int sqlLimit = 2_000_000;

  @Before
  public void setup() throws Exception {
    clearAllDataExceptUser();
    allocator = getSabotContext().getAllocator().newChildAllocator(getClass().getName(), 0, Long.MAX_VALUE);
  }

  @After
  public void cleanup() {
    allocator.close();
  }

  /**
   * Are UI query results honoring offset and limit properly ?
   * Are actual results same as expected results ?
   */
  @Test
  public void testResultsHonoringOffsetLimit() throws Exception {
    SqlQuery sqlQuery = getQueryFromSQL("select * from cp.\"datasets/5000rows/5000rows.parquet\" LIMIT 5000");
    final JobId jobId = submitJobAndWaitUntilCompletion(
      JobRequest.newBuilder()
                .setSqlQuery(sqlQuery)
                .setQueryType(QueryType.UI_RUN)
                .build()
    );

    int[] offsets = new int[]      {   0,   0, 3967, 3968, 3968};
    int[] limits = new int[]       {5000, 100,  100,  100, 6000};
    int[] expectedRows = new int[] {5000, 100,  100,  100, 1032};

    for(int i=0;i<offsets.length; i++) {
      fetchValidateResultsAtOffset(jobId, offsets[i], limits[i], expectedRows[i]);
    }
  }

  private void fetchValidateResultsAtOffset(JobId jobId, int offset, int limit, int expectedRows) throws JobNotFoundException {
    try (
      JobDataFragment expectedResult = l(LocalJobsService.class).getJobData(jobId, offset, limit);
      JobDataFragment actualResult = JobDataClientUtils.getJobData(l(JobsService.class), allocator, jobId, offset, limit);
    ) {
      assertEquals("Number of records received are incorrect for offset:" + offset + ", limit:" + limit,
                   expectedRows,
                   actualResult.getReturnedRowCount());
      validateResults(expectedResult, actualResult);
    }
  }

  /**
   * Are UI query results honoring offset and limit properly ?
   * Are actual results same as expected results ?
   * This unit test tests when results are stored in multiple arrow files
   * and offset and limit spans multiple arrow files.
   */
  @Test
  public void testResultsHonoringOffsetLimitMultipleArrowFiles() throws Exception {
    SqlQuery sqlQuery = getQueryFromSQL("SELECT * " +
                                        "FROM      cp.\"datasets/parquet_offset/offset1.parquet\" a " +
                                        "FULL JOIN cp.\"datasets/parquet_offset/offset2.parquet\" b " +
                                        "ON a.column1 = b.column1 LIMIT " + 3_000_000);

    // There are 5 fragments for above query, so setting MAX_WIDTH_PER_NODE_KEY to 5.
    // This is reset at end of this method.
    setSystemOption(ExecConstants.MAX_WIDTH_PER_NODE_KEY, "5");
    try {
      final JobId jobId = submitJobAndWaitUntilCompletion(
        JobRequest.newBuilder()
          .setSqlQuery(sqlQuery)
          .setQueryType(QueryType.REST)
          .build()
      );

      int[] offsets = new int[]      {2007785};
      int[] limits = new int[]       {    500};
      int[] expectedRows = new int[] {    500};

      for (int i = 0; i < offsets.length; i++) {
        fetchValidateResultsAtOffset(jobId, offsets[i], limits[i], expectedRows[i]);
      }
    } finally {
      resetSystemOption(ExecConstants.MAX_WIDTH_PER_NODE_KEY);
    }
  }

  /**
   * Test truncation of results for UI query having no limit clause
   * with PlannerSettings.OUTPUT_LIMIT_SIZE option value changed to depict
   * the scenario where user changed the value.
   * @throws Exception
   */
  @Test
  public void testTruncateResultsUIQueryWithOutputLimitChanged() throws Exception {
    try {
      // Changing value of PlannerSettings.OUTPUT_LIMIT_SIZE to depict user changed the value.
      // This is reset at end of this method.
      setSystemOption(PlannerSettings.OUTPUT_LIMIT_SIZE.getOptionName(), "" + 200_000L);
      testTruncateResults(QueryType.UI_RUN, "", 130_944);
    } finally {
      resetSystemOption(PlannerSettings.OUTPUT_LIMIT_SIZE.getOptionName());
    }
  }

  /**
   * Test truncation of results for UI query having no limit clause
   * @throws Exception
   */
  @Test
  public void testTruncateResultsUIQueryWithNoLimitClause() throws Exception {
    testTruncateResults(QueryType.UI_RUN, "" , 607_104);
  }

  /**
   * Test truncation of results for UI query having limit clause
   * @throws Exception
   */
  @Test
  public void testTruncateResultsUIQueryWithLimitClause() throws Exception {
    testTruncateResults(QueryType.UI_RUN, "LIMIT " + sqlLimit, 1_003_904);
  }

  /**
   * Feature for truncation of results for UI query should not truncate results for REST query.
   * @throws Exception
   */
  @Test
  public void testTruncateResultsRESTQuery() throws Exception {
    testTruncateResults(QueryType.REST, "LIMIT " + sqlLimit, sqlLimit);
  }

  private void testTruncateResults(QueryType queryType, String limitClause, int expectedNumRecords) throws Exception {
    SqlQuery sqlQuery = getQueryFromSQL("SELECT * " +
                                        "FROM      cp.\"datasets/parquet_offset/offset1.parquet\" a " +
                                        "FULL JOIN cp.\"datasets/parquet_offset/offset2.parquet\" b " +
                                        "ON a.column1 = b.column1 " + limitClause);

    // There are 5 fragments for above query, so setting MAX_WIDTH_PER_NODE_KEY to 5.
    // This is reset at end of this method.
    setSystemOption(ExecConstants.MAX_WIDTH_PER_NODE_KEY, "5");
    try {
      final JobId jobId = submitJobAndWaitUntilCompletion(JobRequest.newBuilder()
                                                                    .setSqlQuery(sqlQuery)
                                                                    .setQueryType(queryType)
                                                                    .build());

      JobDetailsRequest jobDetailsRequest =  JobDetailsRequest.newBuilder()
                                                              .setJobId(JobsProtoUtil.toBuf(jobId))
                                                              .setUserName(SystemUser.SYSTEM_USERNAME)
                                                              .build();

      JobDetails jobDetails = l(JobsService.class).getJobDetails(jobDetailsRequest);
      JobProtobuf.JobAttempt jobAttempt = jobDetails.getAttempts(jobDetails.getAttemptsCount()-1);
      assertEquals("Expected num of records does not match.", expectedNumRecords, jobAttempt.getStats().getOutputRecords());

      // Fetch last 500 records to verify that there is no truncation of results while fetching stored results.
      fetchValidateResultsAtOffset(jobId, expectedNumRecords-500, 500, 500);
    } finally {
      resetSystemOption(ExecConstants.MAX_WIDTH_PER_NODE_KEY);
    }
  }

  @Test
  public void testJobResultStore() throws Exception {
    populateInitialData();
    final JobsService jobsService = l(JobsService.class);
    final DatasetPath ds1 = new DatasetPath("s.ds1");
    final JobId jobId = submitJobAndWaitUntilCompletion(
      JobRequest.newBuilder()
        .setSqlQuery(new SqlQuery("select * from LocalFS1.\"dac-sample1.json\" limit 10", ds1.toParentPathList(), DEFAULT_USERNAME))
        .setDatasetPath(ds1.toNamespaceKey())
        .setDatasetVersion(new DatasetVersion("1"))
        .build()
    );
    try (
      final JobDataFragment storedResult = l(LocalJobsService.class).getJobData(jobId, 0, 10);
      final JobDataFragment result = JobDataClientUtils.getJobData(jobsService, allocator, jobId, 0, 10)) {
      validateResults(storedResult, result);
    }
  }

  private void validateResults(JobDataFragment expectedResult,
                               JobDataFragment actualResult) {
    for (Field column: actualResult.getSchema()) {
      assertTrue(expectedResult.getSchema().getFields().contains(column));
    }
    for (int i=0; i<actualResult.getReturnedRowCount(); i++) {
      boolean found = false;
      List<Object> valuesFromResult = new ArrayList<>();
      for(Field c : actualResult.getSchema()) {
        valuesFromResult.add(actualResult.extractValue(c.getName(), i));
      }

      for (int j = 0; j< expectedResult.getReturnedRowCount(); j++) {
        List<Object> valuesFromStored = new ArrayList<>();
        for(Field c : expectedResult.getSchema()) {
          valuesFromStored.add(expectedResult.extractValue(c.getName(), j));
        }
        if (valuesFromResult.equals(valuesFromStored)) {
          found = true;
          break;
        }
      }
      assertTrue("Missing row numbered [" + i + "] from " + JSONUtil.toString(new JobDataFragmentWrapper(0,
                                                                                                         expectedResult)), found);
    }
  }

  @Test
  public void testCancelBeforeLoadingJob() {
    exception.expect(new UserExceptionMatcher(UserBitShared.DremioPBError.ErrorType.DATA_READ,
      "Could not load results as the query was canceled"));

    final JobResultsStore jobResultsStore = mock(JobResultsStore.class);
    JobResult jobResult = new JobResult();
    JobAttempt jobAttempt = new JobAttempt();
    jobAttempt.setState(JobState.CANCELED);
    List<JobAttempt> attempts = new ArrayList<>();
    attempts.add(jobAttempt);
    jobResult.setAttemptsList(attempts);
    when(jobResultsStore.loadJobData(new JobId("Canceled Job"),jobResult,0,0)).thenCallRealMethod();
    jobResultsStore.loadJobData(new JobId("Canceled Job"),jobResult,0,0);
  }

  /**
   * Test fetching of job data (JobResultsStore#loadJobData()) in case of query failure during execution.
   */
  @Test
  public void testLoadJobDataOfFailedQuery() {
    String failureMessage = "job failed";
    exception.expect(new UserExceptionMatcher(UserBitShared.DremioPBError.ErrorType.DATA_READ,
                     failureMessage));

    final JobResultsStore jobResultsStore = mock(JobResultsStore.class);
    JobFailureInfo.Error error = new JobFailureInfo.Error();
    error.setMessage(failureMessage);

    JobFailureInfo detailedFailureInfo = new JobFailureInfo();
    detailedFailureInfo.setErrorsList(Arrays.asList(error));

    JobInfo jobInfo = new JobInfo();
    jobInfo.setDetailedFailureInfo(detailedFailureInfo);

    JobAttempt jobAttempt = new JobAttempt();
    jobAttempt.setState(JobState.FAILED);
    jobAttempt.setInfo(jobInfo);

    List<JobAttempt> attempts = new ArrayList<>();
    attempts.add(jobAttempt);

    JobResult jobResult = new JobResult();
    jobResult.setAttemptsList(attempts);

    when(jobResultsStore.loadJobData(new JobId("Failed JobID"),jobResult,0,0)).thenCallRealMethod();
    jobResultsStore.loadJobData(new JobId("Failed JobID"),jobResult,0,0);
  }

  @Test
  public void testPromotedDataset() throws Exception {
    final JobsService jobsService = l(JobsService.class);
    final String firstJobId = submitJobAndWaitUntilCompletion(
      JobRequest.newBuilder().setSqlQuery(new SqlQuery("select * from sys.version", DEFAULT_USERNAME)).build()
    ).getId();

    final JobId secondJobId = submitJobAndWaitUntilCompletion(
      JobRequest.newBuilder()
        .setSqlQuery(new SqlQuery(String.format("select * from \"%s\".\"%s\"",
          DACDaemonModule.JOBS_STORAGEPLUGIN_NAME, firstJobId),
          SystemUser.SYSTEM_USERNAME))
        .build()
    );
    final JobSummary secondJob = jobsService.getJobSummary(JobSummaryRequest.newBuilder().setJobId(JobsProtoUtil.toBuf(secondJobId)).build());

    assertTrue(JobsProtoUtil.toStuff(secondJob.getJobState()) == JobState.COMPLETED);

    final NamespaceService namespaceService = l(NamespaceService.class);
    assertNotNull(namespaceService.getDataset(
        new NamespaceKey(Arrays.asList(DACDaemonModule.JOBS_STORAGEPLUGIN_NAME, firstJobId))));
  }

  /**
   * Tests results retrieval from Job Results Store for jobs with multiple attempts.
   */
  @Test
  public void getResultsFromJobWithMultipleAttempts() throws Exception {
    // Create Folder and Files with mixed schema to force Schema Learning and multiple attempts
    File sourceFolder = tmpDir.newFolder();
    File datasetFolder = new java.io.File(sourceFolder.getAbsoluteFile(), "test-folder");
    datasetFolder.mkdir();

    createFile(datasetFolder, "file1.json", "{a:1}{a:2}");
    createFile(datasetFolder, "file2.json", "{a:\"test1\"}{a:\"test2\"}");

    // Create source with the files created
    SourceUI source = new SourceUI();
    source.setName("test");
    source.setCtime(System.currentTimeMillis());
    final NASConf nas = new NASConf();
    nas.path = sourceFolder.getAbsolutePath();
    source.setConfig(nas);
    source.setMetadataPolicy(UIMetadataPolicy.of(CatalogService.DEFAULT_METADATA_POLICY_WITH_AUTO_PROMOTE));
    newSourceService().registerSourceWithRuntime(source.asSourceConfig(), SystemUser.SYSTEM_USERNAME);

    // Query the Folder to auto-promote
    JobId jobId = submitJobAndWaitUntilCompletion(
      JobRequest.newBuilder()
        .setSqlQuery(new SqlQuery(
          String.format("select * from \"%s\".\"%s\"", source.getName(), datasetFolder.getName()),
          SystemUser.SYSTEM_USERNAME))
        .setQueryType(QueryType.UI_RUN)
        .build()
    );

    // Sanity check that the job has multiple attempts
    JobDetails jobDetails = l(JobsService.class).getJobDetails(
      JobDetailsRequest.newBuilder()
        .setJobId(JobsProtoUtil.toBuf(jobId))
        .setUserName(SystemUser.SYSTEM_USERNAME)
        .build());
    assertTrue(jobDetails.getAttemptsCount() > 1);

    // Try to get results from job results store
    submitJobAndWaitUntilCompletion(
      JobRequest.newBuilder()
        .setSqlQuery(new SqlQuery(
          String.format("select * from sys.job_results.\"%s\"", jobId.getId()),
          SystemUser.SYSTEM_USERNAME))
        .setQueryType(QueryType.UI_RUN)
        .build()
    );
  }

  private java.io.File createFile(File parent, String fileName, String contents) throws Exception {
    java.io.File f1 = new java.io.File(parent, fileName);
    Files.asCharSink(f1, StandardCharsets.UTF_8).write(contents);
    return f1;
  }
}
