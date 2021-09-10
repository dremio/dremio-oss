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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import com.dremio.common.utils.protos.AttemptId;
import com.dremio.common.utils.protos.AttemptIdUtils;
import com.dremio.dac.daemon.TestSpacesStoragePlugin;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.model.job.JobsListingUI;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.work.protector.ForemenWorkManager;
import com.dremio.proto.model.attempts.AttemptReason;
import com.dremio.service.job.JobSummary;
import com.dremio.service.job.JobSummaryRequest;
import com.dremio.service.job.proto.JobAttempt;
import com.dremio.service.job.proto.JobDetails;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobInfo;
import com.dremio.service.job.proto.JobState;
import com.dremio.service.job.proto.ParentDatasetInfo;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.job.proto.ResourceSchedulingInfo;
import com.dremio.service.namespace.dataset.proto.DatasetType;

/**
 * Tests for jobs-listing/v1.0/ API.
 */
public class TestJobsListingResource extends BaseTestServer {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();
  private HybridJobsService jobsService;
  private LocalJobsService localJobsService;
  private ForemenWorkManager foremenWorkManager;

  @Before
  public void setup() throws Exception {
    clearAllDataExceptUser();
    jobsService = (HybridJobsService) l(JobsService.class);
    localJobsService = l(LocalJobsService.class);
    foremenWorkManager = l(ForemenWorkManager.class);
  }

  @Test
  public void testJobsListingAPI() throws Exception {
    TestSpacesStoragePlugin.setup();
    getPreview(getDataset(new DatasetPath("testA.dsA1")));
    String jobId = "1f3f8dad-f25e-8cbe-e952-1587f1647a00";
    String sql = "select * from \" testA.dsA1\"";
    UUID id = UUID.fromString(jobId);
    UserBitShared.ExternalId externalId = UserBitShared.ExternalId.newBuilder()
      .setPart1(id.getMostSignificantBits())
      .setPart2(id.getLeastSignificantBits())
      .build();

    final Job job = createJob(jobId, Arrays.asList("testA", "dsA1"), "v1", "A", "testA", JobState.COMPLETED, sql, 100L, 110L, QueryType.UI_RUN);

    AttemptId attemptId = AttemptId.of(externalId);
    job.getJobAttempt()
      .setAttemptId(AttemptIdUtils.toString(attemptId))
      .setState(JobState.FAILED)
      .setDetails(new JobDetails());

    // attempt succeeds
    attemptId = attemptId.nextAttempt();
    final JobAttempt jobAttempt = new JobAttempt()
      .setInfo(newJobInfo(job.getJobAttempt().getInfo(), 106, 107, null))
      .setAttemptId(AttemptIdUtils.toString(attemptId))
      .setState(JobState.COMPLETED)
      .setReason(AttemptReason.SCHEMA_CHANGE)
      .setDetails(new JobDetails());
    job.addAttempt(jobAttempt);

    ParentDatasetInfo parentDatasetInfo = new ParentDatasetInfo();
    parentDatasetInfo.setDatasetPathList(Arrays.asList("testA", "dsA1"));
    parentDatasetInfo.setType(DatasetType.valueOf(2));
    job.getJobAttempt().getInfo().setParentsList(Collections.singletonList(parentDatasetInfo));

    com.dremio.service.job.proto.JobDetails jobDetails = JobDetails.getDefaultInstance();
    jobDetails.setWaitInClient(1L);
    job.getJobAttempt().setDetails(jobDetails);
    localJobsService.storeJob(job);
    List<JobSummary> jobs = new ArrayList<>();
    jobs.add(jobsService.getJobSummary(JobSummaryRequest.newBuilder().setJobId(JobsProtoUtil.toBuf(job.getJobId())).build()));
    JobsListingUI jobsListingUI = new JobsListingUI(jobs, jobsService, null);

    assertEquals(jobId, jobsListingUI.getJobs().get(0).getId());
    assertEquals(3, jobsListingUI.getJobs().get(0).getState().getNumber());
    assertEquals(QueryType.UI_RUN, jobsListingUI.getJobs().get(0).getQueryType());
    assertEquals("A", jobsListingUI.getJobs().get(0).getQueryUser());
    assertEquals(sql, jobsListingUI.getJobs().get(0).getQueryText());
    assertEquals(true, jobsListingUI.getJobs().get(0).getFinalState());
    assertEquals(null, jobsListingUI.getJobs().get(0).getEnqueuedTime());
    assertEquals(false, jobsListingUI.getJobs().get(0).isAccelerated());
    assertEquals("1.0", jobsListingUI.getJobs().get(0).getPlannerEstimatedCost().toString());
    assertEquals("", jobsListingUI.getJobs().get(0).getEngine());
    assertEquals("", jobsListingUI.getJobs().get(0).getSubEngine());
    assertEquals("", jobsListingUI.getJobs().get(0).getWlmQueue());
    assertEquals(JobState.COMPLETED, jobsListingUI.getJobs().get(0).getState());
    assertEquals(true, jobsListingUI.getJobs().get(0).getFinalState());
    assertNotNull(jobsListingUI.getJobs().get(0).getDurationDetails());
    assertEquals("0", jobsListingUI.getJobs().get(0).getRowsScanned().toString());
    assertEquals("0", jobsListingUI.getJobs().get(0).getRowsReturned().toString());
    assertEquals("0 B / 0 Records", jobsListingUI.getJobs().get(0).getInput());
    assertEquals("0 B / 0 Records", jobsListingUI.getJobs().get(0).getOutput());
    assertEquals(1L, jobsListingUI.getJobs().get(0).getWaitInClient());
    assertEquals(false, jobsListingUI.getJobs().get(0).isAccelerated());
    // Queried Datasets
    assertEquals(1, jobsListingUI.getJobs().get(0).getQueriedDatasets().size());
    assertEquals("PHYSICAL_DATASET", jobsListingUI.getJobs().get(0).getQueriedDatasets().get(0).getDatasetType());
    assertEquals("dsA1", jobsListingUI.getJobs().get(0).getQueriedDatasets().get(0).getDatasetName());
    assertEquals("testA.dsA1", jobsListingUI.getJobs().get(0).getQueriedDatasets().get(0).getDatasetPath());
  }

  private Job createJob(final String id, final List<String> datasetPath, final String version, final String user,
                        final String space, final JobState state, final String sql,
                        final Long start, final Long end, QueryType queryType) {
    final JobId jobId = new JobId(id);
    final JobInfo jobInfo =
      new JobInfo(jobId, sql, version, QueryType.UI_RUN)
        .setClient("client")
        .setDatasetPathList(datasetPath)
        .setUser(user)
        .setSpace(space)
        .setStartTime(start)
        .setFinishTime(end)
        .setQueryType(queryType)
        .setResourceSchedulingInfo(new ResourceSchedulingInfo().setQueueName("SMALL")
          .setRuleName("ruleSmall"));

    final JobAttempt jobAttempt =
      new JobAttempt()
        .setState(state)
        .setInfo(jobInfo);

    return new Job(jobId, jobAttempt);
  }

  private static JobInfo newJobInfo(final JobInfo templateJobInfo, long start, long end, String failureInfo) {
    return new JobInfo(templateJobInfo.getJobId(), templateJobInfo.getSql(), templateJobInfo.getDatasetVersion(), templateJobInfo.getQueryType())
      .setSpace(templateJobInfo.getSpace())
      .setUser(templateJobInfo.getUser())
      .setStartTime(start)
      .setFinishTime(end)
      .setFailureInfo(failureInfo)
      .setDatasetPathList(templateJobInfo.getDatasetPathList());
  }
}
