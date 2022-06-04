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
import static org.junit.Assert.assertFalse;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.common.utils.protos.AttemptId;
import com.dremio.common.utils.protos.AttemptIdUtils;
import com.dremio.dac.daemon.TestSpacesStoragePlugin;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.model.job.JobInfoDetailsUI;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.service.reflection.ReflectionServiceHelper;
import com.dremio.dac.util.JobUtil;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.work.protector.ForemenWorkManager;
import com.dremio.proto.model.attempts.AttemptReason;
import com.dremio.service.job.JobDetailsRequest;
import com.dremio.service.job.JobSummary;
import com.dremio.service.job.JobSummaryRequest;
import com.dremio.service.job.proto.JobAttempt;
import com.dremio.service.job.proto.JobDetails;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobInfo;
import com.dremio.service.job.proto.JobProtobuf;
import com.dremio.service.job.proto.JobState;
import com.dremio.service.job.proto.ParentDatasetInfo;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.job.proto.ResourceSchedulingInfo;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetType;

public class TestJobDetails extends BaseTestServer {

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();
  private HybridJobsService jobsService;
  private LocalJobsService localJobsService;
  private ForemenWorkManager foremenWorkManager;
  private JobInfoDetailsUI jobInfoDetailsUI;
  private ReflectionServiceHelper reflectionServiceHelper;
  private NamespaceService namespaceService;

  @Before
  public void setup() throws Exception {
    clearAllDataExceptUser();
    jobsService = (HybridJobsService) l(JobsService.class);
    localJobsService = l(LocalJobsService.class);
    foremenWorkManager = l(ForemenWorkManager.class);
    reflectionServiceHelper = l(ReflectionServiceHelper.class);
    namespaceService = l(NamespaceService.class);
  }

  @Test
  public void testJobDetailsAPI() throws Exception {
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

    JobSummaryRequest jobSummaryRequest = JobSummaryRequest.newBuilder()
      .setJobId(JobProtobuf.JobId.newBuilder().setId(jobId).build())
      .setUserName("A")
      .build();
    JobSummary summary = jobsService.getJobSummary(jobSummaryRequest);

    JobDetailsRequest detailRequest = JobDetailsRequest.newBuilder()
      .setJobId(JobProtobuf.JobId.newBuilder().setId(jobId).build())
      .setUserName("A")
      .setProvideResultInfo(true)
      .build();

    UserBitShared.QueryProfile profile = UserBitShared.QueryProfile.newBuilder().build();

    com.dremio.service.job.JobDetails jobDetailsService = jobsService.getJobDetails(detailRequest);
    jobInfoDetailsUI = new JobInfoDetailsUI();
    jobInfoDetailsUI = jobInfoDetailsUI.of(jobDetailsService, profile, null, reflectionServiceHelper, namespaceService, 1, 0);

    assertEquals(jobId, jobInfoDetailsUI.getId());
    assertEquals("UI_RUN", jobInfoDetailsUI.getQueryType().toString());
    assertEquals("A", jobInfoDetailsUI.getQueryUser());
    assertEquals(sql, jobInfoDetailsUI.getQueryText());
    assertEquals(Long.valueOf(0), jobInfoDetailsUI.getInputBytes());
    assertEquals(Long.valueOf(0), jobInfoDetailsUI.getInputRecords());
    assertEquals(Long.valueOf(0), jobInfoDetailsUI.getOutputBytes());
    assertEquals(Long.valueOf(0), jobInfoDetailsUI.getOutputRecords());
    assertEquals("SMALL", jobInfoDetailsUI.getWlmQueue());
    assertFalse(jobInfoDetailsUI.isAccelerated());
    assertEquals(0, jobInfoDetailsUI.getNrReflectionsMatched());
    assertEquals(0,jobInfoDetailsUI.getNrReflectionsConsidered());
    assertEquals(0, jobInfoDetailsUI.getNrReflectionsUsed());
    assertEquals(0, jobInfoDetailsUI.getReflectionsMatched().size());
    assertEquals(0, jobInfoDetailsUI.getReflectionsUsed().size());
    assertEquals(java.util.Optional.ofNullable(100l).get(), java.util.Optional.ofNullable(jobInfoDetailsUI.getStartTime()).get());
    assertEquals(java.util.Optional.ofNullable(110l).get(), java.util.Optional.ofNullable(jobInfoDetailsUI.getEndTime()).get());
    assertEquals(0, jobInfoDetailsUI.getDatasetGraph().size());
    assertEquals(0, jobInfoDetailsUI.getTotalMemory());
    assertEquals(0, jobInfoDetailsUI.getCpuUsed());
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

  @Test
  public void testGetDuration() {
    JobProtobuf.JobId jobId = JobProtobuf.JobId.newBuilder().setId("1f3f8dad-f25e-8cbe-e952-1587f1647b00").build();

   JobProtobuf.JobInfo jobInfo = JobProtobuf.JobInfo.newBuilder()
      .setStartTime(100).setFinishTime(107)
      .setJobId(jobId)
      .setSql("select * from temp")
      .setQueryType(JobProtobuf.QueryType.UI_RUN)
      .setDatasetVersion("1").build();

   JobProtobuf.JobAttempt jobAttempt = JobProtobuf.JobAttempt.newBuilder().setAttemptId("0")
     .setInfo(jobInfo)
     .setState(JobProtobuf.JobState.FAILED)
     .build();

    com.dremio.service.job.JobDetails jobdetails = com.dremio.service.job.JobDetails.newBuilder()
      .setCompleted(true)
      .addAttempts(jobAttempt)
      .build();

    long actualDuration = JobUtil.getTotalDuration(jobdetails,0);
    assertEquals(7l,actualDuration);
  }
}
