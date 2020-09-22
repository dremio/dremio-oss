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

import java.util.List;

import org.junit.Test;

import com.dremio.dac.daemon.TestSpacesStoragePlugin;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.model.job.JobUI;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.service.job.JobDetailsRequest;
import com.dremio.service.job.JobSummary;
import com.dremio.service.job.SearchJobsRequest;
import com.dremio.service.job.VersionedDatasetPath;
import com.dremio.service.job.proto.JobDetails;
import com.dremio.service.job.proto.JobStats;
import com.dremio.service.jobs.JobsProtoUtil;
import com.dremio.service.jobs.JobsService;
import com.google.common.collect.ImmutableList;

/**
 * Tests for query profile parser.
 */
public class TestQueryProfileParser extends BaseTestServer {

  @Test
  public void testQueryParser() throws Exception {
    TestSpacesStoragePlugin.setup(getCurrentDremioDaemon());

    getPreview(getDataset(new DatasetPath("testA.dsA1")));
    final SearchJobsRequest searchJobsRequest = SearchJobsRequest.newBuilder()
        .setDataset(VersionedDatasetPath.newBuilder()
          .addAllPath(new DatasetPath("testA.dsA1").toPathList())
          .build())
        .setLimit(1000)
        .build();
    List<JobSummary> jobs = ImmutableList.copyOf(l(JobsService.class).searchJobs(searchJobsRequest));

    assertNotNull(jobs);
    assertTrue(jobs.size() > 0);
    JobUI job1 = expectSuccess(getBuilder(getAPIv2().path("job/" + jobs.get(0).getJobId().getId())).buildGet(), JobUI.class);
    assertEquals(JobsProtoUtil.toStuff(jobs.get(0).getJobId()), job1.getJobId());

    JobDetailsRequest jobDetailsRequest = JobDetailsRequest.newBuilder()
      .setJobId(jobs.get(0).getJobId())
      .build();
    final com.dremio.service.job.JobDetails job = l(JobsService.class).getJobDetails(jobDetailsRequest);
    final JobDetails jobDetails = JobsProtoUtil.getLastAttempt(job).getDetails();
    final JobStats jobStats = JobsProtoUtil.getLastAttempt(job).getStats();
    assertEquals(1, jobDetails.getTableDatasetProfilesList().size());
    assertEquals(1000, (long)jobDetails.getOutputRecords()); // leaf limit is 10k
    assertEquals(16250, (long) jobDetails.getDataVolume());

    assertEquals(16250, (long) jobStats.getOutputBytes());
    assertEquals(16250, (long) jobStats.getInputBytes());
    assertEquals(1000, (long) jobStats.getInputRecords());
    assertEquals(1000, (long) jobStats.getOutputRecords());
  }
}
