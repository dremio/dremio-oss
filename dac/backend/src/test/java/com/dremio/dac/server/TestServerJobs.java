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
package com.dremio.dac.server;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.ws.rs.client.WebTarget;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.dremio.dac.daemon.TestSpacesStoragePlugin;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.explore.model.DatasetUI;
import com.dremio.dac.model.job.JobDetailsUI;
import com.dremio.dac.model.job.JobListItem;
import com.dremio.dac.model.job.JobUI;
import com.dremio.dac.model.job.JobsUI;
import com.dremio.dac.proto.model.dataset.VirtualDatasetUI;
import com.dremio.dac.resource.JobResource;
import com.dremio.dac.service.datasets.DatasetVersionMutator;
import com.dremio.dac.util.JSONUtil;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobState;
import com.dremio.service.job.proto.ParentDatasetInfo;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.Job;
import com.dremio.service.jobs.JobDataFragment;
import com.dremio.service.jobs.JobRequest;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.JobsServiceUtil;
import com.dremio.service.jobs.NoOpJobStatusListener;
import com.dremio.service.jobs.SearchJobsRequest;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;

/**
 * Tests for jobs api.
 */
public class TestServerJobs extends BaseTestServer {

  @Before
  public void setup() throws Exception {
    clearAllDataExceptUser();
  }

  // TODO (Amit H) Flaky tests
  @Test
  @Ignore
  public void testRunningJob() throws Exception {
    TestSpacesStoragePlugin.setup(getCurrentDremioDaemon());
    final JobsService jobsService = l(JobsService.class);
    final DatasetVersionMutator datasetService = newDatasetVersionMutator();
    final DatasetPath datasetPath = new DatasetPath("testA.dsA1");
    final VirtualDatasetUI ds1 = datasetService.get(datasetPath);
    Job job = Futures.getUnchecked(
      jobsService.submitJob(
        JobRequest.newBuilder()
          .setSqlQuery(new SqlQuery(ds1.getSql(), ds1.getState().getContextList(), DEFAULT_USERNAME))
          .setQueryType(QueryType.UI_RUN)
          .setDatasetPath(datasetPath.toNamespaceKey())
          .setDatasetVersion(ds1.getVersion())
          .build(),
        NoOpJobStatusListener.INSTANCE)
    );
    boolean runningState;
    while (true) {
      final  JobUI jip = expectSuccess(getBuilder(getAPIv2().path("job/" + job.getJobId().getId())).buildGet(), JobUI.class);
      if (jip.getJobAttempt().getState() == JobState.RUNNING) {
        final JobsUI all = expectSuccess(getBuilder(getAPIv2().path("jobs")).buildGet(), JobsUI.class);
        assertEquals(1, all.getJobs().size());
        runningState = true;
        break;
      }
      if (jip.getJobAttempt().getState() == JobState.COMPLETED) {
        runningState = true;
        break;
      }
    }
    job.getData().loadIfNecessary();
    assertTrue("jobState should have changed to RUNNING at least once", runningState);
  }

  private JobUI waitForCompleteJobInIndex(JobId jobId) {
    while (true) {
      final JobUI job = expectSuccess(getBuilder(getAPIv2().path("job/" + jobId.getId())).buildGet(), JobUI.class);
      final JobState state = job.getJobAttempt().getState();
      if (state == JobState.CANCELED || state == JobState.COMPLETED || state == JobState.FAILED) {
        return job;
      }
      try {
        Thread.sleep(20);
      } catch (InterruptedException e) {
        break;
      }
    }
    return null;
  }

  // TODO (Amit H) Flaky tests
  @Test
  @Ignore
  public void testJob() throws Exception {
    TestSpacesStoragePlugin.setup(getCurrentDremioDaemon());
    expectSuccess(getBuilder(getAPIv2().path("dataset/testA.dsA1/data").queryParam("limit", "10000")).buildGet(), JobDataFragment.class);
    final SearchJobsRequest request1 = SearchJobsRequest.newBuilder()
        .setDatasetPath(new DatasetPath("testA.dsA1").toNamespaceKey())
        .setLimit(100)
        .build();
    List<Job> jobs = ImmutableList.copyOf(l(JobsService.class).searchJobs(request1));
    assertNotNull(jobs);
    assertTrue(jobs.size() > 0);
    JobUI job1 = waitForCompleteJobInIndex(jobs.get(0).getJobId());
    // get list of jobs and job again
    final SearchJobsRequest request = SearchJobsRequest.newBuilder()
        .setDatasetPath(new DatasetPath("testA.dsA1").toNamespaceKey())
        .setLimit(100)
        .build();
    jobs = ImmutableList.copyOf(l(JobsService.class).searchJobs(request));
    job1 = expectSuccess(getBuilder(getAPIv2().path("job/" + job1.getJobId().getId())).buildGet(), JobUI.class);
    assertEquals(jobs.get(0).getJobId(), job1.getJobId());
    assertEquals(jobs.get(0).getJobAttempt().getInfo().getSql(), job1.getJobAttempt().getInfo().getSql());
    assertEquals(jobs.get(0).getJobAttempt().getInfo().getDatasetPathList(), job1.getJobAttempt().getInfo().getDatasetPathList());
    assertEquals(jobs.get(0).getJobAttempt().getInfo().getDatasetVersion(), job1.getJobAttempt().getInfo().getDatasetVersion());
    assertEquals(jobs.get(0).getJobAttempt().getInfo().getParentsList(), job1.getJobAttempt().getInfo().getParentsList());
  }

  @Test
  public void testJobPhysicalDatasetParent() throws Exception {
    populateInitialData();
    final DatasetPath datasetPath = new DatasetPath("DG.dsg1");
    final DatasetUI dsg1 = getDataset(datasetPath);
    final JobsService jobsService = l(JobsService.class);
    Job job = Futures.getUnchecked(
      jobsService.submitJob(
        JobRequest.newBuilder()
          .setSqlQuery(getQueryFromConfig(dsg1))
          .setQueryType(QueryType.UI_RUN)
          .setDatasetPath(datasetPath.toNamespaceKey())
          .setDatasetVersion(dsg1.getDatasetVersion())
          .build(),
        NoOpJobStatusListener.INSTANCE)
    );
    job.getData().loadIfNecessary();
    assertEquals(1, job.getJobAttempt().getInfo().getParentsList().size());
    assertEquals(DEFAULT_USERNAME, job.getJobAttempt().getInfo().getUser());
    assertEquals("/job/"+job.getJobId().getId()+"/data", JobResource.getPaginationURL(job.getJobId()));
    assertEquals(Arrays.asList("LocalFS1", "dac-sample1.json"), job.getJobAttempt().getInfo().getParentsList().get(0).getDatasetPathList());
    assertEquals(DatasetType.PHYSICAL_DATASET_SOURCE_FILE, job.getJobAttempt().getInfo().getParentsList().get(0).getType());
    @SuppressWarnings("unused")
    JobUI job1 = expectSuccess(getBuilder(getAPIv2().path("job/" + job.getJobId().getId())).buildGet(), JobUI.class);
    assertEquals(1, job.getJobAttempt().getInfo().getParentsList().size());
    assertEquals(Arrays.asList("LocalFS1", "dac-sample1.json"), job.getJobAttempt().getInfo().getParentsList().get(0).getDatasetPathList());
    assertEquals(DatasetType.PHYSICAL_DATASET_SOURCE_FILE, job.getJobAttempt().getInfo().getParentsList().get(0).getType());

    JobDetailsUI jobDetails = expectSuccess(getBuilder(getAPIv2().path("job/" + job.getJobId().getId() + "/details")).buildGet(), JobDetailsUI.class);
    assertNotNull(jobDetails);
    assertEquals(JobResource.getPaginationURL(job.getJobId()), jobDetails.getPaginationUrl());
  }

  @Test
  public void testJobVirtualDatasetParent() throws Exception {
    populateInitialData();
    final DatasetPath datasetPath = new DatasetPath("DG.dsg10");
    final VirtualDatasetUI dsg1 = newDatasetVersionMutator().get(datasetPath);
    final JobsService jobsService = l(JobsService.class);
    Job job = Futures.getUnchecked(
      jobsService.submitJob(
        JobRequest.newBuilder()
          .setSqlQuery(getQueryFromConfig(dsg1))
          .setQueryType(QueryType.UI_RUN)
          .setDatasetPath(datasetPath.toNamespaceKey())
          .setDatasetVersion(dsg1.getVersion())
          .build(),
        NoOpJobStatusListener.INSTANCE)
    );
    job.getData().loadIfNecessary();
    assertEquals(2, job.getJobAttempt().getInfo().getParentsList().size());
    assertEquals(DEFAULT_USERNAME, job.getJobAttempt().getInfo().getUser());
    assertEquals("/job/"+job.getJobId().getId()+"/data", JobResource.getPaginationURL(job.getJobId()));
    for (ParentDatasetInfo parentDataset : job.getJobAttempt().getInfo().getParentsList()) {
      assertEquals(DatasetType.VIRTUAL_DATASET, parentDataset.getType());
      assertTrue(format("Parents for %s must be DG.dsg9 and DG.dsg8",  datasetPath),
              Arrays.asList("DG", "dsg9").equals(parentDataset.getDatasetPathList()) || (Arrays.asList("DG","dsg8").equals(parentDataset.getDatasetPathList())));
    }
    JobUI job1 = expectSuccess(getBuilder(getAPIv2().path("job/" + job.getJobId().getId())).buildGet(), JobUI.class);
    assertEquals(2, job1.getJobAttempt().getInfo().getParentsList().size());
    for (ParentDatasetInfo parentDataset : job1.getJobAttempt().getInfo().getParentsList()) {
      assertEquals(DatasetType.VIRTUAL_DATASET, parentDataset.getType());
      assertTrue(format("Parents for %s must be DG.dsg9 and DG.dsg8",  datasetPath),
              Arrays.asList("DG", "dsg9").equals(parentDataset.getDatasetPathList()) ||
                      (Arrays.asList("DG", "dsg8").equals(parentDataset.getDatasetPathList())));
    }

    JobDetailsUI jobDetails = expectSuccess(getBuilder(getAPIv2().path("job/" + job.getJobId().getId() + "/details")).buildGet(), JobDetailsUI.class);
    assertNotNull(jobDetails);
    assertEquals(JobResource.getPaginationURL(job.getJobId()), jobDetails.getPaginationUrl());
  }

  @Test
  public void testJobPhysicalDatasetParentTableau() throws Exception {
    populateInitialData();
    final JobsService jobsService = l(JobsService.class);
    Job job = Futures.getUnchecked(
      jobsService.submitJob(
        JobRequest.newBuilder()
          .setSqlQuery(new SqlQuery("select * from \"LocalFS1\".\"dac-sample1.json\"", USERNAME))
          .setQueryType(QueryType.UI_RUN)
          .build(),
        NoOpJobStatusListener.INSTANCE)
    );
    job.getData().loadIfNecessary();
    assertEquals(1, job.getJobAttempt().getInfo().getParentsList().size());
    assertEquals(Arrays.asList("LocalFS1", "dac-sample1.json"), job.getJobAttempt().getInfo().getParentsList().get(0).getDatasetPathList());
    @SuppressWarnings("unused")
    JobUI job1 = expectSuccess(getBuilder(getAPIv2().path("job/" + job.getJobId().getId())).buildGet(), JobUI.class);
    assertEquals(1, job.getJobAttempt().getInfo().getParentsList().size());
    assertEquals(Arrays.asList("LocalFS1", "dac-sample1.json"), job.getJobAttempt().getInfo().getParentsList().get(0).getDatasetPathList());
  }

  // TODO (Amit H) Flaky tests
  @Test
  @Ignore
  public void testJobDetails() throws Exception {
    TestSpacesStoragePlugin.setup(getCurrentDremioDaemon());
    expectSuccess(getBuilder(getAPIv2().path("dataset/testA.dsA1/data").queryParam("limit", "10000")).buildGet(), JobDataFragment.class);
    final SearchJobsRequest request = SearchJobsRequest.newBuilder()
        .setDatasetPath(new DatasetPath("testA.dsA1").toNamespaceKey())
        .setLimit(100)
        .build();
    List<Job> jobs = ImmutableList.copyOf(l(JobsService.class).searchJobs(request));
    JobDetailsUI jobDetails = expectSuccess(getBuilder(getAPIv2().path("job/" + jobs.get(0).getJobId().getId() + "/details")).buildGet(), JobDetailsUI.class);
    assertNotNull(jobDetails);
    assertNotNull(jobs);
    assertTrue(jobs.size() > 0);
    waitForCompleteJobInIndex(jobs.get(0).getJobId());
    jobDetails = expectSuccess(getBuilder(getAPIv2().path("job/" + jobs.get(0).getJobId().getId() + "/details")).buildGet(), JobDetailsUI.class);
    assertEquals(1, jobDetails.getTableDatasetProfiles().size());
    assertEquals(500, (long) jobDetails.getOutputRecords());
    assertEquals(9000, (long) jobDetails.getDataVolume());
  }

  @Test
  public void testJobsPerUser() throws Exception {
    JobsService jobsService = l(JobsService.class);
    TestSpacesStoragePlugin.setup(getCurrentDremioDaemon());
    // run at least one job
    getPreview(getDataset(new DatasetPath("testB.dsB1"))); // This triggers a job
    doc("getting list of all jobs");
    JobsUI allJobs = expectSuccess(getBuilder(getAPIv2().path("jobs")).buildGet(), JobsUI.class);
    int dsB1Jobs = 0;
    List<JobListItem> foundJobs = new ArrayList<>();
    for (JobListItem job : allJobs.getJobs()) {
      if (Arrays.asList("testB", "dsB1").equals(job.getDatasetPathList())) {
        dsB1Jobs++;
        foundJobs.add(job);
      }
    }
    assertEquals(1, dsB1Jobs);
    doc("getting list of all jobs for dataset testB.dsB1");
    JobsUI jobsUI = expectSuccess(getBuilder(getAPIv2().path("jobs").queryParam("filter", "ds==testB.dsB1")).buildGet(), JobsUI.class);
    assertEquals(
      "from all jobs:\n"
        + JSONUtil.toString(allJobs.getJobs()) + "\n"
        + "we found:\n"
        + JSONUtil.toString(foundJobs) + "\n"
        + "from dataset testB.dsB1 we found:\n"
        + JSONUtil.toString(jobsUI.getJobs()),
      dsB1Jobs, jobsUI.getJobs().size());
    assertEquals(dsB1Jobs, jobsUI.getJobs().size());

    getPreview(getDataset(new DatasetPath("testB.dsB1"))); // this triggers a job
    jobsUI = expectSuccess(getBuilder(getAPIv2().path("jobs").queryParam("filter", "ds==testB.dsB1")).buildGet(), JobsUI.class);
    // getting the data 2x on the same version does not create a new job
    assertEquals(dsB1Jobs, jobsUI.getJobs().size());
    dsB1Jobs++;

    // get version
    DatasetUI dsGet = getDataset(new DatasetPath("testB.dsB1"));

    int dsB1JobsVersion = 0;
    for (JobListItem job : jobsUI.getJobs()) {
      if (Arrays.asList("testB", "dsB1").equals(job.getDatasetPathList()) &&
        dsGet.getDatasetVersion().toString().equals(job.getDatasetVersion())) {
        dsB1JobsVersion++;
      }
    }

    DatasetVersion v2 = DatasetVersion.newVersion();
    doc("Submitting job for dataset testB.dsB1 dataset for version " + v2.getVersion());

    JobsServiceUtil.waitForJobCompletion(
      l(JobsService.class).submitJob(
        JobRequest.newBuilder()
          .setSqlQuery(getQueryFromConfig(dsGet))
          .setQueryType(QueryType.UI_PREVIEW)
          .setDatasetPath(getDatasetPath(dsGet).toNamespaceKey())
          .setDatasetVersion(v2)
          .build(),
        NoOpJobStatusListener.INSTANCE)
    );

    JobsServiceUtil.waitForJobCompletion(
      l(JobsService.class).submitJob(
        JobRequest.newBuilder()
          .setSqlQuery(new SqlQuery(dsGet.getSql(), dsGet.getContext(), USERNAME))
          .setQueryType(QueryType.UI_PREVIEW)
          .setDatasetPath(getDatasetPath(dsGet).toNamespaceKey())
          .setDatasetVersion(v2)
          .build(),
        NoOpJobStatusListener.INSTANCE)
    );

    final SearchJobsRequest request = SearchJobsRequest.newBuilder()
        .setDatasetPath(new DatasetPath("testB.dsB1").toNamespaceKey())
        .setLimit(1000)
        .build();
    List<Job> jobs2  = ImmutableList.copyOf(jobsService.searchJobs(request));
    assertEquals(3, jobs2.size()); // we have already run that query for the latest version in the previous call

    final SearchJobsRequest request1 = SearchJobsRequest.newBuilder()
        .setDatasetPath(new DatasetPath("testB.dsB1").toNamespaceKey())
        .setDatasetVersion(dsGet.getDatasetVersion())
        .setLimit(1000)
        .build();
    jobs2 = ImmutableList.copyOf(jobsService.searchJobs(request1));
    assertEquals(dsB1JobsVersion, jobs2.size());

    final SearchJobsRequest request4 = SearchJobsRequest.newBuilder()
        .setDatasetPath(new DatasetPath("testB.dsB1").toNamespaceKey())
        .setDatasetVersion(dsGet.getDatasetVersion())
        .setLimit(1000)
        .setUsername(DEFAULT_USERNAME)
        .build();
    jobs2  = ImmutableList.copyOf(jobsService.searchJobs(request4));
    assertEquals(1, jobs2.size());

    final SearchJobsRequest request3 = SearchJobsRequest.newBuilder()
        .setDatasetPath(new DatasetPath("testB.dsB1").toNamespaceKey())
        .setDatasetVersion(v2)
        .setLimit(1000)
        .setUsername(DEFAULT_USERNAME)
        .build();
    jobs2  = ImmutableList.copyOf(jobsService.searchJobs(request3));
    assertEquals(1, jobs2.size());

    final SearchJobsRequest request2 = SearchJobsRequest.newBuilder()
        .setDatasetPath(new DatasetPath("testB.dsB1").toNamespaceKey())
        .setDatasetVersion(v2)
        .setLimit(1000)
        .setUsername(USERNAME)
        .build();
    jobs2  = ImmutableList.copyOf(jobsService.searchJobs(request2));
    assertEquals(1, jobs2.size());
  }

  @Test
  public void testJobs() throws Exception {
    JobsService jobsService = l(JobsService.class);
    TestSpacesStoragePlugin.setup(getCurrentDremioDaemon());
    // run at least one job
    getPreview(getDataset(new DatasetPath("testB.dsB1"))); // This triggers a job
    doc("getting list of all jobs");
    JobsUI allJobs = expectSuccess(getBuilder(getAPIv2().path("jobs")).buildGet(), JobsUI.class);
    int dsB1Jobs = 0;
    List<JobListItem> foundJobs = new ArrayList<>();
    for (JobListItem job : allJobs.getJobs()) {
      if (Arrays.asList("testB", "dsB1").equals(job.getDatasetPathList())) {
        dsB1Jobs++;
        foundJobs.add(job);
      }
    }
    assertEquals(1, dsB1Jobs);
    doc("getting list of all jobs for dataset testB.dsB1");
    JobsUI jobsUI = expectSuccess(getBuilder(getAPIv2().path("jobs").queryParam("filter", "ds==testB.dsB1")).buildGet(), JobsUI.class);
    assertEquals(
      "from all jobs:\n"
        + JSONUtil.toString(allJobs.getJobs()) + "\n"
        + "we found:\n"
        + JSONUtil.toString(foundJobs) + "\n"
        + "from dataset testB.dsB1 we found:\n"
        + JSONUtil.toString(jobsUI.getJobs()),
      dsB1Jobs, jobsUI.getJobs().size());
    assertEquals(dsB1Jobs, jobsUI.getJobs().size());

    getPreview(getDataset(new DatasetPath("testB.dsB1"))); // this triggers a job
    jobsUI = expectSuccess(getBuilder(getAPIv2().path("jobs").queryParam("filter", "ds==testB.dsB1")).buildGet(), JobsUI.class);
    // getting the data 2x on the same version does not create a new job
    assertEquals(dsB1Jobs, jobsUI.getJobs().size());
    dsB1Jobs++;

    // get version
    DatasetUI dsGet = getDataset(new DatasetPath("testB.dsB1"));

    int dsB1JobsVersion = 0;
    for (JobListItem job : jobsUI.getJobs()) {
      if (Arrays.asList("testB", "dsB1").equals(job.getDatasetPathList()) &&
              dsGet.getDatasetVersion().toString().equals(job.getDatasetVersion())) {
        dsB1JobsVersion++;
      }
    }

    DatasetVersion v2 = DatasetVersion.newVersion();
    doc("Submitting job for dataset testB.dsB1 dataset for version " + v2.getVersion());
    JobsServiceUtil.waitForJobCompletion(
      l(JobsService.class).submitJob(
        JobRequest.newBuilder()
          .setSqlQuery(getQueryFromConfig(dsGet))
          .setQueryType(QueryType.UI_RUN)
          .setDatasetPath(getDatasetPath(dsGet).toNamespaceKey())
          .setDatasetVersion(v2)
          .build(),
        NoOpJobStatusListener.INSTANCE)
    );

    final SearchJobsRequest request3 = SearchJobsRequest.newBuilder()
        .setDatasetPath(new DatasetPath("testB.dsB1").toNamespaceKey())
        .setDatasetVersion(dsGet.getDatasetVersion())
        .setLimit(1000)
        .build();
    List<Job> jobs  = ImmutableList.copyOf(jobsService.searchJobs(request3));
    assertEquals(dsB1JobsVersion, jobs.size());

    final SearchJobsRequest request2 = SearchJobsRequest.newBuilder()
        .setDatasetPath(new DatasetPath("testB.dsB1").toNamespaceKey())
        .setDatasetVersion(new DatasetVersion(v2.getVersion()))
        .setLimit(1000)
        .build();
    jobs  = ImmutableList.copyOf(jobsService.searchJobs(request2));
    assertEquals(v2.getVersion(), 1, jobs.size());

    final SearchJobsRequest request1 = SearchJobsRequest.newBuilder()
        .setDatasetPath(new DatasetPath("testB.dsB1").toNamespaceKey())
        .setDatasetVersion(dsGet.getDatasetVersion())
        .setLimit(1000)
        .build();
    jobs  = ImmutableList.copyOf(jobsService.searchJobs(request1));
    assertEquals(dsB1JobsVersion, jobs.size());

    final SearchJobsRequest request = SearchJobsRequest.newBuilder()
        .setDatasetPath(new DatasetPath("testB.dsB1").toNamespaceKey())
        .setLimit(1000)
        .build();
    jobs  = ImmutableList.copyOf(jobsService.searchJobs(request));
    assertEquals(dsB1Jobs, jobs.size()); // we have already run that query for the latest version in the previous call

    doc("getting list of all jobs for dataset for a dataset using simple equality filter");
    jobsUI = expectSuccess(getBuilder(getAPIv2().path("jobs").queryParam("filter", "ds==testB.dsB1")).buildGet(), JobsUI.class);
    assertEquals(dsB1Jobs, jobsUI.getJobs().size());

    doc("getting list of all jobs for dataset for a dataset version using simple equality AND filter");
    jobsUI = expectSuccess(getBuilder(getAPIv2().path("jobs").queryParam("filter", "ds==testB.dsB1;dsv=="+dsGet.getDatasetVersion())).buildGet(), JobsUI.class);
    assertEquals(dsB1JobsVersion, jobsUI.getJobs().size());

    doc("Get jobs sort by start time ascending");
    jobsUI = expectSuccess(getBuilder(getAPIv2().path("jobs").queryParam("sort", "st").queryParam("order", "ASCENDING")).buildGet(), JobsUI.class);
    long lastStartTime = Long.MIN_VALUE;
    for (JobListItem job: jobsUI.getJobs()) {
      assertTrue(job.getStartTime() >= lastStartTime);
      lastStartTime = job.getStartTime();
    }

    doc("Get jobs sort by start time ascending");
    lastStartTime = Long.MAX_VALUE;
    jobsUI = expectSuccess(getBuilder(getAPIv2().path("jobs").queryParam("sort", "st").queryParam("order", "DESCENDING")).buildGet(), JobsUI.class);
    for (JobListItem job: jobsUI.getJobs()) {
      assertTrue(job.getStartTime() <= lastStartTime);
      lastStartTime = job.getStartTime();
    }

    doc("Get jobs using contains query/search all fields in all jobs");
    jobsUI = expectSuccess(getBuilder(getAPIv2().path("jobs").queryParam("filter", "*=contains=dsB1")).buildGet(), JobsUI.class);
    assertEquals(dsB1Jobs, jobsUI.getJobs().size());

    doc("Get jobs using contains query/search all fields in all jobs");
    jobsUI = expectSuccess(getBuilder(getAPIv2().path("jobs").queryParam("filter", "all=contains=dsB?")).buildGet(), JobsUI.class);
    assertEquals(dsB1Jobs, jobsUI.getJobs().size());

    TestSpacesStoragePlugin.cleanup(getCurrentDremioDaemon());
  }

  private DatasetUI setupIteratorTests(String datasetName) throws Exception{
    TestSpacesStoragePlugin.setup(getCurrentDremioDaemon());
    DatasetUI dataset = getDataset(new DatasetPath(datasetName));

    // run dataset twice. We do a run and a preview since subsequent previews won't actually rerun...
    getPreview(dataset);
    JobsServiceUtil.waitForJobCompletion(
      l(JobsService.class).submitJob(
        JobRequest.newBuilder()
          .setSqlQuery(getQueryFromConfig(dataset))
          .setQueryType(QueryType.UI_RUN)
          .setDatasetPath(getDatasetPath(dataset).toNamespaceKey())
          .setDatasetVersion(dataset.getDatasetVersion())
          .build(),
        NoOpJobStatusListener.INSTANCE)
    );

    return dataset;
  }

  private void validateIteartorPaginationTest(String path, String datasetName, String datasetVersion) {
    WebTarget webTarget = getAPIv2().path(path);
    if (datasetName != null) {
      if (datasetVersion == null) {
        webTarget.queryParam("filter", "ds==" + datasetName);
      } else {
        webTarget.queryParam("filter", format("ds==%s;dsv==%s", datasetName, datasetVersion));
      }
    }
    JobsUI jobs = expectSuccess(getBuilder(webTarget.queryParam("limit", 1)).buildGet(), JobsUI.class);
    assertEquals(1, jobs.getJobs().size());
    assertNotNull(jobs.getNext());
    JobsUI jobs2 = expectSuccess(getBuilder(jobs.getNext()).buildGet(), JobsUI.class);
    assertEquals(1, jobs2.getJobs().size());
    JobsUI jobs3 = expectSuccess(getBuilder(jobs2.getNext()).buildGet(), JobsUI.class);
    assertEquals(0, jobs3.getJobs().size());
    assertNull(jobs3.getNext());
  }

  @Test
  public void checkNoPagination() throws Exception {
    final String datasetName = "testB.dsB1";
    final DatasetUI dataset = setupIteratorTests(datasetName); // submits 12 jobs

    // get jobs list for dataset (no pagination since default length is 100).
    {
      JobsUI jobs = expectSuccess(getBuilder(getAPIv2().path("jobs").queryParam("filter", "ds==" + datasetName)).buildGet(), JobsUI.class);
      assertEquals(2, jobs.getJobs().size());
    }

    {
      JobsUI jobs = expectSuccess(getBuilder(getAPIv2().path("jobs")
        .queryParam("filter", format("ds==%s;dsv==%s", datasetName, dataset.getDatasetVersion()))).buildGet(), JobsUI.class);
      assertEquals(2, jobs.getJobs().size());
    }

    {
      JobsUI jobs = expectSuccess(getBuilder(getAPIv2().path("jobs/")).buildGet(), JobsUI.class);
      assertEquals(2, jobs.getJobs().size());
    }

  }

  @Test
  public void checkPaginationDataset() throws Exception {
    final String datasetName = "testB.dsB1";
    setupIteratorTests(datasetName);
    validateIteartorPaginationTest("jobs", datasetName, null);

  }

  @Test
  public void checkPaginationDatasetAndVersion() throws Exception {
    final String datasetName = "testB.dsB1";
    final DatasetUI dataset = setupIteratorTests(datasetName);
    validateIteartorPaginationTest("jobs", datasetName, dataset.getDatasetVersion().getVersion());
  }

  @Test
  public void checkPaginationAll() throws Exception {
    final String datasetName = "testB.dsB1";
    setupIteratorTests(datasetName);
    validateIteartorPaginationTest("jobs/", null, null);
  }
}
