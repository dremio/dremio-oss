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

import static com.dremio.dac.server.UIOptions.JOBS_UI_CHECK;
import static com.dremio.options.OptionValue.OptionType.SYSTEM;
import static com.google.common.collect.Iterables.isEmpty;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.server.FamilyExpectation;
import com.dremio.dac.server.GenericErrorMessage;
import com.dremio.dac.server.socket.SocketMessage;
import com.dremio.dac.server.socket.TestWebSocket;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.options.OptionValue;
import com.dremio.service.accelerator.BaseTestReflection;
import com.dremio.service.job.JobDetails;
import com.dremio.service.job.JobDetailsRequest;
import com.dremio.service.job.JobSummary;
import com.dremio.service.job.QueryProfileRequest;
import com.dremio.service.job.ReflectionJobDetailsRequest;
import com.dremio.service.job.ReflectionJobProfileRequest;
import com.dremio.service.job.SearchReflectionJobsRequest;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.reflection.DependencyEntry;
import com.dremio.service.reflection.ReflectionMonitor;
import com.dremio.service.reflection.proto.Materialization;
import com.dremio.service.reflection.proto.MeasureType;
import com.dremio.service.reflection.proto.ReflectionDetails;
import com.dremio.service.reflection.proto.ReflectionDimensionField;
import com.dremio.service.reflection.proto.ReflectionEntry;
import com.dremio.service.reflection.proto.ReflectionField;
import com.dremio.service.reflection.proto.ReflectionGoal;
import com.dremio.service.reflection.proto.ReflectionId;
import com.dremio.service.reflection.proto.ReflectionMeasureField;
import com.dremio.service.reflection.proto.ReflectionType;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.PrintWriter;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Test get job info with reflectionId. */
public class TestReflectionJob extends BaseTestReflection {

  private static final String JOB_PATH = "/job/";

  private static final String DATA1 =
      "{ \"key\" : \"A\", \"value\" : 0 }\n"
          + "{ \"key\" : \"A\", \"value\" : 1 }\n"
          + "{ \"key\" : \"B\", \"value\" : 2 }\n"
          + "{ \"key\" : \"B\", \"value\" : 3 }\n"
          + "{ \"key\" : \"C\", \"value\" : 0 }\n"
          + "{ \"key\" : \"A\", \"value\" : 0 }\n"
          + "{ \"key\" : \"A\", \"value\" : 1 }\n"
          + "{ \"key\" : \"B\", \"value\" : 2 }\n"
          + "{ \"key\" : \"B\", \"value\" : 3 }\n"
          + "{ \"key\" : \"C\", \"value\" : 0 }\n";

  private static final AtomicInteger DATA_ID = new AtomicInteger();
  private static final long REFRESH_DELAY_IN_SECONDS = 2;

  private ReflectionMonitor monitor = newReflectionMonitor(100, 10000);
  private WebSocketClient client = new WebSocketClient();
  private TestWebSocket.TestSocket socket;

  private NamespaceKey datasetKey;
  private String dataFile;
  private String datasetId;
  private DependencyEntry.DatasetDependency datasetDependency;
  private DatasetConfig dataset1;

  private java.io.File createDataFile(String data) throws Exception {
    final File file =
        temp.newFile(
            String.format(
                "my_dataset_%d_%d.json", DATA_ID.getAndIncrement(), System.currentTimeMillis()));
    try (PrintWriter writer = new PrintWriter(file)) {
      writer.print(data);
    }
    return file;
  }

  protected ReflectionId createRaw() throws Exception {
    return createRaw("test-raw");
  }

  private ReflectionId createRaw(String name) throws Exception {
    final DatasetConfig dataset = getNamespaceService().getDataset(datasetKey);
    return getReflectionService()
        .create(
            new ReflectionGoal()
                .setType(ReflectionType.RAW)
                .setDatasetId(dataset.getId().getId())
                .setName(name)
                .setDetails(
                    new ReflectionDetails()
                        .setDisplayFieldList(
                            ImmutableList.<ReflectionField>builder()
                                .add(new ReflectionField("key"))
                                .add(new ReflectionField("value"))
                                .build())));
  }

  private ReflectionId createAgg() throws Exception {
    final DatasetConfig dataset = getNamespaceService().getDataset(datasetKey);
    return getReflectionService()
        .create(
            new ReflectionGoal()
                .setType(ReflectionType.AGGREGATION)
                .setDatasetId(dataset.getId().getId())
                .setName("test-agg")
                .setDetails(
                    new ReflectionDetails()
                        .setDimensionFieldList(
                            Collections.singletonList(new ReflectionDimensionField("key")))
                        .setMeasureFieldList(
                            ImmutableList.of(
                                new ReflectionMeasureField("value")
                                    .setMeasureTypeList(ImmutableList.of(MeasureType.COUNT))))));
  }

  private ReflectionEntry validateAndGetReflectionEntry(ReflectionId id) throws Exception {
    final Iterable<ReflectionGoal> refls = getReflectionService().getAllReflections();
    assertTrue(
        "raw reflection not found",
        !isEmpty(refls)
            && Iterables.any(
                refls,
                new Predicate<ReflectionGoal>() {
                  @Override
                  public boolean apply(ReflectionGoal goal) {
                    return goal.getId().equals(id);
                  }
                }));

    final DatasetConfig dataset = getNamespaceService().getDataset(datasetKey);
    assertTrue(
        "no reflection found for dataset",
        !isEmpty(getReflectionService().getReflectionsByDatasetId(dataset.getId().getId())));

    final Materialization m = monitor.waitUntilMaterialized(id);
    final Optional<ReflectionEntry> reflection = getReflectionService().getEntry(id);
    assertTrue("reflection entry not found", reflection.isPresent());
    assertTrue("reflection materialization failed", reflection.get().getNumFailures().equals(0));
    return reflection.get();
  }

  private ReflectionEntry createAggReflection() throws Exception {
    final ReflectionId id = createAgg();
    return validateAndGetReflectionEntry(id);
  }

  private void prepareWebSocket() throws Exception {
    client.start();
    URI socketUri =
        new URI(getAPIv2().path("socket").getUri().toString().replace("http://", "ws://"));
    ClientUpgradeRequest request = new ClientUpgradeRequest();
    request.setSubProtocols(Lists.newArrayList(getAuthHeaderValue()));
    this.socket = new TestWebSocket.TestSocket();
    client.connect(socket, socketUri, request);
    socket.awaitConnection(2);
    assertEquals(
        getAuthHeaderValue(), socket.getSession().getUpgradeResponse().getAcceptedSubProtocol());
    OptionValue option = OptionValue.createBoolean(SYSTEM, JOBS_UI_CHECK.getOptionName(), false);
    getSabotContext().getOptionManager().setOption(option);
  }

  @Before
  public void prepare() throws Exception {
    // ignore the tests if multinode.
    assumeFalse(isMultinode());

    setDeletionGracePeriod(60);
    setManagerRefreshDelay(REFRESH_DELAY_IN_SECONDS);

    setMaterializationCacheSettings(false, 1000);

    dataFile = createDataFile(DATA1).getAbsolutePath();
    datasetKey = new DatasetPath(ImmutableList.of("dfs", dataFile)).toNamespaceKey();
    dataset1 = addJson(new DatasetPath(ImmutableList.of("dfs", dataFile)));
    datasetId = dataset1.getId().getId();
    datasetDependency = dependency(datasetId, datasetKey);
  }

  @After
  public void clearAll() throws Exception {
    setDeletionGracePeriod(1);
    getReflectionService().clearAll();
    Thread.sleep(1);
    monitor.waitUntilNoMaterializationsAvailable();
    client.stop();
  }

  private ReflectionEntry createRawReflection() throws Exception {
    final ReflectionId rawId = createRaw();

    final Iterable<ReflectionGoal> refls = getReflectionService().getAllReflections();
    assertTrue(
        "raw reflection not found",
        !isEmpty(refls)
            && Iterables.any(
                refls,
                new Predicate<ReflectionGoal>() {
                  @Override
                  public boolean apply(ReflectionGoal goal) {
                    return goal.getId().equals(rawId);
                  }
                }));

    final DatasetConfig dataset = getNamespaceService().getDataset(datasetKey);
    assertTrue(
        "no reflection found for dataset",
        !isEmpty(getReflectionService().getReflectionsByDatasetId(dataset.getId().getId())));

    final Materialization m = monitor.waitUntilMaterialized(rawId);
    final Optional<ReflectionEntry> reflection = getReflectionService().getEntry(rawId);
    assertTrue("reflection entry not found", reflection.isPresent());
    assertTrue("reflection materialization failed", reflection.get().getNumFailures().equals(0));
    assertDependsOn(rawId, datasetDependency);
    return reflection.get();
  }

  private JobDetails getJobDetails(JobId jobId, String reflectionId, String userName)
      throws Exception {
    JobDetailsRequest jobDetailsRequest =
        JobDetailsRequest.newBuilder()
            .setJobId(JobsProtoUtil.toBuf(jobId))
            .setFromStore(true)
            .setUserName(userName)
            .build();
    ReflectionJobDetailsRequest.Builder builder =
        ReflectionJobDetailsRequest.newBuilder()
            .setJobDetailsRequest(jobDetailsRequest)
            .setReflectionId(reflectionId);
    return getJobsService().getReflectionJobDetails(builder.build());
  }

  private Iterable<JobSummary> searchJobs(String reflectionId, String userName) {
    SearchReflectionJobsRequest.Builder builder1 =
        SearchReflectionJobsRequest.newBuilder()
            .setLimit(100)
            .setOffset(0)
            .setReflectionId(reflectionId)
            .setUserName(userName);
    return getJobsService().searchReflectionJobs(builder1.build());
  }

  private UserBitShared.QueryProfile getQueryProfile(
      JobId jobId, String reflectionId, String userName) throws Exception {
    QueryProfileRequest queryProfileRequest =
        QueryProfileRequest.newBuilder()
            .setAttempt(0)
            .setJobId(JobsProtoUtil.toBuf(jobId))
            .setUserName(userName)
            .build();

    ReflectionJobProfileRequest reflectionJobProfileRequest =
        ReflectionJobProfileRequest.newBuilder()
            .setQueryProfileRequest(queryProfileRequest)
            .setReflectionId(reflectionId)
            .build();

    return getJobsService().getReflectionJobProfile(reflectionJobProfileRequest);
  }

  @Test
  public void testSearchJob() throws Exception {
    ReflectionEntry reflectionEntry = createRawReflection();
    ReflectionId reflectionId = reflectionEntry.getId();

    Iterable<JobSummary> jobSummaries = searchJobs(reflectionId.getId(), DEFAULT_USERNAME);
    jobSummaries.forEach(
        jobSummary -> assertTrue(jobSummary.getSql().contains(reflectionId.getId())));
    assertEquals(2, Iterables.size(jobSummaries));
  }

  @Test
  public void testJobDetails() throws Exception {
    ReflectionEntry reflectionEntry = createRawReflection();
    ReflectionId reflectionId = reflectionEntry.getId();

    JobDetails jobDetails =
        getJobDetails(reflectionEntry.getRefreshJobId(), reflectionId.getId(), DEFAULT_USERNAME);
    assertEquals(
        jobDetails.getAttempts(0).getInfo().getMaterializationFor().getReflectionId(),
        reflectionId.getId());
  }

  @Test
  public void testJoProfileWithAdminUser() throws Exception {
    ReflectionEntry reflectionEntry = createRawReflection();
    ReflectionId reflectionId = reflectionEntry.getId();

    UserBitShared.QueryProfile queryProfile =
        getQueryProfile(reflectionEntry.getRefreshJobId(), reflectionId.getId(), DEFAULT_USERNAME);
    assertContains(reflectionId.getId(), queryProfile.getQuery());
  }

  @Test
  public void testSocketListenDetails() throws Exception {
    prepareWebSocket();

    ReflectionEntry reflectionEntry = createRawReflection();
    ReflectionId reflectionId = reflectionEntry.getId();

    JobId jobId = reflectionEntry.getRefreshJobId();
    socket.send(new SocketMessage.ListenReflectionJobDetails(jobId, reflectionId.getId()));
    List<SocketMessage.Payload> payloads = socket.awaitCompletion(2, 10);
    SocketMessage.JobDetailsUpdate detailsUpdate =
        (SocketMessage.JobDetailsUpdate) payloads.get(payloads.size() - 1);
    assertEquals(detailsUpdate.getJobId(), jobId);
  }

  @Test
  public void testSocketListenProgress() throws Exception {
    prepareWebSocket();

    ReflectionEntry reflectionEntry = createRawReflection();
    ReflectionId reflectionId = reflectionEntry.getId();

    JobId jobId = reflectionEntry.getRefreshJobId();
    socket.send(new SocketMessage.ListenReflectionJobProgress(jobId, reflectionId.getId()));
    List<SocketMessage.Payload> payloads = socket.awaitCompletion(2, 10);
    SocketMessage.JobProgressUpdate progressUpdate =
        (SocketMessage.JobProgressUpdate) payloads.get(payloads.size() - 1);
    assertTrue(progressUpdate.getUpdate().isComplete());
  }

  @Test
  public void testReflectionJobValidationExceptionWithJobDetails() throws Exception {
    ReflectionEntry reflectionEntry = createRawReflection();
    ReflectionId reflectionId = reflectionEntry.getId();

    ReflectionEntry reflectionEntry1 = createAggReflection();
    ReflectionId reflectionId1 = reflectionEntry1.getId();

    assertThatThrownBy(
            () ->
                getJobDetails(
                    reflectionEntry.getRefreshJobId(), reflectionId1.getId(), DEFAULT_USERNAME))
        .isInstanceOf(ReflectionJobValidationException.class);
  }

  @Test
  public void testReflectionJobValidationExceptionWithProfile() throws Exception {
    ReflectionEntry reflectionEntry = createRawReflection();
    ReflectionId reflectionId = reflectionEntry.getId();

    ReflectionEntry reflectionEntry1 = createAggReflection();
    ReflectionId reflectionId1 = reflectionEntry1.getId();

    assertThatThrownBy(
            () ->
                getQueryProfile(
                    reflectionEntry.getRefreshJobId(), reflectionId1.getId(), DEFAULT_USERNAME))
        .isInstanceOf(ReflectionJobValidationException.class);
  }

  @Test
  public void testDetailsAPI() throws Exception {
    ReflectionEntry reflectionEntry = createRawReflection();
    ReflectionId reflectionId = reflectionEntry.getId();

    expectSuccess(
        getBuilder(
                getAPIv2()
                    .path(JOB_PATH)
                    .path(reflectionEntry.getRefreshJobId().getId())
                    .path("reflection")
                    .path(reflectionId.getId())
                    .path("details"))
            .buildGet());
  }

  @Test
  public void testDetailsAPINeg() throws Exception {
    ReflectionEntry reflectionEntry = createRawReflection();
    ReflectionId reflectionId = reflectionEntry.getId();

    ReflectionEntry reflectionEntry1 = createAggReflection();
    ReflectionId reflectionId1 = reflectionEntry1.getId();

    expectError(
        FamilyExpectation.CLIENT_ERROR,
        getBuilder(
                getAPIv2()
                    .path(JOB_PATH)
                    .path(reflectionEntry.getRefreshJobId().getId())
                    .path("reflection")
                    .path(reflectionId1.getId())
                    .path("details"))
            .buildGet(),
        GenericErrorMessage.class);
  }

  @Test
  public void testSearchJobsAPI() throws Exception {
    ReflectionEntry reflectionEntry = createRawReflection();
    ReflectionId reflectionId = reflectionEntry.getId();

    expectSuccess(
        getBuilder(getAPIv2().path("/jobs/").path("reflection").path(reflectionId.getId()))
            .buildGet());
  }

  @Test
  public void testProfilesAPI() throws Exception {
    ReflectionEntry reflectionEntry = createRawReflection();
    ReflectionId reflectionId = reflectionEntry.getId();

    expectSuccess(
        getBuilder(
                getAPIv2()
                    .path("/profiles/")
                    .path("reflection")
                    .path(reflectionId.getId())
                    .path(reflectionEntry.getRefreshJobId().getId() + ".json")
                    .queryParam("attempt", "0"))
            .buildGet());
  }

  @Test
  public void testProfilesAPINeg() throws Exception {
    ReflectionEntry reflectionEntry = createRawReflection();
    ReflectionId reflectionId = reflectionEntry.getId();

    ReflectionEntry reflectionEntry1 = createAggReflection();
    ReflectionId reflectionId1 = reflectionEntry1.getId();

    expectError(
        FamilyExpectation.CLIENT_ERROR,
        getBuilder(
                getAPIv2()
                    .path("/profiles/")
                    .path("reflection")
                    .path(reflectionId1.getId())
                    .path(reflectionEntry.getRefreshJobId().getId() + ".json")
                    .queryParam("attempt", "0"))
            .buildGet(),
        GenericErrorMessage.class);
  }
}
