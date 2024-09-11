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

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.explore.model.DatasetUI;
import com.dremio.dac.explore.model.DownloadFormat;
import com.dremio.dac.explore.model.InitialPreviewResponse;
import com.dremio.dac.proto.model.dataset.VirtualDatasetUI;
import com.dremio.dac.service.datasets.DatasetVersionMutator;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.space.proto.FolderConfig;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.Response;
import org.junit.BeforeClass;
import org.junit.Test;

/** Download dataset tests. */
public class TestDatasetDownload extends BaseTestServer {

  private static VirtualDatasetUI dsg1;
  private static final DatasetPath dsg1DatasetPath = new DatasetPath("DG.dsg1");
  private static final DatasetVersionMutator datasetService = getDatasetVersionMutator();

  @BeforeClass
  public static void setup() throws Exception {
    clearAllDataExceptUser();
    populateInitialData();
    dsg1 = datasetService.get(dsg1DatasetPath);
  }

  @Test
  public void testDownloadJsonRest() throws Exception {
    InitialPreviewResponse initialPreviewResponse =
        createDatasetFromSQL(
            String.format("select * from %s", dsg1DatasetPath), Collections.emptyList());

    Response response = download(initialPreviewResponse.getJobId(), DownloadFormat.JSON);

    validateAllRows(readDataJson(response));
  }

  @Test
  public void testDownloadCsvRest() throws Exception {
    InitialPreviewResponse initialPreviewResponse =
        createDatasetFromSQL(
            String.format("select * from %s", dsg1DatasetPath), Collections.emptyList());

    Response response = download(initialPreviewResponse.getJobId(), DownloadFormat.CSV);

    validateAllRows(readDataCsv(response));
  }

  @Test // DX-6142 & DX-9432
  public void testDownloadWithLimitInDatasetSql() throws Exception {
    final DatasetPath dsPath = new DatasetPath("DG.testDS");
    InitialPreviewResponse initialPreviewResponse =
        createDatasetFromSQL("select * from DG.dsg1 LIMIT 10 --- comment", asList("cp"));

    Response response = download(initialPreviewResponse.getJobId(), DownloadFormat.CSV);

    final List<TestData> downloadedData = readDataCsv(response);
    assertEquals(10, downloadedData.size());
    for (int i = 0; i < 10; ++i) {
      assertEquals("user" + i, downloadedData.get(i).getUser());
      assertEquals(i % 25, downloadedData.get(i).getAge());
      assertEquals("address" + i, downloadedData.get(i).getAddress());
    }
  }

  // I do not know how to catch a job in a new flow
  //  @Test
  //  public void cancelledDownloadJob() throws Exception {
  //    final DatasetPath dsPath = new DatasetPath("DG.testDS2");
  //    InitialPreviewResponse initialPreviewResponse = createDatasetFromSQL("select * from DG.dsg1
  // --- comment", asList("cp"));
  //
  //    l(JobsService.class).getJob(GetJobRequest.newBuilder()
  //      .setJobId(initialPreviewResponse.getJobId())
  //      .setUserName(SampleDataPopulator.DEFAULT_USER_NAME)
  //      .build()).getData().loadIfNecessary(); // wait for job completion
  //
  //    getDownloadInvocation(initialPreviewResponse.getJobId(), DownloadFormat.CSV).submit();
  //    final JobSummary job =
  // TestJobResultsDownload.getLastDownloadJob(SampleDataPopulator.DEFAULT_USER_NAME,
  // l(JobsService.class));
  //
  //    l(JobsService.class).cancel(SampleDataPopulator.DEFAULT_USER_NAME, job.getJobId(), "because
  // I can");
  //
  //
  //    GetJobRequest request = GetJobRequest.newBuilder()
  //      .setJobId(job.getJobId())
  //      .build();
  //    if (l(JobsService.class).getJob(request).getJobAttempt().getState() == JobState.CANCELED) {
  //      try {
  //        download(job.getJobId(), DownloadFormat.CSV);
  //        fail();
  //      } catch (Exception e) {
  //        assertTrue(e instanceof FileNotFoundException);
  //      }
  //    }
  //  }

  @Test
  public void testDownloadWithRelativeDatasetPath() throws Exception {
    SpaceConfig spaceConfig = new SpaceConfig();
    spaceConfig.setName("mySpace");
    getNamespaceService().addOrUpdateSpace(new NamespaceKey("mySpace"), spaceConfig);

    FolderConfig folderConfig = new FolderConfig();
    List<String> path = Arrays.asList("mySpace", "folder");
    folderConfig.setFullPathList(path);
    folderConfig.setName("folder");
    getNamespaceService().addOrUpdateFolder(new NamespaceKey(path), folderConfig);

    final DatasetPath dsPath = new DatasetPath("mySpace.folder.testVDS");

    DatasetUI ds =
        createDatasetFromSQLAndSave(dsPath, "select * from DG.dsg1 LIMIT 10", asList("cp"));
    InitialPreviewResponse initialPreviewResponse =
        createDatasetFromSQL("select * from testVDS", path);

    Response response = download(initialPreviewResponse.getJobId(), DownloadFormat.CSV);

    final List<TestData> downloadedData = readDataCsv(response);
    assertEquals(10, downloadedData.size());
    for (int i = 0; i < 10; ++i) {
      assertEquals("user" + i, downloadedData.get(i).getUser());
      assertEquals(i % 25, downloadedData.get(i).getAge());
      assertEquals("address" + i, downloadedData.get(i).getAddress());
    }
  }

  private Response download(JobId jobId, DownloadFormat downloadFormat) {
    return getDownloadInvocation(jobId, downloadFormat).invoke();
  }

  private Invocation getDownloadInvocation(JobId jobId, DownloadFormat downloadFormat) {
    return getBuilder(
            getAPIv2()
                .path("job")
                .path(jobId.getId())
                .path("download")
                .queryParam("downloadFormat", downloadFormat))
        .buildGet();
  }

  private List<TestData> readDataJson(Response response) throws IOException {
    BufferedReader reader =
        new BufferedReader(new InputStreamReader((InputStream) response.getEntity()));
    String line;
    List<TestData> testData = Lists.newArrayList();
    ObjectMapper objectMapper = new ObjectMapper();
    while ((line = reader.readLine()) != null) {
      testData.add(objectMapper.readValue(line, TestData.class));
    }
    return testData;
  }

  private List<TestData> readDataCsv(Response response) throws IOException {
    BufferedReader reader =
        new BufferedReader(new InputStreamReader((InputStream) response.getEntity()));
    String line;
    List<TestData> testData = Lists.newArrayList();
    int i = 0;
    while ((line = reader.readLine()) != null) {
      if (i++ == 0) {
        continue;
      }
      String[] tokens = line.split(",");
      testData.add(new TestData(tokens[0], tokens[2], Integer.parseInt(tokens[1])));
    }
    return testData;
  }

  private void validateAllRows(List<TestData> testData) throws Exception {
    for (int i = 0; i < 100; ++i) {
      assertEquals("user" + i, testData.get(i).getUser());
      assertEquals(i % 25, testData.get(i).getAge());
      assertEquals("address" + i, testData.get(i).getAddress());
    }
  }

  /** Test data from dsg1 dataset composed of user, age and address */
  private static final class TestData {
    private final String user;
    private final String address;
    private final int age;

    @JsonCreator
    public TestData(
        @JsonProperty("user") String user,
        @JsonProperty("address") String address,
        @JsonProperty("age") int age) {
      this.user = user;
      this.address = address;
      this.age = age;
    }

    public String getUser() {
      return user;
    }

    public String getAddress() {
      return address;
    }

    public int getAge() {
      return age;
    }
  }
}
