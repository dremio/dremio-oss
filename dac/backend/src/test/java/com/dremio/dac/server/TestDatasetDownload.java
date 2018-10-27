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
package com.dremio.dac.server;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;

import javax.ws.rs.core.Response;

import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.explore.model.DatasetUI;
import com.dremio.dac.explore.model.DatasetVersionResourcePath;
import com.dremio.dac.explore.model.DownloadFormat;
import com.dremio.dac.explore.model.InitialDownloadResponse;
import com.dremio.dac.proto.model.dataset.VirtualDatasetUI;
import com.dremio.dac.server.test.SampleDataPopulator;
import com.dremio.dac.service.datasets.DatasetDownloadManager.DownloadDataResponse;
import com.dremio.dac.service.datasets.DatasetVersionMutator;
import com.dremio.service.jobs.Job;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.space.proto.FolderConfig;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;

/**
 * Download dataset tests.
 */
public class TestDatasetDownload extends BaseTestServer {

  private static VirtualDatasetUI dsg1;
  private static final DatasetPath dsg1DatasetPath = new DatasetPath("DG.dsg1");
  private static final DatasetVersionMutator datasetService = newDatasetVersionMutator();

  @BeforeClass
  public static void setup() throws Exception {
    clearAllDataExceptUser();
    populateInitialData();
    dsg1 = datasetService.get(dsg1DatasetPath);
  }

  @Test
  public void testDownloadJsonRest() throws Exception {
    final String downloadPath = new DatasetVersionResourcePath(new DatasetPath("DG.dsg1"), dsg1.getVersion()).toString();

    InitialDownloadResponse initialDownloadResponse = expectSuccess(
      getBuilder(getAPIv2().path(downloadPath).path("download").queryParam("downloadFormat", DownloadFormat.JSON)).buildGet(), InitialDownloadResponse.class);
    // wait for job
    final Job job = l(JobsService.class).getJob(initialDownloadResponse.getJobId());
    job.getData().loadIfNecessary();
    // get job data
    Response response = getBuilder(getAPIv2().path(initialDownloadResponse.getDownloadUrl())).buildGet().invoke();
    validateAllRows(readDataJson((InputStream)response.getEntity()));
  }

  @Test
  public void testDownloadCsvRest() throws Exception {
    final String downloadPath = new DatasetVersionResourcePath(new DatasetPath("DG.dsg1"), dsg1.getVersion()).toString();

    InitialDownloadResponse initialDownloadResponse = expectSuccess(
      getBuilder(getAPIv2().path(downloadPath).path("download").queryParam("downloadFormat", DownloadFormat.CSV)).buildGet(), InitialDownloadResponse.class);
    // wait for job
    final Job job = l(JobsService.class).getJob(initialDownloadResponse.getJobId());
    job.getData().loadIfNecessary();
    // get job data
    Response response = getBuilder(getAPIv2().path(initialDownloadResponse.getDownloadUrl())).buildGet().invoke();
    validateAllRows(readDataCsv((InputStream)response.getEntity()));
  }

  @Test
  public void testDownloadJson() throws Exception {
    Job job = datasetService.prepareDownload(dsg1DatasetPath, dsg1.getVersion(), DownloadFormat.JSON, -1, SampleDataPopulator.DEFAULT_USER_NAME);
    job.getData().loadIfNecessary();
    DownloadDataResponse downloadDataResponse = datasetService.downloadData(job.getJobAttempt().getInfo().getDownloadInfo(), SampleDataPopulator.DEFAULT_USER_NAME);
    validateAllRows(readDataJson(downloadDataResponse.getInput()));
  }

  @Test
  public void testDownloadCsv() throws Exception {
    Job job = datasetService.prepareDownload(dsg1DatasetPath, dsg1.getVersion(), DownloadFormat.CSV, -1, SampleDataPopulator.DEFAULT_USER_NAME);
    job.getData().loadIfNecessary();
    DownloadDataResponse downloadDataResponse = datasetService.downloadData(job.getJobAttempt().getInfo().getDownloadInfo(), SampleDataPopulator.DEFAULT_USER_NAME);
    validateAllRows(readDataCsv(downloadDataResponse.getInput()));
  }

  @Test // DX-6142 & DX-9432
  public void testDownloadWithLimitInDatasetSql() throws Exception {
    final DatasetPath dsPath = new DatasetPath("DG.testDS");
    DatasetUI ds = createDatasetFromSQLAndSave(dsPath,"select * from DG.dsg1 LIMIT 10 --- comment", asList("cp"));

    Job job = datasetService.prepareDownload(dsPath, ds.getDatasetVersion(), DownloadFormat.CSV, 50, SampleDataPopulator.DEFAULT_USER_NAME);
    job.getData().loadIfNecessary();
    DownloadDataResponse downloadDataResponse = datasetService.downloadData(job.getJobAttempt().getInfo().getDownloadInfo(), SampleDataPopulator.DEFAULT_USER_NAME);
    final List<TestData> downloadedData = readDataCsv(downloadDataResponse.getInput());
    assertEquals(10, downloadedData.size());
    for (int i = 0; i < 10; ++i) {
      assertEquals("user" + i, downloadedData.get(i).getUser());
      assertEquals(i%25, downloadedData.get(i).getAge());
      assertEquals("address" + i, downloadedData.get(i).getAddress());
    }
  }

  @Test
  public void testDownloadWithRelativeDatasetPath() throws Exception {
    SpaceConfig spaceConfig = new SpaceConfig();
    spaceConfig.setName("mySpace");
    newNamespaceService().addOrUpdateSpace(new NamespaceKey("mySpace"), spaceConfig);


    FolderConfig folderConfig = new FolderConfig();
    List<String> path = Arrays.asList("mySpace", "folder");
    folderConfig.setFullPathList(path);
    folderConfig.setName("folder");
    newNamespaceService().addOrUpdateFolder(new NamespaceKey(path), folderConfig);

    final DatasetPath dsPath = new DatasetPath("mySpace.folder.testVDS");
    DatasetUI ds = createDatasetFromSQLAndSave(dsPath,"select * from DG.dsg1 LIMIT 10", asList("cp"));

    final DatasetPath dsPath2 = new DatasetPath("mySpace.folder.testVDS2");
    DatasetUI ds2 = createDatasetFromSQLAndSave(dsPath2,"select * from testVDS", path);

    Job job = datasetService.prepareDownload(dsPath2, ds2.getDatasetVersion(), DownloadFormat.CSV, 50, SampleDataPopulator.DEFAULT_USER_NAME);
    job.getData().loadIfNecessary();
    DownloadDataResponse downloadDataResponse = datasetService.downloadData(job.getJobAttempt().getInfo().getDownloadInfo(), SampleDataPopulator.DEFAULT_USER_NAME);
    final List<TestData> downloadedData = readDataCsv(downloadDataResponse.getInput());
    assertEquals(10, downloadedData.size());
    for (int i = 0; i < 10; ++i) {
      assertEquals("user" + i, downloadedData.get(i).getUser());
      assertEquals(i%25, downloadedData.get(i).getAge());
      assertEquals("address" + i, downloadedData.get(i).getAddress());
    }
  }

  private List<TestData> readDataJson(InputStream inputStream) throws IOException {
    BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
    String line;
    List<TestData> testData = Lists.newArrayList();
    ObjectMapper objectMapper = new ObjectMapper();
    while ((line = reader.readLine()) != null) {
      testData.add(objectMapper.readValue(line, TestData.class));
    }
    return testData;
  }

  private List<TestData> readDataCsv(InputStream inputStream) throws IOException {
    BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
    String line;
    List<TestData> testData = Lists.newArrayList();
    int i = 0;
    while ((line = reader.readLine()) != null) {
      if (i++ == 0) {
        continue;
      }
      String []tokens = line.split(",");
      testData.add(new TestData(tokens[0], tokens[2], Integer.parseInt(tokens[1])));
    }
    return testData;
  }

  private void validateAllRows(List<TestData> testData) throws Exception {
    for (int i = 0; i < 100; ++i) {
     assertEquals("user" + i, testData.get(i).getUser());
      assertEquals(i%25, testData.get(i).getAge());
      assertEquals("address" + i, testData.get(i).getAddress());
    }
  }

  /**
   * Test data from dsg1 dataset composed of user, age and address
   */
  private static final class TestData {
    private final String user;
    private final String address;
    private final  int age;

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
