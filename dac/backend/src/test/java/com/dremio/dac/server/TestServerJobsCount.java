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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.List;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.model.sources.SourceUI;
import com.dremio.dac.server.test.SampleDataPopulator;
import com.dremio.file.File;
import com.dremio.service.job.JobCountsRequest;
import com.dremio.service.job.VersionedDatasetPath;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.JobRequest;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.SqlQuery;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

/**
 * Test for job count for datasets.
 */
public class TestServerJobsCount extends BaseTestServer {

  private static final JobsService jobsService = l(JobsService.class);
  private static final DatasetPath dsg1 = new DatasetPath("DG.dsg1");
  private static final DatasetPath dsg2 = new DatasetPath("DG.dsg2");
  private static final DatasetPath dsg3 = new DatasetPath("DG.dsg3");
  private static final DatasetPath dsg4 = new DatasetPath("DG.dsg4");
  private static final DatasetPath dsg5 = new DatasetPath("DG.dsg5");
  private static final DatasetPath dsg6 = new DatasetPath("DG.dsg6");
  private static final DatasetPath dsg7 = new DatasetPath("DG.dsg7");
  private static final DatasetPath dsg8 = new DatasetPath("DG.dsg8");
  private static final DatasetPath dsg9 = new DatasetPath("DG.dsg9");
  private static final DatasetPath dsg10 = new DatasetPath("DG.dsg10");
  private static final DatasetPath unknown = new DatasetPath("UNKNOWN");
  private static final DatasetPath sample1 = new DatasetPath("LocalFS1.\"dac-sample1.json\"");
  private static final DatasetPath sample2 = new DatasetPath("LocalFS2.\"dac-sample2.json\"");

  private static final List<DatasetPath> allDatasets = ImmutableList.of(dsg1, dsg2, dsg3, dsg4, dsg5, dsg6, dsg7, dsg8, dsg9, dsg10, unknown, sample1, sample2);
  private static final Map<DatasetPath, Integer> jobsCount = Maps.newHashMap();

  @BeforeClass
  public static void setup() throws Exception {
    clearAllDataExceptUser();
    populateInitialData();
    for (DatasetPath datasetPath :  allDatasets) {
      jobsCount.put(datasetPath,
          jobsService.getJobCounts(JobCountsRequest.newBuilder()
              .addDatasets(VersionedDatasetPath.newBuilder()
                  .addAllPath(datasetPath.toNamespaceKey().getPathComponents()))
              .build())
              .getCountList()
              .get(0));
    }
  }

  private int inc(DatasetPath datasetPath) {
    int count = jobsCount.get(datasetPath) + 1;
    jobsCount.put(datasetPath, count);
    return count;
  }

  @Test
  public void testDsg1External() {
    submitJobAndWaitUntilCompletion(
      JobRequest.newBuilder()
        .setSqlQuery(new SqlQuery("select * from DG.dsg1", SampleDataPopulator.DEFAULT_USER_NAME))
        .setQueryType(QueryType.UI_RUN)
        .build()
    );

    assertEquals(inc(dsg1), (int) jobsService.getJobCounts(JobCountsRequest.newBuilder()
        .addDatasets(VersionedDatasetPath.newBuilder()
            .addAllPath(dsg1.toNamespaceKey().getPathComponents()))
        .build())
        .getCountList()
        .get(0));

    assertEquals(inc(sample1), (int) jobsService.getJobCounts(JobCountsRequest.newBuilder()
        .addDatasets(VersionedDatasetPath.newBuilder()
            .addAllPath(sample1.toNamespaceKey().getPathComponents()))
        .build())
        .getCountList()
        .get(0));
  }

  @Test
  public void testDsg2UI() {
    submitJobAndWaitUntilCompletion(
      JobRequest.newBuilder()
        .setSqlQuery(new SqlQuery("select * from DG.dsg2", SampleDataPopulator.DEFAULT_USER_NAME))
        .setQueryType(QueryType.UI_RUN)
        .build()
    );

    assertEquals(inc(dsg2), (int) jobsService.getJobCounts(JobCountsRequest.newBuilder()
        .addDatasets(VersionedDatasetPath.newBuilder()
            .addAllPath(dsg2.toNamespaceKey().getPathComponents()))
        .build())
        .getCountList()
        .get(0));

    assertEquals(inc(sample2), (int) jobsService.getJobCounts(JobCountsRequest.newBuilder()
        .addDatasets(VersionedDatasetPath.newBuilder()
            .addAllPath(sample2.toNamespaceKey().getPathComponents()))
        .build())
        .getCountList()
        .get(0));
  }

  @Test
  public void testDsg2Internal() {
    submitJobAndWaitUntilCompletion(
      JobRequest.newBuilder()
        .setSqlQuery(new SqlQuery("select * from DG.dsg2", SampleDataPopulator.DEFAULT_USER_NAME))
        .setQueryType(QueryType.UI_INTERNAL_PREVIEW)
        .build()
    );

    // internal jobs don't get counted
    assertEquals((int) jobsCount.get(dsg2),
        (int) jobsService.getJobCounts(JobCountsRequest.newBuilder()
            .addDatasets(VersionedDatasetPath.newBuilder()
                .addAllPath(dsg2.toNamespaceKey().getPathComponents()))
            .build())
            .getCountList()
            .get(0));

    assertEquals((int) jobsCount.get(sample2),
        (int) jobsService.getJobCounts(JobCountsRequest.newBuilder()
            .addDatasets(VersionedDatasetPath.newBuilder()
                .addAllPath(sample2.toNamespaceKey().getPathComponents()))
            .build())
            .getCountList()
            .get(0));
  }

  @Test
  public void testDsg1Unknown() {
    submitJobAndWaitUntilCompletion(
      JobRequest.newBuilder()
        .setSqlQuery(new SqlQuery("select * from DG.dsg1", SampleDataPopulator.DEFAULT_USER_NAME))
        .setQueryType(QueryType.UNKNOWN)
        .build()
    );
    // unkown jobs are not counted
    assertEquals((int) jobsCount.get(dsg1),
        (int) jobsService.getJobCounts(JobCountsRequest.newBuilder()
            .addDatasets(VersionedDatasetPath.newBuilder()
                .addAllPath(dsg1.toNamespaceKey().getPathComponents()))
            .build())
            .getCountList()
            .get(0));

    assertEquals((int) jobsCount.get(sample1),
        (int) jobsService.getJobCounts(JobCountsRequest.newBuilder()
            .addDatasets(VersionedDatasetPath.newBuilder()
                .addAllPath(sample1.toNamespaceKey().getPathComponents()))
            .build())
            .getCountList()
            .get(0));
  }


  @Test
  public void testDsg10External() {
    submitJobAndWaitUntilCompletion(
      JobRequest.newBuilder()
        .setSqlQuery(new SqlQuery("select * from DG.dsg10", SampleDataPopulator.DEFAULT_USER_NAME))
        .setQueryType(QueryType.UI_RUN)
        .build()
    );

    assertEquals(inc(dsg10), (int) jobsService.getJobCounts(JobCountsRequest.newBuilder()
        .addDatasets(VersionedDatasetPath.newBuilder()
            .addAllPath(dsg10.toNamespaceKey().getPathComponents()))
        .build())
        .getCountList()
        .get(0));

    assertEquals(inc(dsg9), (int) jobsService.getJobCounts(JobCountsRequest.newBuilder()
        .addDatasets(VersionedDatasetPath.newBuilder()
            .addAllPath(dsg9.toNamespaceKey().getPathComponents()))
        .build())
        .getCountList()
        .get(0));

    assertEquals(inc(dsg8), (int) jobsService.getJobCounts(JobCountsRequest.newBuilder()
        .addDatasets(VersionedDatasetPath.newBuilder()
        .addAllPath(dsg8.toNamespaceKey().getPathComponents()))
        .build())
        .getCountList()
        .get(0));

    assertEquals(inc(dsg3), (int) jobsService.getJobCounts(JobCountsRequest.newBuilder()
        .addDatasets(VersionedDatasetPath.newBuilder()
            .addAllPath(dsg3.toNamespaceKey().getPathComponents()))
        .build())
        .getCountList()
        .get(0));

    assertEquals(inc(dsg2), (int) jobsService.getJobCounts(JobCountsRequest.newBuilder()
        .addDatasets(VersionedDatasetPath.newBuilder()
            .addAllPath(dsg2.toNamespaceKey().getPathComponents()))
        .build())
        .getCountList()
        .get(0));

    assertEquals(inc(dsg4), (int) jobsService.getJobCounts(JobCountsRequest.newBuilder()
        .addDatasets(VersionedDatasetPath.newBuilder()
            .addAllPath(dsg4.toNamespaceKey().getPathComponents()))
        .build())
        .getCountList()
        .get(0));

    assertEquals(inc(dsg1), (int) jobsService.getJobCounts(JobCountsRequest.newBuilder()
        .addDatasets(VersionedDatasetPath.newBuilder()
            .addAllPath(dsg1.toNamespaceKey().getPathComponents()))
        .build())
        .getCountList()
        .get(0));

    assertEquals(inc(sample1), (int) jobsService.getJobCounts(JobCountsRequest.newBuilder()
        .addDatasets(VersionedDatasetPath.newBuilder()
            .addAllPath(sample1.toNamespaceKey().getPathComponents()))
        .build())
        .getCountList()
        .get(0));

    assertEquals(inc(sample2), (int) jobsService.getJobCounts(JobCountsRequest.newBuilder()
        .addDatasets(VersionedDatasetPath.newBuilder()
            .addAllPath(sample2.toNamespaceKey().getPathComponents()))
        .build())
        .getCountList()
        .get(0));

    assertEquals((int) jobsCount.get(dsg5), (int) jobsService.getJobCounts(JobCountsRequest.newBuilder()
        .addDatasets(VersionedDatasetPath.newBuilder()
            .addAllPath(dsg5.toNamespaceKey().getPathComponents()))
        .build())
        .getCountList()
        .get(0));

    assertEquals((int) jobsCount.get(dsg6), (int) jobsService.getJobCounts(JobCountsRequest.newBuilder()
        .addDatasets(VersionedDatasetPath.newBuilder()
            .addAllPath(dsg6.toNamespaceKey().getPathComponents()))
        .build())
        .getCountList()
        .get(0));

    assertEquals((int) jobsCount.get(dsg7), (int) jobsService.getJobCounts(JobCountsRequest.newBuilder()
        .addDatasets((VersionedDatasetPath.newBuilder()
        .addAllPath(dsg7.toNamespaceKey().getPathComponents())))
        .build())
        .getCountList()
        .get(0));
  }

  // tests job count for physical datasets
  @Test
  public void testCountsLocalFS1Rest() {
    doc("list source LocalFS1");
    SourceUI fs1 = expectSuccess(getBuilder(getAPIv2().path("/source/LocalFS1")).buildGet(), SourceUI.class);
    assertNotNull(fs1.getContents());
    assertEquals(0, fs1.getContents().getDatasets().size());
    assertEquals(0, fs1.getContents().getPhysicalDatasets().size());
    assertEquals(1, fs1.getContents().getFolders().size());
    assertEquals(4, fs1.getContents().getFiles().size());

    for (File file: fs1.getContents().getFiles()) {
      Integer cnt = jobsCount.get(new DatasetPath(file.getFilePath().toPathList()));
      assertEquals(cnt == null ? 0 : cnt, (int) file.getJobCount());
    }
  }

  @Test
  public void testCountsLocalFS2Rest() {
    doc("list source LocalFS2");
    SourceUI fs2 = expectSuccess(getBuilder(getAPIv2().path("/source/LocalFS2")).buildGet(), SourceUI.class);
    assertNotNull(fs2.getContents());
    assertEquals(0, fs2.getContents().getDatasets().size());
    assertEquals(0, fs2.getContents().getPhysicalDatasets().size());
    assertEquals(1, fs2.getContents().getFolders().size());
    assertEquals(4, fs2.getContents().getFiles().size());

    for (File file: fs2.getContents().getFiles()) {
      Integer cnt = jobsCount.get(new DatasetPath(file.getFilePath().toPathList()));
      assertEquals(cnt == null ? 0 : cnt, (int) file.getJobCount());
    }
  }

}
