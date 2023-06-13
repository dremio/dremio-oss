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
package com.dremio.exec.catalog.dataplane;

import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.DATAPLANE_PLUGIN_NAME;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.DEFAULT_BRANCH_NAME;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createBranchAtBranchQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createTableAsQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createViewAtSpecifierQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createViewQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueTableName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueViewName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.joinTpcdsTablesQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.selectStarQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.selectStarQueryWithSpecifier;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.tablePathWithFolders;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.tablePathWithSource;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.useBranchQuery;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.dremio.dac.util.JobUtil;
import com.dremio.exec.catalog.TableVersionContext;
import com.dremio.exec.catalog.VersionContext;
import com.dremio.service.job.JobDetails;
import com.dremio.service.job.JobDetailsRequest;
import com.dremio.service.job.RequestType;
import com.dremio.service.job.proto.DataSet;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobProtobuf;
import com.dremio.service.jobs.JobsProtoUtil;
import com.dremio.service.jobs.JobsService;

public class ITDatasetVersionContextTestCases extends ITBaseTestVersioned {
  @Test
  public void testArcticTableVersionContext() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final List<String> tableFullPath = tablePathWithSource(DATAPLANE_PLUGIN_NAME, tablePath);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);

    runQuery(createTableAsQuery(tablePath, 1000));

    final JobId jobId = runQuery(selectStarQuery(tablePath), null);
    final JobDetails jobDetails =
        l(JobsService.class)
            .getJobDetails(
                JobDetailsRequest.newBuilder().setJobId(JobsProtoUtil.toBuf(jobId)).build());
    final JobProtobuf.JobInfo jobInfo = jobDetails.getAttempts(0).getInfo();
    final List<DataSet> queriedDatasets =
        JobUtil.getQueriedDatasets(
            JobsProtoUtil.toStuff(jobInfo),
            RequestType.valueOf(jobInfo.getRequestType().toString()));

    assertThat(queriedDatasets.size()).isEqualTo(1);
    assertThat(queriedDatasets.get(0).getDatasetPath()).isEqualTo(String.join(".", tableFullPath));
    assertThat(queriedDatasets.get(0).getVersionContext())
        .isEqualTo(
            TableVersionContext.of(VersionContext.ofBranch(DEFAULT_BRANCH_NAME)).serialize());
  }

  @Test
  public void testArcticTableOnTestBranchVersionContext() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final List<String> tableFullPath = tablePathWithSource(DATAPLANE_PLUGIN_NAME, tablePath);
    final String testBranch = "test";
    final String sessionId =
        runQueryAndGetSessionId(createBranchAtBranchQuery(testBranch, DEFAULT_BRANCH_NAME));

    runQuery(useBranchQuery(testBranch), sessionId);
    createFolders(tablePath, VersionContext.ofBranch(testBranch), sessionId);
    runQuery(createTableAsQuery(tablePath, 1000), sessionId);

    final JobId jobId =
        runQuery(selectStarQueryWithSpecifier(tablePath, "BRANCH " + testBranch), sessionId);
    final JobDetails jobDetails =
        l(JobsService.class)
            .getJobDetails(
                JobDetailsRequest.newBuilder().setJobId(JobsProtoUtil.toBuf(jobId)).build());
    final JobProtobuf.JobInfo jobInfo = jobDetails.getAttempts(0).getInfo();
    final List<DataSet> queriedDatasets =
        JobUtil.getQueriedDatasets(
            JobsProtoUtil.toStuff(jobInfo),
            RequestType.valueOf(jobInfo.getRequestType().toString()));

    assertThat(queriedDatasets.size()).isEqualTo(1);
    assertThat(queriedDatasets.get(0).getDatasetPath()).isEqualTo(String.join(".", tableFullPath));
    assertThat(queriedDatasets.get(0).getVersionContext())
        .isEqualTo(TableVersionContext.of(VersionContext.ofBranch(testBranch)).serialize());
  }

  @Test
  public void testArcticViewVersionContext() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final List<String> tableFullPath = tablePathWithSource(DATAPLANE_PLUGIN_NAME, tablePath);
    final String viewName = generateUniqueViewName();
    final List<String> viewPath = tablePathWithFolders(viewName);
    final List<String> viewFullPath = tablePathWithSource(DATAPLANE_PLUGIN_NAME, viewPath);

    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    createFolders(viewPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);

    runQuery(createTableAsQuery(tablePath, 1000));
    runQuery(createViewQuery(viewPath, tablePath));

    final JobId jobId = runQuery(selectStarQuery(viewPath), null);
    final JobDetails jobDetails =
        l(JobsService.class)
            .getJobDetails(
                JobDetailsRequest.newBuilder().setJobId(JobsProtoUtil.toBuf(jobId)).build());
    final JobProtobuf.JobInfo jobInfo = jobDetails.getAttempts(0).getInfo();
    final List<DataSet> queriedDatasets =
        JobUtil.getQueriedDatasets(
            JobsProtoUtil.toStuff(jobInfo),
            RequestType.valueOf(jobInfo.getRequestType().toString()));

    assertThat(queriedDatasets.size()).isEqualTo(1);
    assertThat(queriedDatasets.get(0).getDatasetPath()).isEqualTo(String.join(".", viewFullPath));
    assertThat(queriedDatasets.get(0).getVersionContext())
        .isEqualTo(
            TableVersionContext.of(VersionContext.ofBranch(DEFAULT_BRANCH_NAME)).serialize());
  }

  @Test
  public void testArcticViewOnDifferentBranchVersionContext() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final String devBranch = "dev";
    final String sessionId =
        runQueryAndGetSessionId(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));

    runQuery(useBranchQuery(devBranch), sessionId);
    createFolders(tablePath, VersionContext.ofBranch(devBranch), sessionId);
    runQuery(createTableAsQuery(tablePath, 1000), sessionId);

    final String viewName = generateUniqueViewName();
    final List<String> viewPath = tablePathWithFolders(viewName);
    final List<String> viewFullPath = tablePathWithSource(DATAPLANE_PLUGIN_NAME, viewPath);

    runQuery(useBranchQuery(DEFAULT_BRANCH_NAME), sessionId);
    createFolders(viewPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), sessionId);

    runQuery(createViewAtSpecifierQuery(viewPath, tablePath, "BRANCH " + devBranch), sessionId);

    final JobId jobId =
        runQuery(
            selectStarQueryWithSpecifier(viewPath, "BRANCH " + DEFAULT_BRANCH_NAME), sessionId);
    final JobDetails jobDetails =
        l(JobsService.class)
            .getJobDetails(
                JobDetailsRequest.newBuilder().setJobId(JobsProtoUtil.toBuf(jobId)).build());
    final JobProtobuf.JobInfo jobInfo = jobDetails.getAttempts(0).getInfo();
    final List<DataSet> queriedDatasets =
        JobUtil.getQueriedDatasets(
            JobsProtoUtil.toStuff(jobInfo),
            RequestType.valueOf(jobInfo.getRequestType().toString()));

    assertThat(queriedDatasets.size()).isEqualTo(1);
    assertThat(queriedDatasets.get(0).getDatasetPath()).isEqualTo(String.join(".", viewFullPath));
    assertThat(queriedDatasets.get(0).getVersionContext())
        .isEqualTo(
            TableVersionContext.of(VersionContext.ofBranch(DEFAULT_BRANCH_NAME)).serialize());
  }

  @Test
  public void testNonArcticTableVersionContext() throws Exception {
    final JobId jobId = runQuery(joinTpcdsTablesQuery());
    final JobDetails jobDetails =
        l(JobsService.class)
            .getJobDetails(
                JobDetailsRequest.newBuilder().setJobId(JobsProtoUtil.toBuf(jobId)).build());
    final JobProtobuf.JobInfo jobInfo = jobDetails.getAttempts(0).getInfo();
    final List<DataSet> queriedDatasets =
        JobUtil.getQueriedDatasets(
            JobsProtoUtil.toStuff(jobInfo),
            RequestType.valueOf(jobInfo.getRequestType().toString()));

    assertThat(queriedDatasets.size()).isEqualTo(2);
    assertThat(queriedDatasets.get(0).getVersionContext()).isNull();
    assertThat(queriedDatasets.get(1).getVersionContext()).isNull();
  }

}
