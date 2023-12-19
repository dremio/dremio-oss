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
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createTableAsQueryWithAt;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createViewAtSpecifierQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createViewQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueBranchName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueTableName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueViewName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.joinTpcdsTablesQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.selectStarQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.selectStarQueryWithSpecifier;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.tablePathWithFolders;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.tablePathWithSource;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.dremio.catalog.model.VersionContext;
import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.dac.util.JobUtil;
import com.dremio.service.job.JobDetails;
import com.dremio.service.job.JobDetailsRequest;
import com.dremio.service.job.RequestType;
import com.dremio.service.job.proto.DataSet;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobProtobuf;
import com.dremio.service.jobs.JobsProtoUtil;
import com.dremio.service.jobs.JobsService;

public class ITDatasetVersionContext extends ITBaseTestVersioned {
  @Test
  public void testVersionedTableVersionContext() throws Exception {
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
  public void testVersionedTableOnTestBranchVersionContext() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final List<String> tableFullPath = tablePathWithSource(DATAPLANE_PLUGIN_NAME, tablePath);
    final String testBranch = generateUniqueBranchName();

    runQuery(createBranchAtBranchQuery(testBranch, DEFAULT_BRANCH_NAME));
    createFolders(tablePath, VersionContext.ofBranch(testBranch), null);
    runQuery(createTableAsQueryWithAt(tablePath, 1000, testBranch));

    final JobId jobId =
        runQuery(selectStarQueryWithSpecifier(tablePath, "BRANCH " + testBranch));
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
  public void testVersionedViewVersionContext() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
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
  public void testVersionedViewOnDifferentBranchVersionContext() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final String devBranch = generateUniqueBranchName();

    runQuery(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));
    createFolders(tablePath, VersionContext.ofBranch(devBranch), null);
    runQuery(createTableAsQueryWithAt(tablePath, 1000, devBranch));

    final String viewName = generateUniqueViewName();
    final List<String> viewPath = tablePathWithFolders(viewName);
    final List<String> viewFullPath = tablePathWithSource(DATAPLANE_PLUGIN_NAME, viewPath);

    createFolders(viewPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);

    runQuery(createViewAtSpecifierQuery(viewPath, tablePath, "BRANCH " + devBranch), null);

    final JobId jobId =
        runQuery(
            selectStarQueryWithSpecifier(viewPath, "BRANCH " + DEFAULT_BRANCH_NAME), null);
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
  public void testNonVersionedTableVersionContext() throws Exception {
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
