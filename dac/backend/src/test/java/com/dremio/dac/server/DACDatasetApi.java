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

import static com.dremio.common.utils.PathUtils.getPathJoiner;
import static com.dremio.dac.server.FamilyExpectation.CLIENT_ERROR;
import static com.dremio.service.namespace.dataset.DatasetVersion.newVersion;
import static javax.ws.rs.client.Entity.entity;

import com.dremio.dac.explore.model.CreateFromSQL;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.explore.model.DatasetSearchUIs;
import com.dremio.dac.explore.model.DatasetUI;
import com.dremio.dac.explore.model.DatasetUIWithHistory;
import com.dremio.dac.explore.model.DatasetVersionResourcePath;
import com.dremio.dac.explore.model.InitialDataPreviewResponse;
import com.dremio.dac.explore.model.InitialPendingTransformResponse;
import com.dremio.dac.explore.model.InitialPreviewResponse;
import com.dremio.dac.explore.model.InitialRunResponse;
import com.dremio.dac.explore.model.TransformBase;
import com.dremio.dac.explore.model.VersionContextReq;
import com.dremio.dac.model.job.JobDataFragment;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.Response.Status;

public class DACDatasetApi extends BaseClientUtils {
  private final DACHttpClient httpClient;

  public DACDatasetApi(DACHttpClient httpClient) {
    this.httpClient = httpClient;
  }

  public InitialPreviewResponse createDatasetFromParent(String parentDataset) {
    final Invocation invocation =
        httpClient
            .request(
                c ->
                    c.getAPIv2()
                        .path("datasets/new_untitled")
                        .queryParam("newVersion", newVersion())
                        .queryParam("parentDataset", parentDataset))
            .buildPost(null);

    return expectSuccess(invocation, InitialPreviewResponse.class);
  }

  public DatasetUI createDatasetFromParentAndSave(
      DatasetPath newDatasetPath, String parentDataset) {
    InitialPreviewResponse response = createDatasetFromParent(parentDataset);

    return saveAs(response.getDataset(), newDatasetPath).getDataset();
  }

  public DatasetUI createDatasetFromParentAndSave(String newDataSetName, String parentDataset)
      throws Exception {
    // TODO: why save everything below spacefoo.folderbar.folderbaz? remove this hack
    BaseTestServer.setSpace();

    InitialPreviewResponse response = createDatasetFromParent(parentDataset);

    return saveAs(
            response.getDataset(),
            new DatasetPath("spacefoo.folderbar.folderbaz." + newDataSetName))
        .getDataset();
  }

  public Invocation getDatasetInvocation(DatasetPath datasetPath) {
    return httpClient
        .request(c -> c.getAPIv2().path("dataset/" + datasetPath.toString()))
        .buildGet();
  }

  public DatasetUI getDataset(DatasetPath datasetPath) {
    return expectSuccess(getDatasetInvocation(datasetPath), DatasetUI.class);
  }

  public DatasetUI getVersionedDataset(DatasetVersionResourcePath datasetVersionPath) {
    final Invocation invocation =
        httpClient
            .request(c -> c.getAPIv2().path(datasetVersionPath.toString()).path("preview"))
            .buildGet();

    return expectSuccess(invocation, InitialPreviewResponse.class).getDataset();
  }

  public InitialPreviewResponse getPreview(DatasetUI datasetUI) {
    final Invocation invocation =
        httpClient
            .request(
                c ->
                    c.getAPIv2()
                        .path(getPathJoiner().join(versionedResourcePath(datasetUI), "preview")))
            .buildGet();

    return expectSuccess(invocation, InitialPreviewResponse.class);
  }

  /** Get the preview response for given dataset path. Dataset can be physical or virutal. */
  public InitialDataPreviewResponse getPreview(DatasetPath datasetPath) {
    final Invocation invocation =
        httpClient
            .request(c -> c.getAPIv2().path("dataset/" + datasetPath.toPathString() + "/preview"))
            .buildGet();

    return expectSuccess(invocation, InitialDataPreviewResponse.class);
  }

  public InitialPreviewResponse transform(DatasetUI datasetUI, TransformBase transformBase) {
    final Invocation invocation =
        httpClient
            .request(
                c ->
                    c.getAPIv2()
                        .path(
                            getPathJoiner()
                                .join(versionedResourcePath(datasetUI), "transformAndPreview"))
                        .queryParam("newVersion", newVersion()))
            .buildPost(entity(transformBase, JSON));

    return expectSuccess(invocation, InitialPreviewResponse.class);
  }

  public DatasetSearchUIs search(String filter) {
    final Invocation invocation =
        httpClient
            .request(c -> c.getAPIv2().path("datasets/search").queryParam("filter", filter))
            .buildGet();

    return expectSuccess(invocation, DatasetSearchUIs.class);
  }

  public Invocation reapplyInvocation(DatasetVersionResourcePath versionResourcePath) {
    return httpClient
        .request(c -> c.getAPIv2().path(versionResourcePath.toString() + "/editOriginalSql"))
        .buildPost(null);
  }

  public InitialPreviewResponse reapply(DatasetVersionResourcePath versionResourcePath) {
    return expectSuccess(reapplyInvocation(versionResourcePath), InitialPreviewResponse.class);
  }

  public DatasetUIWithHistory save(DatasetUI datasetUI, String saveVersion) {
    final Invocation invocation =
        httpClient
            .request(
                c ->
                    c.getAPIv2()
                        .path(versionedResourcePath(datasetUI) + "/save")
                        .queryParam("savedTag", saveVersion))
            .buildPost(entity("", JSON));

    return expectSuccess(invocation, DatasetUIWithHistory.class);
  }

  public DatasetUI rename(DatasetPath datasetPath, String newName) {
    final Invocation invocation =
        httpClient
            .request(
                c ->
                    c.getAPIv2()
                        .path("dataset/" + datasetPath.toString() + "/rename")
                        .queryParam("renameTo", newName))
            .buildPost(null);

    return expectSuccess(invocation, DatasetUI.class);
  }

  public DatasetUI move(DatasetPath currenPath, DatasetPath newPath) {
    final Invocation invocation =
        httpClient
            .request(
                c ->
                    c.getAPIv2()
                        .path("dataset/" + currenPath.toString() + "/moveTo/" + newPath.toString()))
            .buildPost(null);

    return expectSuccess(invocation, DatasetUI.class);
  }

  public DatasetUIWithHistory saveAs(DatasetUI datasetUI, DatasetPath newName) {
    final Invocation invocation =
        httpClient
            .request(
                c ->
                    c.getAPIv2()
                        .path(versionedResourcePath(datasetUI) + "/save")
                        .queryParam("as", newName))
            .buildPost(entity("", JSON));

    return expectSuccess(invocation, DatasetUIWithHistory.class);
  }

  public <T extends GenericErrorMessage> T saveAsExpectError(
      DatasetUI datasetUI, DatasetPath newName, Class<T> errorClass) {
    final Invocation invocation =
        httpClient
            .request(
                c ->
                    c.getAPIv2()
                        .path(versionedResourcePath(datasetUI) + "/save")
                        .queryParam("as", newName))
            .buildPost(entity("", JSON));

    return expectError(CLIENT_ERROR, invocation, errorClass);
  }

  public ValidationErrorMessage saveAsExpectError(DatasetUI datasetUI, DatasetPath newName) {
    return saveAsExpectError(datasetUI, newName, ValidationErrorMessage.class);
  }

  public DatasetUIWithHistory saveAsInBranch(
      DatasetUI datasetUI, DatasetPath newName, String savedTag, String branchName) {
    final Invocation invocation =
        httpClient
            .request(
                c ->
                    c.getAPIv2()
                        .path(versionedResourcePath(datasetUI) + "/save")
                        .queryParam("as", newName)
                        .queryParam("branchName", branchName)
                        .queryParam("savedTag", savedTag))
            .buildPost(entity("", JSON));

    return expectSuccess(invocation, DatasetUIWithHistory.class);
  }

  public UserExceptionMapper.ErrorMessageWithContext saveAsInBranchExpectError(
      DatasetUI datasetUI, DatasetPath newName, String savedTag, String branchName) {
    final Invocation invocation =
        httpClient
            .request(
                c ->
                    c.getAPIv2()
                        .path(versionedResourcePath(datasetUI) + "/save")
                        .queryParam("as", newName)
                        .queryParam("branchName", branchName)
                        .queryParam("savedTag", savedTag))
            .buildPost(entity("", JSON));

    return expectError(CLIENT_ERROR, invocation, UserExceptionMapper.ErrorMessageWithContext.class);
  }

  public DatasetUI delete(DatasetUI dataset) {
    final Invocation invocation =
        httpClient
            .request(
                c ->
                    c.getAPIv2()
                        .path(resourcePath(dataset))
                        .queryParam("savedTag", dataset.getVersion()))
            .buildDelete();

    return expectSuccess(invocation, DatasetUI.class);
  }

  public void saveExpectConflict(DatasetUI datasetUI, String saveVersion) {
    final Invocation invocation =
        httpClient
            .request(
                c ->
                    c.getAPIv2()
                        .path(versionedResourcePath(datasetUI) + "/save")
                        .queryParam("savedTag", saveVersion))
            .buildPost(entity("", JSON));

    expectStatus(Status.CONFLICT, invocation);
  }

  public InitialPendingTransformResponse transformPeek(
      DatasetUI datasetUI, TransformBase transform) {
    final Invocation invocation =
        httpClient
            .request(
                c ->
                    c.getAPIv2()
                        .path(versionedResourcePath(datasetUI) + "/transformPeek")
                        .queryParam("newVersion", newVersion()))
            .buildPost(entity(transform, JSON));

    return expectSuccess(invocation, InitialPendingTransformResponse.class);
  }

  public DatasetUI createDatasetFromSQLAndSave(
      DatasetPath datasetPath, String sql, List<String> context) {
    InitialPreviewResponse datasetCreateResponse = createDatasetFromSQL(sql, context);
    return saveAs(datasetCreateResponse.getDataset(), datasetPath).getDataset();
  }

  public UserExceptionMapper.ErrorMessageWithContext createDatasetFromSQLAndSaveExpectError(
      DatasetPath datasetPath, String sql, List<String> context) {
    InitialPreviewResponse datasetCreateResponse = createDatasetFromSQL(sql, context);
    DatasetUI datasetUI = datasetCreateResponse.getDataset();
    return saveAsExpectError(
        datasetUI, datasetPath, UserExceptionMapper.ErrorMessageWithContext.class);
  }

  public InitialPreviewResponse createDatasetFromSQL(String sql, List<String> context) {
    return expectSuccess(
        httpClient
            .request(
                c ->
                    c.getAPIv2()
                        .path("datasets/new_untitled_sql")
                        .queryParam("newVersion", newVersion()))
            .buildPost(entity(new CreateFromSQL(sql, context), JSON)), // => sending
        InitialPreviewResponse.class); // <= receiving
  }

  public InitialPreviewResponse createVersionedDatasetFromSQL(
      String sql, List<String> context, String pluginName, String branchName) {
    final Map<String, VersionContextReq> references = new HashMap<>();
    references.put(
        pluginName, new VersionContextReq(VersionContextReq.VersionContextType.BRANCH, branchName));

    return expectSuccess(
        httpClient
            .request(
                c ->
                    c.getAPIv2()
                        .path("datasets/new_untitled_sql")
                        .queryParam("newVersion", newVersion()))
            .buildPost(entity(new CreateFromSQL(sql, context, references), JSON)), // => sending
        InitialPreviewResponse.class); // <= receiving
  }

  public DatasetUI createVersionedDatasetFromSQLAndSave(
      DatasetPath datasetPath,
      String sql,
      List<String> context,
      String pluginName,
      String branchName) {
    InitialPreviewResponse datasetCreateResponse =
        createVersionedDatasetFromSQL(sql, context, pluginName, branchName);
    return saveAsInBranch(datasetCreateResponse.getDataset(), datasetPath, null, branchName)
        .getDataset();
  }

  public DatasetUI getDatasetWithVersion(DatasetPath datasetPath, String refType, String refValue) {
    return expectSuccess(
        httpClient
            .request(
                c ->
                    c.getAPIv2()
                        .path("dataset/" + datasetPath.toString())
                        .queryParam("refType", refType)
                        .queryParam("refValue", refValue))
            .buildGet(),
        DatasetUI.class);
  }

  public NotFoundErrorMessage getDatasetWithVersionExpectError(
      DatasetPath datasetPath, String refType, String refValue) {
    return expectError(
        CLIENT_ERROR,
        httpClient
            .request(
                c ->
                    c.getAPIv2()
                        .path("dataset/" + datasetPath.toString())
                        .queryParam("refType", refType)
                        .queryParam("refValue", refValue))
            .buildGet(),
        NotFoundErrorMessage.class);
  }

  public DatasetUI deleteVersioned(DatasetUI dataset, String refType, String refValue) {
    final Invocation invocation =
        httpClient
            .request(
                c ->
                    c.getAPIv2()
                        .path(resourcePath(dataset))
                        .queryParam("savedTag", dataset.getVersion())
                        .queryParam("refType", refType)
                        .queryParam("refValue", refValue))
            .buildDelete();

    return expectSuccess(invocation, DatasetUI.class);
  }

  public JobDataFragment getJobData(InitialPreviewResponse response, long offset, long limit) {
    return getJobData(response.getPaginationUrl(), offset, limit);
  }

  public JobDataFragment getJobData(InitialDataPreviewResponse response, long offset, long limit) {
    return getJobData(response.getPaginationUrl(), offset, limit);
  }

  public JobDataFragment getJobData(InitialRunResponse response, long offset, long limit) {
    return getJobData(response.getPaginationUrl(), offset, limit);
  }

  public JobDataFragment getJobData(String paginationUrl, long offset, long limit) {
    final Invocation invocation =
        httpClient
            .request(
                c ->
                    c.getAPIv2()
                        .path(paginationUrl)
                        .queryParam("offset", offset)
                        .queryParam("limit", limit))
            .buildGet();

    return expectSuccess(invocation, JobDataFragment.class);
  }
}
