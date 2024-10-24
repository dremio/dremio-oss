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
package com.dremio.dac.service.datasets;

import static com.dremio.dac.api.Dataset.DatasetType.VIRTUAL_DATASET;
import static com.dremio.dac.server.FamilyExpectation.CLIENT_ERROR;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.DATAPLANE_PLUGIN_NAME;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.DATAPLANE_PLUGIN_NAME_FOR_REFLECTION_TEST;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.DEFAULT_BRANCH_NAME;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createBranchAtBranchQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createEmptyTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.fullyQualifiedTableName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueBranchName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueTableName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueViewName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.insertTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.joinedTableKey;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.selectCountQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.selectStarQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.selectStarQueryWithSpecifier;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.tablePathWithFolders;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.tablePathWithSource;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.useBranchQuery;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.dremio.catalog.model.VersionContext;
import com.dremio.catalog.model.VersionedDatasetId;
import com.dremio.common.utils.PathUtils;
import com.dremio.dac.api.CatalogEntity;
import com.dremio.dac.api.Space;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.explore.model.DatasetSummary;
import com.dremio.dac.explore.model.DatasetUI;
import com.dremio.dac.explore.model.DatasetUIWithHistory;
import com.dremio.dac.explore.model.InitialPreviewResponse;
import com.dremio.dac.explore.model.VersionContextReq;
import com.dremio.dac.server.UserExceptionMapper;
import com.dremio.exec.catalog.dataplane.ITBaseTestVersioned;
import com.dremio.exec.store.iceberg.IcebergViewMetadata;
import com.dremio.exec.store.iceberg.IcebergViewMetadata.SupportedViewDialectsForRead;
import com.google.common.collect.ImmutableList;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import org.apache.arrow.memory.BufferAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.tools.compatibility.api.NessieServerProperty;
import org.projectnessie.tools.compatibility.internal.OlderNessieServersExtension;

@ExtendWith(OlderNessieServersExtension.class)
@NessieServerProperty(name = "nessie.test.storage.kind", value = "PERSIST")
public class ITTestVersionedViews extends ITBaseTestVersioned {
  private static final String DEFAULT_SPACE_NAME = "viewSpace";
  private BufferAllocator allocator;

  @BeforeEach
  public void setUp() throws Exception {
    allocator = getRootAllocator().newChildAllocator(getClass().getName(), 0, Long.MAX_VALUE);
    createSpace(DEFAULT_SPACE_NAME);
  }

  @AfterEach
  public void cleanUp() throws Exception {
    dropSpace(DEFAULT_SPACE_NAME);
    allocator.close();
  }

  @Test
  public void testSaveAsOnNewView() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final String viewName = generateUniqueViewName();
    final List<String> viewPath = tablePathWithFolders(viewName);
    final String newViewName = generateUniqueViewName();
    final List<String> newViewPath = tablePathWithFolders(newViewName);

    createFoldersForTablePath(
        DATAPLANE_PLUGIN_NAME, tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    runQuery(createEmptyTableQuery(tablePath));
    runQuery(insertTableQuery(tablePath));
    runQueryCheckResults(getJobsService(), DATAPLANE_PLUGIN_NAME, tablePath, 3, 3, allocator, null);
    createFoldersForTablePath(
        DATAPLANE_PLUGIN_NAME, viewPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    createFoldersForTablePath(
        DATAPLANE_PLUGIN_NAME, newViewPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    DatasetPath targetViewPath =
        new DatasetPath(tablePathWithSource(DATAPLANE_PLUGIN_NAME, newViewPath));
    saveAsVersionedDataset(
        targetViewPath,
        selectStarQuery(tablePath),
        emptyList(),
        DATAPLANE_PLUGIN_NAME,
        DEFAULT_BRANCH_NAME);
  }

  @Test
  public void testSaveAsOnExistingView() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final String viewName = generateUniqueViewName();
    final List<String> viewPath = tablePathWithFolders(viewName);
    createFoldersForTablePath(
        DATAPLANE_PLUGIN_NAME, tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    runQuery(createEmptyTableQuery(tablePath));
    runQuery(insertTableQuery(tablePath));
    runQueryCheckResults(getJobsService(), DATAPLANE_PLUGIN_NAME, tablePath, 3, 3, allocator, null);
    createFoldersForTablePath(
        DATAPLANE_PLUGIN_NAME, viewPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);

    DatasetPath targetViewPath =
        new DatasetPath(tablePathWithSource(DATAPLANE_PLUGIN_NAME, viewPath));
    final DatasetUI createdDatasetUI =
        getHttpClient()
            .getDatasetApi()
            .createVersionedDatasetFromSQLAndSave(
                targetViewPath,
                selectStarQuery(tablePath),
                emptyList(),
                DATAPLANE_PLUGIN_NAME,
                DEFAULT_BRANCH_NAME);
    UserExceptionMapper.ErrorMessageWithContext errorMessage =
        saveAsVersionedDatasetExpectError(
            targetViewPath,
            selectStarQuery(tablePath),
            emptyList(),
            DATAPLANE_PLUGIN_NAME,
            DEFAULT_BRANCH_NAME);
    assertThat(errorMessage.getErrorMessage())
        .contains("The specified location already contains a view");
  }

  @Test
  public void testSaveAsWithConcurrentUpdates() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final String viewName = generateUniqueViewName();
    final List<String> viewPath = tablePathWithFolders(viewName);
    final String newViewName = generateUniqueViewName();
    final List<String> newViewPath = tablePathWithFolders(newViewName);

    createFoldersForTablePath(
        DATAPLANE_PLUGIN_NAME, tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    runQuery(createEmptyTableQuery(tablePath));
    runQuery(insertTableQuery(tablePath));
    runQueryCheckResults(getJobsService(), DATAPLANE_PLUGIN_NAME, tablePath, 3, 3, allocator, null);
    createFoldersForTablePath(
        DATAPLANE_PLUGIN_NAME, viewPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    createFoldersForTablePath(
        DATAPLANE_PLUGIN_NAME, newViewPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    DatasetPath targetViewPath =
        new DatasetPath(tablePathWithSource(DATAPLANE_PLUGIN_NAME, newViewPath));
    final Map<String, VersionContextReq> references = new HashMap<>();
    references.put(
        DATAPLANE_PLUGIN_NAME,
        new VersionContextReq(VersionContextReq.VersionContextType.BRANCH, DEFAULT_BRANCH_NAME));
    InitialPreviewResponse datasetCreateResponse1 =
        getHttpClient()
            .getDatasetApi()
            .createVersionedDatasetFromSQL(
                selectStarQuery(tablePath),
                emptyList(),
                DATAPLANE_PLUGIN_NAME,
                DEFAULT_BRANCH_NAME);
    InitialPreviewResponse datasetCreateResponse2 =
        getHttpClient()
            .getDatasetApi()
            .createVersionedDatasetFromSQL(
                selectCountQuery(tablePath, "c1"),
                emptyList(),
                DATAPLANE_PLUGIN_NAME,
                DEFAULT_BRANCH_NAME);

    // Assert
    // First save should work
    getHttpClient()
        .getDatasetApi()
        .saveAsInBranch(
            datasetCreateResponse1.getDataset(), targetViewPath, null, DEFAULT_BRANCH_NAME);
    // Conflicting save with null tag should fail
    getHttpClient()
        .getDatasetApi()
        .saveAsInBranchExpectError(
            datasetCreateResponse2.getDataset(), targetViewPath, null, DEFAULT_BRANCH_NAME);
  }

  protected UserExceptionMapper.ErrorMessageWithContext saveAsVersionedDatasetExpectError(
      DatasetPath datasetPath,
      String sql,
      List<String> context,
      String pluginName,
      String branchName) {
    final Map<String, VersionContextReq> references = new HashMap<>();
    references.put(
        pluginName, new VersionContextReq(VersionContextReq.VersionContextType.BRANCH, branchName));
    InitialPreviewResponse datasetCreateResponse =
        getHttpClient()
            .getDatasetApi()
            .createVersionedDatasetFromSQL(sql, context, pluginName, branchName);
    return getHttpClient()
        .getDatasetApi()
        .saveAsInBranchExpectError(
            datasetCreateResponse.getDataset(), datasetPath, null, branchName);
  }

  @Test
  public void testSaveAsInSpaceWithUnspecifiedVersion() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    final String newViewName = generateUniqueViewName();

    createFoldersForTablePath(
        DATAPLANE_PLUGIN_NAME, tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    runQuery(createEmptyTableQuery(tablePath));
    runQuery(insertTableQuery(tablePath));
    String viewQuery = selectStarQuery(tablePath);

    UserExceptionMapper.ErrorMessageWithContext errorMessage =
        createViewInSpaceFromQueryExpectError(viewQuery, DEFAULT_SPACE_NAME, newViewName);
    String expectedContains =
        "Validation of view sql failed. Version context for entity "
            + fullyQualifiedTableName(DATAPLANE_PLUGIN_NAME, tablePath)
            + " must be specified using AT SQL syntax";
    assertThat(errorMessage.getErrorMessage()).contains(expectedContains);
  }

  @Test
  public void testSaveAsInSpaceWithSpecifiedVersion() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    final String newViewName = generateUniqueViewName();

    createFoldersForTablePath(
        DATAPLANE_PLUGIN_NAME, tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    runQuery(createEmptyTableQuery(tablePath));
    runQuery(insertTableQuery(tablePath));
    String viewQuery = selectStarQueryWithSpecifier(tablePath, "BRANCH " + DEFAULT_BRANCH_NAME);
    runQueryCheckResults(getJobsService(), DATAPLANE_PLUGIN_NAME, tablePath, 3, 3, allocator, null);

    createViewInSpaceFromQuery(viewQuery, DEFAULT_SPACE_NAME, newViewName);
  }

  @Test
  public void testSaveAsInSpaceWithUnspecifiedVersionInJoin() throws Exception {
    final String table1 = generateUniqueTableName();
    final List<String> table1Path = tablePathWithFolders(table1);

    final String table2 = generateUniqueTableName();
    final List<String> table2Path = tablePathWithFolders(table2);

    final String newViewName = generateUniqueViewName();

    createFoldersForTablePath(
        DATAPLANE_PLUGIN_NAME, table1Path, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    runQuery(createEmptyTableQuery(table1Path));
    runQuery(insertTableQuery(table1Path));

    createFoldersForTablePath(
        DATAPLANE_PLUGIN_NAME, table2Path, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    runQuery(createEmptyTableQuery(table2Path));
    runQuery(insertTableQuery(table2Path));

    String viewQuery =
        String.format(
            "Select * from %s.%s AT BRANCH \"main\" JOIN %s.%s  ON TRUE",
            DATAPLANE_PLUGIN_NAME,
            joinedTableKey(table1Path),
            DATAPLANE_PLUGIN_NAME,
            joinedTableKey(table2Path));

    UserExceptionMapper.ErrorMessageWithContext errorMessage =
        createViewInSpaceFromQueryExpectError(viewQuery, DEFAULT_SPACE_NAME, newViewName);
    String expectedContains =
        "Validation of view sql failed. Version context for entity "
            + fullyQualifiedTableName(DATAPLANE_PLUGIN_NAME, table2Path)
            + " must be specified using AT SQL syntax";
    assertThat(errorMessage.getErrorMessage()).contains(expectedContains);
  }

  @Test
  public void testCreateViewFromPublicAPIWithUnspecifiedVersionInJoin() throws Exception {
    final String table1 = generateUniqueTableName();
    final List<String> table1Path = tablePathWithFolders(table1);

    final String table2 = generateUniqueTableName();
    final List<String> table2Path = tablePathWithFolders(table2);

    createFoldersForTablePath(
        DATAPLANE_PLUGIN_NAME, table1Path, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    runQuery(createEmptyTableQuery(table1Path));
    runQuery(insertTableQuery(table1Path));

    createFoldersForTablePath(
        DATAPLANE_PLUGIN_NAME, table2Path, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    runQuery(createEmptyTableQuery(table2Path));
    runQuery(insertTableQuery(table2Path));

    final String viewName = generateUniqueViewName();
    String viewQuery =
        String.format(
            "Select * from %s.%s AT BRANCH \"main\" JOIN %s.%s  ON TRUE",
            DATAPLANE_PLUGIN_NAME,
            joinedTableKey(table1Path),
            DATAPLANE_PLUGIN_NAME,
            joinedTableKey(table2Path));

    com.dremio.dac.api.Dataset dataset =
        new com.dremio.dac.api.Dataset(
            null,
            VIRTUAL_DATASET,
            ImmutableList.of(DATAPLANE_PLUGIN_NAME_FOR_REFLECTION_TEST, viewName),
            null,
            null,
            null,
            null,
            viewQuery,
            null,
            null,
            null);

    UserExceptionMapper.ErrorMessageWithContext errorMessage =
        createViewFromPublicAPIExpectError(dataset);
    String expectedContains =
        "Validation of view sql failed. Version context for entity "
            + fullyQualifiedTableName(DATAPLANE_PLUGIN_NAME, table2Path)
            + " must be specified using AT SQL syntax";
    assertThat(errorMessage.getErrorMessage()).contains(expectedContains);
  }

  @Test
  public void testUpdateViewFromPublicAPIWithUnspecifiedVersionInJoin() throws Exception {
    final String table1 = generateUniqueTableName();
    final List<String> table1Path = tablePathWithFolders(table1);

    final String table2 = generateUniqueTableName();
    final List<String> table2Path = tablePathWithFolders(table2);

    createFoldersForTablePath(
        DATAPLANE_PLUGIN_NAME, table1Path, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    runQuery(createEmptyTableQuery(table1Path));
    runQuery(insertTableQuery(table1Path));

    createFoldersForTablePath(
        DATAPLANE_PLUGIN_NAME, table2Path, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    runQuery(createEmptyTableQuery(table2Path));
    runQuery(insertTableQuery(table2Path));

    final String viewName = generateUniqueViewName();
    String viewQuery =
        String.format(
            "Select * from %s.%s AT BRANCH \"main\"",
            DATAPLANE_PLUGIN_NAME, joinedTableKey(table1Path));

    com.dremio.dac.api.Dataset dataset =
        new com.dremio.dac.api.Dataset(
            null,
            VIRTUAL_DATASET,
            ImmutableList.of(DATAPLANE_PLUGIN_NAME_FOR_REFLECTION_TEST, viewName),
            null,
            null,
            null,
            null,
            viewQuery,
            null,
            null,
            null);

    CatalogEntity catalogEntity = createViewFromPublicAPI(dataset);

    String updatedViewQuery =
        String.format(
            "Select * from %s.%s AT BRANCH \"main\" JOIN %s.%s  ON TRUE",
            DATAPLANE_PLUGIN_NAME,
            joinedTableKey(table1Path),
            DATAPLANE_PLUGIN_NAME,
            joinedTableKey(table2Path));

    com.dremio.dac.api.Dataset updatedDataset =
        new com.dremio.dac.api.Dataset(
            catalogEntity.getId(),
            VIRTUAL_DATASET,
            ImmutableList.of(DATAPLANE_PLUGIN_NAME_FOR_REFLECTION_TEST, viewName),
            null,
            null,
            null,
            null,
            updatedViewQuery,
            null,
            null,
            null);

    UserExceptionMapper.ErrorMessageWithContext errorMessage =
        updateViewFromPublicAPIExpectError(updatedDataset);
    String expectedContains =
        "Validation of view sql failed. Version context for entity "
            + fullyQualifiedTableName(DATAPLANE_PLUGIN_NAME, table2Path)
            + " must be specified using AT SQL syntax";
    assertThat(errorMessage.getErrorMessage()).contains(expectedContains);
  }

  @Test
  public void testDropViewInDefaultBranch() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final String viewName = generateUniqueViewName();
    final List<String> viewPath = tablePathWithFolders(viewName);
    runQuery(createEmptyTableQuery(tablePath));
    runQuery(insertTableQuery(tablePath));
    runQueryCheckResults(getJobsService(), DATAPLANE_PLUGIN_NAME, tablePath, 3, 3, allocator, null);
    InitialPreviewResponse createViewResponse =
        getHttpClient()
            .getDatasetApi()
            .createVersionedDatasetFromSQL(
                selectStarQuery(tablePath),
                emptyList(),
                DATAPLANE_PLUGIN_NAME,
                DEFAULT_BRANCH_NAME);
    DatasetPath targetViewPath =
        new DatasetPath(tablePathWithSource(DATAPLANE_PLUGIN_NAME, viewPath));
    DatasetUIWithHistory datasetUIWithHistory =
        getHttpClient()
            .getDatasetApi()
            .saveAsInBranch(
                createViewResponse.getDataset(), targetViewPath, null, DEFAULT_BRANCH_NAME);
    DatasetUI createdDataset = datasetUIWithHistory.getDataset();
    getHttpClient().getDatasetApi().deleteVersioned(createdDataset, "BRANCH", DEFAULT_BRANCH_NAME);
    getHttpClient()
        .getDatasetApi()
        .getDatasetWithVersionExpectError(
            new DatasetPath(createdDataset.getFullPath()), "BRANCH", DEFAULT_BRANCH_NAME);
  }

  @Test
  public void testDropViewInNonDefaultBranch() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final String viewName = generateUniqueViewName();
    final List<String> viewPath = tablePathWithFolders(viewName);
    String devBranch = generateUniqueBranchName();
    String sessionId =
        runQueryAndGetSessionId(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));
    runQueryInSession(useBranchQuery(devBranch), sessionId);
    runQueryInSession(createEmptyTableQuery(tablePath), sessionId);
    runQueryInSession(insertTableQuery(tablePath), sessionId);
    runQueryCheckResults(
        getJobsService(), DATAPLANE_PLUGIN_NAME, tablePath, 3, 3, allocator, sessionId);
    InitialPreviewResponse createViewResponse =
        getHttpClient()
            .getDatasetApi()
            .createVersionedDatasetFromSQL(
                selectStarQuery(tablePath), emptyList(), DATAPLANE_PLUGIN_NAME, devBranch);
    DatasetPath targetViewPath =
        new DatasetPath(tablePathWithSource(DATAPLANE_PLUGIN_NAME, viewPath));
    DatasetUIWithHistory datasetUIWithHistory =
        getHttpClient()
            .getDatasetApi()
            .saveAsInBranch(createViewResponse.getDataset(), targetViewPath, null, devBranch);
    DatasetUI createdDataset = datasetUIWithHistory.getDataset();
    getHttpClient().getDatasetApi().deleteVersioned(createdDataset, "BRANCH", devBranch);
    getHttpClient()
        .getDatasetApi()
        .getDatasetWithVersionExpectError(
            new DatasetPath(createdDataset.getFullPath()), "BRANCH", devBranch);
  }

  @Test
  public void testDatasetSummaryWithVersion() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final String viewName = generateUniqueViewName();
    final List<String> viewPath = tablePathWithFolders(viewName);
    final List<String> viewFullPath = tablePathWithSource(DATAPLANE_PLUGIN_NAME, viewPath);
    runQuery(createEmptyTableQuery(tablePath));
    runQuery(insertTableQuery(tablePath));

    runQueryCheckResults(getJobsService(), DATAPLANE_PLUGIN_NAME, tablePath, 3, 3, allocator, null);
    InitialPreviewResponse createViewResponse =
        getHttpClient()
            .getDatasetApi()
            .createVersionedDatasetFromSQL(
                selectStarQuery(tablePath),
                emptyList(),
                DATAPLANE_PLUGIN_NAME,
                DEFAULT_BRANCH_NAME);
    DatasetPath targetViewPath =
        new DatasetPath(tablePathWithSource(DATAPLANE_PLUGIN_NAME, viewPath));
    DatasetUIWithHistory datasetUIWithHistory =
        getHttpClient()
            .getDatasetApi()
            .saveAsInBranch(
                createViewResponse.getDataset(), targetViewPath, null, DEFAULT_BRANCH_NAME);
    Map<String, VersionContextReq> references = new HashMap<>();
    references.put(
        DATAPLANE_PLUGIN_NAME,
        new VersionContextReq(VersionContextReq.VersionContextType.BRANCH, DEFAULT_BRANCH_NAME));
    WebTarget webTarget =
        getHttpClient()
            .getAPIv2()
            .path(String.format("/datasets/summary/" + PathUtils.toFSPath(viewFullPath)))
            .queryParam("refType", "BRANCH")
            .queryParam("refValue", DEFAULT_BRANCH_NAME);
    DatasetSummary summary = expectSuccess(getBuilder(webTarget).buildGet(), DatasetSummary.class);
    assertEquals(0, (int) summary.getDescendants());
    assertEquals(0, (int) summary.getJobCount());
    assertEquals(3, summary.getFields().size());
    assertNotNull(VersionedDatasetId.tryParse(summary.getEntityId()));
    assertFalse(summary.getHasReflection());
    assertEquals(
        summary.getViewSpecVersion(),
        IcebergViewMetadata.SupportedIcebergViewSpecVersion.V1.name());
    assertEquals(summary.getViewDialect(), SupportedViewDialectsForRead.DREMIOSQL.toString());
  }

  protected void saveAsVersionedDataset(
      DatasetPath datasetPath,
      String sql,
      List<String> context,
      String pluginName,
      String branchName) {
    final Map<String, VersionContextReq> references = new HashMap<>();
    references.put(
        pluginName, new VersionContextReq(VersionContextReq.VersionContextType.BRANCH, branchName));
    InitialPreviewResponse datasetCreateResponse =
        getHttpClient()
            .getDatasetApi()
            .createVersionedDatasetFromSQL(sql, context, pluginName, branchName);
    getHttpClient()
        .getDatasetApi()
        .saveAsInBranch(datasetCreateResponse.getDataset(), datasetPath, null, branchName);
  }

  protected void createSpace(String name) {
    expectSuccess(
        getBuilder(getHttpClient().getCatalogApi())
            .buildPost(Entity.json(new com.dremio.dac.api.Space(null, name, null, null, null))),
        new GenericType<Space>() {});
  }

  protected void dropSpace(String name) {
    com.dremio.dac.api.Space s =
        expectSuccess(
            getBuilder(getHttpClient().getCatalogApi().path("by-path").path(name)).buildGet(),
            new GenericType<com.dremio.dac.api.Space>() {});
    expectSuccess(getBuilder(getHttpClient().getCatalogApi().path(s.getId())).buildDelete());
  }

  protected DatasetUI createViewInSpaceFromQuery(String query, String space, String viewName) {
    final DatasetPath datasetPath = new DatasetPath(ImmutableList.of(space, viewName));
    return getHttpClient()
        .getDatasetApi()
        .createDatasetFromSQLAndSave(datasetPath, query, Collections.emptyList());
  }

  protected UserExceptionMapper.ErrorMessageWithContext createViewInSpaceFromQueryExpectError(
      String query, String space, String viewName) {
    final DatasetPath datasetPath = new DatasetPath(ImmutableList.of(space, viewName));
    return getHttpClient()
        .getDatasetApi()
        .createDatasetFromSQLAndSaveExpectError(datasetPath, query, Collections.emptyList());
  }

  private UserExceptionMapper.ErrorMessageWithContext createViewFromPublicAPIExpectError(
      com.dremio.dac.api.Dataset dataset) {
    final Invocation invocation =
        getBuilder(getHttpClient().getCatalogApi()).buildPost(Entity.json(dataset));

    return expectError(CLIENT_ERROR, invocation, UserExceptionMapper.ErrorMessageWithContext.class);
  }

  private CatalogEntity createViewFromPublicAPI(com.dremio.dac.api.Dataset dataset) {
    final Invocation invocation =
        getBuilder(getHttpClient().getCatalogApi()).buildPost(Entity.json(dataset));

    return expectSuccess(invocation, CatalogEntity.class);
  }

  private UserExceptionMapper.ErrorMessageWithContext updateViewFromPublicAPIExpectError(
      com.dremio.dac.api.Dataset dataset) {
    final Invocation invocation =
        getBuilder(
                getHttpClient()
                    .getCatalogApi()
                    .path(URLEncoder.encode(dataset.getId(), StandardCharsets.UTF_8)))
            .buildPut(Entity.json(dataset));

    return expectError(CLIENT_ERROR, invocation, UserExceptionMapper.ErrorMessageWithContext.class);
  }

  private CatalogEntity updateViewFromPublicAPI(com.dremio.dac.api.Dataset dataset) {
    final Invocation invocation =
        getBuilder(
                getHttpClient()
                    .getCatalogApi()
                    .path(URLEncoder.encode(dataset.getId(), StandardCharsets.UTF_8)))
            .buildPut(Entity.json(dataset));

    return expectSuccess(invocation, CatalogEntity.class);
  }
}
