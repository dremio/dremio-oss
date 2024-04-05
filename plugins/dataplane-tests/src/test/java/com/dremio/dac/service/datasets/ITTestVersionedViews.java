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
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createEmptyTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.fullyQualifiedTableName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueTableName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueViewName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.insertTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.joinedTableKey;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.selectCountQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.selectStarQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.selectStarQueryWithSpecifier;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.tablePathWithFolders;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.tablePathWithSource;
import static java.util.Collections.emptyList;

import com.dremio.catalog.model.VersionContext;
import com.dremio.dac.api.CatalogEntity;
import com.dremio.dac.api.Space;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.explore.model.DatasetUI;
import com.dremio.dac.explore.model.InitialPreviewResponse;
import com.dremio.dac.explore.model.VersionContextReq;
import com.dremio.dac.server.UserExceptionMapper;
import com.dremio.exec.catalog.dataplane.ITBaseTestVersioned;
import com.dremio.service.jobs.JobsService;
import com.google.common.collect.ImmutableList;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
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
    allocator =
        getSabotContext().getAllocator().newChildAllocator(getClass().getName(), 0, Long.MAX_VALUE);
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
    runQueryCheckResults(
        l(JobsService.class), DATAPLANE_PLUGIN_NAME, tablePath, 3, 3, allocator, null);
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
    runQueryCheckResults(
        l(JobsService.class), DATAPLANE_PLUGIN_NAME, tablePath, 3, 3, allocator, null);
    createFoldersForTablePath(
        DATAPLANE_PLUGIN_NAME, viewPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);

    DatasetPath targetViewPath =
        new DatasetPath(tablePathWithSource(DATAPLANE_PLUGIN_NAME, viewPath));
    final DatasetUI createdDatasetUI =
        createVersionedDatasetFromSQLAndSave(
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
    assertContains(
        "The specified location already contains a view", errorMessage.getErrorMessage());
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
    runQueryCheckResults(
        l(JobsService.class), DATAPLANE_PLUGIN_NAME, tablePath, 3, 3, allocator, null);
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
        createVersionedDatasetFromSQL(
            selectStarQuery(tablePath), emptyList(), DATAPLANE_PLUGIN_NAME, DEFAULT_BRANCH_NAME);
    InitialPreviewResponse datasetCreateResponse2 =
        createVersionedDatasetFromSQL(
            selectCountQuery(tablePath, "c1"),
            emptyList(),
            DATAPLANE_PLUGIN_NAME,
            DEFAULT_BRANCH_NAME);

    // Assert
    // First save should work
    saveAsInBranch(datasetCreateResponse1.getDataset(), targetViewPath, null, DEFAULT_BRANCH_NAME);
    // Conflicting save with null tag should fail
    saveAsInBranchExpectError(
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
        createVersionedDatasetFromSQL(sql, context, pluginName, branchName);
    return saveAsInBranchExpectError(
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
    assertContains(
        "Validation of view sql failed. Version context for table "
            + fullyQualifiedTableName(DATAPLANE_PLUGIN_NAME, tablePath)
            + " must be specified using AT SQL syntax",
        errorMessage.getErrorMessage());
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
    runQueryCheckResults(
        l(JobsService.class), DATAPLANE_PLUGIN_NAME, tablePath, 3, 3, allocator, null);

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
    assertContains(
        "Validation of view sql failed. Version context for table "
            + fullyQualifiedTableName(DATAPLANE_PLUGIN_NAME, table2Path)
            + " must be specified using AT SQL syntax",
        errorMessage.getErrorMessage());
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
    assertContains(
        "Validation of view sql failed. Version context for table "
            + fullyQualifiedTableName(DATAPLANE_PLUGIN_NAME, table2Path)
            + " must be specified using AT SQL syntax",
        errorMessage.getErrorMessage());
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
    assertContains(
        "Validation of view sql failed. Version context for table "
            + fullyQualifiedTableName(DATAPLANE_PLUGIN_NAME, table2Path)
            + " must be specified using AT SQL syntax",
        errorMessage.getErrorMessage());
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
        createVersionedDatasetFromSQL(sql, context, pluginName, branchName);
    saveAsInBranch(datasetCreateResponse.getDataset(), datasetPath, null, branchName);
  }

  protected void createSpace(String name) {
    expectSuccess(
        getBuilder(getPublicAPI(3).path("/catalog/"))
            .buildPost(Entity.json(new com.dremio.dac.api.Space(null, name, null, null, null))),
        new GenericType<Space>() {});
  }

  protected void dropSpace(String name) {
    com.dremio.dac.api.Space s =
        expectSuccess(
            getBuilder(getPublicAPI(3).path("/catalog/").path("by-path").path(name)).buildGet(),
            new GenericType<com.dremio.dac.api.Space>() {});
    expectSuccess(getBuilder(getPublicAPI(3).path("/catalog/").path(s.getId())).buildDelete());
  }

  protected DatasetUI createViewInSpaceFromQuery(String query, String space, String viewName) {
    final DatasetPath datasetPath = new DatasetPath(ImmutableList.of(space, viewName));
    return createDatasetFromSQLAndSave(datasetPath, query, Collections.emptyList());
  }

  protected UserExceptionMapper.ErrorMessageWithContext createViewInSpaceFromQueryExpectError(
      String query, String space, String viewName) {
    final DatasetPath datasetPath = new DatasetPath(ImmutableList.of(space, viewName));
    return createDatasetFromSQLAndSaveExpectError(datasetPath, query, Collections.emptyList());
  }

  private UserExceptionMapper.ErrorMessageWithContext createViewFromPublicAPIExpectError(
      com.dremio.dac.api.Dataset dataset) {
    final Invocation invocation =
        getBuilder(getPublicAPI(3).path("catalog")).buildPost(Entity.json(dataset));

    return expectError(CLIENT_ERROR, invocation, UserExceptionMapper.ErrorMessageWithContext.class);
  }

  private CatalogEntity createViewFromPublicAPI(com.dremio.dac.api.Dataset dataset) {
    final Invocation invocation =
        getBuilder(getPublicAPI(3).path("catalog")).buildPost(Entity.json(dataset));

    return expectSuccess(invocation, CatalogEntity.class);
  }

  private UserExceptionMapper.ErrorMessageWithContext updateViewFromPublicAPIExpectError(
      com.dremio.dac.api.Dataset dataset) {
    try {
      final Invocation invocation =
          getBuilder(
                  getPublicAPI(3)
                      .path("catalog")
                      .path(URLEncoder.encode(dataset.getId(), StandardCharsets.UTF_8.toString())))
              .buildPut(Entity.json(dataset));

      return expectError(
          CLIENT_ERROR, invocation, UserExceptionMapper.ErrorMessageWithContext.class);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  private CatalogEntity updateViewFromPublicAPI(com.dremio.dac.api.Dataset dataset) {
    try {
      final Invocation invocation =
          getBuilder(
                  getPublicAPI(3)
                      .path("catalog")
                      .path(URLEncoder.encode(dataset.getId(), StandardCharsets.UTF_8.toString())))
              .buildPut(Entity.json(dataset));

      return expectSuccess(invocation, CatalogEntity.class);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }
}
