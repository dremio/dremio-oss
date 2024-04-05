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

import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.DATAPLANE_PLUGIN_NAME;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.DEFAULT_BRANCH_NAME;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.alterTableAddColumnsQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createBranchAtBranchQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createEmptyTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createFolderAtQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createFolderQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createTableAsQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createViewQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.dropTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.folderPathWithSource;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateFolderPath;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueBranchName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueFolderName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueTableName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueViewName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.insertSelectQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.joinedTableKey;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.tablePathWithFolders;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.tablePathWithSource;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.useBranchQuery;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.dremio.catalog.model.VersionContext;
import com.dremio.common.util.TestTools;
import com.dremio.common.utils.PathUtils;
import com.dremio.dac.daemon.DACDaemon;
import com.dremio.dac.explore.model.DatasetSummary;
import com.dremio.dac.model.folder.Folder;
import com.dremio.dac.model.folder.SourceFolderPath;
import com.dremio.dac.model.sources.SourceName;
import com.dremio.dac.server.FamilyExpectation;
import com.dremio.dac.server.UserExceptionMapper;
import com.dremio.dac.service.collaboration.CollaborationHelper;
import com.dremio.dac.service.collaboration.Tags;
import com.dremio.dac.service.collaboration.Wiki;
import com.dremio.dac.service.source.SourceService;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.catalog.CatalogOptions;
import com.dremio.exec.catalog.VersionedDatasetId;
import com.dremio.exec.store.dfs.NASConf;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceUtils;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.VirtualDataset;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.space.proto.FolderConfig;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import com.dremio.service.users.SystemUser;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.ws.rs.client.Entity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ITVersionedSourceCollaborationResource extends ITBaseTestVersioned {

  @BeforeEach
  public void testSetup() throws Exception {
    enableWikiLabel();
  }

  @Test
  public void testVersionedTableIdFromSummaryEndpoint() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final List<String> tableFullPath = tablePathWithSource(DATAPLANE_PLUGIN_NAME, tablePath);
    createFoldersForTablePath(
        DATAPLANE_PLUGIN_NAME, tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    runQuery(createTableAsQuery(tablePath, 1000));
    // Act
    DatasetSummary summary =
        expectSuccess(
            getBuilder(getAPIv2().path("/datasets/summary" + PathUtils.toFSPath(tableFullPath)))
                .buildGet(),
            DatasetSummary.class);
    // Assert
    assertThat(summary).isNotNull();
    assertThat(VersionedDatasetId.isVersionedDatasetId(summary.getEntityId())).isTrue();
  }

  @Test
  public void testVersionedViewIdFromSummaryEndpoint() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    createFoldersForTablePath(
        DATAPLANE_PLUGIN_NAME, tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    runQuery(createTableAsQuery(tablePath, 1000));
    final String viewName = generateUniqueViewName();
    List<String> viewPath = tablePathWithFolders(viewName);
    final List<String> viewFullPath = tablePathWithSource(DATAPLANE_PLUGIN_NAME, viewPath);

    // Act
    createFoldersForTablePath(
        DATAPLANE_PLUGIN_NAME, viewPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    runQuery(createViewQuery(viewPath, tablePath));
    DatasetSummary summary =
        expectSuccess(
            getBuilder(getAPIv2().path("/datasets/summary" + PathUtils.toFSPath(viewFullPath)))
                .buildGet(),
            DatasetSummary.class);
    // Assert
    assertThat(summary).isNotNull();
    assertThat(VersionedDatasetId.isVersionedDatasetId(summary.getEntityId())).isTrue();
  }

  // ****************Wiki test cases*****************//
  @Test
  public void testVersionedSetWikiWithoutFeatureFlag() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final List<String> tableFullPath = tablePathWithSource(DATAPLANE_PLUGIN_NAME, tablePath);
    createFoldersForTablePath(
        DATAPLANE_PLUGIN_NAME, tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    runQuery(createTableAsQuery(tablePath, 1000));
    // Act
    disableWikiLabel();
    DatasetSummary summary =
        expectSuccess(
            getBuilder(getAPIv2().path("/datasets/summary" + PathUtils.toFSPath(tableFullPath)))
                .buildGet(),
            DatasetSummary.class);
    String versionedDatasetIdAsString = summary.getEntityId();
    // create wiki
    Wiki newWiki = new Wiki("sample wiki text", null);

    // Assert
    UserExceptionMapper.ErrorMessageWithContext error =
        expectError(
            FamilyExpectation.CLIENT_ERROR,
            getBuilder(
                    getPublicAPI(3)
                        .path("catalog")
                        .path(PathUtils.encodeURIComponent(versionedDatasetIdAsString))
                        .path("collaboration")
                        .path("wiki"))
                .buildPost(Entity.json(newWiki)),
            UserExceptionMapper.ErrorMessageWithContext.class);
    assertTrue(
        error.getErrorMessage().contains("Wiki and Label not supported on entities in source"));
  }

  @Test
  public void testVersionedGetWikiOnTableWithoutFeatureFlag() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final List<String> tableFullPath = tablePathWithSource(DATAPLANE_PLUGIN_NAME, tablePath);
    createFoldersForTablePath(
        DATAPLANE_PLUGIN_NAME, tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    runQuery(createTableAsQuery(tablePath, 1000));

    // Act
    DatasetSummary summary =
        expectSuccess(
            getBuilder(getAPIv2().path("/datasets/summary" + PathUtils.toFSPath(tableFullPath)))
                .buildGet(),
            DatasetSummary.class);
    String versionedDatasetIdAsString = summary.getEntityId();
    disableWikiLabel();

    // Assert
    UserExceptionMapper.ErrorMessageWithContext error =
        expectError(
            FamilyExpectation.CLIENT_ERROR,
            getBuilder(
                    getPublicAPI(3)
                        .path("catalog")
                        .path(PathUtils.encodeURIComponent(versionedDatasetIdAsString))
                        .path("collaboration")
                        .path("wiki"))
                .buildGet(),
            UserExceptionMapper.ErrorMessageWithContext.class);
    assertTrue(
        error.getErrorMessage().contains("Wiki and Label not supported on entities in source"));
  }

  @Test
  public void testVersionedSetWikiOnTable() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final List<String> tableFullPath = tablePathWithSource(DATAPLANE_PLUGIN_NAME, tablePath);
    createFoldersForTablePath(
        DATAPLANE_PLUGIN_NAME, tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    runQuery(createTableAsQuery(tablePath, 1000));
    // Act
    Wiki newWiki = new Wiki("sample wiki text", null);
    Wiki createdWiki = setWikiForVersionedEntity(tableFullPath, newWiki);

    // Assert
    assertEquals(createdWiki.getText(), newWiki.getText());
  }

  @Test
  public void testVersionedGetWikiOnTable() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final List<String> tableFullPath = tablePathWithSource(DATAPLANE_PLUGIN_NAME, tablePath);
    createFoldersForTablePath(
        DATAPLANE_PLUGIN_NAME, tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    runQuery(createTableAsQuery(tablePath, 1000));

    // Act
    Wiki newWiki = new Wiki("sample wiki text", null);
    Wiki wiki = setWikiForVersionedEntity(tableFullPath, newWiki);
    Wiki readWiki = getWikiForVersionedEntity(tableFullPath);

    // Assert
    assertEquals(wiki.getText(), newWiki.getText());
    assertEquals(readWiki.getText(), newWiki.getText());
  }

  // Negative test
  @Test
  public void testVersionedGetWikiOnTableAfterDrop() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final List<String> tableFullPath = tablePathWithSource(DATAPLANE_PLUGIN_NAME, tablePath);
    createFoldersForTablePath(
        DATAPLANE_PLUGIN_NAME, tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    runQuery(createTableAsQuery(tablePath, 1000));

    // Act
    Wiki newWiki = new Wiki("sample wiki text", null);
    setWikiForVersionedEntity(tableFullPath, newWiki);

    DatasetSummary summary =
        expectSuccess(
            getBuilder(getAPIv2().path("/datasets/summary" + PathUtils.toFSPath(tableFullPath)))
                .buildGet(),
            DatasetSummary.class);
    String versionedDatasetIdAsStringBeforeDrop = summary.getEntityId();
    runQuery(dropTableQuery(tablePath));

    // Assert
    // Should not be able to retrieve label for dropped table
    UserExceptionMapper.ErrorMessageWithContext error =
        expectError(
            FamilyExpectation.CLIENT_ERROR,
            getBuilder(
                    getPublicAPI(3)
                        .path("catalog")
                        .path(PathUtils.encodeURIComponent(versionedDatasetIdAsStringBeforeDrop))
                        .path("collaboration")
                        .path("wiki"))
                .buildGet(),
            UserExceptionMapper.ErrorMessageWithContext.class);
    assertTrue(error.getErrorMessage().contains("Could not find entity with key"));
  }

  @Test
  public void testVersionedUpdateWikiOnTable() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final List<String> tableFullPath = tablePathWithSource(DATAPLANE_PLUGIN_NAME, tablePath);
    createFoldersForTablePath(
        DATAPLANE_PLUGIN_NAME, tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    runQuery(createTableAsQuery(tablePath, 1000));

    // Act
    // create wiki
    Wiki newWiki = new Wiki("sample wiki text", null);
    setWikiForVersionedEntity(tableFullPath, newWiki);
    Wiki readWiki = getWikiForVersionedEntity(tableFullPath);
    assertEquals(readWiki.getText(), newWiki.getText());
    // Update wiki
    Wiki updatedWikiText = new Wiki("updated wiki text", readWiki.getVersion());
    Wiki updatedWiki = setWikiForVersionedEntity(tableFullPath, updatedWikiText);
    assertEquals(updatedWiki.getText(), updatedWikiText.getText());

    // Assert
    readWiki = getWikiForVersionedEntity(tableFullPath);
    assertEquals(readWiki.getText(), updatedWiki.getText());
  }

  @Test
  public void testVersionedGetWikiOnTableAfterUpdate() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final List<String> tableFullPath = tablePathWithSource(DATAPLANE_PLUGIN_NAME, tablePath);
    createFoldersForTablePath(
        DATAPLANE_PLUGIN_NAME, tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    runQuery(createTableAsQuery(tablePath, 1000));
    DatasetSummary summary =
        expectSuccess(
            getBuilder(getAPIv2().path("/datasets/summary" + PathUtils.toFSPath(tableFullPath)))
                .buildGet(),
            DatasetSummary.class);
    String versionedDatasetIdAsString = summary.getEntityId();
    // Act
    // create wiki
    Wiki firstWiki = new Wiki("first wiki", null);
    Wiki wiki = setWikiForVersionedEntity(tableFullPath, firstWiki);
    assertEquals(wiki.getText(), firstWiki.getText());
    Wiki readWiki = getWikiForVersionedEntity(tableFullPath);
    assertEquals(readWiki.getText(), firstWiki.getText());

    // Do DML and retry fetching the wiki
    runQuery(insertSelectQuery(tablePath, 10));

    // Act 2
    summary =
        expectSuccess(
            getBuilder(getAPIv2().path("/datasets/summary" + PathUtils.toFSPath(tableFullPath)))
                .buildGet(),
            DatasetSummary.class);
    String versionedDatasetIdAsStringAfterUpdate = summary.getEntityId();

    // Assert
    assertEquals(versionedDatasetIdAsString, versionedDatasetIdAsStringAfterUpdate);
    Wiki readWikiAfterUpdate = getWikiForVersionedEntity(tableFullPath);
    assertEquals(readWikiAfterUpdate.getText(), readWiki.getText());
  }

  @Test
  public void testVersionedGetWikiOnTableAfterAlter() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final List<String> tableFullPath = tablePathWithSource(DATAPLANE_PLUGIN_NAME, tablePath);
    createFoldersForTablePath(
        DATAPLANE_PLUGIN_NAME, tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    runQuery(createEmptyTableQuery(tablePath));

    // Act
    DatasetSummary summary =
        expectSuccess(
            getBuilder(getAPIv2().path("/datasets/summary" + PathUtils.toFSPath(tableFullPath)))
                .buildGet(),
            DatasetSummary.class);
    String versionedDatasetIdAsString = summary.getEntityId();
    // create wiki
    Wiki firstWiki = new Wiki("first wiki", null);
    Wiki wiki = setWikiForVersionedEntity(tableFullPath, firstWiki);
    assertEquals(wiki.getText(), firstWiki.getText());
    Wiki readWiki = getWikiForVersionedEntity(tableFullPath);

    // Do an alter on the table and retry  fetching the Wiki
    final List<String> addedColDef = Collections.singletonList("col2 int");
    runQuery(alterTableAddColumnsQuery(tablePath, addedColDef));

    summary =
        expectSuccess(
            getBuilder(getAPIv2().path("/datasets/summary" + PathUtils.toFSPath(tableFullPath)))
                .buildGet(),
            DatasetSummary.class);
    String versionedDatasetIdAsStringAfterAlter = summary.getEntityId();

    // Assert
    assertEquals(versionedDatasetIdAsString, versionedDatasetIdAsStringAfterAlter);
    Wiki readWikiAfterAlter = getWikiForVersionedEntity(tableFullPath);
    assertEquals(readWikiAfterAlter.getText(), readWiki.getText());
  }

  @Test
  public void testVersionedSetWikiOnView() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFoldersForTablePath(
        DATAPLANE_PLUGIN_NAME, tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    runQuery(createTableAsQuery(tablePath, 1000));
    final String viewName = generateUniqueViewName();
    List<String> viewPath = tablePathWithFolders(viewName);
    final List<String> viewFullPath = tablePathWithSource(DATAPLANE_PLUGIN_NAME, viewPath);

    // Act
    createFoldersForTablePath(
        DATAPLANE_PLUGIN_NAME, viewPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    runQuery(createViewQuery(viewPath, tablePath));
    Wiki newWiki = new Wiki("sample wiki text", null);
    Wiki createdWiki = setWikiForVersionedEntity(viewFullPath, newWiki);

    // Assert
    assertEquals(createdWiki.getText(), newWiki.getText());
  }

  @Test
  public void testVersionedGetWikiOnView() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    createFoldersForTablePath(
        DATAPLANE_PLUGIN_NAME, tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    runQuery(createTableAsQuery(tablePath, 1000));
    final String viewName = generateUniqueViewName();
    List<String> viewPath = tablePathWithFolders(viewName);
    final List<String> viewFullPath = tablePathWithSource(DATAPLANE_PLUGIN_NAME, viewPath);

    // Act
    createFoldersForTablePath(
        DATAPLANE_PLUGIN_NAME, viewPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    runQuery(createViewQuery(viewPath, tablePath));
    Wiki newWiki = new Wiki("sample wiki text", null);
    Wiki wiki = setWikiForVersionedEntity(viewFullPath, newWiki);
    Wiki readWiki = getWikiForVersionedEntity(viewFullPath);

    // Assert
    assertEquals(wiki.getText(), newWiki.getText());
    assertEquals(readWiki.getText(), newWiki.getText());
  }

  @Test
  public void testVersionedUpdateWikiOnView() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFoldersForTablePath(
        DATAPLANE_PLUGIN_NAME, tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    runQuery(createTableAsQuery(tablePath, 1000));
    final String viewName = generateUniqueViewName();
    List<String> viewPath = tablePathWithFolders(viewName);
    final List<String> viewFullPath = tablePathWithSource(DATAPLANE_PLUGIN_NAME, viewPath);

    // Act
    createFoldersForTablePath(
        DATAPLANE_PLUGIN_NAME, viewPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    runQuery(createViewQuery(viewPath, tablePath));
    // create wiki
    Wiki newWiki = new Wiki("sample wiki text", null);
    setWikiForVersionedEntity(viewFullPath, newWiki);
    Wiki readWiki = getWikiForVersionedEntity(viewFullPath);
    assertEquals(readWiki.getText(), newWiki.getText());
    // Update wiki
    Wiki updatedWikiText = new Wiki("updated wiki text", readWiki.getVersion());
    Wiki updatedWiki = setWikiForVersionedEntity(viewFullPath, updatedWikiText);
    assertEquals(updatedWiki.getText(), updatedWikiText.getText());

    // Assert
    readWiki = getWikiForVersionedEntity(viewFullPath);
    assertEquals(readWiki.getText(), updatedWiki.getText());
  }

  @Test
  public void testVersionedSetWikiOnBranch() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final List<String> tableFullPath = tablePathWithSource(DATAPLANE_PLUGIN_NAME, tablePath);
    createFoldersForTablePath(
        DATAPLANE_PLUGIN_NAME, tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    runQuery(createTableAsQuery(tablePath, 1000));

    String devBranchName = generateUniqueBranchName();
    String sessionId =
        runQueryAndGetSessionId(createBranchAtBranchQuery(devBranchName, DEFAULT_BRANCH_NAME));
    // create table at dev
    runQueryInSession(useBranchQuery(devBranchName), sessionId);
    List<String> testTablePathInBranch = tablePathWithFolders(generateUniqueTableName());
    final List<String> testTableFullPathInBranch =
        tablePathWithSource(DATAPLANE_PLUGIN_NAME, testTablePathInBranch);
    createFoldersForTablePath(
        DATAPLANE_PLUGIN_NAME,
        testTablePathInBranch,
        VersionContext.ofBranch(devBranchName),
        sessionId);
    runQueryInSession(createTableAsQuery(testTablePathInBranch, 500), sessionId);

    // Act
    DatasetSummary summary =
        expectSuccess(
            getBuilder(
                    getAPIv2()
                        .path("/datasets/summary" + PathUtils.toFSPath(testTableFullPathInBranch))
                        .queryParam("refType", "BRANCH")
                        .queryParam("refValue", devBranchName))
                .buildGet(),
            DatasetSummary.class);
    String versionedDatasetIdAsString = summary.getEntityId();
    // create wiki
    Wiki newWiki = new Wiki("sample wiki text", null);

    // Assert
    UserExceptionMapper.ErrorMessageWithContext error =
        expectError(
            FamilyExpectation.CLIENT_ERROR,
            getBuilder(
                    getPublicAPI(3)
                        .path("catalog")
                        .path(PathUtils.encodeURIComponent(versionedDatasetIdAsString))
                        .path("collaboration")
                        .path("wiki"))
                .buildPost(Entity.json(newWiki)),
            UserExceptionMapper.ErrorMessageWithContext.class);
    assertTrue(
        error.getErrorMessage().contains("Wiki and Label can only be set on the default branch"));
  }

  // ****************Label test cases*****************//

  @Test
  public void testNoTags() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final List<String> tableFullPath = tablePathWithSource(DATAPLANE_PLUGIN_NAME, tablePath);
    createFoldersForTablePath(
        DATAPLANE_PLUGIN_NAME, tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    runQuery(createTableAsQuery(tablePath, 1000));
    DatasetSummary summary =
        expectSuccess(
            getBuilder(getAPIv2().path("/datasets/summary" + PathUtils.toFSPath(tableFullPath)))
                .buildGet(),
            DatasetSummary.class);
    String versionedDatasetIdAsString = summary.getEntityId();
    // Test no tags
    Tags noLabels =
        expectSuccess(
            getBuilder(
                    getPublicAPI(3)
                        .path("catalog")
                        .path(PathUtils.encodeURIComponent(versionedDatasetIdAsString))
                        .path("collaboration")
                        .path("tag"))
                .buildGet(),
            Tags.class);
    assertEquals(noLabels.getTags().size(), 0);
  }

  @Test
  public void testVersionedSetlabelWithoutFeatureFlag() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final List<String> tableFullPath = tablePathWithSource(DATAPLANE_PLUGIN_NAME, tablePath);
    createFoldersForTablePath(
        DATAPLANE_PLUGIN_NAME, tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    runQuery(createTableAsQuery(tablePath, 1000));

    // Act
    disableWikiLabel();
    DatasetSummary summary =
        expectSuccess(
            getBuilder(getAPIv2().path("/datasets/summary" + PathUtils.toFSPath(tableFullPath)))
                .buildGet(),
            DatasetSummary.class);
    String versionedDatasetIdAsString = summary.getEntityId();
    // create label
    List<String> labelList = Arrays.asList("label1");
    Tags newlabel = new Tags(labelList, null);

    // Assert
    UserExceptionMapper.ErrorMessageWithContext error =
        expectError(
            FamilyExpectation.CLIENT_ERROR,
            getBuilder(
                    getPublicAPI(3)
                        .path("catalog")
                        .path(PathUtils.encodeURIComponent(versionedDatasetIdAsString))
                        .path("collaboration")
                        .path("tag"))
                .buildPost(Entity.json(newlabel)),
            UserExceptionMapper.ErrorMessageWithContext.class);
    assertTrue(
        error.getErrorMessage().contains("Wiki and Label not supported on entities in source"));
  }

  @Test
  public void testVersionedGetlabelOnTableWithoutFeatureFlag() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final List<String> tableFullPath = tablePathWithSource(DATAPLANE_PLUGIN_NAME, tablePath);
    createFoldersForTablePath(
        DATAPLANE_PLUGIN_NAME, tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    runQuery(createTableAsQuery(tablePath, 1000));

    // Act
    DatasetSummary summary =
        expectSuccess(
            getBuilder(getAPIv2().path("/datasets/summary" + PathUtils.toFSPath(tableFullPath)))
                .buildGet(),
            DatasetSummary.class);
    String versionedDatasetIdAsString = summary.getEntityId();
    disableWikiLabel();

    // Assert
    UserExceptionMapper.ErrorMessageWithContext error =
        expectError(
            FamilyExpectation.CLIENT_ERROR,
            getBuilder(
                    getPublicAPI(3)
                        .path("catalog")
                        .path(PathUtils.encodeURIComponent(versionedDatasetIdAsString))
                        .path("collaboration")
                        .path("tag"))
                .buildGet(),
            UserExceptionMapper.ErrorMessageWithContext.class);
    assertTrue(
        error.getErrorMessage().contains("Wiki and Label not supported on entities in source"));
  }

  @Test
  public void testVersionedGetLabelsOnTable() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final List<String> tableFullPath = tablePathWithSource(DATAPLANE_PLUGIN_NAME, tablePath);
    createFoldersForTablePath(
        DATAPLANE_PLUGIN_NAME, tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    runQuery(createTableAsQuery(tablePath, 1000));
    // Act
    List<String> labelList = Arrays.asList("label1", "label2");

    Tags newLabels = new Tags(labelList, null);
    setLabelsForVersionedEntity(tableFullPath, newLabels);

    Tags labels = getLabelsForVersionedEntity(tableFullPath);
    assertEquals(labels.getTags().size(), 2);
    assertTrue(labels.getTags().containsAll(labelList));
  }

  @Test
  public void testVersionedSetLabelsOnTable() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final List<String> tableFullPath = tablePathWithSource(DATAPLANE_PLUGIN_NAME, tablePath);
    createFoldersForTablePath(
        DATAPLANE_PLUGIN_NAME, tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    runQuery(createTableAsQuery(tablePath, 1000));

    // Act
    List<String> labelList = Arrays.asList("label1", "label2");
    Tags newLabels = new Tags(labelList, null);
    Tags setLabels = setLabelsForVersionedEntity(tableFullPath, newLabels);
    // Assert
    assertEquals(setLabels.getTags().size(), 2);
    assertTrue(setLabels.getTags().containsAll(labelList));
  }

  // Negative test
  @Test
  public void testVersionedGetLabelsOnTableAfterDrop() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final List<String> tableFullPath = tablePathWithSource(DATAPLANE_PLUGIN_NAME, tablePath);
    createFoldersForTablePath(
        DATAPLANE_PLUGIN_NAME, tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    runQuery(createTableAsQuery(tablePath, 1000));

    // Act
    List<String> labelList = Arrays.asList("label1", "label2");
    Tags newLabels = new Tags(labelList, null);
    setLabelsForVersionedEntity(tableFullPath, newLabels);

    DatasetSummary summary =
        expectSuccess(
            getBuilder(getAPIv2().path("/datasets/summary" + PathUtils.toFSPath(tableFullPath)))
                .buildGet(),
            DatasetSummary.class);
    String versionedDatasetIdAsStringBeforeDrop = summary.getEntityId();
    runQuery(dropTableQuery(tablePath));

    // Assert
    // Should not be able to retrieve label for dropped table
    UserExceptionMapper.ErrorMessageWithContext error =
        expectError(
            FamilyExpectation.CLIENT_ERROR,
            getBuilder(
                    getPublicAPI(3)
                        .path("catalog")
                        .path(PathUtils.encodeURIComponent(versionedDatasetIdAsStringBeforeDrop))
                        .path("collaboration")
                        .path("tag"))
                .buildGet(),
            UserExceptionMapper.ErrorMessageWithContext.class);
    assertTrue(error.getErrorMessage().contains("Could not find entity with key"));
  }

  @Test
  public void testVersionedUpdateLabelsOnTable() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final List<String> tableFullPath = tablePathWithSource(DATAPLANE_PLUGIN_NAME, tablePath);
    createFoldersForTablePath(
        DATAPLANE_PLUGIN_NAME, tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    runQuery(createTableAsQuery(tablePath, 1000));

    // Act
    // create new labels
    List<String> labelList = Arrays.asList("label1", "label2");
    Tags newLabels = new Tags(labelList, null);
    setLabelsForVersionedEntity(tableFullPath, newLabels);

    Tags readLabels = getLabelsForVersionedEntity(tableFullPath);
    assertEquals(readLabels.getTags().size(), 2);
    assertTrue(readLabels.getTags().containsAll(labelList));

    // Update labels
    List<String> newLabelList = Arrays.asList("label1", "label2", "label3");
    Tags newLabels2 = new Tags(newLabelList, readLabels.getVersion());
    Tags updatedLabels = setLabelsForVersionedEntity(tableFullPath, newLabels2);

    // Assert
    assertEquals(updatedLabels.getTags().size(), 3);
    assertTrue(updatedLabels.getTags().containsAll(newLabelList));
  }

  @Test
  public void testVersionedUpdateLabelsOnView() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFoldersForTablePath(
        DATAPLANE_PLUGIN_NAME, tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    runQuery(createTableAsQuery(tablePath, 1000));
    final String viewName = generateUniqueViewName();
    List<String> viewPath = tablePathWithFolders(viewName);
    final List<String> viewFullPath = tablePathWithSource(DATAPLANE_PLUGIN_NAME, viewPath);
    createFoldersForTablePath(
        DATAPLANE_PLUGIN_NAME, viewPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    runQuery(createViewQuery(viewPath, tablePath));
    // Act

    // create new labels
    List<String> labelList1 = Arrays.asList("label1", "label2");
    Tags labels1 = new Tags(labelList1, null);
    setLabelsForVersionedEntity(viewFullPath, labels1);

    Tags readLabels = getLabelsForVersionedEntity(viewFullPath);
    assertEquals(readLabels.getTags().size(), 2);
    assertTrue(readLabels.getTags().containsAll(labelList1));

    // Update labels
    List<String> labelList2 = Arrays.asList("label1", "label2", "label3");
    Tags labels2 = new Tags(labelList2, readLabels.getVersion());
    Tags updatedLabels = setLabelsForVersionedEntity(viewFullPath, labels2);

    // Assert
    assertEquals(updatedLabels.getTags().size(), 3);
    assertTrue(updatedLabels.getTags().containsAll(labelList2));
  }

  @Test
  public void testVersionedSetLabelsOnBranch() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFoldersForTablePath(
        DATAPLANE_PLUGIN_NAME, tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    runQuery(createTableAsQuery(tablePath, 1000));
    String devBranchName = generateUniqueBranchName();

    String sessionId =
        runQueryAndGetSessionId(createBranchAtBranchQuery(devBranchName, DEFAULT_BRANCH_NAME));

    // create table at dev
    runQueryInSession(useBranchQuery(devBranchName), sessionId);
    List<String> testTablePathInBranch = tablePathWithFolders(generateUniqueTableName());
    final List<String> testTableFullPathInBranch =
        tablePathWithSource(DATAPLANE_PLUGIN_NAME, testTablePathInBranch);
    createFoldersForTablePath(
        DATAPLANE_PLUGIN_NAME,
        testTablePathInBranch,
        VersionContext.ofBranch(devBranchName),
        sessionId);
    runQueryInSession(createTableAsQuery(testTablePathInBranch, 500), sessionId);

    // Act
    DatasetSummary summary =
        expectSuccess(
            getBuilder(
                    getAPIv2()
                        .path("/datasets/summary" + PathUtils.toFSPath(testTableFullPathInBranch))
                        .queryParam("refType", "BRANCH")
                        .queryParam("refValue", devBranchName))
                .buildGet(),
            DatasetSummary.class);
    String versionedDatasetIdAsString = summary.getEntityId();
    // create wiki
    Wiki newWiki = new Wiki("sample wiki text", null);

    // Assert
    UserExceptionMapper.ErrorMessageWithContext error =
        expectError(
            FamilyExpectation.CLIENT_ERROR,
            getBuilder(
                    getPublicAPI(3)
                        .path("catalog")
                        .path(PathUtils.encodeURIComponent(versionedDatasetIdAsString))
                        .path("collaboration")
                        .path("wiki"))
                .buildPost(Entity.json(newWiki)),
            UserExceptionMapper.ErrorMessageWithContext.class);
    assertTrue(
        error.getErrorMessage().contains("Wiki and Label can only be set on the default branch"));
  }

  @Test
  public void testOrphanPruningWithVersioned() throws Exception {
    final DACDaemon daemon = isMultinode() ? getMasterDremioDaemon() : getCurrentDremioDaemon();
    CollaborationHelper.pruneOrphans(
        daemon.getBindingProvider().lookup(LegacyKVStoreProvider.class));

    // Setup
    // create a source
    final NASConf nasConf = new NASConf();
    nasConf.path = TestTools.getWorkingPath() + "/src/test/resources";

    final NamespaceKey sourceKey = new NamespaceKey("mysource");
    final SourceConfig sourceConfig = new SourceConfig();
    sourceConfig.setName(sourceKey.getRoot());
    sourceConfig.setConfig(nasConf.toBytesString());
    sourceConfig.setType("NAS");
    newNamespaceService().addOrUpdateSource(sourceKey, sourceConfig);

    // create space
    final NamespaceKey spacePath = new NamespaceKey("testspace");
    final List<String> vds1 = Arrays.asList(spacePath.getRoot(), "vds1");
    String spaceVersion = createSpaceAndVDS(spacePath, vds1);

    final List<String> vds2 = Arrays.asList(spacePath.getRoot(), "vds2");
    createVDS(vds2);

    createFolderInSpace(spacePath.getPathComponents(), "testFolder");

    final List<String> vdsInFolder =
        Arrays.asList(spacePath.getRoot(), "testFolder", "vdsInFolder");
    createVDS(vdsInFolder);

    addWikiToNamespaceEntity(spacePath.getPathComponents(), "wiki for testspace");
    addWikiToNamespaceEntity(vds1, "wiki for vds1");
    addWikiToNamespaceEntity(vds2, "wiki for vds2 ");
    addWikiToNamespaceEntity(vdsInFolder, "wiki for vdsInFolder ");
    addlabelsToNamespaceEntity(vds2, Collections.singletonList("label for vds2"));
    addWikiToNamespaceEntity(
        Arrays.asList(spacePath.getRoot(), "testFolder"), "wiki for testfolder");

    // add wiki to the source
    addWikiToNamespaceEntity(sourceKey.getPathComponents(), "wiki for source");

    // Create Versioned entries
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final List<String> tableFullPath = tablePathWithSource(DATAPLANE_PLUGIN_NAME, tablePath);
    createFoldersForTablePath(
        DATAPLANE_PLUGIN_NAME, tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    runQuery(createTableAsQuery(tablePath, 1000));
    final String viewName = generateUniqueViewName();
    List<String> viewPath = tablePathWithFolders(viewName);
    final List<String> viewFullPath = tablePathWithSource(DATAPLANE_PLUGIN_NAME, viewPath);

    createFoldersForTablePath(
        DATAPLANE_PLUGIN_NAME, viewPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    runQuery(createViewQuery(viewPath, tablePath));
    Wiki tableWiki = new Wiki("sample wiki on Versioned table", null);
    setWikiForVersionedEntity(tableFullPath, tableWiki);

    Wiki viewWiki = new Wiki("sample wiki on Versioned view", null);
    setWikiForVersionedEntity(viewFullPath, viewWiki);
    // create new labels
    List<String> tableLabels = Arrays.asList("tll1", "tl2");
    setLabelsForVersionedEntity(tableFullPath, new Tags(tableLabels, null));
    // create new labels
    List<String> viewLabels = Arrays.asList("vl1", "vl2");
    setLabelsForVersionedEntity(viewFullPath, new Tags(viewLabels, null));

    // Act
    // nothing deleted so no pruned items
    int pruneCount =
        CollaborationHelper.pruneOrphans(
            daemon.getBindingProvider().lookup(LegacyKVStoreProvider.class));
    assertEquals(0, pruneCount);

    // delete the space and children
    newNamespaceService().deleteSpace(spacePath, spaceVersion);
    pruneCount =
        CollaborationHelper.pruneOrphans(
            daemon.getBindingProvider().lookup(LegacyKVStoreProvider.class));
    assertEquals(6, pruneCount);

    // delete the source
    newNamespaceService().deleteSource(sourceKey, sourceConfig.getTag());
    pruneCount =
        CollaborationHelper.pruneOrphans(
            daemon.getBindingProvider().lookup(LegacyKVStoreProvider.class));
    assertEquals(1, pruneCount);

    // Assert
    // Ensure the Versioned entries are still in the store.

    assertEquals(getWikiForVersionedEntity(tableFullPath).getText(), tableWiki.getText());
    assertEquals(getWikiForVersionedEntity(viewFullPath).getText(), viewWiki.getText());
    assertEquals(getLabelsForVersionedEntity(tableFullPath).getTags(), tableLabels);
    assertEquals(getLabelsForVersionedEntity(viewFullPath).getTags(), viewLabels);
  }

  /***Folder test cases ***/
  @Test
  public void testVersionedGetFolderId() throws Exception {
    // Arrange
    SourceService sourceService = l(SourceService.class);
    final String folderName = generateUniqueFolderName();
    final List<String> folderPath = generateFolderPath(Collections.singletonList(folderName));
    runQuery(createFolderQuery(DATAPLANE_PLUGIN_NAME, folderPath));

    // Act
    Folder folder =
        sourceService.getFolder(
            new SourceName(DATAPLANE_PLUGIN_NAME),
            new SourceFolderPath(folderPathWithSource(DATAPLANE_PLUGIN_NAME, folderPath)),
            false,
            "dremio",
            "BRANCH",
            "main");

    // Assert
    assertThat(folder).isNotNull();
    assertThat(VersionedDatasetId.isVersionedDatasetId(folder.getId())).isTrue();
  }

  @Test
  public void testVersionedSetFolderWiki() throws Exception {
    // Arrange
    final String folderName = generateUniqueFolderName();
    final List<String> folderPath = generateFolderPath(Collections.singletonList(folderName));
    runQuery(createFolderQuery(DATAPLANE_PLUGIN_NAME, folderPath));

    // Act
    Wiki newWiki = new Wiki("sample wiki text", null);
    Wiki setWiki =
        setWikiForVersionedFolder(
            DATAPLANE_PLUGIN_NAME,
            folderPathWithSource(DATAPLANE_PLUGIN_NAME, folderPath),
            newWiki);

    // Assert
    assertEquals(setWiki.getText(), newWiki.getText());
  }

  @Test
  public void testVersionedGetFolderWiki() throws Exception {
    // Arrange
    final String folderName = generateUniqueFolderName();
    final List<String> folderPath = generateFolderPath(Collections.singletonList(folderName));
    runQuery(createFolderQuery(DATAPLANE_PLUGIN_NAME, folderPath));

    // Act
    Wiki newWiki = new Wiki("sample wiki text", null);
    Wiki setWiki =
        setWikiForVersionedFolder(
            DATAPLANE_PLUGIN_NAME,
            folderPathWithSource(DATAPLANE_PLUGIN_NAME, folderPath),
            newWiki);

    // Assert
    Wiki readWiki =
        getWikiForVersionedFolder(
            DATAPLANE_PLUGIN_NAME, folderPathWithSource(DATAPLANE_PLUGIN_NAME, folderPath));
    assertEquals(readWiki.getText(), setWiki.getText());
  }

  @Test
  public void testVersionedSetFolderLabel() throws Exception {
    // Arrange
    SourceService sourceService = l(SourceService.class);
    final String folderName = generateUniqueFolderName();
    final List<String> folderPath = generateFolderPath(Collections.singletonList(folderName));
    runQuery(createFolderQuery(DATAPLANE_PLUGIN_NAME, folderPath));

    // Act
    List<String> labelList = Arrays.asList("tag1", "tag2");
    Tags newLabels = new Tags(labelList, null);
    Tags setTags =
        setLabelsForVersionedFolder(
            DATAPLANE_PLUGIN_NAME,
            folderPathWithSource(DATAPLANE_PLUGIN_NAME, folderPath),
            newLabels);

    // Assert
    assertEquals(setTags.getTags().size(), 2);
    assertTrue(setTags.getTags().containsAll(labelList));
  }

  @Test
  public void testVersionedGetFolderLabel() throws Exception {
    // Arrange
    SourceService sourceService = l(SourceService.class);
    final String folderName = generateUniqueFolderName();
    final List<String> folderPath = generateFolderPath(Collections.singletonList(folderName));
    runQuery(createFolderQuery(DATAPLANE_PLUGIN_NAME, folderPath));

    // Act
    Folder folder =
        sourceService.getFolder(
            new SourceName(DATAPLANE_PLUGIN_NAME),
            new SourceFolderPath(folderPathWithSource(DATAPLANE_PLUGIN_NAME, folderPath)),
            false,
            "dremio",
            "BRANCH",
            "main");
    String versionedDatasetIdAsString = folder.getId();
    CollaborationHelper collaborationHelper = l(CollaborationHelper.class);
    List<String> labelList = Arrays.asList("tag1", "tag2");
    Tags newLabels = new Tags(labelList, null);
    collaborationHelper.setTags(versionedDatasetIdAsString, newLabels);

    // Assert
    Tags tags =
        getLabelsForVersionedFolder(
            DATAPLANE_PLUGIN_NAME, folderPathWithSource(DATAPLANE_PLUGIN_NAME, folderPath));
    assertEquals(tags.getTags().size(), 2);
    assertTrue(tags.getTags().containsAll(labelList));
  }

  @Test
  public void testVersionedSetFolderWikiNoFeatureFlag() throws Exception {
    // Arrange
    SourceService sourceService = l(SourceService.class);
    final String folderName = generateUniqueFolderName();
    final List<String> folderPath = generateFolderPath(Collections.singletonList(folderName));
    runQuery(createFolderQuery(DATAPLANE_PLUGIN_NAME, folderPath));
    final List<String> folderPathWithSource =
        folderPathWithSource(DATAPLANE_PLUGIN_NAME, Collections.singletonList(folderName));
    // Act
    Folder folder =
        sourceService.getFolder(
            new SourceName(DATAPLANE_PLUGIN_NAME),
            new SourceFolderPath(folderPathWithSource),
            false,
            "dremio",
            "BRANCH",
            "main");
    String versionedDatasetIdAsString = folder.getId();
    // create wiki
    Wiki newWiki = new Wiki("sample wiki text", null);
    disableWikiLabel();

    // Assert
    UserExceptionMapper.ErrorMessageWithContext error =
        expectError(
            FamilyExpectation.CLIENT_ERROR,
            getBuilder(
                    getPublicAPI(3)
                        .path("catalog")
                        .path(PathUtils.encodeURIComponent(versionedDatasetIdAsString))
                        .path("collaboration")
                        .path("wiki"))
                .buildPost(Entity.json(newWiki)),
            UserExceptionMapper.ErrorMessageWithContext.class);
    assertTrue(
        error.getErrorMessage().contains("Wiki and Label not supported on entities in source"));
  }

  @Test
  public void testVersionedGetFolderWikiNoFeatureFlag() throws Exception {
    // Arrange
    SourceService sourceService = l(SourceService.class);
    final String folderName = generateUniqueFolderName();
    final List<String> folderPath = generateFolderPath(Collections.singletonList(folderName));
    runQuery(createFolderQuery(DATAPLANE_PLUGIN_NAME, folderPath));

    // Act
    Folder folder =
        sourceService.getFolder(
            new SourceName(DATAPLANE_PLUGIN_NAME),
            new SourceFolderPath(folderPathWithSource(DATAPLANE_PLUGIN_NAME, folderPath)),
            false,
            "dremio",
            "BRANCH",
            "main");
    String versionedDatasetIdAsString = folder.getId();
    disableWikiLabel();

    // Assert
    UserExceptionMapper.ErrorMessageWithContext error =
        expectError(
            FamilyExpectation.CLIENT_ERROR,
            getBuilder(
                    getPublicAPI(3)
                        .path("catalog")
                        .path(PathUtils.encodeURIComponent(versionedDatasetIdAsString))
                        .path("collaboration")
                        .path("tag"))
                .buildGet(),
            UserExceptionMapper.ErrorMessageWithContext.class);
    assertTrue(
        error.getErrorMessage().contains("Wiki and Label not supported on entities in source"));
  }

  @Test
  public void testVersionedSetFolderTagNoFeatureFlag() throws Exception {
    // Arrange
    SourceService sourceService = l(SourceService.class);
    final String folderName = generateUniqueFolderName();
    final List<String> folderPath = generateFolderPath(Collections.singletonList(folderName));
    runQuery(createFolderQuery(DATAPLANE_PLUGIN_NAME, folderPath));

    // Act
    Folder folder =
        sourceService.getFolder(
            new SourceName(DATAPLANE_PLUGIN_NAME),
            new SourceFolderPath(folderPathWithSource(DATAPLANE_PLUGIN_NAME, folderPath)),
            false,
            "dremio",
            "BRANCH",
            "main");
    String versionedDatasetIdAsString = folder.getId();
    Wiki newWiki = new Wiki("sample wiki text", null);
    disableWikiLabel();

    // Assert
    List<String> tagList = Arrays.asList("tag1", "tag2");
    Tags newTags = new Tags(tagList, null);
    UserExceptionMapper.ErrorMessageWithContext error =
        expectError(
            FamilyExpectation.CLIENT_ERROR,
            getBuilder(
                    getPublicAPI(3)
                        .path("catalog")
                        .path(PathUtils.encodeURIComponent(versionedDatasetIdAsString))
                        .path("collaboration")
                        .path("tag"))
                .buildPost(Entity.json(newTags)),
            UserExceptionMapper.ErrorMessageWithContext.class);
    assertTrue(
        error.getErrorMessage().contains("Wiki and Label not supported on entities in source"));
  }

  @Test
  public void testVersionedGetFolderTagNoFeatureFlag() throws Exception {
    // Arrange
    SourceService sourceService = l(SourceService.class);
    final String folderName = generateUniqueFolderName();
    final List<String> folderPath = generateFolderPath(Collections.singletonList(folderName));
    runQuery(createFolderQuery(DATAPLANE_PLUGIN_NAME, folderPath));

    // Act
    Folder folder =
        sourceService.getFolder(
            new SourceName(DATAPLANE_PLUGIN_NAME),
            new SourceFolderPath(folderPathWithSource(DATAPLANE_PLUGIN_NAME, folderPath)),
            false,
            "dremio",
            "BRANCH",
            "main");
    String versionedDatasetIdAsString = folder.getId();
    disableWikiLabel();

    // Assert
    UserExceptionMapper.ErrorMessageWithContext error =
        expectError(
            FamilyExpectation.CLIENT_ERROR,
            getBuilder(
                    getPublicAPI(3)
                        .path("catalog")
                        .path(PathUtils.encodeURIComponent(versionedDatasetIdAsString))
                        .path("collaboration")
                        .path("tag"))
                .buildGet(),
            UserExceptionMapper.ErrorMessageWithContext.class);
    assertTrue(
        error.getErrorMessage().contains("Wiki and Label not supported on entities in source"));
  }

  @Test
  public void testVersionedUpdateLabelOnFolder() throws Exception {
    // Arrange
    final String folderName = generateUniqueFolderName();
    final List<String> folderPath = generateFolderPath(Collections.singletonList(folderName));
    runQuery(
        createFolderAtQuery(
            DATAPLANE_PLUGIN_NAME, folderPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME)));

    // Act
    // create new tags
    List<String> labelList = Arrays.asList("label1", "label2");
    Tags newLabels = new Tags(labelList, null);
    List<String> sourceFolderPath = folderPathWithSource(DATAPLANE_PLUGIN_NAME, folderPath);
    setLabelsForVersionedFolder(DATAPLANE_PLUGIN_NAME, sourceFolderPath, newLabels);

    Tags readTags = getLabelsForVersionedFolder(DATAPLANE_PLUGIN_NAME, sourceFolderPath);
    assertEquals(readTags.getTags().size(), 2);
    assertTrue(readTags.getTags().containsAll(labelList));

    // Update tags
    List<String> updatedLabelList = Arrays.asList("label1", "label2", "label3");
    Tags updatedLabels = new Tags(updatedLabelList, readTags.getVersion());
    Tags setLabels =
        setLabelsForVersionedFolder(DATAPLANE_PLUGIN_NAME, sourceFolderPath, updatedLabels);

    // Assert
    assertEquals(setLabels.getTags().size(), 3);
    assertTrue(setLabels.getTags().containsAll(updatedLabelList));
  }

  @Test
  public void testVersionedUpdateWikiOnFolder() throws Exception {
    // Arrange

    final String folderName = generateUniqueFolderName();
    final List<String> folderPath = generateFolderPath(Collections.singletonList(folderName));
    final List<String> folderPathWithSource =
        folderPathWithSource(DATAPLANE_PLUGIN_NAME, folderPath);
    runQuery(createFolderQuery(DATAPLANE_PLUGIN_NAME, folderPath));

    // Act
    // create new wiki
    Wiki newWiki = new Wiki("Folder wiki", null);
    setWikiForVersionedFolder(DATAPLANE_PLUGIN_NAME, folderPathWithSource, newWiki);
    Wiki readWiki = getWikiForVersionedFolder(DATAPLANE_PLUGIN_NAME, folderPathWithSource);
    assertEquals(readWiki.getText(), newWiki.getText());
    // Update wiki
    Wiki updatedWiki = new Wiki("updated wiki text", readWiki.getVersion());
    readWiki = setWikiForVersionedFolder(DATAPLANE_PLUGIN_NAME, folderPathWithSource, updatedWiki);

    // Assert
    assertEquals(readWiki.getText(), updatedWiki.getText());
  }

  @Test
  public void testVersionedSetFolderWikiOnBranch() throws Exception {
    // Arrange
    SourceService sourceService = l(SourceService.class);
    final String folderName = generateUniqueFolderName();
    final List<String> folderPath = generateFolderPath(Collections.singletonList(folderName));
    runQuery(createFolderQuery(DATAPLANE_PLUGIN_NAME, folderPath));

    String devBranchName = generateUniqueBranchName();
    String sessionId =
        runQueryAndGetSessionId(createBranchAtBranchQuery(devBranchName, DEFAULT_BRANCH_NAME));
    // create folder at dev
    runQueryInSession(useBranchQuery(devBranchName), sessionId);
    List<String> folderPathInBranch =
        generateFolderPath(Collections.singletonList(generateUniqueFolderName()));
    runQueryInSession(
        createFolderAtQuery(
            DATAPLANE_PLUGIN_NAME, folderPathInBranch, VersionContext.ofBranch(devBranchName)),
        sessionId);

    // Act
    Folder folderInBranch =
        sourceService.getFolder(
            new SourceName(DATAPLANE_PLUGIN_NAME),
            new SourceFolderPath(folderPathWithSource(DATAPLANE_PLUGIN_NAME, folderPathInBranch)),
            false,
            "dremio",
            "BRANCH",
            devBranchName);
    String versionedDatasetIdAsString = folderInBranch.getId();
    // create wiki
    Wiki newWiki = new Wiki("sample wiki text", null);

    // Assert
    UserExceptionMapper.ErrorMessageWithContext error =
        expectError(
            FamilyExpectation.CLIENT_ERROR,
            getBuilder(
                    getPublicAPI(3)
                        .path("catalog")
                        .path(PathUtils.encodeURIComponent(versionedDatasetIdAsString))
                        .path("collaboration")
                        .path("wiki"))
                .buildPost(Entity.json(newWiki)),
            UserExceptionMapper.ErrorMessageWithContext.class);
    assertTrue(
        error.getErrorMessage().contains("Wiki and Label can only be set on the default branch"));
  }

  /** Helpers * */
  protected static AutoCloseable enableWikiLabel() {
    setSystemOption(
        CatalogOptions.WIKILABEL_ENABLED_FOR_VERSIONED_SOURCE_DEFAULT_BRANCH.getOptionName(),
        "true");
    return () ->
        setSystemOption(
            CatalogOptions.WIKILABEL_ENABLED_FOR_VERSIONED_SOURCE_DEFAULT_BRANCH.getOptionName(),
            CatalogOptions.WIKILABEL_ENABLED_FOR_VERSIONED_SOURCE_DEFAULT_BRANCH
                .getDefault()
                .getBoolVal()
                .toString());
  }

  protected static AutoCloseable disableWikiLabel() {
    setSystemOption(
        CatalogOptions.WIKILABEL_ENABLED_FOR_VERSIONED_SOURCE_DEFAULT_BRANCH.getOptionName(),
        "false");
    return () ->
        setSystemOption(
            CatalogOptions.WIKILABEL_ENABLED_FOR_VERSIONED_SOURCE_DEFAULT_BRANCH.getOptionName(),
            CatalogOptions.WIKILABEL_ENABLED_FOR_VERSIONED_SOURCE_DEFAULT_BRANCH
                .getDefault()
                .getBoolVal()
                .toString());
  }

  private Wiki getWikiForVersionedEntity(List<String> fullPath) {
    DatasetSummary summary =
        expectSuccess(
            getBuilder(getAPIv2().path("/datasets/summary" + PathUtils.toFSPath(fullPath)))
                .buildGet(),
            DatasetSummary.class);
    String versionedDatasetIdAsString = summary.getEntityId();
    Wiki returnedWiki =
        expectSuccess(
            getBuilder(
                    getPublicAPI(3)
                        .path("catalog")
                        .path(PathUtils.encodeURIComponent(versionedDatasetIdAsString))
                        .path("collaboration")
                        .path("wiki"))
                .buildGet(),
            Wiki.class);
    return returnedWiki;
  }

  private Wiki setWikiForVersionedEntity(List<String> fullPath, Wiki wiki) {
    DatasetSummary summary =
        expectSuccess(
            getBuilder(getAPIv2().path("/datasets/summary" + PathUtils.toFSPath(fullPath)))
                .buildGet(),
            DatasetSummary.class);
    String versionedDatasetIdAsString = summary.getEntityId();
    Wiki createdWiki =
        expectSuccess(
            getBuilder(
                    getPublicAPI(3)
                        .path("catalog")
                        .path(PathUtils.encodeURIComponent(versionedDatasetIdAsString))
                        .path("collaboration")
                        .path("wiki"))
                .buildPost(Entity.json(wiki)),
            Wiki.class);
    return createdWiki;
  }

  private Tags setLabelsForVersionedEntity(List<String> fullPath, Tags labelList) {
    DatasetSummary summary =
        expectSuccess(
            getBuilder(getAPIv2().path("/datasets/summary" + PathUtils.toFSPath(fullPath)))
                .buildGet(),
            DatasetSummary.class);
    String versionedDatasetIdAsString = summary.getEntityId();

    // create new labels
    Tags createdlabels =
        expectSuccess(
            getBuilder(
                    getPublicAPI(3)
                        .path("catalog")
                        .path(PathUtils.encodeURIComponent(versionedDatasetIdAsString))
                        .path("collaboration")
                        .path("tag"))
                .buildPost(Entity.json(labelList)),
            Tags.class);
    return createdlabels;
  }

  private Tags getLabelsForVersionedEntity(List<String> fullPath) {
    DatasetSummary summary =
        expectSuccess(
            getBuilder(getAPIv2().path("/datasets/summary" + PathUtils.toFSPath(fullPath)))
                .buildGet(),
            DatasetSummary.class);
    String versionedDatasetIdAsString = summary.getEntityId();

    // Get labels
    Tags readlabels =
        expectSuccess(
            getBuilder(
                    getPublicAPI(3)
                        .path("catalog")
                        .path(PathUtils.encodeURIComponent(versionedDatasetIdAsString))
                        .path("collaboration")
                        .path("tag"))
                .buildGet(),
            Tags.class);
    return readlabels;
  }

  private Wiki getWikiForVersionedFolder(final String sourceName, List<String> sourceFolderPath)
      throws NamespaceException, IOException {
    SourceService sourceService = l(SourceService.class);
    Folder folder =
        sourceService.getFolder(
            new SourceName(sourceName),
            new SourceFolderPath(joinedTableKey(sourceFolderPath)),
            false,
            "dremio",
            "BRANCH",
            "main");
    String versionedDatasetIdAsString = folder.getId();
    Wiki returnedWiki =
        expectSuccess(
            getBuilder(
                    getPublicAPI(3)
                        .path("catalog")
                        .path(PathUtils.encodeURIComponent(versionedDatasetIdAsString))
                        .path("collaboration")
                        .path("wiki"))
                .buildGet(),
            Wiki.class);
    return returnedWiki;
  }

  private Wiki setWikiForVersionedFolder(
      final String sourceName, List<String> sourceFolderPath, Wiki wiki)
      throws NamespaceException, IOException {
    SourceService sourceService = l(SourceService.class);
    Folder folder =
        sourceService.getFolder(
            new SourceName(sourceName),
            new SourceFolderPath(joinedTableKey(sourceFolderPath)),
            false,
            "dremio",
            "BRANCH",
            "main");
    String versionedDatasetIdAsString = folder.getId();
    Wiki createdWiki =
        expectSuccess(
            getBuilder(
                    getPublicAPI(3)
                        .path("catalog")
                        .path(PathUtils.encodeURIComponent(versionedDatasetIdAsString))
                        .path("collaboration")
                        .path("wiki"))
                .buildPost(Entity.json(wiki)),
            Wiki.class);
    return createdWiki;
  }

  private Tags setLabelsForVersionedFolder(
      final String sourceName, List<String> sourceFolderPath, Tags labelList)
      throws NamespaceException, IOException {
    SourceService sourceService = l(SourceService.class);
    Folder folder =
        sourceService.getFolder(
            new SourceName(sourceName),
            new SourceFolderPath(joinedTableKey(sourceFolderPath)),
            false,
            "dremio",
            "BRANCH",
            "main");
    String versionedDatasetIdAsString = folder.getId();

    // create new labels
    Tags createdlabels =
        expectSuccess(
            getBuilder(
                    getPublicAPI(3)
                        .path("catalog")
                        .path(PathUtils.encodeURIComponent(versionedDatasetIdAsString))
                        .path("collaboration")
                        .path("tag"))
                .buildPost(Entity.json(labelList)),
            Tags.class);
    return createdlabels;
  }

  private Tags getLabelsForVersionedFolder(final String sourceName, List<String> sourceFolderPath)
      throws NamespaceException, IOException {
    SourceService sourceService = l(SourceService.class);
    Folder folder =
        sourceService.getFolder(
            new SourceName(sourceName),
            new SourceFolderPath(joinedTableKey(sourceFolderPath)),
            false,
            "dremio",
            "BRANCH",
            "main");
    String versionedDatasetIdAsString = folder.getId();
    // Get labels
    Tags readlabels =
        expectSuccess(
            getBuilder(
                    getPublicAPI(3)
                        .path("catalog")
                        .path(PathUtils.encodeURIComponent(versionedDatasetIdAsString))
                        .path("collaboration")
                        .path("tag"))
                .buildGet(),
            Tags.class);
    return readlabels;
  }

  private void createFolderInSpace(List<String> path, String folderName) throws NamespaceException {
    final List<String> folderPath = new ArrayList<>(path);
    folderPath.add(folderName);

    final FolderConfig config = new FolderConfig();
    config.setName(folderName);
    config.setFullPathList(folderPath);

    newNamespaceService().addOrUpdateFolder(new NamespaceKey(folderPath), config);
  }

  private void addWikiToNamespaceEntity(List<String> path, String text) throws Exception {
    final NameSpaceContainer container =
        newNamespaceService().getEntities(Collections.singletonList(new NamespaceKey(path))).get(0);
    final CollaborationHelper collaborationHelper = l(CollaborationHelper.class);

    collaborationHelper.setWiki(NamespaceUtils.getIdOrNull(container), new Wiki(text, null));
  }

  private void addlabelsToNamespaceEntity(List<String> path, List<String> labels) throws Exception {
    final NameSpaceContainer container =
        newNamespaceService().getEntities(Collections.singletonList(new NamespaceKey(path))).get(0);
    final CollaborationHelper collaborationHelper = l(CollaborationHelper.class);

    collaborationHelper.setTags(NamespaceUtils.getIdOrNull(container), new Tags(labels, null));
  }

  private String createSpaceAndVDS(NamespaceKey spacePath, List<String> vdsPath)
      throws NamespaceException {
    // create space
    final SpaceConfig spaceConfig = new SpaceConfig();
    spaceConfig.setName(spacePath.getRoot());
    newNamespaceService().addOrUpdateSpace(spacePath, spaceConfig);

    createVDS(vdsPath);
    return spaceConfig.getTag();
  }

  private void createVDS(List<String> vdsPath) {
    // create vds
    final VirtualDataset virtualDataset = new VirtualDataset();
    virtualDataset.setSql("select * from sys.version");

    final DatasetConfig datasetConfig = new DatasetConfig();
    datasetConfig.setName(vdsPath.get(vdsPath.size() - 1));
    datasetConfig.setFullPathList(vdsPath);
    datasetConfig.setType(DatasetType.VIRTUAL_DATASET);
    datasetConfig.setVirtualDataset(virtualDataset);

    getSabotContext()
        .getViewCreator(SystemUser.SYSTEM_USERNAME)
        .createView(vdsPath, "select * from sys.version", null, false);
  }
}
