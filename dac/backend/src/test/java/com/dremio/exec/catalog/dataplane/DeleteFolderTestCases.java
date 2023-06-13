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
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createEmptyTableQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createViewQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueFolderName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueTableName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueViewName;
import static com.dremio.exec.catalog.dataplane.TestDataplaneAssertions.assertNessieDoesNotHaveTable;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.projectnessie.model.ContentKey;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.VersionContext;
import com.dremio.service.namespace.NamespaceKey;
/**
 *
 * To run these tests, run through container class ITDataplanePlugin
 * To run all tests run {@link ITDataplanePlugin.NestedDeleteFolderTestCases}
 * To run single test, see instructions at the top of {@link ITDataplanePlugin}
 */
public class DeleteFolderTestCases {
  private ITDataplanePluginTestSetup base;

  DeleteFolderTestCases(ITDataplanePluginTestSetup base) {
    this.base = base;
  }

  @Test
  public void deleteEmptyFolder() throws Exception {
    final String rootFolder = generateUniqueFolderName();
    final List<String> folderPath = Arrays.asList(DATAPLANE_PLUGIN_NAME, rootFolder);
    final NamespaceKey namespaceKey = new NamespaceKey(folderPath);
    final VersionContext version = VersionContext.ofBranch(DEFAULT_BRANCH_NAME);

    base.getDataplanePlugin().createNamespace(namespaceKey, version);
    base.getDataplanePlugin().deleteFolder(namespaceKey, version);

    // Assert
    assertNessieDoesNotHaveTable(Arrays.asList(rootFolder), DEFAULT_BRANCH_NAME, base);
  }

  @Test
  public void deleteNonEmptyFolderWithTableThenThrowError() throws Exception {
    final String rootFolder = generateUniqueFolderName();
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = Arrays.asList(rootFolder, tableName);
    final List<String> folderPath = Arrays.asList(DATAPLANE_PLUGIN_NAME, rootFolder);
    final NamespaceKey namespaceKey = new NamespaceKey(folderPath);
    ContentKey contentKey = ContentKey.of(rootFolder);
    String folderName = contentKey.toPathString();
    final VersionContext version = VersionContext.ofBranch(DEFAULT_BRANCH_NAME);

    base.getDataplanePlugin().createNamespace(namespaceKey, version);
    base.runSQL(createEmptyTableQuery(tablePath));

    // Assert
    assertThatThrownBy(
      () -> base.getDataplanePlugin().deleteFolder(namespaceKey, version))
      .isInstanceOf(UserException.class)
      .hasMessageContaining("Folder '%s' is not empty", folderName);
  }

  @Test
  public void deleteNonEmptyFolderWithViewThenThrowError() throws Exception {
    final String rootFolder = generateUniqueFolderName();
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = Arrays.asList(rootFolder, tableName);
    final List<String> folderPath = Arrays.asList(DATAPLANE_PLUGIN_NAME, rootFolder);
    final NamespaceKey namespaceKey = new NamespaceKey(folderPath);
    ContentKey contentKey = ContentKey.of(rootFolder);
    String folderName = contentKey.toPathString();
    final VersionContext version = VersionContext.ofBranch(DEFAULT_BRANCH_NAME);

    base.getDataplanePlugin().createNamespace(namespaceKey, version);
    base.runSQL(createEmptyTableQuery(tablePath));

    final String viewName = generateUniqueViewName();
    List<String> viewKey = Arrays.asList(rootFolder, viewName);
    base.runSQL(createViewQuery(viewKey, tablePath));

    // Assert
    assertThatThrownBy(
      () -> base.getDataplanePlugin().deleteFolder(namespaceKey, version))
      .isInstanceOf(UserException.class)
      .hasMessageContaining("Folder '%s' is not empty", folderName);
  }

  @Test
  public void deleteNonEmptyFolderWithSubFolderThenThrowError() throws Exception {
    final String rootFolder = generateUniqueFolderName();
    final List<String> rootFolderPath = Arrays.asList(DATAPLANE_PLUGIN_NAME, rootFolder);
    final String leafFolder = generateUniqueFolderName();
    final List<String> leafFolderPath = Arrays.asList(DATAPLANE_PLUGIN_NAME, rootFolder, leafFolder);
    final NamespaceKey rootNamespaceKey = new NamespaceKey(rootFolderPath);
    final NamespaceKey leafNamespaceKey = new NamespaceKey(leafFolderPath);
    ContentKey contentKey = ContentKey.of(rootFolder);
    String folderName = contentKey.toPathString();
    final VersionContext version = VersionContext.ofBranch(DEFAULT_BRANCH_NAME);

    base.getDataplanePlugin().createNamespace(rootNamespaceKey, version);
    base.getDataplanePlugin().createNamespace(leafNamespaceKey, version);

    // Assert
    assertThatThrownBy(
      () -> base.getDataplanePlugin().deleteFolder(rootNamespaceKey, version))
      .isInstanceOf(UserException.class)
      .hasMessageContaining("Folder '%s' is not empty", folderName);
  }

  @Test
  public void deleteParentAndChildFolders() throws Exception {
    final String rootFolder = generateUniqueFolderName();
    final List<String> rootFolderPath = Arrays.asList(DATAPLANE_PLUGIN_NAME, rootFolder);
    final String leafFolder = generateUniqueFolderName();
    final List<String> leafFolderPath = Arrays.asList(DATAPLANE_PLUGIN_NAME, rootFolder, leafFolder);
    final NamespaceKey rootNamespaceKey = new NamespaceKey(rootFolderPath);
    final NamespaceKey leafNamespaceKey = new NamespaceKey(leafFolderPath);
    final VersionContext version = VersionContext.ofBranch(DEFAULT_BRANCH_NAME);

    base.getDataplanePlugin().createNamespace(rootNamespaceKey, version);
    base.getDataplanePlugin().createNamespace(leafNamespaceKey, version);

    // Delete child folder and then parent folder
    base.getDataplanePlugin().deleteFolder(leafNamespaceKey, version);
    base.getDataplanePlugin().deleteFolder(rootNamespaceKey, version);

    // Assert
    assertNessieDoesNotHaveTable(Arrays.asList(rootFolder), DEFAULT_BRANCH_NAME, base);
  }
}
