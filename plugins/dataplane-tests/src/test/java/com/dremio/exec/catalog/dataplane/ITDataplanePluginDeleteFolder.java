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
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createEmptyTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createViewQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueFolderName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueTableName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueViewName;
import static com.dremio.exec.catalog.dataplane.test.TestDataplaneAssertions.assertNessieDoesNotHaveTable;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dremio.catalog.model.VersionContext;
import com.dremio.exec.catalog.dataplane.test.ITDataplanePluginTestSetup;
import com.dremio.exec.store.NessieNamespaceNotEmptyException;
import com.dremio.service.namespace.NamespaceKey;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.projectnessie.model.ContentKey;

public class ITDataplanePluginDeleteFolder extends ITDataplanePluginTestSetup {

  @Test
  public void deleteEmptyFolder() throws Exception {
    final String rootFolder = generateUniqueFolderName();
    final List<String> folderPath = Arrays.asList(DATAPLANE_PLUGIN_NAME, rootFolder);
    final NamespaceKey namespaceKey = new NamespaceKey(folderPath);
    final VersionContext version = VersionContext.ofBranch(DEFAULT_BRANCH_NAME);

    getDataplanePlugin().createNamespace(namespaceKey, version);
    getDataplanePlugin().deleteFolder(namespaceKey, version);

    // Assert
    assertNessieDoesNotHaveTable(Collections.singletonList(rootFolder), DEFAULT_BRANCH_NAME, this);
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

    getDataplanePlugin().createNamespace(namespaceKey, version);
    runSQL(createEmptyTableQuery(tablePath));

    // Assert
    assertThatThrownBy(() -> getDataplanePlugin().deleteFolder(namespaceKey, version))
        .isInstanceOf(NessieNamespaceNotEmptyException.class)
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

    getDataplanePlugin().createNamespace(namespaceKey, version);
    runSQL(createEmptyTableQuery(tablePath));

    final String viewName = generateUniqueViewName();
    List<String> viewKey = Arrays.asList(rootFolder, viewName);
    runSQL(createViewQuery(viewKey, tablePath));

    // Assert
    assertThatThrownBy(() -> getDataplanePlugin().deleteFolder(namespaceKey, version))
        .isInstanceOf(NessieNamespaceNotEmptyException.class)
        .hasMessageContaining("Folder '%s' is not empty", folderName);
  }

  @Test
  public void deleteNonEmptyFolderWithSubFolderThenThrowError() throws Exception {
    final String rootFolder = generateUniqueFolderName();
    final List<String> rootFolderPath = Arrays.asList(DATAPLANE_PLUGIN_NAME, rootFolder);
    final String leafFolder = generateUniqueFolderName();
    final List<String> leafFolderPath =
        Arrays.asList(DATAPLANE_PLUGIN_NAME, rootFolder, leafFolder);
    final NamespaceKey rootNamespaceKey = new NamespaceKey(rootFolderPath);
    final NamespaceKey leafNamespaceKey = new NamespaceKey(leafFolderPath);
    ContentKey contentKey = ContentKey.of(rootFolder);
    String folderName = contentKey.toPathString();
    final VersionContext version = VersionContext.ofBranch(DEFAULT_BRANCH_NAME);

    getDataplanePlugin().createNamespace(rootNamespaceKey, version);
    getDataplanePlugin().createNamespace(leafNamespaceKey, version);

    // Assert
    assertThatThrownBy(() -> getDataplanePlugin().deleteFolder(rootNamespaceKey, version))
        .isInstanceOf(NessieNamespaceNotEmptyException.class)
        .hasMessageContaining("Folder '%s' is not empty", folderName);
  }

  @Test
  public void deleteParentAndChildFolders() throws Exception {
    final String rootFolder = generateUniqueFolderName();
    final List<String> rootFolderPath = Arrays.asList(DATAPLANE_PLUGIN_NAME, rootFolder);
    final String leafFolder = generateUniqueFolderName();
    final List<String> leafFolderPath =
        Arrays.asList(DATAPLANE_PLUGIN_NAME, rootFolder, leafFolder);
    final NamespaceKey rootNamespaceKey = new NamespaceKey(rootFolderPath);
    final NamespaceKey leafNamespaceKey = new NamespaceKey(leafFolderPath);
    final VersionContext version = VersionContext.ofBranch(DEFAULT_BRANCH_NAME);

    getDataplanePlugin().createNamespace(rootNamespaceKey, version);
    getDataplanePlugin().createNamespace(leafNamespaceKey, version);

    // Delete child folder and then parent folder
    getDataplanePlugin().deleteFolder(leafNamespaceKey, version);
    getDataplanePlugin().deleteFolder(rootNamespaceKey, version);

    // Assert
    assertNessieDoesNotHaveTable(Collections.singletonList(rootFolder), DEFAULT_BRANCH_NAME, this);
  }
}
