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
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.DEFAULT_COUNT_COLUMN;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.convertFolderNameToList;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createBranchAtBranchQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createEmptyTableQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createFolderAtQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createFolderQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createTagQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.dropFolderAtQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.dropFolderAtQueryWithIfNotExists;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.dropFolderQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.dropTableIfExistsQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.dropTableQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.dropTableQueryWithAt;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.fullyQualifiedTableName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateFolderPath;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateNestedFolderPath;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueBranchName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueFolderName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueTableName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueTagName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.selectCountQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.sqlFolderPathToNamespaceKey;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.tablePathWithFolders;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.useBranchQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.useTagQuery;
import static com.dremio.exec.catalog.dataplane.TestDataplaneAssertions.assertIcebergTableExistsAtSubPath;
import static com.dremio.exec.catalog.dataplane.TestDataplaneAssertions.assertLastCommitMadeBySpecifiedAuthor;
import static com.dremio.exec.catalog.dataplane.TestDataplaneAssertions.assertNessieDoesNotHaveNamespace;
import static com.dremio.exec.catalog.dataplane.TestDataplaneAssertions.assertNessieDoesNotHaveTable;
import static com.dremio.exec.catalog.dataplane.TestDataplaneAssertions.assertNessieHasCommitForTable;
import static com.dremio.exec.catalog.dataplane.TestDataplaneAssertions.assertNessieHasNamespace;
import static com.dremio.exec.catalog.dataplane.TestDataplaneAssertions.assertNessieHasTable;
import static com.dremio.test.UserExceptionAssert.assertThatThrownBy;

import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.projectnessie.model.Operation;

import com.dremio.BaseTestQuery;
import com.dremio.catalog.model.VersionContext;


public class ITDataplanePluginDrop extends ITDataplanePluginTestSetup {

  @Test
  public void dropTable() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(tablePath));

    // Act
    runSQL(dropTableQuery(tablePath));
    // TODO Check for correct message

    // Assert
    assertNessieHasCommitForTable(tablePath, Operation.Delete.class, DEFAULT_BRANCH_NAME, this);
    assertNessieDoesNotHaveTable(tablePath, DEFAULT_BRANCH_NAME, this);
    // TODO For now, we aren't doing filesystem cleanup, so this check is correct. Might change in the future.
    assertIcebergTableExistsAtSubPath(tablePath);
  }

  @Test
  public void dropTableInMainWithAt() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(tablePath));

    // Act
    runSQL(dropTableQueryWithAt(tablePath, DEFAULT_BRANCH_NAME));
    // TODO Check for correct message

    // Assert
    assertNessieHasCommitForTable(tablePath, Operation.Delete.class, DEFAULT_BRANCH_NAME, this);
    assertNessieDoesNotHaveTable(tablePath, DEFAULT_BRANCH_NAME, this);
    // TODO For now, we aren't doing filesystem cleanup, so this check is correct. Might change in the future.
    assertIcebergTableExistsAtSubPath(tablePath);
  }

  @Test
  public void dropTableInDevWithAt() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = Collections.singletonList(tableName);
    final String devBranch = generateUniqueBranchName();

    runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));
    runSQL(useBranchQuery(devBranch));
    runSQL(createEmptyTableQuery(tablePath));

    // Act
    runSQL(dropTableQueryWithAt(tablePath, devBranch));
    // TODO Check for correct message

    // Assert
    assertNessieHasCommitForTable(tablePath, Operation.Delete.class, devBranch, this);
    assertNessieDoesNotHaveTable(tablePath, devBranch, this);
    // TODO For now, we aren't doing filesystem cleanup, so this check is correct. Might change in the future.
    assertIcebergTableExistsAtSubPath(tablePath);
  }

  @Test
  public void dropNonExistentTable() {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    // Act and Assert
    assertQueryThrowsExpectedError(dropTableQuery(tablePath),
      "does not exist");
  }

  @Test
  public void dropIfExistsNonExistentTable() throws Exception {
    // Arrange
    // Expect no error for non existent table
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    // Act
    runSQL(dropTableIfExistsQuery(tablePath));

    // Assert
    // No exception
  }

  @Test
  public void dropIfExistsExistingTable() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(tablePath));

    // Act
    runSQL(dropTableIfExistsQuery(tablePath));

    // Assert
    // No exception
  }

  @Test
  public void dropExistingTableTwice() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(tablePath));
    // Ensure contents
    assertNessieHasCommitForTable(tablePath, Operation.Put.class, DEFAULT_BRANCH_NAME, this);
    assertNessieHasTable(tablePath, DEFAULT_BRANCH_NAME, this);
    assertIcebergTableExistsAtSubPath(tablePath);

    // First drop
    runSQL(dropTableQuery(tablePath));
    // Assert removal of key from Nessie
    assertNessieDoesNotHaveTable(tablePath, DEFAULT_BRANCH_NAME, this);
    // Contents must still exist
    assertIcebergTableExistsAtSubPath(tablePath);

    // Act
    // Try second drop
    assertQueryThrowsExpectedError(dropTableQuery(tablePath),
      "does not exist");

    // Assert
    // Contents must still exist
    assertIcebergTableExistsAtSubPath(tablePath);
  }

  @Test
  public void dropSameTableNameDifferentBranches() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final String devBranchName = generateUniqueBranchName();

    // Create a dev branch from main
    runSQL(createBranchAtBranchQuery(devBranchName, DEFAULT_BRANCH_NAME));

    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(tablePath));
    assertTableHasExpectedNumRows(tablePath, 0);

    // Switch to branch dev
    runSQL(useBranchQuery(devBranchName));
    // Now try to create the same table in that branch - should succeed.
    createFolders(tablePath, VersionContext.ofBranch(devBranchName));
    runSQL(createEmptyTableQuery(tablePath));
    assertTableHasExpectedNumRows(tablePath, 0);

    // Act
    runSQL(dropTableQuery(tablePath));
    // ensure it's dropped in dev branch
    assertQueryThrowsExpectedError(selectCountQuery(tablePath, DEFAULT_COUNT_COLUMN),
      String.format("Object '%s' not found within '%s'", tablePath.get(0), DATAPLANE_PLUGIN_NAME));
    // Switch back to main
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));

    // Assert
    // Ensure table is still there
    assertTableHasExpectedNumRows(tablePath, 0);

    runSQL(dropTableQuery(tablePath));
    // Now ensure it's gone in main too
    assertQueryThrowsExpectedError(selectCountQuery(tablePath, DEFAULT_COUNT_COLUMN),
      String.format("Object '%s' not found within '%s'", tablePath.get(0), DATAPLANE_PLUGIN_NAME));
  }

  @Test
  public void dropTableInNonBranchVersionContext() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final String tag = generateUniqueTagName();
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(tablePath));

    runSQL(createTagQuery(tag, DEFAULT_BRANCH_NAME));
    runSQL(useTagQuery(tag));

    // Act and Assert
    assertQueryThrowsExpectedError(dropTableQuery(tablePath),
            String.format("DDL and DML operations are only supported for branches - not on tags or commits. %s is not a branch.",
                    tag));
  }

  @Test
  public void dropFolder() throws Exception {
    // Arrange
    final String folderName = generateUniqueFolderName();
    final List<String> sqlFolderPath = generateFolderPath(folderName);
    final List<String> namespaceKey = sqlFolderPathToNamespaceKey(sqlFolderPath);

    // Act
    runSQL(createFolderQuery(sqlFolderPath));

    // Assert
    assertLastCommitMadeBySpecifiedAuthor(DEFAULT_BRANCH_NAME, this);
    assertNessieHasNamespace(namespaceKey, DEFAULT_BRANCH_NAME, this);

    runSQL(dropFolderQuery(sqlFolderPath));

    assertLastCommitMadeBySpecifiedAuthor(DEFAULT_BRANCH_NAME, this);
    assertNessieDoesNotHaveNamespace(namespaceKey, DEFAULT_BRANCH_NAME, this);
  }

  @Test
  public void dropNestedFolder() throws Exception {
    // Arrange
    final String folderName = generateUniqueFolderName();
    final List<String> sqlFolderPath = generateFolderPath(folderName);
    runSQL(createFolderQuery(sqlFolderPath));

    final String folderName2 = generateUniqueFolderName();
    final List<String> sqlFolderPath2 = generateNestedFolderPath(folderName, folderName2);
    final List<String> namespaceKey = sqlFolderPathToNamespaceKey(sqlFolderPath2);
    runSQL(createFolderQuery(sqlFolderPath2));

    // Assert
    assertLastCommitMadeBySpecifiedAuthor(DEFAULT_BRANCH_NAME, this);
    assertNessieHasNamespace(namespaceKey, DEFAULT_BRANCH_NAME, this);

    // drop nested folder
    runSQL(dropFolderQuery(sqlFolderPath2));

    // Assert nested folder was dropped and its parent is still there
    assertLastCommitMadeBySpecifiedAuthor(DEFAULT_BRANCH_NAME, this);
    assertNessieDoesNotHaveNamespace(namespaceKey, DEFAULT_BRANCH_NAME, this);
    assertNessieHasNamespace(sqlFolderPathToNamespaceKey(sqlFolderPath), DEFAULT_BRANCH_NAME, this);
  }

  @Test
  public void dropFolderWithSingleElementWithContext() throws Exception {
    //set context
    BaseTestQuery.test(String.format("USE %s", DATAPLANE_PLUGIN_NAME));
    // Arrange
    final String folderName = generateUniqueFolderName();
    final List<String> sqlFolderPath = convertFolderNameToList(folderName);
    //since sqlFolderPath only has the name of the folder, its namespaceKey should be DATAPLANE_PLUGIN_NAME.folderName
    final List<String> namespaceKey = sqlFolderPathToNamespaceKey(generateFolderPath(folderName));

    runSQL(createFolderQuery(sqlFolderPath));

    // Act
    assertLastCommitMadeBySpecifiedAuthor(DEFAULT_BRANCH_NAME, this);
    assertNessieHasNamespace(namespaceKey, DEFAULT_BRANCH_NAME, this);

    runSQL(dropFolderQuery(sqlFolderPath));

    // Act
    assertLastCommitMadeBySpecifiedAuthor(DEFAULT_BRANCH_NAME, this);
    assertNessieDoesNotHaveNamespace(namespaceKey, DEFAULT_BRANCH_NAME, this);
  }

  @Test
  public void dropFolderUsingAt() throws Exception {
    // Arrange
    final String folderName = generateUniqueFolderName();
    final List<String> sqlFolderPath = generateFolderPath(folderName);
    final List<String> namespaceKey = sqlFolderPathToNamespaceKey(sqlFolderPath);

    runSQL(createFolderAtQuery(sqlFolderPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME)));

    // Act
    assertLastCommitMadeBySpecifiedAuthor(DEFAULT_BRANCH_NAME, this);
    assertNessieHasNamespace(namespaceKey, DEFAULT_BRANCH_NAME, this);

    runSQL(dropFolderAtQuery(sqlFolderPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME)));

    assertLastCommitMadeBySpecifiedAuthor(DEFAULT_BRANCH_NAME, this);
    assertNessieDoesNotHaveNamespace(namespaceKey, DEFAULT_BRANCH_NAME, this);
  }

  @Test
  public void dropNonExistentFolder() throws Exception {
    // Arrange
    final String folderName = generateUniqueFolderName();
    final List<String> sqlFolderPath = generateFolderPath(folderName);
    final List<String> namespaceKey = sqlFolderPathToNamespaceKey(sqlFolderPath);

    // Act
    runSQL(createFolderQuery(sqlFolderPath));

    // Assert
    assertLastCommitMadeBySpecifiedAuthor(DEFAULT_BRANCH_NAME, this);
    assertNessieHasNamespace(namespaceKey, DEFAULT_BRANCH_NAME, this);

    final String nonExistentFolderName = generateUniqueFolderName();
    final List<String> nonExistentFolderPath = generateFolderPath(nonExistentFolderName);

    assertThatThrownBy(() -> runSQL(dropFolderQuery(nonExistentFolderPath)))
      .hasMessageContaining(String.format("Key '%s' does not exist", nonExistentFolderName));
  }

  @Test
  public void dropNonExistentFolderWithIfNotExists() throws Exception {
    // Arrange
    final String folderName = generateUniqueFolderName();
    final List<String> sqlFolderPath = generateFolderPath(folderName);
    final List<String> namespaceKey = sqlFolderPathToNamespaceKey(sqlFolderPath);

    // Act
    runSQL(createFolderQuery(sqlFolderPath));

    // Assert
    assertLastCommitMadeBySpecifiedAuthor(DEFAULT_BRANCH_NAME, this);
    assertNessieHasNamespace(namespaceKey, DEFAULT_BRANCH_NAME, this);

    VersionContext versionContext = VersionContext.ofBranch(DEFAULT_BRANCH_NAME);

    final String nonExistentFolderName = generateUniqueFolderName();
    final List<String> nonExistentFolderPath = generateFolderPath(nonExistentFolderName);

    runSQL(dropFolderAtQueryWithIfNotExists(nonExistentFolderPath, versionContext));
  }

  @Test
  public void forgetTableNotSupported() throws Exception {
    // Arrange
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(tablePath));

    // Act + Assert
    String forgetTableQuery = String.format("ALTER TABLE %s FORGET METADATA", fullyQualifiedTableName(DATAPLANE_PLUGIN_NAME, tablePath));
    assertThatThrownBy(() -> runSQL(forgetTableQuery))
      .hasMessageContaining(String.format("Forget table is not a valid operation for objects in '%s' which is a versioned source.", DATAPLANE_PLUGIN_NAME));
  }

  @Test
  public void dropFolderWithContext() throws Exception {
    // Arrange
    final String folderName = generateUniqueFolderName();
    final List<String> sqlFolderPath = generateFolderPath(folderName);
    final List<String> namespaceKey = sqlFolderPathToNamespaceKey(sqlFolderPath);
    final String devBranch = generateUniqueBranchName();
    runSQL(createFolderAtQuery(sqlFolderPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME)));
    runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));

    // Act
    runSQL(useBranchQuery(devBranch));
    runSQL(dropFolderQuery(sqlFolderPath));

    //Assert
    assertNessieHasNamespace(namespaceKey, DEFAULT_BRANCH_NAME, this);
    assertNessieDoesNotHaveNamespace(namespaceKey, devBranch, this);
  }

  @Test
  public void dropFolderWithContextAndMultipleBranches() throws Exception {
    // Arrange
    final String folderName = generateUniqueFolderName();
    final List<String> sqlFolderPath = generateFolderPath(folderName);
    final List<String> namespaceKey = sqlFolderPathToNamespaceKey(sqlFolderPath);
    final String devBranch = generateUniqueBranchName();
    final String devBranch2 = generateUniqueBranchName();

    runSQL(createFolderAtQuery(sqlFolderPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME)));
    runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));
    runSQL(createBranchAtBranchQuery(devBranch2, devBranch));

    // Act
    runSQL(useBranchQuery(devBranch));
    runSQL(dropFolderAtQuery(sqlFolderPath, VersionContext.ofBranch(devBranch2)));

    //Assert
    assertNessieHasNamespace(namespaceKey, DEFAULT_BRANCH_NAME, this);
    assertNessieHasNamespace(namespaceKey, devBranch, this);
    assertNessieDoesNotHaveNamespace(namespaceKey, devBranch2, this);
  }
}
