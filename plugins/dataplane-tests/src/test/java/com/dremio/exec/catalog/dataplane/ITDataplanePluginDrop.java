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
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.DEFAULT_COUNT_COLUMN;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createBranchAtBranchQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createEmptyTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createFolderAtQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createFolderQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createTagQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.dropFolderAtQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.dropFolderAtQueryWithIfNotExists;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.dropFolderQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.dropTableIfExistsQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.dropTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.dropTableQueryWithAt;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.fullyQualifiedTableName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateNestedFolderPath;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueBranchName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueFolderName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueTableName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueTagName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.selectCountQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.tablePathWithFolders;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.useBranchQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.useTagQuery;
import static com.dremio.exec.catalog.dataplane.test.TestDataplaneAssertions.assertIcebergTableExistsAtSubPath;
import static com.dremio.exec.catalog.dataplane.test.TestDataplaneAssertions.assertLastCommitMadeBySpecifiedAuthor;
import static com.dremio.exec.catalog.dataplane.test.TestDataplaneAssertions.assertNessieDoesNotHaveNamespace;
import static com.dremio.exec.catalog.dataplane.test.TestDataplaneAssertions.assertNessieDoesNotHaveTable;
import static com.dremio.exec.catalog.dataplane.test.TestDataplaneAssertions.assertNessieHasCommitForTable;
import static com.dremio.exec.catalog.dataplane.test.TestDataplaneAssertions.assertNessieHasNamespace;
import static com.dremio.exec.catalog.dataplane.test.TestDataplaneAssertions.assertNessieHasTable;
import static com.dremio.test.UserExceptionAssert.assertThatThrownBy;

import com.dremio.BaseTestQuery;
import com.dremio.catalog.model.VersionContext;
import com.dremio.exec.catalog.dataplane.test.ITDataplanePluginTestSetup;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.projectnessie.model.Operation;

public class ITDataplanePluginDrop extends ITDataplanePluginTestSetup {

  @Test
  public void dropTable() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createEmptyTableQuery(tablePath));

    // Act
    runSQL(dropTableQuery(tablePath));
    // TODO Check for correct message

    // Assert
    assertNessieHasCommitForTable(tablePath, Operation.Delete.class, DEFAULT_BRANCH_NAME, this);
    assertNessieDoesNotHaveTable(tablePath, DEFAULT_BRANCH_NAME, this);
    // TODO For now, we aren't doing filesystem cleanup, so this check is correct. Might change in
    // the future.
    assertIcebergTableExistsAtSubPath(tablePath);
  }

  @Test
  public void dropTableInMainWithAt() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createEmptyTableQuery(tablePath));

    // Act
    runSQL(dropTableQueryWithAt(tablePath, DEFAULT_BRANCH_NAME));
    // TODO Check for correct message

    // Assert
    assertNessieHasCommitForTable(tablePath, Operation.Delete.class, DEFAULT_BRANCH_NAME, this);
    assertNessieDoesNotHaveTable(tablePath, DEFAULT_BRANCH_NAME, this);
    // TODO For now, we aren't doing filesystem cleanup, so this check is correct. Might change in
    // the future.
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
    // TODO For now, we aren't doing filesystem cleanup, so this check is correct. Might change in
    // the future.
    assertIcebergTableExistsAtSubPath(tablePath);
  }

  @Test
  public void dropNonExistentTable() {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    // Act and Assert
    assertQueryThrowsExpectedError(dropTableQuery(tablePath), "does not exist");
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
    assertQueryThrowsExpectedError(dropTableQuery(tablePath), "does not exist");

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
    runSQL(createEmptyTableQuery(tablePath));
    assertTableHasExpectedNumRows(tablePath, 0);

    // Switch to branch dev
    runSQL(useBranchQuery(devBranchName));
    // Now try to create the same table in that branch - should succeed.
    runSQL(createEmptyTableQuery(tablePath));
    assertTableHasExpectedNumRows(tablePath, 0);

    // Act
    runSQL(dropTableQuery(tablePath));
    // ensure it's dropped in dev branch
    assertQueryThrowsExpectedError(
        selectCountQuery(tablePath, DEFAULT_COUNT_COLUMN),
        String.format(
            "Object '%s' not found within '%s'", tablePath.get(0), DATAPLANE_PLUGIN_NAME));
    // Switch back to main
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));

    // Assert
    // Ensure table is still there
    assertTableHasExpectedNumRows(tablePath, 0);

    runSQL(dropTableQuery(tablePath));
    // Now ensure it's gone in main too
    assertQueryThrowsExpectedError(
        selectCountQuery(tablePath, DEFAULT_COUNT_COLUMN),
        String.format(
            "Object '%s' not found within '%s'", tablePath.get(0), DATAPLANE_PLUGIN_NAME));
  }

  @Test
  public void dropTableInNonBranchVersionContext() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final String tag = generateUniqueTagName();
    runSQL(createEmptyTableQuery(tablePath));

    runSQL(createTagQuery(tag, DEFAULT_BRANCH_NAME));
    runSQL(useTagQuery(tag));

    // Act and Assert
    assertQueryThrowsExpectedError(
        dropTableQuery(tablePath),
        String.format(
            "DDL and DML operations are only supported for branches - not on tags or commits. %s is not a branch.",
            tag));
  }

  @Test
  public void dropFolder() throws Exception {
    // Arrange
    final String folderName = generateUniqueFolderName();
    final List<String> folderPath = Collections.singletonList(folderName);

    // Act
    runSQL(createFolderQuery(DATAPLANE_PLUGIN_NAME, folderPath));

    // Assert
    assertLastCommitMadeBySpecifiedAuthor(DEFAULT_BRANCH_NAME, this);
    assertNessieHasNamespace(folderPath, DEFAULT_BRANCH_NAME, this);

    runSQL(dropFolderQuery(DATAPLANE_PLUGIN_NAME, folderPath));

    assertLastCommitMadeBySpecifiedAuthor(DEFAULT_BRANCH_NAME, this);
    assertNessieDoesNotHaveNamespace(folderPath, DEFAULT_BRANCH_NAME, this);
  }

  @Test
  public void dropNestedFolder() throws Exception {
    // Arrange
    final String folderName = generateUniqueFolderName();
    final List<String> folderPath = Collections.singletonList(folderName);
    runSQL(createFolderQuery(DATAPLANE_PLUGIN_NAME, folderPath));

    final String folderName2 = generateUniqueFolderName();
    final List<String> folderPath2 = generateNestedFolderPath(folderName, folderName2);
    runSQL(createFolderQuery(DATAPLANE_PLUGIN_NAME, folderPath2));

    // Assert
    assertLastCommitMadeBySpecifiedAuthor(DEFAULT_BRANCH_NAME, this);
    assertNessieHasNamespace(folderPath, DEFAULT_BRANCH_NAME, this);

    // drop nested folder
    runSQL(dropFolderQuery(DATAPLANE_PLUGIN_NAME, folderPath2));

    // Assert nested folder was dropped and its parent is still there
    assertLastCommitMadeBySpecifiedAuthor(DEFAULT_BRANCH_NAME, this);
    assertNessieDoesNotHaveNamespace(folderPath2, DEFAULT_BRANCH_NAME, this);
    assertNessieHasNamespace(folderPath, DEFAULT_BRANCH_NAME, this);
  }

  @Test
  public void dropFolderWithSingleElementWithContext() throws Exception {
    // set context
    BaseTestQuery.test(String.format("USE %s", DATAPLANE_PLUGIN_NAME));
    // Arrange
    final String folderName = generateUniqueFolderName();
    final List<String> folderPath = Collections.singletonList(folderName);
    // since sqlFolderPath only has the name of the folder, its namespaceKey should be
    // DATAPLANE_PLUGIN_NAME.folderName

    runSQL(createFolderQuery(DATAPLANE_PLUGIN_NAME, folderPath));

    // Act
    assertLastCommitMadeBySpecifiedAuthor(DEFAULT_BRANCH_NAME, this);
    assertNessieHasNamespace(folderPath, DEFAULT_BRANCH_NAME, this);

    runSQL(dropFolderQuery(DATAPLANE_PLUGIN_NAME, folderPath));

    // Act
    assertLastCommitMadeBySpecifiedAuthor(DEFAULT_BRANCH_NAME, this);
    assertNessieDoesNotHaveNamespace(folderPath, DEFAULT_BRANCH_NAME, this);
  }

  @Test
  public void dropFolderUsingAt() throws Exception {
    // Arrange
    final String folderName = generateUniqueFolderName();
    final List<String> folderPath = Collections.singletonList(folderName);

    runSQL(
        createFolderAtQuery(
            DATAPLANE_PLUGIN_NAME, folderPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME)));

    // Act
    assertLastCommitMadeBySpecifiedAuthor(DEFAULT_BRANCH_NAME, this);
    assertNessieHasNamespace(folderPath, DEFAULT_BRANCH_NAME, this);

    runSQL(
        dropFolderAtQuery(
            DATAPLANE_PLUGIN_NAME, folderPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME)));

    assertLastCommitMadeBySpecifiedAuthor(DEFAULT_BRANCH_NAME, this);
    assertNessieDoesNotHaveNamespace(folderPath, DEFAULT_BRANCH_NAME, this);
  }

  @Test
  public void dropNonExistentFolder() throws Exception {
    // Arrange
    final String folderName = generateUniqueFolderName();
    final List<String> folderPath = Collections.singletonList(folderName);

    // Act
    runSQL(createFolderQuery(DATAPLANE_PLUGIN_NAME, folderPath));

    // Assert
    assertLastCommitMadeBySpecifiedAuthor(DEFAULT_BRANCH_NAME, this);
    assertNessieHasNamespace(folderPath, DEFAULT_BRANCH_NAME, this);

    final String nonExistentFolderName = generateUniqueFolderName();
    final List<String> nonExistentFolderPath = Collections.singletonList(nonExistentFolderName);

    assertThatThrownBy(() -> runSQL(dropFolderQuery(DATAPLANE_PLUGIN_NAME, nonExistentFolderPath)))
        .hasMessageContaining(String.format("Key '%s' does not exist", nonExistentFolderName));
  }

  @Test
  public void dropNonExistentFolderWithIfNotExists() throws Exception {
    // Arrange
    final String folderName = generateUniqueFolderName();
    final List<String> folderPath = Collections.singletonList(folderName);

    // Act
    runSQL(createFolderQuery(DATAPLANE_PLUGIN_NAME, folderPath));

    // Assert
    assertLastCommitMadeBySpecifiedAuthor(DEFAULT_BRANCH_NAME, this);
    assertNessieHasNamespace(folderPath, DEFAULT_BRANCH_NAME, this);

    VersionContext versionContext = VersionContext.ofBranch(DEFAULT_BRANCH_NAME);

    final String nonExistentFolderName = generateUniqueFolderName();
    final List<String> nonExistentFolderPath = Collections.singletonList(nonExistentFolderName);

    runSQL(
        dropFolderAtQueryWithIfNotExists(
            DATAPLANE_PLUGIN_NAME, nonExistentFolderPath, versionContext));
  }

  @Test
  public void forgetTableNotSupported() throws Exception {
    // Arrange
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createEmptyTableQuery(tablePath));

    // Act + Assert
    String forgetTableQuery =
        String.format(
            "ALTER TABLE %s FORGET METADATA",
            fullyQualifiedTableName(DATAPLANE_PLUGIN_NAME, tablePath));
    assertThatThrownBy(() -> runSQL(forgetTableQuery))
        .hasMessageContaining(
            String.format(
                "Forget table is not a valid operation for objects in '%s' which is a versioned source.",
                DATAPLANE_PLUGIN_NAME));
  }

  @Test
  public void dropFolderWithContext() throws Exception {
    // Arrange
    final String folderName = generateUniqueFolderName();
    final List<String> folderPath = Collections.singletonList(folderName);
    final String devBranch = generateUniqueBranchName();
    runSQL(
        createFolderAtQuery(
            DATAPLANE_PLUGIN_NAME, folderPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME)));
    runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));

    // Act
    runSQL(useBranchQuery(devBranch));
    runSQL(dropFolderQuery(DATAPLANE_PLUGIN_NAME, folderPath));

    // Assert
    assertNessieHasNamespace(folderPath, DEFAULT_BRANCH_NAME, this);
    assertNessieDoesNotHaveNamespace(folderPath, devBranch, this);
  }

  @Test
  public void dropFolderWithContextAndMultipleBranches() throws Exception {
    // Arrange
    final String folderName = generateUniqueFolderName();
    final List<String> folderPath = Collections.singletonList(folderName);
    final String devBranch = generateUniqueBranchName();
    final String devBranch2 = generateUniqueBranchName();

    runSQL(
        createFolderAtQuery(
            DATAPLANE_PLUGIN_NAME, folderPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME)));
    runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));
    runSQL(createBranchAtBranchQuery(devBranch2, devBranch));

    // Act
    runSQL(useBranchQuery(devBranch));
    runSQL(
        dropFolderAtQuery(DATAPLANE_PLUGIN_NAME, folderPath, VersionContext.ofBranch(devBranch2)));

    // Assert
    assertNessieHasNamespace(folderPath, DEFAULT_BRANCH_NAME, this);
    assertNessieHasNamespace(folderPath, devBranch, this);
    assertNessieDoesNotHaveNamespace(folderPath, devBranch2, this);
  }
}
