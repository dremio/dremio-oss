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
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.DEFAULT_COLUMN_DEFINITION;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.DEFAULT_COUNT_COLUMN;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.convertFolderNameToList;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createBranchAtBranchQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createEmptyTableQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createFolderAtQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createFolderQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createTagQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.dropBranchQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.dropTableQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateFolderPath;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateNestedFolderPath;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueBranchName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueFolderName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueTableName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueTagName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.mergeBranchQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.selectCountQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.sqlFolderPathToNamespaceKey;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.tablePathWithFolders;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.useBranchQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.useTagQuery;
import static com.dremio.exec.catalog.dataplane.ITDataplanePluginTestSetup.createFolders;
import static com.dremio.exec.catalog.dataplane.TestDataplaneAssertions.assertIcebergTableExistsAtSubPath;
import static com.dremio.exec.catalog.dataplane.TestDataplaneAssertions.assertLastCommitMadeBySpecifiedAuthor;
import static com.dremio.exec.catalog.dataplane.TestDataplaneAssertions.assertNessieDoesNotHaveTable;
import static com.dremio.exec.catalog.dataplane.TestDataplaneAssertions.assertNessieHasCommitForTable;
import static com.dremio.exec.catalog.dataplane.TestDataplaneAssertions.assertNessieHasNamespace;
import static com.dremio.exec.catalog.dataplane.TestDataplaneAssertions.assertNessieHasTable;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.projectnessie.model.Operation;

import com.dremio.BaseTestQuery;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.VersionContext;

/**
 *
 * To run these tests, run through container class ITDataplanePlugin
 * To run all tests run {@link ITDataplanePlugin.NestedCreateTests}
 * To run single test, see instructions at the top of {@link ITDataplanePlugin}
 */
public class CreateTestCases {
  private ITDataplanePluginTestSetup base;

  CreateTestCases(ITDataplanePluginTestSetup base) {
    this.base = base;
  }


  @Test
  public void checkCreateSourceWithWrongUrl() throws Exception {
    // Arrange + Act + Assert

    assertThatThrownBy(() -> base.setUpDataplanePluginWithWrongUrl())
      .isInstanceOf(UserException.class)
      .hasMessageContaining("must be a valid http or https address");
  }

  @Test
  public void createEmptyTable() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    // Act
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));
    // TODO Check for correct message

    // Assert
    assertNessieHasCommitForTable(tablePath, Operation.Put.class, DEFAULT_BRANCH_NAME, base);
    assertNessieHasTable(tablePath, DEFAULT_BRANCH_NAME, base);
    assertIcebergTableExistsAtSubPath(tablePath);
  }

  @Test
  public void createEmptyTableTwice() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));
    assertNessieHasCommitForTable(tablePath, Operation.Put.class, DEFAULT_BRANCH_NAME, base);
    assertNessieHasTable(tablePath, DEFAULT_BRANCH_NAME, base);
    assertIcebergTableExistsAtSubPath(tablePath);

    // Act and Assert
    base.assertQueryThrowsExpectedError(
      createEmptyTableQuery(tablePath),
      "A table with the given name already exists");
  }

  @Test
  public void useNonExistentBranch() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final String invalidBranch = "xyz";

    base.runSQL(createBranchAtBranchQuery(invalidBranch, DEFAULT_BRANCH_NAME));
    base.runSQL(dropBranchQuery(invalidBranch));

    // Act and Assert
    base.assertQueryThrowsExpectedError(useBranchQuery(invalidBranch),
      String.format("%s not found", invalidBranch));
  }

  @Test
  public void createTableInNonBranchVersionContext() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final String tag = generateUniqueTagName();

    base.runSQL(createTagQuery(tag, DEFAULT_BRANCH_NAME));
    base.runSQL(useTagQuery(tag));

    // Act and Assert
    base.assertQueryThrowsExpectedError(createEmptyTableQuery(tablePath),
      String.format("DDL and DML operations are only supported for branches - not on tags or commits. %s is not a branch.",
        tag));
  }

  @Test
  public void createInDiffBranchesAndMerge() throws Exception {
    // Arrange
    final List<String> shareFolderPath = generateFolderPath(generateUniqueFolderName());
    final String mainTableName = generateUniqueTableName();
    final String devTableName = generateUniqueTableName();
    final List<String> mainTablePath = tablePathWithFolders(mainTableName);
    final List<String> devTablePath = tablePathWithFolders(devTableName);
    final String devBranchName = generateUniqueBranchName();

    // Creating an arbitrary commit to Nessie to make a common ancestor between two branches otherwise
    // those are un-related branches
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    createFolders(shareFolderPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));

    // Create a dev branch from main
    base.runSQL(createBranchAtBranchQuery(devBranchName, DEFAULT_BRANCH_NAME));

    // Set context to main
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    createFolders(mainTablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(mainTablePath));
    base.assertTableHasExpectedNumRows(mainTablePath, 0);

    // switch to branch dev
    base.runSQL(useBranchQuery(devBranchName));
    // Check that table does not exist in Nessie in branch dev (since it was branched off before create table)
    base.assertQueryThrowsExpectedError(selectCountQuery(mainTablePath, DEFAULT_COUNT_COLUMN),
      String.format("VALIDATION ERROR: Object '%s' not found within '%s",
        mainTablePath.get(0),
        DATAPLANE_PLUGIN_NAME));
    createFolders(devTablePath, VersionContext.ofBranch(devBranchName));
    base.runSQL(createEmptyTableQuery(devTablePath));
    base.assertTableHasExpectedNumRows(devTablePath, 0);

    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    // Check that dev table cannot be seen in branch main
    base.assertQueryThrowsExpectedError(selectCountQuery(devTablePath, DEFAULT_COUNT_COLUMN),
      String.format("VALIDATION ERROR: Object '%s' not found within '%s",
        devTablePath.get(0),
        DATAPLANE_PLUGIN_NAME));

    // Act
    base.runSQL(mergeBranchQuery(devBranchName, DEFAULT_BRANCH_NAME));

    // Assert
    // Table must now be visible in main.
   base.assertTableHasExpectedNumRows(devTablePath, 0);

    // Drop tables
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    base.runSQL(dropTableQuery(mainTablePath));
    base.runSQL(dropTableQuery(devTablePath));
  }

  @Test
  public void createAfterDrop() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));
    assertNessieHasCommitForTable(tablePath, Operation.Put.class, DEFAULT_BRANCH_NAME, base);
    assertNessieHasTable(tablePath, DEFAULT_BRANCH_NAME, base);
    assertIcebergTableExistsAtSubPath(tablePath);

    base.runSQL(dropTableQuery(tablePath));
    assertNessieDoesNotHaveTable(tablePath, DEFAULT_BRANCH_NAME, base);
    assertIcebergTableExistsAtSubPath(tablePath);

    // Act
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));

    // Assert
    assertNessieHasCommitForTable(tablePath, Operation.Put.class, DEFAULT_BRANCH_NAME, base);
    assertNessieHasTable(tablePath, DEFAULT_BRANCH_NAME, base);
    assertIcebergTableExistsAtSubPath(tablePath);
    base.runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void createEmptyTableInvalidPluginName() {
    // Arrange
    final String invalidDataplanePlugin = "invalid_plugin";
    final String tableName = generateUniqueTableName();
    final String createInvTableDirQuery = String.format(
      "CREATE TABLE %s.%s %s",
      invalidDataplanePlugin,
      tableName,
      DEFAULT_COLUMN_DEFINITION);

    // Act and Assert
    base.assertQueryThrowsExpectedError(createInvTableDirQuery,
      String.format("Invalid path. Given path, [%s.%s] is not valid", invalidDataplanePlugin, tableName));
  }

  @Test
  public void checkTableVisibilityInDerivedBranch() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));
    base.assertTableHasExpectedNumRows(tablePath, 0);

    final String devBranch = generateUniqueBranchName();

    // Act
    base.runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));

    // Assert
    // Table must be visible in dev
    base.runSQL(useBranchQuery(devBranch));
    base.assertTableHasExpectedNumRows(tablePath, 0);

    // Cleanup
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    base.runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void checkTableVisibilityInParentBranch() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final String devBranch = generateUniqueBranchName();

    // Create a dev branch from main
    base.runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));
    base.runSQL(useBranchQuery(devBranch));
    createFolders(tablePath, VersionContext.ofBranch(devBranch));
    base.runSQL(createEmptyTableQuery(tablePath));
    base.assertTableHasExpectedNumRows(tablePath, 0);

    // Act and Assert
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));

    // Check that table created in dev branch cannot be seen in branch main
    base.assertQueryThrowsExpectedError(selectCountQuery(tablePath, DEFAULT_COUNT_COLUMN),
      String.format("VALIDATION ERROR: Object '%s' not found within '%s'",
        tablePath.get(0),
        DATAPLANE_PLUGIN_NAME));
  }

  @Test
  public void createFolder() throws Exception {
    // Arrange
    final String folderName = generateUniqueFolderName();
    final List<String> sqlFolderPath = generateFolderPath(folderName);
    final List<String> namespaceKey = sqlFolderPathToNamespaceKey(sqlFolderPath);

    // Act
    base.runSQL(createFolderQuery(sqlFolderPath));

    // Assert
    assertLastCommitMadeBySpecifiedAuthor(DEFAULT_BRANCH_NAME, base);
    assertNessieHasNamespace(namespaceKey, DEFAULT_BRANCH_NAME, base);
  }

  @Test
  public void createNestedFolder() throws Exception {
    // Arrange
    final String folderName = generateUniqueFolderName();
    final List<String> sqlFolderPath = generateFolderPath(folderName);
    base.runSQL(createFolderQuery(sqlFolderPath));

    final String folderName2 = generateUniqueFolderName();
    final List<String> sqlFolderPath2 = generateNestedFolderPath(folderName, folderName2);
    final List<String> namespaceKey = sqlFolderPathToNamespaceKey(sqlFolderPath2);
    base.runSQL(createFolderQuery(sqlFolderPath2));

    // Assert
    assertLastCommitMadeBySpecifiedAuthor(DEFAULT_BRANCH_NAME, base);
    assertNessieHasNamespace(namespaceKey, DEFAULT_BRANCH_NAME, base);
  }

  @Test
  public void createFolderWithSingleElementWithContext() throws Exception {
    BaseTestQuery.test(String.format("USE %s", DATAPLANE_PLUGIN_NAME));
    // Arrange
    final String folderName = generateUniqueFolderName();
    final List<String> sqlFolderPath = convertFolderNameToList(folderName);
    //since sqlFolderPath only has the name of the folder, its namespaceKey should be DATAPLANE_PLUGIN_NAME.folderName
    final List<String> namespaceKey = sqlFolderPathToNamespaceKey(generateFolderPath(folderName));

    base.runSQL(createFolderQuery(sqlFolderPath));

    // Act
    assertLastCommitMadeBySpecifiedAuthor(DEFAULT_BRANCH_NAME, base);
    assertNessieHasNamespace(namespaceKey, DEFAULT_BRANCH_NAME, base);
  }

  @Test
  public void createFolderUsingAt() throws Exception {
    // Arrange
    final String folderName = generateUniqueFolderName();
    final List<String> sqlFolderPath = generateFolderPath(folderName);
    final List<String> namespaceKey = sqlFolderPathToNamespaceKey(sqlFolderPath);

    base.runSQL(createFolderAtQuery(sqlFolderPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME)));

    // Act
    assertLastCommitMadeBySpecifiedAuthor(DEFAULT_BRANCH_NAME, base);
    assertNessieHasNamespace(namespaceKey, DEFAULT_BRANCH_NAME, base);
  }

  @Test
  public void createWithImplicitFolders() throws Exception {
    // Arrange
    final String folderName = generateUniqueFolderName();
    final String folderName2 = generateUniqueFolderName();
    final List<String> sqlFolderPath2 = generateNestedFolderPath(folderName, folderName2);
    // Assert
    base.assertQueryThrowsExpectedError(createFolderQuery(sqlFolderPath2),
      String.format("VALIDATION ERROR: Namespace '%s' must exist.",
        folderName));
  }

  @Test
  public void createTableWithImplicitFolders() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    base.assertQueryThrowsExpectedError(createEmptyTableQuery(tablePath),
      String.format("VALIDATION ERROR: Namespace '%s' must exist.",
        String.join(".", tablePath.subList(0, tablePath.size()-1))));
  }
}
