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
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.DEFAULT_COLUMN_DEFINITION;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.DEFAULT_COUNT_COLUMN;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.convertFolderNameToList;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createBranchAtBranchQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createBranchFromBranchQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createEmptyTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createEmptyTableQueryWithAt;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createEmptyTableWithTablePropertiesQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createFolderAtQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createFolderQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createTableAsQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createTableQueryWithAt;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createTagQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createTagQueryWithFrom;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.dropBranchForceQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.dropTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.dropTagQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateNestedFolderPath;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueBranchName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueFolderName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueTableName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueTagName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.joinedTableKey;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.mergeBranchQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.quoted;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.selectCountQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.showObjectWithSpecifierQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.showTablePropertiesQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.tablePathWithFolders;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.useBranchQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.useTagQuery;
import static com.dremio.exec.catalog.dataplane.test.TestDataplaneAssertions.assertIcebergTableExistsAtSubPath;
import static com.dremio.exec.catalog.dataplane.test.TestDataplaneAssertions.assertLastCommitMadeBySpecifiedAuthor;
import static com.dremio.exec.catalog.dataplane.test.TestDataplaneAssertions.assertNessieDoesNotHaveEntity;
import static com.dremio.exec.catalog.dataplane.test.TestDataplaneAssertions.assertNessieDoesNotHaveNamespace;
import static com.dremio.exec.catalog.dataplane.test.TestDataplaneAssertions.assertNessieHasCommitForTable;
import static com.dremio.exec.catalog.dataplane.test.TestDataplaneAssertions.assertNessieHasNamespace;
import static com.dremio.exec.catalog.dataplane.test.TestDataplaneAssertions.assertNessieHasTable;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dremio.BaseTestQuery;
import com.dremio.catalog.model.VersionContext;
import com.dremio.common.exceptions.UserRemoteException;
import com.dremio.exec.catalog.dataplane.test.ITDataplanePluginTestSetup;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.sabot.rpc.user.QueryDataBatch;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.projectnessie.model.Operation;

public class ITDataplanePluginCreate extends ITDataplanePluginTestSetup {

  @Test
  public void createEmptyTable() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    // Act
    runSQL(createEmptyTableQuery(tablePath));
    // TODO Check for correct message

    // Assert
    assertNessieHasCommitForTable(tablePath, Operation.Put.class, DEFAULT_BRANCH_NAME, this);
    assertNessieHasTable(tablePath, DEFAULT_BRANCH_NAME, this);
    assertIcebergTableExistsAtSubPath(tablePath);
  }

  @Test
  public void createEmptyTableWithTableProperties() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    // Act
    runSQL(createEmptyTableWithTablePropertiesQuery(tablePath));

    // Assert
    assertNessieHasCommitForTable(tablePath, Operation.Put.class, DEFAULT_BRANCH_NAME, this);
    assertNessieHasTable(tablePath, DEFAULT_BRANCH_NAME, this);
    assertIcebergTableExistsAtSubPath(tablePath);

    // check table properties
    List<QueryDataBatch> queryDataBatches =
        testRunAndReturn(UserBitShared.QueryType.SQL, showTablePropertiesQuery(tablePath));
    String resultString = getResultString(queryDataBatches, ",", false);
    assertThat(resultString).isNotNull().contains("property_name");
  }

  @Test
  public void createEmptyTableTwice() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createEmptyTableQuery(tablePath));
    assertNessieHasCommitForTable(tablePath, Operation.Put.class, DEFAULT_BRANCH_NAME, this);
    assertNessieHasTable(tablePath, DEFAULT_BRANCH_NAME, this);
    assertIcebergTableExistsAtSubPath(tablePath);

    // Act and Assert
    assertQueryThrowsExpectedError(
        createEmptyTableQuery(tablePath),
        String.format(
            "A table or view with given name [%s.%s] already exists",
            DATAPLANE_PLUGIN_NAME, joinedTableKey(tablePath)));
  }

  @Test
  public void createEmptyTableInMainWithAtSyntax() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    // Act
    runSQL(createEmptyTableQueryWithAt(tablePath, DEFAULT_BRANCH_NAME));
    // TODO Check for correct message

    // Assert
    assertNessieHasCommitForTable(tablePath, Operation.Put.class, DEFAULT_BRANCH_NAME, this);
    assertNessieHasTable(tablePath, DEFAULT_BRANCH_NAME, this);
    assertIcebergTableExistsAtSubPath(tablePath);
  }

  @Test
  public void createEmptyTableInDevWithAtSyntax() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = Collections.singletonList(tableName);
    final String devBranch = generateUniqueBranchName();

    // Act
    runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQueryWithAt(tablePath, devBranch));

    // Assert
    assertNessieHasCommitForTable(tablePath, Operation.Put.class, devBranch, this);
    assertNessieHasTable(tablePath, devBranch, this);
    assertIcebergTableExistsAtSubPath(tablePath);
  }

  @Test
  void createEmptyTableInBranchWithFromSyntax() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = Collections.singletonList(tableName);
    final String devBranch = generateUniqueBranchName();

    // Act
    runSQL(createBranchFromBranchQuery(devBranch, DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQueryWithAt(tablePath, devBranch));

    // Assert
    assertNessieHasCommitForTable(tablePath, Operation.Put.class, devBranch, this);
    assertNessieHasTable(tablePath, devBranch, this);
    assertIcebergTableExistsAtSubPath(tablePath);
  }

  @Test
  public void useNonExistentBranch() throws Exception {
    // Arrange
    final String invalidBranch = "xyz";

    runSQL(createBranchAtBranchQuery(invalidBranch, DEFAULT_BRANCH_NAME));
    runSQL(dropBranchForceQuery(invalidBranch));

    // Act and Assert
    assertQueryThrowsExpectedError(
        useBranchQuery(invalidBranch), String.format("%s not found", invalidBranch));
  }

  @Test
  public void createTableInNonBranchVersionContext() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final String tag = generateUniqueTagName();

    runSQL(createTagQuery(tag, DEFAULT_BRANCH_NAME));
    runSQL(useTagQuery(tag));

    // Act and Assert
    assertQueryThrowsExpectedError(
        createEmptyTableQuery(tablePath),
        String.format(
            "DDL and DML operations are only supported for branches - not on tags or commits. %s is not a branch.",
            tag));
  }

  @Test
  public void createInDiffBranchesAndMerge() throws Exception {
    // Arrange
    final List<String> shareFolderPath = Collections.singletonList(generateUniqueFolderName());
    final String mainTableName = generateUniqueTableName();
    final String devTableName = generateUniqueTableName();
    final List<String> mainTablePath = tablePathWithFolders(mainTableName);
    final List<String> devTablePath = tablePathWithFolders(devTableName);
    final String devBranchName = generateUniqueBranchName();

    // Creating an arbitrary commit to Nessie to make a common ancestor between two branches
    // otherwise
    // those are un-related branches
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    runSQL(
        createFolderAtQuery(
            DATAPLANE_PLUGIN_NAME, shareFolderPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME)));

    // Create a dev branch from main
    runSQL(createBranchAtBranchQuery(devBranchName, DEFAULT_BRANCH_NAME));

    // Set context to main
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(mainTablePath));
    assertTableHasExpectedNumRows(mainTablePath, 0);

    // switch to branch dev
    runSQL(useBranchQuery(devBranchName));
    // Check that table does not exist in Nessie in branch dev (since it was branched off before
    // create table)
    assertQueryThrowsExpectedError(
        selectCountQuery(mainTablePath, DEFAULT_COUNT_COLUMN),
        String.format(
            "VALIDATION ERROR: Object '%s' not found within '%s",
            mainTablePath.get(0), DATAPLANE_PLUGIN_NAME));
    runSQL(createEmptyTableQuery(devTablePath));
    assertTableHasExpectedNumRows(devTablePath, 0);

    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    // Check that dev table cannot be seen in branch main
    assertQueryThrowsExpectedError(
        selectCountQuery(devTablePath, DEFAULT_COUNT_COLUMN),
        String.format(
            "VALIDATION ERROR: Object '%s' not found within '%s",
            devTablePath.get(0), DATAPLANE_PLUGIN_NAME));

    // Act
    runSQL(mergeBranchQuery(devBranchName, DEFAULT_BRANCH_NAME));

    // Assert
    // Table must now be visible in main.
    assertTableHasExpectedNumRows(devTablePath, 0);

    // Drop tables
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    runSQL(dropTableQuery(mainTablePath));
    runSQL(dropTableQuery(devTablePath));
  }

  @Test
  public void createAfterDrop() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createEmptyTableQuery(tablePath));
    assertNessieHasCommitForTable(tablePath, Operation.Put.class, DEFAULT_BRANCH_NAME, this);
    assertNessieHasTable(tablePath, DEFAULT_BRANCH_NAME, this);
    assertIcebergTableExistsAtSubPath(tablePath);

    runSQL(dropTableQuery(tablePath));
    assertNessieDoesNotHaveEntity(tablePath, DEFAULT_BRANCH_NAME, this);
    assertIcebergTableExistsAtSubPath(tablePath);

    // Act
    runSQL(createEmptyTableQuery(tablePath));

    // Assert
    assertNessieHasCommitForTable(tablePath, Operation.Put.class, DEFAULT_BRANCH_NAME, this);
    assertNessieHasTable(tablePath, DEFAULT_BRANCH_NAME, this);
    assertIcebergTableExistsAtSubPath(tablePath);
    runSQL(dropTableQuery(tablePath));

    assertCommitLogTail(
        String.format("CREATE TABLE %s", joinedTableKey(tablePath)),
        String.format("DROP TABLE %s", joinedTableKey(tablePath)),
        String.format("CREATE TABLE %s", joinedTableKey(tablePath)),
        String.format("DROP TABLE %s", joinedTableKey(tablePath)));
  }

  @Test
  public void createEmptyTableInvalidPluginName() {
    // Arrange
    final String invalidDataplanePlugin = "invalid_plugin";
    final String tableName = generateUniqueTableName();
    final String createInvTableDirQuery =
        String.format(
            "CREATE TABLE %s.%s %s", invalidDataplanePlugin, tableName, DEFAULT_COLUMN_DEFINITION);

    // Act and Assert
    assertQueryThrowsExpectedError(
        createInvTableDirQuery,
        String.format(
            "Invalid path. Given path, [%s.%s] is not valid", invalidDataplanePlugin, tableName));
  }

  @Test
  public void checkTableVisibilityInDerivedBranch() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(tablePath));
    assertTableHasExpectedNumRows(tablePath, 0);

    final String devBranch = generateUniqueBranchName();

    // Act
    runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));

    // Assert
    // Table must be visible in dev
    runSQL(useBranchQuery(devBranch));
    assertTableHasExpectedNumRows(tablePath, 0);

    // Cleanup
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    runSQL(dropTableQuery(tablePath));

    assertCommitLogTail(
        VersionContext.ofBranch(DEFAULT_BRANCH_NAME),
        String.format("CREATE TABLE %s", joinedTableKey(tablePath)),
        String.format("DROP TABLE %s", joinedTableKey(tablePath)));

    // Table must remain visible in dev
    runSQL(useBranchQuery(devBranch));
    assertTableHasExpectedNumRows(tablePath, 0);

    assertCommitLogTail(
        VersionContext.ofBranch(devBranch),
        String.format("CREATE TABLE %s", joinedTableKey(tablePath))
        // NO DROP TABLE
        );
  }

  @Test
  public void checkTableVisibilityInParentBranch() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final String devBranch = generateUniqueBranchName();

    // Create a dev branch from main
    runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));
    runSQL(useBranchQuery(devBranch));
    runSQL(createEmptyTableQuery(tablePath));
    assertTableHasExpectedNumRows(tablePath, 0);

    // Act and Assert
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));

    // Check that table created in dev branch cannot be seen in branch main
    assertQueryThrowsExpectedError(
        selectCountQuery(tablePath, DEFAULT_COUNT_COLUMN),
        String.format(
            "VALIDATION ERROR: Object '%s' not found within '%s'",
            tablePath.get(0), DATAPLANE_PLUGIN_NAME));
  }

  @Test
  public void createFolder() throws Exception {
    // Arrange
    final String folderName = generateUniqueFolderName();
    final List<String> folderPath = Collections.singletonList(folderName);

    // Act
    runSQL(createFolderQuery(DATAPLANE_PLUGIN_NAME, folderPath));

    // Assert
    assertLastCommitMadeBySpecifiedAuthor(DEFAULT_BRANCH_NAME, this);
    assertNessieHasNamespace(folderPath, DEFAULT_BRANCH_NAME, this);
  }

  @Test
  public void createNestedFolder() throws Exception {
    // Arrange
    final String folderName1 = generateUniqueFolderName();
    final String folderName2 = generateUniqueFolderName();
    final List<String> folderPath = generateNestedFolderPath(folderName1, folderName2);
    runSQL(createFolderQuery(DATAPLANE_PLUGIN_NAME, folderPath));

    // Assert
    assertLastCommitMadeBySpecifiedAuthor(DEFAULT_BRANCH_NAME, this);
    assertNessieHasNamespace(folderPath, DEFAULT_BRANCH_NAME, this);
  }

  @Test
  public void createFolderWithSingleElementWithContext() throws Exception {
    BaseTestQuery.test(String.format("USE %s", DATAPLANE_PLUGIN_NAME));
    // Arrange
    final String folderName = generateUniqueFolderName();
    final List<String> sqlFolderPath = convertFolderNameToList(folderName);
    // since sqlFolderPath only has the name of the folder, its namespaceKey should be
    // DATAPLANE_PLUGIN_NAME.folderName

    runSQL(createFolderQuery(DATAPLANE_PLUGIN_NAME, sqlFolderPath));

    // Act
    assertLastCommitMadeBySpecifiedAuthor(DEFAULT_BRANCH_NAME, this);
    assertNessieHasNamespace(Collections.singletonList(folderName), DEFAULT_BRANCH_NAME, this);
  }

  @Test
  public void createFolderUsingAt() throws Exception {
    // Arrange
    final String folderName = generateUniqueFolderName();
    final List<String> folderPath = Collections.singletonList(folderName);

    runSQL(
        createFolderAtQuery(
            DATAPLANE_PLUGIN_NAME, folderPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME)));

    // Act
    assertLastCommitMadeBySpecifiedAuthor(DEFAULT_BRANCH_NAME, this);
    assertNessieHasNamespace(folderPath, DEFAULT_BRANCH_NAME, this);
  }

  @Test
  public void createTableWithImplicitFolders() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    // Run
    runSQL(createEmptyTableQuery(tablePath));

    // Assert
    assertNessieHasTable(tablePath, DEFAULT_BRANCH_NAME, this);
  }

  @Test
  public void testCreateFolderWithAtTag() throws Exception {
    String tagName = "myTag";
    final String folderName = generateUniqueFolderName();
    final List<String> folderPath = Collections.singletonList(folderName);
    runSQL(createTagQuery(tagName, DEFAULT_BRANCH_NAME));
    // expect error for TAG
    assertThatThrownBy(
            () ->
                runSQL(
                    createFolderAtQuery(
                        DATAPLANE_PLUGIN_NAME,
                        folderPath,
                        VersionContext.ofTag(DEFAULT_BRANCH_NAME))))
        .isInstanceOf(UserRemoteException.class);
  }

  @Test
  public void testCreateFolderWithAtCommit() throws Exception {
    String commitHash = "c7a79c74adf76649e643354c34ed69abfee5a3b070ef68cbe782a072b0a418ba";
    final String folderName = generateUniqueFolderName();
    final List<String> folderPath = Collections.singletonList(folderName);
    // expect error for TAG
    assertThatThrownBy(
            () ->
                runSQL(
                    createFolderAtQuery(
                        DATAPLANE_PLUGIN_NAME, folderPath, VersionContext.ofCommit(commitHash))))
        .isInstanceOf(UserRemoteException.class);
  }

  @Test
  public void testCreateTagFromNonExistentBranch() throws Exception {
    final String tagName = generateUniqueTagName();
    final String branchName = generateUniqueBranchName();

    // Assert
    assertQueryThrowsExpectedError(
        createTagQueryWithFrom(tagName, branchName),
        String.format(
            "VALIDATION ERROR: Source branch %s not found in source %s",
            branchName, DATAPLANE_PLUGIN_NAME));
  }

  @Test
  public void testCreateFolderWithContext() throws Exception {
    // Arrange
    final String folderName = generateUniqueFolderName();
    final List<String> folderPath = Collections.singletonList(folderName);

    // Act
    runSQL(createFolderQuery(DATAPLANE_PLUGIN_NAME, folderPath));

    // Assert
    assertLastCommitMadeBySpecifiedAuthor(DEFAULT_BRANCH_NAME, this);
    assertNessieHasNamespace(folderPath, DEFAULT_BRANCH_NAME, this);

    final String folderName2 = generateUniqueFolderName();
    final List<String> folderPath2 = Collections.singletonList(folderName2);
    final String branch2 = "branch2";

    runSQL(createBranchAtBranchQuery(branch2, DEFAULT_BRANCH_NAME));
    // set current context to branch2
    runSQL(useBranchQuery(branch2));
    runSQL(createFolderQuery(DATAPLANE_PLUGIN_NAME, folderPath2));

    // Assert that when we do not have [AT] token, we use
    // context as a default.
    assertLastCommitMadeBySpecifiedAuthor(branch2, this);
    assertNessieHasNamespace(folderPath2, branch2, this);
    assertNessieDoesNotHaveNamespace(folderPath2, DEFAULT_BRANCH_NAME, this);

    final String folderName3 = generateUniqueFolderName();
    final List<String> folderPath3 = Collections.singletonList(folderName3);
    final String branch3 = "branch3";

    // create folder3 at branch3 with current context branch2
    runSQL(createBranchAtBranchQuery(branch3, DEFAULT_BRANCH_NAME));
    runSQL(
        createFolderAtQuery(DATAPLANE_PLUGIN_NAME, folderPath3, VersionContext.ofBranch(branch3)));

    // the version context specified in AT token should override the context.
    // Therefore we have folder in branch3 not in branch2 nor main.
    assertLastCommitMadeBySpecifiedAuthor(branch3, this);
    assertNessieHasNamespace(folderPath3, branch3, this);
    assertNessieDoesNotHaveNamespace(folderPath3, DEFAULT_BRANCH_NAME, this);
    assertNessieDoesNotHaveNamespace(folderPath3, branch2, this);
  }

  @Test
  public void createEmptyTableWithSameNameInMultipleBranches() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = Collections.singletonList(tableName);
    final String devBranch = generateUniqueBranchName();

    // Creating branch, table in main and within devbranch
    runSQL(createBranchFromBranchQuery(devBranch, DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(tablePath));
    runSQL(createEmptyTableQueryWithAt(tablePath, devBranch));
  }

  @Test
  public void createTagsToCompareHashWithBranch() throws Exception {
    // Arrange
    final String tagWithFrom = generateUniqueTagName();
    final String tagWithAt = generateUniqueTagName();
    final String branchHash = getCommitHashForBranch(DEFAULT_BRANCH_NAME);

    // Act, create tag and get commitHash
    runSQL(createTagQueryWithFrom(tagWithFrom, DEFAULT_BRANCH_NAME));
    runSQL(createTagQuery(tagWithAt, DEFAULT_BRANCH_NAME));

    final String tagWithFromHash = getCommitHashForTag(tagWithFrom);
    final String tagWithAtHash = getCommitHashForTag(tagWithAt);

    // Assert
    assertThat(branchHash).isEqualTo(tagWithFromHash);
    assertThat(branchHash).isEqualTo(tagWithAtHash);

    // cleanup
    runSQL(dropTagQuery(tagWithFrom));
    runSQL(dropTagQuery(tagWithAt));
  }

  @Test
  public void createEmptyTableInDevAndMainWithAtSyntax() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = Collections.singletonList(tableName);
    final String devBranch = generateUniqueBranchName();

    // Act
    runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQueryWithAt(tablePath, DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQueryWithAt(tablePath, devBranch));

    // Assert
    assertNessieHasTable(tablePath, devBranch, this);
    assertNessieHasTable(tablePath, DEFAULT_BRANCH_NAME, this);
  }

  @Test
  public void createTableInDevAndMainWithAtSyntax() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = Collections.singletonList(tableName);
    final String devBranch = generateUniqueBranchName();

    // Act
    runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));
    runSQL(createTableQueryWithAt(tablePath, DEFAULT_BRANCH_NAME));
    runSQL(createTableQueryWithAt(tablePath, devBranch));

    // Assert
    assertNessieHasTable(tablePath, devBranch, this);
    assertNessieHasTable(tablePath, DEFAULT_BRANCH_NAME, this);
  }

  @Test
  public void createCoexistingTablesWithAtSyntax() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = Collections.singletonList(tableName);
    final String path = String.join(".", Arrays.asList(DATAPLANE_PLUGIN_NAME, tableName));

    // Act
    runSQL(createTableQueryWithAt(tablePath, DEFAULT_BRANCH_NAME));

    // Assert
    // Act and Assert
    assertQueryThrowsExpectedError(
        createTableQueryWithAt(tablePath, DEFAULT_BRANCH_NAME),
        String.format("A table or view with given name [%s] already exists", path));
  }

  @Test
  public void showLogsWithTimestamp() throws Exception {
    // Arrange
    final List<String> tablePath = Arrays.asList(generateUniqueTableName());
    final List<String> secondTablePath = Arrays.asList(generateUniqueTableName());

    // create table
    runSQL(createEmptyTableQuery(tablePath));
    final Instant timeInBetweenCommits = Instant.now();

    runSQL(createEmptyTableQuery(secondTablePath));
    final Instant timeAfterCommits = Instant.now();

    // ACT + ASSERT
    String showLogsWithTimestampBeforeCommit =
        String.format(
            "AT BRANCH %s AS OF '%s'", DEFAULT_BRANCH_NAME, Timestamp.from(timeInBetweenCommits));
    String showLogsWithTimestampAfterCommit =
        String.format(
            "AT BRANCH %s AS OF '%s'", DEFAULT_BRANCH_NAME, Timestamp.from(timeAfterCommits));
    List<List<String>> beforeLogs =
        runSqlWithResults(showObjectWithSpecifierQuery("LOGS", showLogsWithTimestampBeforeCommit));
    List<List<String>> afterLogs =
        runSqlWithResults(showObjectWithSpecifierQuery("LOGS", showLogsWithTimestampAfterCommit));
    assertThat(beforeLogs).hasSize(1);
    assertThat(afterLogs).hasSize(2);

    // Drop tables
    runSQL(dropTableQuery(tablePath));
    runSQL(dropTableQuery(secondTablePath));
  }

  @Test
  public void showTablesWithTimestamp() throws Exception {
    // Arrange
    final List<String> tablePath = Arrays.asList(generateUniqueTableName());
    final List<String> secondTablePath = Arrays.asList(generateUniqueTableName());

    // create table
    runSQL(createEmptyTableQuery(tablePath));
    final Instant timeInBetweenCommits = Instant.now();

    runSQL(createEmptyTableQuery(secondTablePath));
    final Instant timeAfterCommits = Instant.now();

    // ACT + ASSERT
    String showLogsWithTimestampBeforeCommit =
        String.format(
            "AT BRANCH %s AS OF '%s'", DEFAULT_BRANCH_NAME, Timestamp.from(timeInBetweenCommits));
    String showLogsWithTimestampAfterCommit =
        String.format(
            "AT BRANCH %s AS OF '%s'", DEFAULT_BRANCH_NAME, Timestamp.from(timeAfterCommits));
    List<List<String>> tablesBefore =
        runSqlWithResults(
            showObjectWithSpecifierQuery("TABLES", showLogsWithTimestampBeforeCommit));
    List<List<String>> tablesAfter =
        runSqlWithResults(showObjectWithSpecifierQuery("TABLES", showLogsWithTimestampAfterCommit));
    assertThat(tablesBefore).hasSize(1);
    assertThat(tablesAfter).hasSize(2);

    // Drop tables
    runSQL(dropTableQuery(tablePath));
    runSQL(dropTableQuery(secondTablePath));
  }

  @Test
  public void useContextWithTimestamp() throws Exception {
    // Arrange
    final List<String> tablePath = Arrays.asList(generateUniqueTableName());
    final List<String> secondTablePath = Arrays.asList(generateUniqueTableName());

    // create table
    runSQL(createEmptyTableQuery(tablePath));
    final Instant timeInBetweenCommits = Instant.now();

    runSQL(createEmptyTableQuery(secondTablePath));

    // ACT + ASSERT
    List<List<String>> afterLogs = runSqlWithResults("SHOW LOGS IN dataPlane_Test");
    String useContext =
        String.format(
            "USE BRANCH %s AS OF '%s' IN %s",
            DEFAULT_BRANCH_NAME, Timestamp.from(timeInBetweenCommits), DATAPLANE_PLUGIN_NAME);
    runSQL(useContext);
    List<List<String>> beforeLogs = runSqlWithResults("SHOW LOGS IN dataPlane_Test");
    assertThat(afterLogs).hasSize(2);
    assertThat(beforeLogs).hasSize(1);

    // Drop tables
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    runSQL(dropTableQuery(tablePath));
    runSQL(dropTableQuery(secondTablePath));
  }

  @Test
  public void useContextWithCommitAndTimestampThrowsException() throws Exception {
    // Arrange
    final Instant timeBeforeCommit = Instant.now();
    final String hashBeforeCommit = getCommitHashForBranch(DEFAULT_BRANCH_NAME);

    // Act + Assert
    String useContext =
        String.format(
            "USE COMMIT %s AS OF '%s' IN %s",
            quoted(hashBeforeCommit), Timestamp.from(timeBeforeCommit), DATAPLANE_PLUGIN_NAME);
    assertThatThrownBy(() -> runSQL(useContext))
        .isInstanceOf(UserRemoteException.class)
        .hasMessageContaining("Reference type COMMIT does not support specifying a timestamp.");
  }

  @Test
  public void testCreateTableErrorExistingFolder() throws Exception {
    // Arrange
    final String name = generateUniqueFolderName();
    final List<String> path = Collections.singletonList(name);
    runSQL(createFolderQuery(DATAPLANE_PLUGIN_NAME, path));

    // Act and Assert
    assertQueryThrowsExpectedError(
        createEmptyTableQuery(path),
        String.format(
            " An Entity of type FOLDER with given name [%s] already exists", joinedTableKey(path)));
  }

  @Test
  public void testCtasErrorExistingFolder() throws Exception {
    // Arrange
    final String name = generateUniqueFolderName();
    final List<String> path = Collections.singletonList(name);
    runSQL(createFolderQuery(DATAPLANE_PLUGIN_NAME, path));

    // Act and Assert
    assertQueryThrowsExpectedError(
        createTableAsQuery(path, 10),
        String.format(
            " An Entity of type FOLDER with given name [%s] already exists", joinedTableKey(path)));
  }
}
