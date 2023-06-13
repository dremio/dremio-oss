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
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.alterBranchAssignBranchQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.alterBranchAssignCommitQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.alterBranchAssignTagQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.alterTagAssignBranchQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.alterTagAssignCommitQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.alterTagAssignTagQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createBranchAtBranchQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createBranchAtSpecifierQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createEmptyTableQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createTagQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.dropBranchQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.dropTableQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.dropTagQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueBranchName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueTableName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueTagName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.quoted;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.showBranchQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.showTagQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.tablePathWithFolders;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.useBranchQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.useTagQuery;
import static com.dremio.exec.catalog.dataplane.ITDataplanePluginTestSetup.createFolders;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.dremio.exec.catalog.VersionContext;
import com.google.common.collect.Sets;

/**
 *
 * To run these tests, run through container class ITDataplanePlugin
 * To run all tests run {@link ITDataplanePlugin.NestedBranchTagTests}
 * To run single test, see instructions at the top of {@link ITDataplanePlugin}
 */

public class BranchTagTestCases {
  private ITDataplanePluginTestSetup base;

  BranchTagTestCases(ITDataplanePluginTestSetup base) {
    this.base = base;
  }

  @Test
  void createBranchAtCommit() throws Exception {
    // Arrange
    // Make some commit on another branch to make sure we're not working with any defaults
    final String temporaryBranchName = generateUniqueBranchName();
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    base.runSQL(createBranchAtBranchQuery(temporaryBranchName, DEFAULT_BRANCH_NAME));
    base.runSQL(useBranchQuery(temporaryBranchName));
    createFolders(tablePath, VersionContext.ofBranch(temporaryBranchName));
    base.runSQL(createEmptyTableQuery(tablePath));
    String commitHashTemporaryBranch = base.getCommitHashForBranch(temporaryBranchName);

    final String branchName = generateUniqueBranchName();

    // Act
    base.runSQL(createBranchAtSpecifierQuery(branchName, "COMMIT " + quoted(commitHashTemporaryBranch)));

    // Assert
    assertThat(base.getCommitHashForBranch(branchName)).isEqualTo(commitHashTemporaryBranch);
  }

  @Test
  public void alterBranchAssignBranch() throws Exception {
    // Arrange
    final String branchName = generateUniqueBranchName();
    final String mainTableName = generateUniqueTableName();
    final List<String> mainTablePath = tablePathWithFolders(mainTableName);

    base.runSQL(createBranchAtBranchQuery(branchName, DEFAULT_BRANCH_NAME));
    createFolders(mainTablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(mainTablePath));

    // Act
    base.runSQL(alterBranchAssignBranchQuery(branchName, DEFAULT_BRANCH_NAME));
    base.runSQL(useBranchQuery(branchName));

    // ASSERT
    base.assertTableHasExpectedNumRows(mainTablePath, 0);

    // Drop tables
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    base.runSQL(dropTableQuery(mainTablePath));

  }

  @Test
  public void alterBranchAssignTag() throws Exception {
    // Arrange
    final String tagName = generateUniqueTagName();
    final String mainTableName = generateUniqueTableName();
    final List<String> mainTablePath = tablePathWithFolders(mainTableName);

    createFolders(mainTablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(mainTablePath));
    base.runSQL(createTagQuery(tagName, DEFAULT_BRANCH_NAME));
    base.runSQL(dropTableQuery(mainTablePath));

    // Act
    base.runSQL(alterBranchAssignTagQuery(DEFAULT_BRANCH_NAME, tagName));
    base.runSQL(useTagQuery(tagName));

    // ASSERT
    base.assertTableHasExpectedNumRows(mainTablePath, 0);

    // Drop tables
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    base.runSQL(dropTableQuery(mainTablePath));
  }

  @Test
  public void alterBranchAssignCommit() throws Exception {
    // Arrange
    final String branchName = generateUniqueBranchName();
    final String mainTableName = generateUniqueTableName();
    final List<String> mainTablePath = tablePathWithFolders(mainTableName);

    createFolders(mainTablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createBranchAtBranchQuery(branchName, DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(mainTablePath));
    String commitHash = base.getCommitHashForBranch(DEFAULT_BRANCH_NAME);

    // Act
    base.runSQL(alterBranchAssignCommitQuery(branchName, quoted(commitHash)));
    base.runSQL(useBranchQuery(branchName));

    // ASSERT
    base.assertTableHasExpectedNumRows(mainTablePath, 0);

    // Drop tables
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    base.runSQL(dropTableQuery(mainTablePath));
  }

  //This validation check is added as a part of 0.52.3 Nessie versions
  //PR For reference: https://github.com/projectnessie/nessie/pull/6224.
  @Test
  public void alterBranchAssignCommitUsingTag() throws Exception {
    // Arrange
    final String tagName = generateUniqueTagName();
    final String mainTableName = generateUniqueTableName();
    final List<String> mainTablePath = tablePathWithFolders(mainTableName);

    createFolders(mainTablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createTagQuery(tagName, DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(mainTablePath));
    String commitHash = base.getCommitHashForBranch(DEFAULT_BRANCH_NAME);

    // Act
    assertThatThrownBy(() -> base.runSQL(alterBranchAssignCommitQuery(tagName, quoted(commitHash))))
      .hasMessageContaining("Expected reference type BRANCH does not match existing reference TagName");
  }

  @Test
  public void alterTagAssignTag() throws Exception {
    // Arrange
    final String tagName = generateUniqueTagName();
    final String tagName2 = generateUniqueTagName();
    final String mainTableName = generateUniqueTableName();
    final List<String> mainTablePath = tablePathWithFolders(mainTableName);

    base.runSQL(createTagQuery(tagName, DEFAULT_BRANCH_NAME));
    createFolders(mainTablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(mainTablePath));
    base.runSQL(createTagQuery(tagName2, DEFAULT_BRANCH_NAME));

    // Act
    base.runSQL(alterTagAssignTagQuery(tagName, tagName2));
    base.runSQL(useTagQuery(tagName));

    // ASSERT
    base.assertTableHasExpectedNumRows(mainTablePath, 0);

    // Drop tables
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    base.runSQL(dropTableQuery(mainTablePath));
  }

  @Test
  public void alterTagAssignBranch() throws Exception {
    // Arrange
    final String tagName = generateUniqueTagName();
    final String branchName = generateUniqueBranchName();
    final String mainTableName = generateUniqueTableName();
    final List<String> mainTablePath = tablePathWithFolders(mainTableName);

    base.runSQL(createTagQuery(tagName, DEFAULT_BRANCH_NAME));

    base.runSQL(createBranchAtBranchQuery(branchName, DEFAULT_BRANCH_NAME));
    base.runSQL(useBranchQuery(branchName));
    createFolders(mainTablePath, VersionContext.ofBranch(branchName));
    base.runSQL(createEmptyTableQuery(mainTablePath));

    // Act
    base.runSQL(alterTagAssignBranchQuery(tagName, branchName));
    base.runSQL(useTagQuery(tagName));

    // ASSERT
    base.assertTableHasExpectedNumRows(mainTablePath, 0);
  }

  @Test
  public void alterTagAssignCommit() throws Exception {
    // Arrange
    final String tagName = generateUniqueTagName();
    final String branchName = generateUniqueBranchName();
    final String mainTableName = generateUniqueTableName();
    final List<String> mainTablePath = tablePathWithFolders(mainTableName);

    base.runSQL(createTagQuery(tagName, DEFAULT_BRANCH_NAME));

    base.runSQL(createBranchAtBranchQuery(branchName, DEFAULT_BRANCH_NAME));
    base.runSQL(useBranchQuery(branchName));
    createFolders(mainTablePath, VersionContext.ofBranch(branchName));
    base.runSQL(createEmptyTableQuery(mainTablePath));
    String commitHash = base.getCommitHashForBranch(branchName);

    // Act
    base.runSQL(alterTagAssignCommitQuery(tagName, quoted(commitHash)));
    base.runSQL(useTagQuery(tagName));

    // ASSERT
    base.assertTableHasExpectedNumRows(mainTablePath, 0);
  }

  @Test
  public void showNewlyCreatedBranch() throws Exception {
    final String branchName = generateUniqueBranchName();
    base.runSQL(createBranchAtBranchQuery(branchName, DEFAULT_BRANCH_NAME));
    List<List<String>> branchresults = base.runSqlWithResults(showBranchQuery(DATAPLANE_PLUGIN_NAME));
    assertThat(branchresults.stream().filter(row -> row.contains(branchName))).isNotEmpty();
  }

  @Test
  public void showBranchAfterDelete() throws Exception {
    final String branchName1 = generateUniqueBranchName();
    final String branchName2 = generateUniqueBranchName();
    final String branchName3 = generateUniqueBranchName();
    Set<String> branchSetBeforeDelete = Sets.newHashSet(branchName1, branchName2, branchName3);
    Set<String> branchSetAfterDelete = Sets.newHashSet(branchName1, branchName3);
    base.runSQL(createBranchAtBranchQuery(branchName1, DEFAULT_BRANCH_NAME));
    base.runSQL(createBranchAtBranchQuery(branchName2, DEFAULT_BRANCH_NAME));
    base.runSQL(createBranchAtBranchQuery(branchName3, DEFAULT_BRANCH_NAME));

    List<List<String>> branchresults = base.runSqlWithResults(showBranchQuery(DATAPLANE_PLUGIN_NAME));
    assertThat(branchresults.stream().filter(row -> branchSetBeforeDelete.contains(row.get(1))).count()).isEqualTo(3);
    base.runSQL(dropBranchQuery(branchName2));
    branchresults = base.runSqlWithResults(showBranchQuery(DATAPLANE_PLUGIN_NAME));
    assertThat(branchresults.stream().filter(row -> branchSetAfterDelete.contains(row.get(1))).count()).isEqualTo(2);
    assertThat(branchresults.stream().filter(row -> row.contains(branchName2)).count()).isEqualTo(0);
  }

  @Test
  public void showMultipleCreatedBranches() throws Exception {
    Set<String> branchSet = Sets.newHashSet();
    String branchName;
    for (int i = 0; i < 100; i++) {
      branchName = generateUniqueBranchName();
      branchSet.add(branchName);
      base.runSQL(createBranchAtBranchQuery(branchName, DEFAULT_BRANCH_NAME));
    }

    List<List<String>> branchresults = base.runSqlWithResults(showBranchQuery(DATAPLANE_PLUGIN_NAME));
    assertThat(branchresults.stream().filter(row -> branchSet.contains(row.get(1))).count()).isEqualTo(100);
  }

  @Test
  public void showNewlyCreatedTag() throws Exception {
    final String tagName = generateUniqueTagName();
    base.runSQL(createTagQuery(tagName, DEFAULT_BRANCH_NAME));
    List<List<String>> tagResults = base.runSqlWithResults(showTagQuery(DATAPLANE_PLUGIN_NAME));
    assertThat(tagResults.stream().filter(row -> row.contains(tagName))).isNotEmpty();
  }


  @Test
  public void showTagAfterDelete() throws Exception {
    final String tagName1 = generateUniqueBranchName();
    final String tagName2 = generateUniqueBranchName();
    final String tagName3 = generateUniqueBranchName();
    Set<String> tagSetBeforeDelete = Sets.newHashSet(tagName1, tagName2, tagName3);
    Set<String> tagSetAfterDelete = Sets.newHashSet(tagName1, tagName3);
    base.runSQL(createTagQuery(tagName1, DEFAULT_BRANCH_NAME));
    base.runSQL(createTagQuery(tagName2, DEFAULT_BRANCH_NAME));
    base.runSQL(createTagQuery(tagName3, DEFAULT_BRANCH_NAME));

    List<List<String>> tagresults = base.runSqlWithResults(showTagQuery(DATAPLANE_PLUGIN_NAME));
    assertThat(tagresults.stream().filter(row -> tagSetBeforeDelete.contains(row.get(1))).count()).isEqualTo(3);
    base.runSQL(dropTagQuery(tagName2));
    tagresults = base.runSqlWithResults(showTagQuery(DATAPLANE_PLUGIN_NAME));
    assertThat(tagresults.stream().filter(row -> tagSetAfterDelete.contains(row.get(1))).count()).isEqualTo(2);
    assertThat(tagresults.stream().filter(row -> row.contains(tagName2)).count()).isEqualTo(0);
  }

  @Test
  void dropCurrentBranch() throws Exception {
    // Arrange
    final String branchName = generateUniqueBranchName();
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    base.runSQL(createBranchAtBranchQuery(branchName, DEFAULT_BRANCH_NAME));
    createFolders(tablePath, VersionContext.ofBranch(branchName));


    //Act and Assert
    base.runSQL(useBranchQuery(branchName));
    base.runSQL(createEmptyTableQuery(tablePath));
    assertThatThrownBy(() -> base.runSQL(dropBranchQuery(branchName)))
      .hasMessageContaining(String.format("Cannot drop branch %s for source %s while it is set in the current session's reference context",
        branchName,
        DATAPLANE_PLUGIN_NAME));
  }

  @Test
  void dropCurrentTag() throws Exception {
    // Arrange
    final String tagName = generateUniqueBranchName();
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    final String branchName = generateUniqueBranchName();
    base.runSQL(createBranchAtBranchQuery(branchName, DEFAULT_BRANCH_NAME));
    base.runSQL(useBranchQuery(branchName));
    createFolders(tablePath, VersionContext.ofBranch(branchName));
    base.runSQL(createEmptyTableQuery(tablePath));
    base.runSQL(createTagQuery(tagName, branchName));

    // Act and Assert
    base.runSQL(useTagQuery(tagName));
    assertThatThrownBy(() -> base.runSQL(dropTagQuery(tagName)))
      .hasMessageContaining(String.format("Cannot drop tag %s for source %s while it is set in the current session's reference context",
        tagName,
        DATAPLANE_PLUGIN_NAME));
  }

}
