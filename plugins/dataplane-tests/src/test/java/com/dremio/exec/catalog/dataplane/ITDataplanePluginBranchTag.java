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
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.alterBranchAssignBranchQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.alterBranchAssignCommitQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.alterBranchAssignSpecifierQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.alterBranchAssignTagQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.alterTagAssignBranchQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.alterTagAssignCommitQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.alterTagAssignSpecifierQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.alterTagAssignTagQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createBranchAtBranchQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createBranchAtSpecifierQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createBranchFromBranchQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createBranchFromSpecifierQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createEmptyTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createTagAtSpecifierQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createTagQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.dropBranchAtCommitQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.dropBranchForceQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.dropBranchQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.dropTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.dropTagBranchAtCommitQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.dropTagForceQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.dropTagQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueBranchName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueTableName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueTagName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.quoted;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.showBranchQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.showTagQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.tablePathWithFolders;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.useBranchQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.useTagQuery;
import static com.dremio.exec.catalog.dataplane.test.TestDataplaneAssertions.assertNessieDoesHotHaveBranch;
import static com.dremio.exec.catalog.dataplane.test.TestDataplaneAssertions.assertNessieDoesHotHaveTag;
import static com.dremio.exec.catalog.dataplane.test.TestDataplaneAssertions.assertNessieDoesNotHaveTable;
import static com.dremio.exec.catalog.dataplane.test.TestDataplaneAssertions.assertNessieHasTable;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dremio.common.exceptions.UserRemoteException;
import com.dremio.exec.catalog.dataplane.test.ITDataplanePluginTestSetup;
import com.google.common.collect.Sets;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

public class ITDataplanePluginBranchTag extends ITDataplanePluginTestSetup {

  @Test
  void createBranchAtCommit() throws Exception {
    // Arrange
    // Make some commit on another branch to make sure we're not working with any defaults
    final String temporaryBranchName = generateUniqueBranchName();
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createBranchAtBranchQuery(temporaryBranchName, DEFAULT_BRANCH_NAME));
    runSQL(useBranchQuery(temporaryBranchName));
    runSQL(createEmptyTableQuery(tablePath));
    String commitHashTemporaryBranch = getCommitHashForBranch(temporaryBranchName);

    final String branchName = generateUniqueBranchName();
    final String branchName1 = generateUniqueBranchName();

    // Act
    runSQL(createBranchAtSpecifierQuery(branchName, "COMMIT " + quoted(commitHashTemporaryBranch)));
    runSQL(
        createBranchFromSpecifierQuery(branchName1, "COMMIT " + quoted(commitHashTemporaryBranch)));

    // Assert
    assertThat(getCommitHashForBranch(branchName)).isEqualTo(commitHashTemporaryBranch);
    assertThat(getCommitHashForBranch(branchName1)).isEqualTo(commitHashTemporaryBranch);
  }

  @Test
  public void alterBranchAssignBranch() throws Exception {
    // Arrange
    final String branchName = generateUniqueBranchName();
    final String mainTableName = generateUniqueTableName();
    final List<String> mainTablePath = tablePathWithFolders(mainTableName);

    runSQL(createBranchAtBranchQuery(branchName, DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(mainTablePath));

    // Act
    runSQL(alterBranchAssignBranchQuery(branchName, DEFAULT_BRANCH_NAME));
    runSQL(useBranchQuery(branchName));

    // ASSERT
    assertTableHasExpectedNumRows(mainTablePath, 0);

    // Drop tables
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    runSQL(dropTableQuery(mainTablePath));
  }

  @Test
  public void alterBranchAssignBranchWithTimestamp() throws Exception {
    // Arrange
    final String branchName = generateUniqueBranchName();
    final List<String> tablePath = Arrays.asList(generateUniqueTableName());

    // create table
    runSQL(createEmptyTableQuery(Arrays.asList(generateUniqueTableName())));
    final Instant timeBeforeCommit = Instant.now();
    final String hashBeforeCommit = getCommitHashForBranch(DEFAULT_BRANCH_NAME);
    runSQL(createEmptyTableQuery(tablePath));
    runSQL(createBranchAtBranchQuery(branchName, DEFAULT_BRANCH_NAME));
    assertNessieHasTable(tablePath, branchName, this);

    // Act
    String alterAssignSpecifierQuery =
        String.format(
            "BRANCH %s AS OF '%s'", DEFAULT_BRANCH_NAME, Timestamp.from(timeBeforeCommit));
    runSQL(alterBranchAssignSpecifierQuery(branchName, alterAssignSpecifierQuery));

    // ASSERT
    assertNessieDoesNotHaveTable(tablePath, branchName, this);
    assertThat(getCommitHashForBranch(branchName)).isEqualTo(hashBeforeCommit);

    // Drop tables
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void alterBranchAssignBranchWithCommitAndTimestampThrowsException() throws Exception {
    // Arrange
    final String branchName = generateUniqueBranchName();
    final Instant timeBeforeCommit = Instant.now();
    final String hashBeforeCommit = getCommitHashForBranch(DEFAULT_BRANCH_NAME);

    // Act + Assert
    String alterAssignSpecifierQuery =
        String.format(
            "COMMIT %s AS OF '%s'", quoted(hashBeforeCommit), Timestamp.from(timeBeforeCommit));
    assertThatThrownBy(
            () -> runSQL(alterBranchAssignSpecifierQuery(branchName, alterAssignSpecifierQuery)))
        .isInstanceOf(UserRemoteException.class)
        .hasMessageContaining("Reference type COMMIT does not support specifying a timestamp.");
  }

  @Test
  public void alterBranchAssignTag() throws Exception {
    // Arrange
    final String tagName = generateUniqueTagName();
    final String mainTableName = generateUniqueTableName();
    final List<String> mainTablePath = tablePathWithFolders(mainTableName);

    runSQL(createEmptyTableQuery(mainTablePath));
    runSQL(createTagQuery(tagName, DEFAULT_BRANCH_NAME));
    runSQL(dropTableQuery(mainTablePath));

    // Act
    runSQL(alterBranchAssignTagQuery(DEFAULT_BRANCH_NAME, tagName));
    runSQL(useTagQuery(tagName));

    // ASSERT
    assertTableHasExpectedNumRows(mainTablePath, 0);

    // Drop tables
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    runSQL(dropTableQuery(mainTablePath));
  }

  @Test
  public void alterBranchAssignCommit() throws Exception {
    // Arrange
    final String branchName = generateUniqueBranchName();
    final String mainTableName = generateUniqueTableName();
    final List<String> mainTablePath = tablePathWithFolders(mainTableName);

    runSQL(createBranchAtBranchQuery(branchName, DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(mainTablePath));
    String commitHash = getCommitHashForBranch(DEFAULT_BRANCH_NAME);

    // Act
    runSQL(alterBranchAssignCommitQuery(branchName, quoted(commitHash)));
    runSQL(useBranchQuery(branchName));

    // ASSERT
    assertTableHasExpectedNumRows(mainTablePath, 0);

    // Drop tables
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    runSQL(dropTableQuery(mainTablePath));
  }

  // This validation check is added as a part of 0.52.3 Nessie versions
  // PR For reference: https://github.com/projectnessie/nessie/pull/6224.
  @Test
  public void alterBranchAssignCommitUsingTag() throws Exception {
    // Arrange
    final String tagName = generateUniqueTagName();
    final String mainTableName = generateUniqueTableName();
    final List<String> mainTablePath = tablePathWithFolders(mainTableName);

    runSQL(createTagQuery(tagName, DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(mainTablePath));
    String commitHash = getCommitHashForBranch(DEFAULT_BRANCH_NAME);

    // Act
    assertThatThrownBy(() -> runSQL(alterBranchAssignCommitQuery(tagName, quoted(commitHash))))
        .hasMessageContaining(
            "Expected reference type BRANCH does not match existing reference TagName");
  }

  @Test
  public void alterTagAssignTag() throws Exception {
    // Arrange
    final String tagName = generateUniqueTagName();
    final String tagName2 = generateUniqueTagName();
    final String mainTableName = generateUniqueTableName();
    final List<String> mainTablePath = tablePathWithFolders(mainTableName);

    runSQL(createTagQuery(tagName, DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(mainTablePath));
    runSQL(createTagQuery(tagName2, DEFAULT_BRANCH_NAME));

    // Act
    runSQL(alterTagAssignTagQuery(tagName, tagName2));
    runSQL(useTagQuery(tagName));

    // ASSERT
    assertTableHasExpectedNumRows(mainTablePath, 0);

    // Drop tables
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    runSQL(dropTableQuery(mainTablePath));
  }

  @Test
  public void alterTagAssignBranch() throws Exception {
    // Arrange
    final String tagName = generateUniqueTagName();
    final String branchName = generateUniqueBranchName();
    final String mainTableName = generateUniqueTableName();
    final List<String> mainTablePath = tablePathWithFolders(mainTableName);

    runSQL(createTagQuery(tagName, DEFAULT_BRANCH_NAME));

    runSQL(createBranchAtBranchQuery(branchName, DEFAULT_BRANCH_NAME));
    runSQL(useBranchQuery(branchName));
    runSQL(createEmptyTableQuery(mainTablePath));

    // Act
    runSQL(alterTagAssignBranchQuery(tagName, branchName));
    runSQL(useTagQuery(tagName));

    // ASSERT
    assertTableHasExpectedNumRows(mainTablePath, 0);
  }

  @Test
  public void alterTagAssignBranchWithTimestamp() throws Exception {
    // Arrange
    final String tagName = generateUniqueTagName();
    final List<String> tablePath = Arrays.asList(generateUniqueTableName());
    final List<String> secondTablePath = Arrays.asList(generateUniqueTableName());

    // create tables and create tag
    runSQL(createEmptyTableQuery(tablePath));

    final Instant timeBetweenCommits = Instant.now();
    final String hashInBetweenCommits = getCommitHashForBranch(DEFAULT_BRANCH_NAME);
    runSQL(createEmptyTableQuery(secondTablePath));
    runSQL(createTagQuery(tagName, DEFAULT_BRANCH_NAME));
    final String hashAfterCommits = getCommitHashForBranch(DEFAULT_BRANCH_NAME);
    final String tagHashBeforeAlter = getCommitHashForTag(tagName);
    // Act
    String alterAssignSpecifierQuery =
        String.format(
            "BRANCH %s AS OF '%s'", DEFAULT_BRANCH_NAME, Timestamp.from(timeBetweenCommits));
    runSQL(alterTagAssignSpecifierQuery(tagName, alterAssignSpecifierQuery));

    // ASSERT
    assertThat(tagHashBeforeAlter).isEqualTo(hashAfterCommits);
    assertThat(getCommitHashForTag(tagName)).isEqualTo(hashInBetweenCommits);

    // Drop tables
    runSQL(dropTableQuery(tablePath));
    runSQL(dropTableQuery(secondTablePath));
    runSQL(dropTagQuery(tagName));
  }

  @Test
  public void alterTagAssignBranchWithCommitAndTimestampThrowsException() throws Exception {
    // Arrange
    final String branchName = generateUniqueBranchName();
    final Instant timeBeforeCommit = Instant.now();
    final String hashBeforeCommit = getCommitHashForBranch(DEFAULT_BRANCH_NAME);

    // Act + Assert
    String alterAssignSpecifierQuery =
        String.format(
            "COMMIT %s AS OF '%s'", quoted(hashBeforeCommit), Timestamp.from(timeBeforeCommit));
    assertThatThrownBy(
            () -> runSQL(alterTagAssignSpecifierQuery(branchName, alterAssignSpecifierQuery)))
        .isInstanceOf(UserRemoteException.class)
        .hasMessageContaining("Reference type COMMIT does not support specifying a timestamp.");
  }

  @Test
  public void alterTagAssignCommit() throws Exception {
    // Arrange
    final String tagName = generateUniqueTagName();
    final String branchName = generateUniqueBranchName();
    final String mainTableName = generateUniqueTableName();
    final List<String> mainTablePath = tablePathWithFolders(mainTableName);

    runSQL(createTagQuery(tagName, DEFAULT_BRANCH_NAME));

    runSQL(createBranchAtBranchQuery(branchName, DEFAULT_BRANCH_NAME));
    runSQL(useBranchQuery(branchName));
    runSQL(createEmptyTableQuery(mainTablePath));
    String commitHash = getCommitHashForBranch(branchName);

    // Act
    runSQL(alterTagAssignCommitQuery(tagName, quoted(commitHash)));
    runSQL(useTagQuery(tagName));

    // ASSERT
    assertTableHasExpectedNumRows(mainTablePath, 0);
  }

  @Test
  public void showNewlyCreatedBranchWithAt() throws Exception {
    final String branchName = generateUniqueBranchName();
    runSQL(createBranchAtBranchQuery(branchName, DEFAULT_BRANCH_NAME));
    List<List<String>> branchresults = runSqlWithResults(showBranchQuery(DATAPLANE_PLUGIN_NAME));
    assertThat(branchresults.stream().filter(row -> row.contains(branchName))).isNotEmpty();
  }

  @Test
  public void showNewlyCreatedBranchWithFrom() throws Exception {
    final String branchName = generateUniqueBranchName();
    runSQL(createBranchFromBranchQuery(branchName, DEFAULT_BRANCH_NAME));
    List<List<String>> branchresults = runSqlWithResults(showBranchQuery(DATAPLANE_PLUGIN_NAME));
    assertThat(branchresults.stream().filter(row -> row.contains(branchName))).isNotEmpty();
  }

  @Test
  public void showBranchAfterDelete() throws Exception {
    final String branchName1 = generateUniqueBranchName();
    final String branchName2 = generateUniqueBranchName();
    final String branchName3 = generateUniqueBranchName();
    Set<String> branchSetBeforeDelete = Sets.newHashSet(branchName1, branchName2, branchName3);
    Set<String> branchSetAfterDelete = Sets.newHashSet(branchName1, branchName3);
    runSQL(createBranchAtBranchQuery(branchName1, DEFAULT_BRANCH_NAME));
    runSQL(createBranchAtBranchQuery(branchName2, DEFAULT_BRANCH_NAME));
    runSQL(createBranchAtBranchQuery(branchName3, DEFAULT_BRANCH_NAME));

    List<List<String>> branchresults = runSqlWithResults(showBranchQuery(DATAPLANE_PLUGIN_NAME));
    assertThat(
            branchresults.stream()
                .filter(row -> branchSetBeforeDelete.contains(row.get(1)))
                .count())
        .isEqualTo(3);
    runSQL(dropBranchQuery(branchName2));
    branchresults = runSqlWithResults(showBranchQuery(DATAPLANE_PLUGIN_NAME));
    assertThat(
            branchresults.stream().filter(row -> branchSetAfterDelete.contains(row.get(1))).count())
        .isEqualTo(2);
    assertThat(branchresults.stream().filter(row -> row.contains(branchName2)).count())
        .isEqualTo(0);
  }

  @Test
  public void showMultipleCreatedBranches() throws Exception {
    Set<String> branchSet = Sets.newHashSet();
    String branchName;
    for (int i = 0; i < 50; i++) {
      branchName = generateUniqueBranchName();
      branchSet.add(branchName);
      runSQL(createBranchAtBranchQuery(branchName, DEFAULT_BRANCH_NAME));
    }

    for (int i = 0; i < 50; i++) {
      branchName = generateUniqueBranchName();
      branchSet.add(branchName);
      runSQL(createBranchFromBranchQuery(branchName, DEFAULT_BRANCH_NAME));
    }

    List<List<String>> branchresults = runSqlWithResults(showBranchQuery(DATAPLANE_PLUGIN_NAME));
    assertThat(branchresults.stream().filter(row -> branchSet.contains(row.get(1))).count())
        .isEqualTo(100);
  }

  @Test
  public void showNewlyCreatedTag() throws Exception {
    final String tagName = generateUniqueTagName();
    runSQL(createTagQuery(tagName, DEFAULT_BRANCH_NAME));
    List<List<String>> tagResults = runSqlWithResults(showTagQuery(DATAPLANE_PLUGIN_NAME));
    assertThat(tagResults.stream().filter(row -> row.contains(tagName))).isNotEmpty();
  }

  @Test
  public void showTagAfterDelete() throws Exception {
    final String tagName1 = generateUniqueBranchName();
    final String tagName2 = generateUniqueBranchName();
    final String tagName3 = generateUniqueBranchName();
    Set<String> tagSetBeforeDelete = Sets.newHashSet(tagName1, tagName2, tagName3);
    Set<String> tagSetAfterDelete = Sets.newHashSet(tagName1, tagName3);
    runSQL(createTagQuery(tagName1, DEFAULT_BRANCH_NAME));
    runSQL(createTagQuery(tagName2, DEFAULT_BRANCH_NAME));
    runSQL(createTagQuery(tagName3, DEFAULT_BRANCH_NAME));

    List<List<String>> tagresults = runSqlWithResults(showTagQuery(DATAPLANE_PLUGIN_NAME));
    assertThat(tagresults.stream().filter(row -> tagSetBeforeDelete.contains(row.get(1))).count())
        .isEqualTo(3);
    runSQL(dropTagQuery(tagName2));
    tagresults = runSqlWithResults(showTagQuery(DATAPLANE_PLUGIN_NAME));
    assertThat(tagresults.stream().filter(row -> tagSetAfterDelete.contains(row.get(1))).count())
        .isEqualTo(2);
    assertThat(tagresults.stream().filter(row -> row.contains(tagName2)).count()).isEqualTo(0);
  }

  @Test
  public void dropCurrentBranch() throws Exception {
    // Arrange
    final String branchName = generateUniqueBranchName();
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createBranchAtBranchQuery(branchName, DEFAULT_BRANCH_NAME));

    // Act and Assert
    runSQL(useBranchQuery(branchName));
    runSQL(createEmptyTableQuery(tablePath));
    assertThatThrownBy(() -> runSQL(dropBranchQuery(branchName)))
        .hasMessageContaining(
            String.format(
                "Cannot drop branch %s for source %s while it is set in the current session's reference context",
                branchName, DATAPLANE_PLUGIN_NAME));
  }

  @Test
  public void dropCurrentTag() throws Exception {
    // Arrange
    final String tagName = generateUniqueBranchName();
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    final String branchName = generateUniqueBranchName();
    runSQL(createBranchAtBranchQuery(branchName, DEFAULT_BRANCH_NAME));
    runSQL(useBranchQuery(branchName));
    runSQL(createEmptyTableQuery(tablePath));
    runSQL(createTagQuery(tagName, branchName));

    // Act and Assert
    runSQL(useTagQuery(tagName));
    assertThatThrownBy(() -> runSQL(dropTagQuery(tagName)))
        .hasMessageContaining(
            String.format(
                "Cannot drop tag %s for source %s while it is set in the current session's reference context",
                tagName, DATAPLANE_PLUGIN_NAME));
  }

  @Test
  public void createBranchIfNotExistsWhenBranchAlreadyExistsNoError() throws Exception {
    // Arrange
    final String branchName = generateUniqueBranchName();
    runSQL(createBranchAtBranchQuery(branchName, DEFAULT_BRANCH_NAME));

    // Act + Asset (does not throw)
    runSQL(
        String.format(
            "CREATE BRANCH IF NOT EXISTS %s AT BRANCH %s IN %s",
            branchName, DEFAULT_BRANCH_NAME, DATAPLANE_PLUGIN_NAME));
  }

  @Test
  public void createBranchWithoutIfNotExistsWhenBranchAlreadyExistsErrors() throws Exception {
    // Arrange
    final String branchName = generateUniqueBranchName();
    runSQL(createBranchAtBranchQuery(branchName, DEFAULT_BRANCH_NAME));

    // Act + Asset
    assertThatThrownBy(() -> runSQL(createBranchAtBranchQuery(branchName, DEFAULT_BRANCH_NAME)))
        .hasMessageContaining("already exists in")
        .hasMessageContaining(branchName)
        .hasMessageContaining(DATAPLANE_PLUGIN_NAME);
  }

  @Test
  public void createTagIfNotExistsWhenTagAlreadyExistsNoError() throws Exception {
    // Arrange
    final String tagName = generateUniqueTagName();
    runSQL(createTagQuery(tagName, DEFAULT_BRANCH_NAME));

    // Act + Asset (does not throw)
    runSQL(
        String.format(
            "CREATE TAG IF NOT EXISTS %s AT BRANCH %s IN %s",
            tagName, DEFAULT_BRANCH_NAME, DATAPLANE_PLUGIN_NAME));
  }

  @Test
  public void createTagWithoutIfNotExistsWhenTagAlreadyExistsErrors() throws Exception {
    // Arrange
    final String tagName = generateUniqueTagName();
    runSQL(createTagQuery(tagName, DEFAULT_BRANCH_NAME));

    // Act + Asset
    assertThatThrownBy(() -> runSQL(createTagQuery(tagName, DEFAULT_BRANCH_NAME)))
        .hasMessageContaining("already exists in")
        .hasMessageContaining(tagName)
        .hasMessageContaining(DATAPLANE_PLUGIN_NAME);
  }

  @Test
  public void dropBranchIfExistsWhenBranchDoesNotExistNoError() throws Exception {
    // Arrange
    final String branchName = generateUniqueBranchName();

    // Act + Asset (does not throw)
    runSQL(
        String.format("DROP BRANCH IF EXISTS %s FORCE IN %s", branchName, DATAPLANE_PLUGIN_NAME));
  }

  @Test
  void dropBranchWithoutIfExistsWhenBranchDoesNotExistErrors() {
    // Arrange
    final String branchName = generateUniqueBranchName();

    // Act + Asset
    assertThatThrownBy(() -> runSQL(dropBranchQuery(branchName)))
        .hasMessageContaining("not found on source")
        .hasMessageContaining(branchName)
        .hasMessageContaining(DATAPLANE_PLUGIN_NAME);
  }

  @Test
  public void dropTagIfExistsWhenTagDoesNotExistNoError() throws Exception {
    // Arrange
    final String tagName = generateUniqueTagName();

    // Act + Asset (does not throw)
    runSQL(String.format("DROP TAG IF EXISTS %s FORCE IN %s", tagName, DATAPLANE_PLUGIN_NAME));
  }

  @Test
  public void dropTagWithoutIfExistsWhenTagDoesNotExistErrors() {
    // Arrange
    final String tagName = generateUniqueTagName();

    // Act + Asset
    assertThatThrownBy(() -> runSQL(dropTagQuery(tagName)))
        .hasMessageContaining("not found on source")
        .hasMessageContaining(tagName)
        .hasMessageContaining(DATAPLANE_PLUGIN_NAME);
  }

  @Test
  void dropBranch() throws Exception {
    // Arrange
    final String branchName = generateUniqueBranchName();
    runSqlWithResults(createBranchAtBranchQuery(branchName, DEFAULT_BRANCH_NAME));

    // Act + Asset (does not throw)
    runSQL(dropBranchQuery(branchName));
    assertNessieDoesHotHaveBranch(branchName, this);
  }

  @Test
  void dropBranchForce() throws Exception {
    // Arrange
    final String branchName = generateUniqueBranchName();
    runSqlWithResults(createBranchAtBranchQuery(branchName, DEFAULT_BRANCH_NAME));

    // Act + Asset (does not throw) And should not have any issues with FORCE keyword.
    runSQL(dropBranchForceQuery(branchName));
    assertNessieDoesHotHaveBranch(branchName, this);
  }

  @Test
  void dropBranchAtCommitHash() throws Exception {
    final String testBranch = generateUniqueBranchName();
    runSQL(createBranchAtBranchQuery(testBranch, DEFAULT_BRANCH_NAME));
    String commitHash = getCommitHashForBranch(testBranch);

    runSqlWithResults(dropBranchAtCommitQuery(testBranch, commitHash));
    assertNessieDoesHotHaveBranch(testBranch, this);
  }

  @Test
  void dropTag() throws Exception {
    // Arrange
    final String tagName = generateUniqueTagName();
    runSqlWithResults(createTagQuery(tagName, DEFAULT_BRANCH_NAME));

    // Act + Asset (does not throw)
    runSQL(dropTagQuery(tagName));
    assertNessieDoesHotHaveTag(tagName, this);
  }

  @Test
  void dropTagForce() throws Exception {
    // Arrange
    final String tagName = generateUniqueTagName();
    runSqlWithResults(createTagQuery(tagName, DEFAULT_BRANCH_NAME));

    // Act + Asset (does not throw) And should not have any issues with FORCE keyword.
    runSQL(dropTagForceQuery(tagName));
    assertNessieDoesHotHaveTag(tagName, this);
  }

  @Test
  void dropTagAtCommitHash() throws Exception {
    final String tagName = generateUniqueTagName();
    runSQL(createTagQuery(tagName, DEFAULT_BRANCH_NAME));
    String commitHash = getCommitHashForTag(tagName);

    runSqlWithResults(dropTagBranchAtCommitQuery(tagName, commitHash));
    assertNessieDoesHotHaveTag(tagName, this);
  }

  @Test
  public void createBranchAsOfTimestamp() throws Exception {
    // Arrange
    final String workingBranchName = generateUniqueBranchName();
    final String tableName = generateUniqueTableName();
    final String afterBranchName = generateUniqueBranchName();

    // Avoid main branch to reduce chance of collisions with other tests
    runSQL(createBranchAtBranchQuery(workingBranchName, DEFAULT_BRANCH_NAME));
    runSQL(useBranchQuery(workingBranchName));

    // Set up a commit where we know the time before and after
    runSQL(createEmptyTableQuery(Collections.singletonList(tableName)));
    final Instant timeAfterCommit = Instant.now();

    // Act
    runSQL(
        createBranchAtSpecifierQuery(
            afterBranchName,
            "BRANCH " + workingBranchName + " AS OF '" + Timestamp.from(timeAfterCommit) + "'"));

    // Assert
    List<List<String>> beforeLogs =
        runSqlWithResults("SHOW LOGS AT BRANCH " + DEFAULT_BRANCH_NAME + " IN dataPlane_Test");
    List<List<String>> afterLogs =
        runSqlWithResults("SHOW LOGS AT BRANCH " + afterBranchName + " IN dataPlane_Test");
    assertThat(beforeLogs).hasSize(0);
    assertThat(afterLogs).hasSize(1);
  }

  @Test
  public void createTagAsOfTimestamp() throws Exception {
    // Arrange
    final String workingBranchName = generateUniqueBranchName();
    final String tableName = generateUniqueTableName();
    final String beforeTag = generateUniqueTagName();
    final String afterTag = generateUniqueTagName();

    // Avoid main branch to reduce chance of collisions with other tests
    runSQL(createBranchAtBranchQuery(workingBranchName, DEFAULT_BRANCH_NAME));
    runSQL(useBranchQuery(workingBranchName));

    // Set up a commit where we know the time before and after
    runSQL(createEmptyTableQuery(Collections.singletonList(generateUniqueTableName())));
    final Instant timeBeforeCommit = Instant.now();

    runSQL(createEmptyTableQuery(Collections.singletonList(tableName)));
    final Instant timeAfterCommit = Instant.now();

    // Act, create tag and get commitHash
    runSQL(
        createTagAtSpecifierQuery(
            beforeTag,
            "BRANCH " + workingBranchName + " AS OF '" + Timestamp.from(timeBeforeCommit) + "'"));
    runSQL(
        createTagAtSpecifierQuery(
            afterTag,
            "BRANCH " + workingBranchName + " AS OF '" + Timestamp.from(timeAfterCommit) + "'"));

    // Assert
    List<List<String>> beforeLogs =
        runSqlWithResults("SHOW LOGS AT TAG " + beforeTag + " IN dataPlane_Test");
    List<List<String>> afterLogs =
        runSqlWithResults("SHOW LOGS AT TAG " + afterTag + " IN dataPlane_Test");
    assertThat(beforeLogs).hasSize(1);
    assertThat(afterLogs).hasSize(2);

    // cleanup
    runSQL(dropTagQuery(beforeTag));
    runSQL(dropTagQuery(afterTag));
  }
}
