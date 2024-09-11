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

import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dremio.catalog.model.VersionContext;
import com.dremio.common.exceptions.UserRemoteException;
import com.dremio.exec.catalog.dataplane.test.ITDataplanePluginTestSetup;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

public class ITDataplanePluginDelete extends ITDataplanePluginTestSetup {

  @Test
  public void deleteAll() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final String devBranch = generateUniqueBranchName();
    // Set context to main branch
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(tablePath));
    runSQL(insertTableQuery(tablePath));
    // Verify with select
    assertTableHasExpectedNumRows(tablePath, 3);
    // Create dev branch
    runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));
    // Switch to dev
    runSQL(useBranchQuery(devBranch));

    // Act
    runSQL(deleteAllQuery(tablePath));

    // Assert
    assertTableHasExpectedNumRows(tablePath, 0);
    // Check that main context still has the table
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    assertTableHasExpectedNumRows(tablePath, 3);

    assertCommitLogTail(
        VersionContext.ofBranch(DEFAULT_BRANCH_NAME),
        String.format("CREATE TABLE %s", joinedTableKey(tablePath)),
        String.format("INSERT on TABLE %s", joinedTableKey(tablePath)));

    assertCommitLogTail(
        VersionContext.ofBranch(devBranch),
        String.format("CREATE TABLE %s", joinedTableKey(tablePath)),
        String.format("INSERT on TABLE %s", joinedTableKey(tablePath)),
        String.format("DELETE on TABLE %s", joinedTableKey(tablePath)));

    // cleanup
    runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void deleteAllWithContext() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final String devBranch = generateUniqueBranchName();
    // Set context to main branch
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(tablePath));
    runSQL(insertTableQuery(tablePath));
    // Verify with select
    assertTableHasExpectedNumRows(tablePath, 3);
    // Create dev branch
    runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));
    // Switch to dev
    runSQL(useBranchQuery(devBranch));

    // Act
    runSQL(useContextQuery(Collections.singletonList(DATAPLANE_PLUGIN_NAME)));
    runSQL(deleteAllQueryWithoutContext(tablePath));

    // Assert
    assertTableHasExpectedNumRows(tablePath, 0);
    // Check that main context still has the table
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    assertTableHasExpectedNumRows(tablePath, 3);

    // cleanup
    runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void deleteAllInATag() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    String tag = generateUniqueTagName();
    // Set context to main branch
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(tablePath));
    runSQL(insertTableQuery(tablePath));
    // Verify with select
    assertTableHasExpectedNumRows(tablePath, 3);
    // Create a tag to mark it
    runSQL(createTagQuery(tag, DEFAULT_BRANCH_NAME));

    // Switch to the tag
    runSQL(useTagQuery(tag));

    assertQueryThrowsExpectedError(
        deleteAllQuery(tablePath),
        "DDL and DML operations are only supported for branches - not on tags or commits");

    // Check that main context still has the table
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    assertTableHasExpectedNumRows(tablePath, 3);

    // cleanup
    runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void deleteAgnosticOfSourceBucket() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    runSQL(createEmptyTableQuery(tablePath));
    runSQL(insertTableQuery(tablePath));

    // Act
    runWithAlternateSourcePath(deleteAllQuery(tablePath));

    // Assert
    assertTableHasExpectedNumRows(tablePath, 0);

    // cleanup
    runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void deleteWithTagSet() throws Exception {
    // Arrange
    String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createEmptyTableQuery(tablePath));
    runSQL(insertTableQuery(tablePath));
    final String tag = generateUniqueTagName();
    // Act and Assert
    runSQL(createTagQuery(tag, DEFAULT_BRANCH_NAME));
    runSQL(useTagQuery(tag));
    assertQueryThrowsExpectedError(
        deleteAllQuery(tablePath),
        String.format(
            "DDL and DML operations are only supported for branches - not on tags or commits. %s is not a branch.",
            tag));
  }

  @Test
  public void testDmlDeleteWithAt() throws Exception {
    String devBranchName = "devBranch";
    String mainBranch = String.format("BRANCH %s", DEFAULT_BRANCH_NAME);
    String tableName = "myTable";
    runSQL(useContextQuery(Collections.singletonList(DATAPLANE_PLUGIN_NAME)));
    runSQL(String.format("CREATE table %s.%s as select 1,2,3", DATAPLANE_PLUGIN_NAME, tableName));
    runSQL(createBranchAtSpecifierQuery(devBranchName, mainBranch));
    // Delete row in dev.
    runSQL(
        String.format(
            "DELETE FROM %s.%s at branch %s where EXPR$0 < 2",
            DATAPLANE_PLUGIN_NAME, tableName, devBranchName));
    // Should have no row since EXPR$0 = 1
    assertThat(
            runSqlWithResults(
                String.format(
                    "select * from %s.%s at branch %s",
                    DATAPLANE_PLUGIN_NAME, tableName, devBranchName)))
        .isEqualTo(Collections.emptyList());
    // we should not modify table in the main branch
    assertThat(
            runSqlWithResults(
                String.format(
                    "select * from %s.%s at branch %s",
                    DATAPLANE_PLUGIN_NAME, tableName, DEFAULT_BRANCH_NAME)))
        .isEqualTo(Collections.singletonList(Arrays.asList("1", "2", "3")));
  }

  @Test
  public void testDmlDeleteWithAtWithoutContext() throws Exception {
    String devBranchName = "devBranch";
    String mainBranch = String.format("BRANCH %s", DEFAULT_BRANCH_NAME);
    String tableName = "myTable";
    runSQL(String.format("CREATE table %s.%s as select 1,2,3", DATAPLANE_PLUGIN_NAME, tableName));
    runSQL(createBranchAtSpecifierQuery(devBranchName, mainBranch));
    // Delete row in dev.
    runSQL(
        String.format(
            "DELETE FROM %s.%s at branch %s where EXPR$0 < 2",
            DATAPLANE_PLUGIN_NAME, tableName, devBranchName));
    // Should have no row since EXPR$0 = 1
    assertThat(
            runSqlWithResults(
                String.format(
                    "select * from %s.%s at branch %s",
                    DATAPLANE_PLUGIN_NAME, tableName, devBranchName)))
        .isEqualTo(Collections.emptyList());
    // we should not modify table in the main branch
    assertThat(
            runSqlWithResults(
                String.format(
                    "select * from %s.%s at branch %s",
                    DATAPLANE_PLUGIN_NAME, tableName, DEFAULT_BRANCH_NAME)))
        .isEqualTo(Collections.singletonList(Arrays.asList("1", "2", "3")));
  }

  @Test
  public void testDmlDeleteWithAtSettingToAnotherContext() throws Exception {
    setupForAnotherPlugin();
    String devBranchName = "devBranch";
    String mainBranch = String.format("BRANCH %s", DEFAULT_BRANCH_NAME);
    String tableName = "myTable";
    runSQL(useContextQuery(Collections.singletonList(DATAPLANE_PLUGIN_NAME_FOR_REFLECTION_TEST)));
    runSQL(String.format("CREATE table %s.%s as select 1,2,3", DATAPLANE_PLUGIN_NAME, tableName));
    runSQL(createBranchAtSpecifierQuery(devBranchName, mainBranch));
    // Delete row in dev.
    runSQL(
        String.format(
            "DELETE FROM %s.%s at branch %s where EXPR$0 < 2",
            DATAPLANE_PLUGIN_NAME, tableName, devBranchName));
    // Should have no row since EXPR$0 = 1
    assertThat(
            runSqlWithResults(
                String.format(
                    "select * from %s.%s at branch %s",
                    DATAPLANE_PLUGIN_NAME, tableName, devBranchName)))
        .isEqualTo(Collections.emptyList());
    // we should not modify table in the main branch
    assertThat(
            runSqlWithResults(
                String.format(
                    "select * from %s.%s at branch %s",
                    DATAPLANE_PLUGIN_NAME, tableName, DEFAULT_BRANCH_NAME)))
        .isEqualTo(Collections.singletonList(Arrays.asList("1", "2", "3")));
  }

  @Test
  public void testDmlDeleteWithAtWrongReferenceType() throws Exception {
    String tagName = "myTag";
    String tableName = "myTruncateTableWithAt";
    runSQL(String.format("CREATE table %s.%s as select 1,2,3", DATAPLANE_PLUGIN_NAME, tableName));
    runSQL(createTagQuery(tagName, DEFAULT_BRANCH_NAME));
    // expect error for TAG
    assertThatThrownBy(
            () ->
                runSQL(
                    String.format(
                        "DELETE FROM %s.%s at TAG %s", DATAPLANE_PLUGIN_NAME, tableName, tagName)))
        .isInstanceOf(UserRemoteException.class);
  }

  @Test
  public void testDmlDelete() throws Exception {
    String tableName = "myTable";
    runSQL(String.format("CREATE table %s.%s as select 1,2,3", DATAPLANE_PLUGIN_NAME, tableName));
    runSQL(useContextQuery(Collections.singletonList(DATAPLANE_PLUGIN_NAME)));
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    runSQL(String.format("DELETE FROM %s ", tableName));
    assertThat(
            runSqlWithResults(
                String.format(
                    "select * from %s.%s at branch %s",
                    DATAPLANE_PLUGIN_NAME, tableName, DEFAULT_BRANCH_NAME)))
        .isEqualTo(Collections.emptyList());
  }

  @Test
  public void testDmlDeleteWithContextWithDifferentAtStatement() throws Exception {
    String tableName = "myTable";
    String devBranch = "dev";
    String mainBranch = String.format("BRANCH %s", DEFAULT_BRANCH_NAME);
    runSQL(String.format("CREATE table %s.%s as select 1,2,3", DATAPLANE_PLUGIN_NAME, tableName));
    runSQL(createBranchAtSpecifierQuery(devBranch, mainBranch));
    runSQL(useContextQuery(Collections.singletonList(DATAPLANE_PLUGIN_NAME)));
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    runSQL(String.format("delete from %s at branch %s", tableName, devBranch));
    assertThat(
            runSqlWithResults(
                String.format(
                    "select * from %s.%s at branch %s",
                    DATAPLANE_PLUGIN_NAME, tableName, DEFAULT_BRANCH_NAME)))
        .isEqualTo(Collections.singletonList(Arrays.asList("1", "2", "3")));
    assertThat(
            runSqlWithResults(
                String.format(
                    "select * from %s.%s at branch %s",
                    DATAPLANE_PLUGIN_NAME, tableName, devBranch)))
        .isEqualTo(Collections.emptyList());
  }

  @Test
  public void testDmlDeleteWithContextWithDifferentAtStatementToNonExistentTable()
      throws Exception {
    String tableName = "myTable";
    String devBranch = "dev";
    String mainBranch = String.format("BRANCH %s", DEFAULT_BRANCH_NAME);
    runSQL(createBranchAtSpecifierQuery(devBranch, mainBranch));
    runSQL(useContextQuery(Collections.singletonList(DATAPLANE_PLUGIN_NAME)));
    runSQL(useBranchQuery(devBranch));
    runSQL(String.format("CREATE table %s.%s as select 1,2,3", DATAPLANE_PLUGIN_NAME, tableName));
    assertThatThrownBy(
            () -> runSQL(String.format("delete %s at branch %s", tableName, DEFAULT_BRANCH_NAME)))
        .isInstanceOf(UserRemoteException.class);
  }

  // TODO: When DX-69248 is fixed, this test should be modified.
  // TODO: Current behaviour: when we only have version for the target table, DML query throws an
  // error (either you should specify both, or only source table, or no specification)
  @Test
  public void testDmlDeleteOnlyTargetVersionSpecifiedThrowsError() throws Exception {
    String mainTable = "mainTable";
    String devTable1 = "devTable1";
    String devTable2 = "devTable2";
    String devBranchName = "dev";
    String mainBranch = String.format("BRANCH %s", DEFAULT_BRANCH_NAME);
    runSQL(createBranchAtSpecifierQuery(devBranchName, mainBranch));

    // current session is DATAPLANE_PLUGIN_NAME, DEFAULT_BRANCH
    runSQL(useContextQuery(Collections.singletonList(DATAPLANE_PLUGIN_NAME)));
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    runSQL(
        String.format(
            "CREATE table %s at branch %s as select 1,2,3", mainTable, DEFAULT_BRANCH_NAME));
    runSQL(String.format("CREATE table %s at branch %s as select 1,2,3", devTable1, devBranchName));
    runSQL(String.format("CREATE table %s at branch %s as select 1,2,3", devTable2, devBranchName));

    // update devTable1 using source in dev
    assertThatThrownBy(
            () ->
                runSQL(
                    String.format(
                        "delete from %s at branch %s as t USING %s as s where t.EXPR$0 = s.EXPR$1",
                        devTable1, devBranchName, devTable2)))
        .isInstanceOf(UserRemoteException.class)
        .hasMessageContaining(
            "When specifying the version of the table to be modified, you must also specify the version of the source table")
        .hasMessageContaining("using AT SQL syntax");

    assertThatThrownBy(
            () ->
                runSQL(
                    String.format(
                        "delete from %s at branch %s as t USING %s as s where t.EXPR$0 = s.EXPR$1",
                        devTable1, devBranchName, mainTable)))
        .isInstanceOf(UserRemoteException.class);

    // current session is DATAPLANE_PLUGIN_NAME, dev
    runSQL(useContextQuery(Collections.singletonList(DATAPLANE_PLUGIN_NAME)));
    runSQL(useBranchQuery(devBranchName));

    // update devTable1 using source in dev
    assertThatThrownBy(
            () ->
                runSQL(
                    String.format(
                        "delete from %s at branch %s as t USING %s as s where t.EXPR$0 = s.EXPR$1",
                        devTable1, devBranchName, devTable2)))
        .isInstanceOf(UserRemoteException.class)
        .hasMessageContaining(
            "When specifying the version of the table to be modified, you must also specify the version of the source table")
        .hasMessageContaining("using AT SQL syntax");

    assertThatThrownBy(
            () ->
                runSQL(
                    String.format(
                        "delete from %s at branch %s as t USING %s as s where t.EXPR$0 = s.EXPR$1",
                        devTable1, devBranchName, mainTable)))
        .isInstanceOf(UserRemoteException.class);
  }

  @Test
  public void testDeleteFromWithAtSyntax() throws Exception {
    String tableName = generateUniqueTableName();
    List<String> tablePath = Arrays.asList(tableName);
    runSQL(useContextQuery(Collections.singletonList(DATAPLANE_PLUGIN_NAME)));

    runSQL(createEmptyTableQueryWithAt(tablePath, DEFAULT_BRANCH_NAME));
    runSQL(insertTableQuery(tablePath));
    assertTableHasExpectedNumRows(tablePath, 3);

    runSQL(deleteQueryWithSpecifier(tablePath, "AT REF main"));
    assertTableHasExpectedNumRows(tablePath, 0);
  }
}
