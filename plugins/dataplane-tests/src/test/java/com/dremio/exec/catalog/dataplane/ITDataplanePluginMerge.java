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
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createBranchAtBranchQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createBranchAtSpecifierQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createEmptyTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createTagQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.dropTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueBranchName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueTableName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueTagName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.insertTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.insertTableWithValuesQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.joinedTableKey;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.mergeByIdQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.selectStarQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.tablePathWithFolders;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.useBranchQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.useContextQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.useTagQuery;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import com.dremio.catalog.model.VersionContext;
import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.catalog.model.dataset.TableVersionType;
import com.dremio.common.exceptions.UserRemoteException;
import com.dremio.exec.catalog.dataplane.test.ITDataplanePluginTestSetup;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

public class ITDataplanePluginMerge extends ITDataplanePluginTestSetup {

  @Test
  public void mergeAll() throws Exception {
    // Arrange
    final String sourceTableName = generateUniqueTableName();
    final List<String> sourceTablePath = tablePathWithFolders(sourceTableName);
    final String devBranch = generateUniqueBranchName();

    final String targetTableName = generateUniqueTableName();
    final List<String> targetTablePath = tablePathWithFolders(targetTableName);

    // Set context to main branch
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(targetTablePath));
    runSQL(insertTableQuery(targetTablePath));
    runSQL(createEmptyTableQuery(sourceTablePath));
    runSQL(insertTableQuery(sourceTablePath));
    long mtime1 =
        getMtimeForTable(
            targetTablePath,
            new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME),
            this);
    // Add an extra row in the source
    runSQL(
        insertTableWithValuesQuery(
            sourceTablePath,
            Collections.singletonList(
                "(4, CAST('fourth row' AS VARCHAR(65536)), CAST(4000 AS DECIMAL(38,3)))")));

    // Verify with select
    assertTableHasExpectedNumRows(targetTablePath, 3);
    assertTableHasExpectedNumRows(sourceTablePath, 4);
    // Create dev branch
    runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));
    // Switch to dev
    runSQL(useBranchQuery(devBranch));

    // Act
    runSQL(mergeByIdQuery(targetTablePath, sourceTablePath));
    long mtime2 =
        getMtimeForTable(
            targetTablePath, new TableVersionContext(TableVersionType.BRANCH, devBranch), this);
    // Assert
    assertTableHasExpectedNumRows(targetTablePath, 4);
    assertTableHasExpectedNumRows(sourceTablePath, 4);
    assertThat(mtime2 > mtime1).isTrue();
    // Select
    testBuilder()
        .sqlQuery(selectStarQuery(targetTablePath))
        .unOrdered()
        .baselineColumns("id", "name", "distance")
        .baselineValues(1, "first row", new BigDecimal("1.000"))
        .baselineValues(2, "second row", new BigDecimal("1.000"))
        .baselineValues(3, "third row", new BigDecimal("1.000"))
        .baselineValues(4, "fourth row", new BigDecimal("0.000"))
        .go();

    // Check that main context still has the table
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    long mtime3 =
        getMtimeForTable(
            targetTablePath,
            new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME),
            this);
    // Assert
    assertTableHasExpectedNumRows(targetTablePath, 3);
    assertTableHasExpectedNumRows(sourceTablePath, 4);
    assertThat(mtime3 == mtime1).isTrue();

    // Select
    testBuilder()
        .sqlQuery(selectStarQuery(targetTablePath))
        .unOrdered()
        .baselineColumns("id", "name", "distance")
        .baselineValues(1, "first row", new BigDecimal("1000.000"))
        .baselineValues(2, "second row", new BigDecimal("2000.000"))
        .baselineValues(3, "third row", new BigDecimal("3000.000"))
        .go();

    assertCommitLogTail(
        VersionContext.ofBranch(DEFAULT_BRANCH_NAME),
        String.format("INSERT on TABLE %s", joinedTableKey(sourceTablePath)),
        String.format("INSERT on TABLE %s", joinedTableKey(sourceTablePath)));

    assertCommitLogTail(
        VersionContext.ofBranch(devBranch),
        String.format("INSERT on TABLE %s", joinedTableKey(sourceTablePath)),
        String.format("INSERT on TABLE %s", joinedTableKey(sourceTablePath)),
        String.format("MERGE on TABLE %s", joinedTableKey(targetTablePath)));

    // cleanup
    runSQL(dropTableQuery(targetTablePath));
    runSQL(dropTableQuery(sourceTablePath));
  }

  @Test
  public void mergeWithTagSet() throws Exception {
    // Arrange
    String sourceTableName = generateUniqueTableName();
    final List<String> sourceTablePath = tablePathWithFolders(sourceTableName);
    final String targetTableName = generateUniqueTableName();
    final List<String> targetTablePath = tablePathWithFolders(targetTableName);
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(targetTablePath));
    runSQL(insertTableQuery(targetTablePath));
    runSQL(createEmptyTableQuery(sourceTablePath));
    runSQL(insertTableQuery(sourceTablePath));
    final String tag = generateUniqueTagName();
    // Act and Assert
    runSQL(createTagQuery(tag, DEFAULT_BRANCH_NAME));
    runSQL(useTagQuery(tag));
    assertQueryThrowsExpectedError(
        mergeByIdQuery(targetTablePath, sourceTablePath),
        String.format(
            "DDL and DML operations are only supported for branches - not on tags or commits. %s is not a branch.",
            tag));
  }

  @Test
  public void testDmlMergeWithAtTargetTable() throws Exception {
    String branchName = "dev";
    String mainBranch = String.format("BRANCH %s", DEFAULT_BRANCH_NAME);
    String tableName = "myTable";
    String tableName2 = "myTable2";
    runSQL(useContextQuery(Collections.singletonList(DATAPLANE_PLUGIN_NAME)));
    runSQL(String.format("CREATE table %s.%s as select 1,2,3", DATAPLANE_PLUGIN_NAME, tableName));
    runSQL(String.format("CREATE table %s.%s as select 1,5,6", DATAPLANE_PLUGIN_NAME, tableName2));
    // Merge into myTable at branch dev using myTable2 at branch dev
    // If the EXPR$0 matches, change the target's EXPR$1 to sources EXPR$2
    runSQL(createBranchAtSpecifierQuery(branchName, mainBranch));

    assertThatThrownBy(
            () ->
                runSQL(
                    String.format(
                        "MERGE INTO %s.%s AT BRANCH %s AS t USING %s.%s AS s ON (t.EXPR$0 = s.EXPR$0)\n"
                            + "  WHEN MATCHED THEN UPDATE SET EXPR$1 = s.EXPR$2",
                        DATAPLANE_PLUGIN_NAME,
                        tableName,
                        branchName,
                        DATAPLANE_PLUGIN_NAME,
                        tableName2)))
        .isInstanceOf(UserRemoteException.class)
        .hasMessageContaining(
            "When specifying the version of the table to be modified, you must also specify the version of the source table")
        .hasMessageContaining("using AT SQL syntax");
  }

  @Test
  public void testDmlMergeWithAtTargetTableNotMatched() throws Exception {
    String branchName = "dev";
    String mainBranch = String.format("BRANCH %s", DEFAULT_BRANCH_NAME);
    String tableName = "myTable";
    String tableName2 = "myTable2";
    runSQL(useContextQuery(Collections.singletonList(DATAPLANE_PLUGIN_NAME)));
    runSQL(String.format("CREATE table %s.%s as select 1,2,3", DATAPLANE_PLUGIN_NAME, tableName));
    runSQL(String.format("CREATE table %s.%s as select 4,5,6", DATAPLANE_PLUGIN_NAME, tableName2));
    // Merge into myTable at branch dev using myTable2 at branch dev
    // If the EXPR$0 matches, change the target's EXPR$1 to sources EXPR$2
    runSQL(createBranchAtSpecifierQuery(branchName, mainBranch));
    assertThatThrownBy(
            () ->
                runSQL(
                    String.format(
                        "MERGE INTO %s.%s AT BRANCH %s AS t USING %s.%s AS s ON (t.EXPR$0 = s.EXPR$0)\n"
                            + "  WHEN NOT MATCHED THEN INSERT (EXPR$0, EXPR$1, EXPR$2) VALUES (9,9,9))",
                        DATAPLANE_PLUGIN_NAME,
                        tableName,
                        branchName,
                        DATAPLANE_PLUGIN_NAME,
                        tableName2)))
        .isInstanceOf(UserRemoteException.class)
        .hasMessageContaining(
            "When specifying the version of the table to be modified, you must also specify the version of the source table")
        .hasMessageContaining("using AT SQL syntax");
  }

  @Test
  public void testDmlMergeWithAtWrongReferenceType() throws Exception {
    String tagName = "myTag";
    String tableName = "myMergeTableWithAt";
    runSQL(String.format("CREATE table %s.%s as select 1,2,3", DATAPLANE_PLUGIN_NAME, tableName));
    runSQL(createTagQuery(tagName, DEFAULT_BRANCH_NAME));
    // expect error for TAG
    assertThatThrownBy(
            () ->
                runSQL(
                    String.format(
                        "MERGE INTO %s.%s AT TAG %s AS t USING %s.%s AS s ON (t.EXPR$0 = s.EXPR$0)\n"
                            + "  WHEN MATCHED THEN UPDATE SET EXPR$1 = s.EXPR$2",
                        DATAPLANE_PLUGIN_NAME,
                        tableName,
                        tagName,
                        DATAPLANE_PLUGIN_NAME,
                        tableName)))
        .isInstanceOf(UserRemoteException.class);
  }

  @Test
  public void testDmlMerge() throws Exception {
    String tableName = "myTable";
    String tableName2 = "myTable2";
    String branchName = "dev";
    String mainBranch = String.format("BRANCH %s", DEFAULT_BRANCH_NAME);
    runSQL(String.format("CREATE table %s.%s as select 1,2,3", DATAPLANE_PLUGIN_NAME, tableName));
    runSQL(String.format("CREATE table %s.%s as select 1,5,6", DATAPLANE_PLUGIN_NAME, tableName2));
    runSQL(createBranchAtSpecifierQuery(branchName, mainBranch));
    runSQL(useContextQuery(Collections.singletonList(DATAPLANE_PLUGIN_NAME)));
    // use branch dev
    runSQL(useBranchQuery(branchName));
    runSQL(
        String.format(
            "MERGE INTO %s AS t USING %s.%s AS s ON (t.EXPR$0 = s.EXPR$0)\n"
                + "  WHEN MATCHED THEN UPDATE SET EXPR$1 = s.EXPR$2",
            tableName, DATAPLANE_PLUGIN_NAME, tableName2));
    // dev branch should have 1, 6, 3
    assertThat(
            runSqlWithResults(
                String.format(
                    "select * from %s.%s at branch %s",
                    DATAPLANE_PLUGIN_NAME, tableName, branchName)))
        .isEqualTo(Collections.singletonList(Arrays.asList("1", "6", "3")));
    // no effects on the main branch.
    assertThat(
            runSqlWithResults(
                String.format(
                    "select * from %s.%s at branch %s",
                    DATAPLANE_PLUGIN_NAME, tableName, DEFAULT_BRANCH_NAME)))
        .isEqualTo(Collections.singletonList(Arrays.asList("1", "2", "3")));
  }

  @Test
  public void testDmlMergeWithContextWithDifferentAtStatement() throws Exception {
    String tableName = "myTable";
    String tableName2 = "myTable2";
    String branchName = "dev";
    String mainBranch = String.format("BRANCH %s", DEFAULT_BRANCH_NAME);
    runSQL(String.format("CREATE table %s.%s as select 1,2,3", DATAPLANE_PLUGIN_NAME, tableName));
    runSQL(String.format("CREATE table %s.%s as select 1,5,6", DATAPLANE_PLUGIN_NAME, tableName2));
    runSQL(createBranchAtSpecifierQuery(branchName, mainBranch));
    runSQL(useContextQuery(Collections.singletonList(DATAPLANE_PLUGIN_NAME)));
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));

    // branch is set to main branch, but we are explicitly using dev Branch

    assertThatThrownBy(
            () ->
                runSQL(
                    String.format(
                        "MERGE INTO %s at branch %s AS t USING %s.%s AS s ON (t.EXPR$0 = s.EXPR$0)\n"
                            + "  WHEN MATCHED THEN UPDATE SET EXPR$1 = s.EXPR$2",
                        tableName, branchName, DATAPLANE_PLUGIN_NAME, tableName2)))
        .isInstanceOf(UserRemoteException.class)
        .hasMessageContaining(
            "When specifying the version of the table to be modified, you must also specify the version of the source table")
        .hasMessageContaining("using AT SQL syntax");
  }

  @Test
  public void testDmlMergeWithContextWithDifferentAtStatementToNonExistentTable() throws Exception {
    String tableName = "myTable";
    String branchName = "dev";
    String mainBranch = String.format("BRANCH %s", DEFAULT_BRANCH_NAME);
    runSQL(createBranchAtSpecifierQuery(branchName, mainBranch));
    runSQL(useContextQuery(Collections.singletonList(DATAPLANE_PLUGIN_NAME)));
    runSQL(useBranchQuery(branchName));
    runSQL(String.format("CREATE table %s.%s as select 1,2,3", DATAPLANE_PLUGIN_NAME, tableName));
    // we only have table in the dev branch.
    assertThatThrownBy(
            () ->
                runSQL(
                    String.format(
                        "MERGE INTO %s.%s at branch %s AS t USING %s.%s AS s ON (t.EXPR$0 = s.EXPR$0)\n"
                            + "  WHEN MATCHED THEN UPDATE SET EXPR$1 = s.EXPR$2",
                        DATAPLANE_PLUGIN_NAME,
                        tableName,
                        DEFAULT_BRANCH_NAME,
                        DATAPLANE_PLUGIN_NAME,
                        tableName)))
        .isInstanceOf(UserRemoteException.class);
  }

  @Test
  public void testMergeDifferentVersionContext() throws Exception {
    String devBranch = "devBranch";
    String mainBranch = String.format("BRANCH %s", DEFAULT_BRANCH_NAME);
    String devTable = "devTable";
    String mainTable = "mainTable";
    runSQL(createBranchAtSpecifierQuery(devBranch, mainBranch));
    runSQL(useContextQuery(Collections.singletonList(DATAPLANE_PLUGIN_NAME)));
    runSQL(useBranchQuery(devBranch));

    runSQL(String.format("CREATE table %s.%s as select 1,2,3", DATAPLANE_PLUGIN_NAME, devTable));
    runSQL(
        String.format(
            "CREATE table %s.%s at branch %s as select 1,2,3",
            DATAPLANE_PLUGIN_NAME, mainTable, DEFAULT_BRANCH_NAME));

    runSQL(
        String.format(
            "MERGE INTO %s.%s AT BRANCH %s as t using %s.%s at branch %s as s on t.EXPR$0 = s.EXPR$0\n"
                + "when matched then update set EXPR$0 = 888",
            DATAPLANE_PLUGIN_NAME,
            devTable,
            devBranch,
            DATAPLANE_PLUGIN_NAME,
            mainTable,
            DEFAULT_BRANCH_NAME));
    assertThat(
            runSqlWithResults(
                String.format(
                    "select * from %s.%s at branch %s",
                    DATAPLANE_PLUGIN_NAME, mainTable, DEFAULT_BRANCH_NAME)))
        .isEqualTo(Collections.singletonList(Arrays.asList("1", "2", "3")));
    assertThat(
            runSqlWithResults(
                String.format(
                    "select * from %s.%s at branch %s",
                    DATAPLANE_PLUGIN_NAME, devTable, devBranch)))
        .isEqualTo(Collections.singletonList(Arrays.asList("888", "2", "3")));
  }

  @Test
  public void testMergeUseSelectWithAt() throws Exception {
    String devBranch = "devBranch";
    String mainBranch = String.format("BRANCH %s", DEFAULT_BRANCH_NAME);
    String devTable = "devTable";
    String mainTable = "mainTable";
    runSQL(createBranchAtSpecifierQuery(devBranch, mainBranch));
    runSQL(useContextQuery(Collections.singletonList(DATAPLANE_PLUGIN_NAME)));
    runSQL(useBranchQuery(devBranch));

    runSQL(String.format("CREATE table %s.%s as select 1,2,3", DATAPLANE_PLUGIN_NAME, devTable));
    runSQL(
        String.format(
            "CREATE table %s.%s at branch %s as select 1,2,3",
            DATAPLANE_PLUGIN_NAME, mainTable, DEFAULT_BRANCH_NAME));

    runSQL(
        String.format(
            "MERGE INTO %s.%s at branch %s as t\n"
                + "USING (SELECT * FROM %s.%s at branch %s ) AS s\n"
                + "ON (t.EXPR$0 = s.EXPR$0) WHEN MATCHED THEN UPDATE SET EXPR$2 = 999",
            DATAPLANE_PLUGIN_NAME,
            devTable,
            devBranch,
            DATAPLANE_PLUGIN_NAME,
            mainTable,
            DEFAULT_BRANCH_NAME));
    assertThat(
            runSqlWithResults(
                String.format(
                    "select * from %s.%s at branch %s",
                    DATAPLANE_PLUGIN_NAME, mainTable, DEFAULT_BRANCH_NAME)))
        .isEqualTo(Collections.singletonList(Arrays.asList("1", "2", "3")));
    assertThat(
            runSqlWithResults(
                String.format(
                    "select * from %s.%s at branch %s",
                    DATAPLANE_PLUGIN_NAME, devTable, devBranch)))
        .isEqualTo(Collections.singletonList(Arrays.asList("1", "2", "999")));
  }

  @Test
  public void testMergeUseSelectWithAtRef() throws Exception {
    String devBranch = "devBranch";
    String mainBranch = String.format("BRANCH %s", DEFAULT_BRANCH_NAME);
    String devTable = "devTable";
    String mainTable = "mainTable";
    runSQL(createBranchAtSpecifierQuery(devBranch, mainBranch));
    runSQL(useContextQuery(Collections.singletonList(DATAPLANE_PLUGIN_NAME)));
    runSQL(useBranchQuery(devBranch));

    runSQL(String.format("CREATE table %s.%s as select 1,2,3", DATAPLANE_PLUGIN_NAME, devTable));
    runSQL(
        String.format(
            "CREATE table %s.%s at branch %s as select 1,2,3",
            DATAPLANE_PLUGIN_NAME, mainTable, DEFAULT_BRANCH_NAME));

    runSQL(
        String.format(
            "MERGE INTO %s.%s at REF %s as t\n"
                + "USING (SELECT * FROM %s.%s at branch %s ) AS s\n"
                + "ON (t.EXPR$0 = s.EXPR$0) WHEN MATCHED THEN UPDATE SET EXPR$2 = 999",
            DATAPLANE_PLUGIN_NAME,
            devTable,
            devBranch,
            DATAPLANE_PLUGIN_NAME,
            mainTable,
            DEFAULT_BRANCH_NAME));
    assertThat(
            runSqlWithResults(
                String.format(
                    "select * from %s.%s at branch %s",
                    DATAPLANE_PLUGIN_NAME, mainTable, DEFAULT_BRANCH_NAME)))
        .isEqualTo(Collections.singletonList(Arrays.asList("1", "2", "3")));
    assertThat(
            runSqlWithResults(
                String.format(
                    "select * from %s.%s at branch %s",
                    DATAPLANE_PLUGIN_NAME, devTable, devBranch)))
        .isEqualTo(Collections.singletonList(Arrays.asList("1", "2", "999")));
  }

  @Test
  public void testDmlMergeOnlySourceVersionSpecified() throws Exception {
    String mainTable = "mainTable";
    String devTable = "devTable";
    String devBranchName = "dev";
    String mainBranch = String.format("BRANCH %s", DEFAULT_BRANCH_NAME);
    runSQL(createBranchAtSpecifierQuery(devBranchName, mainBranch));

    // current session is DATAPLANE_PLUGIN_NAME, DEFAULT_BRANCH
    runSQL(useContextQuery(Collections.singletonList(DATAPLANE_PLUGIN_NAME)));
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    runSQL(
        String.format(
            "CREATE table %s at branch %s as select 1,2,3", mainTable, DEFAULT_BRANCH_NAME));
    runSQL(String.format("CREATE table %s at branch %s as select 1,2,3", devTable, devBranchName));

    // update mainTable using source in dev
    runSQL(
        String.format(
            "MERGE INTO %s.%s as t using %s.%s at branch %s as s on t.EXPR$0 = s.EXPR$0\n"
                + "when matched then update set EXPR$0 = 12345",
            DATAPLANE_PLUGIN_NAME, mainTable, DATAPLANE_PLUGIN_NAME, devTable, devBranchName));

    assertThat(
            runSqlWithResults(
                String.format(
                    "select * from %s.%s at branch %s",
                    DATAPLANE_PLUGIN_NAME, mainTable, DEFAULT_BRANCH_NAME)))
        .isEqualTo(Collections.singletonList(Arrays.asList("12345", "2", "3")));
    assertThat(
            runSqlWithResults(
                String.format(
                    "select * from %s.%s at branch %s",
                    DATAPLANE_PLUGIN_NAME, devTable, devBranchName)))
        .isEqualTo(Collections.singletonList(Arrays.asList("1", "2", "3")));

    // current session is DATAPLANE_PLUGIN_NAME, dev
    runSQL(useContextQuery(Collections.singletonList(DATAPLANE_PLUGIN_NAME)));
    runSQL(useBranchQuery(devBranchName));

    // update mainTable using source in dev should error since the target version was not specified
    // and the session is dev.
    assertThatThrownBy(
            () ->
                runSQL(
                    String.format(
                        "MERGE INTO %s.%s as t using %s.%s at branch %s as s on t.EXPR$0 = s.EXPR$0\n"
                            + "when matched then update set EXPR$0 = 12345",
                        DATAPLANE_PLUGIN_NAME,
                        mainTable,
                        DATAPLANE_PLUGIN_NAME,
                        devTable,
                        devBranchName)))
        .isInstanceOf(UserRemoteException.class);
  }

  @Test
  public void testDmlUpdateBothSourceVersionSpecified() throws Exception {
    String mainTable = "mainTable";
    String devTable = "devTable";
    String devBranchName = "dev";
    String mainBranch = String.format("BRANCH %s", DEFAULT_BRANCH_NAME);
    runSQL(createBranchAtSpecifierQuery(devBranchName, mainBranch));

    // current session is DATAPLANE_PLUGIN_NAME, DEFAULT_BRANCH
    runSQL(useContextQuery(Collections.singletonList(DATAPLANE_PLUGIN_NAME)));
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    runSQL(
        String.format(
            "CREATE table %s at branch %s as select 1,2,3", mainTable, DEFAULT_BRANCH_NAME));
    runSQL(String.format("CREATE table %s at branch %s as select 1,2,3", devTable, devBranchName));

    // update mainTable using source in dev
    runSQL(
        String.format(
            "MERGE INTO %s.%s at branch %s as t using %s.%s at branch %s as s on t.EXPR$0 = s.EXPR$0\n"
                + "when matched then update set EXPR$0 = 321",
            DATAPLANE_PLUGIN_NAME,
            mainTable,
            DEFAULT_BRANCH_NAME,
            DATAPLANE_PLUGIN_NAME,
            devTable,
            devBranchName));

    assertThat(
            runSqlWithResults(
                String.format(
                    "select * from %s.%s at branch %s",
                    DATAPLANE_PLUGIN_NAME, mainTable, DEFAULT_BRANCH_NAME)))
        .isEqualTo(Collections.singletonList(Arrays.asList("321", "2", "3")));
    assertThat(
            runSqlWithResults(
                String.format(
                    "select * from %s.%s at branch %s",
                    DATAPLANE_PLUGIN_NAME, devTable, devBranchName)))
        .isEqualTo(Collections.singletonList(Arrays.asList("1", "2", "3")));

    // current session is DATAPLANE_PLUGIN_NAME, dev
    runSQL(useContextQuery(Collections.singletonList(DATAPLANE_PLUGIN_NAME)));
    runSQL(useBranchQuery(devBranchName));

    // update mainTable using source in dev
    runSQL(
        String.format(
            "MERGE INTO %s.%s at branch %s as t using %s.%s at branch %s as s on t.EXPR$1 = s.EXPR$1\n"
                + "when matched then update set EXPR$1 = 54321",
            DATAPLANE_PLUGIN_NAME,
            mainTable,
            DEFAULT_BRANCH_NAME,
            DATAPLANE_PLUGIN_NAME,
            devTable,
            devBranchName));

    assertThat(
            runSqlWithResults(
                String.format(
                    "select * from %s.%s at branch %s",
                    DATAPLANE_PLUGIN_NAME, mainTable, DEFAULT_BRANCH_NAME)))
        .isEqualTo(Collections.singletonList(Arrays.asList("321", "54321", "3")));
    assertThat(
            runSqlWithResults(
                String.format(
                    "select * from %s.%s at branch %s",
                    DATAPLANE_PLUGIN_NAME, devTable, devBranchName)))
        .isEqualTo(Collections.singletonList(Arrays.asList("1", "2", "3")));
  }

  // TODO: When DX-69248 is fixed, this test should be modified.
  // TODO: Current behaviour: when we only have version for the target table, DML query throws an
  // error (either you should specify both, or only source table, or no specification)
  @Test
  public void testDmlMergeOnlyTargetVersionSpecified() throws Exception {
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
                        "MERGE INTO %s.%s at branch %s as t using %s.%s as s on t.EXPR$0 = s.EXPR$0\n"
                            + "when matched then update set EXPR$0 = 12345",
                        DATAPLANE_PLUGIN_NAME,
                        devTable1,
                        devBranchName,
                        DATAPLANE_PLUGIN_NAME,
                        devTable2)))
        .isInstanceOf(UserRemoteException.class)
        .hasMessageContaining(
            "When specifying the version of the table to be modified, you must also specify the version of the source table")
        .hasMessageContaining("using AT SQL syntax");

    assertThatThrownBy(
            () ->
                runSQL(
                    String.format(
                        "MERGE INTO %s.%s at branch %s as t using %s.%s as s on t.EXPR$0 = s.EXPR$0\n"
                            + "when matched then update set EXPR$0 = 12345",
                        DATAPLANE_PLUGIN_NAME,
                        devTable1,
                        devBranchName,
                        DATAPLANE_PLUGIN_NAME,
                        mainTable)))
        .isInstanceOf(UserRemoteException.class);

    // current session is DATAPLANE_PLUGIN_NAME, dev
    runSQL(useContextQuery(Collections.singletonList(DATAPLANE_PLUGIN_NAME)));
    runSQL(useBranchQuery(devBranchName));

    // update devTable1 using source in dev
    assertThatThrownBy(
            () ->
                runSQL(
                    String.format(
                        "MERGE INTO %s.%s at branch %s as t using %s.%s as s on t.EXPR$1 = s.EXPR$1\n"
                            + "when matched then update set EXPR$1 = 67890",
                        DATAPLANE_PLUGIN_NAME,
                        devTable1,
                        devBranchName,
                        DATAPLANE_PLUGIN_NAME,
                        devTable2)))
        .isInstanceOf(UserRemoteException.class)
        .hasMessageContaining(
            "When specifying the version of the table to be modified, you must also specify the version of the source table")
        .hasMessageContaining("using AT SQL syntax");

    assertThatThrownBy(
            () ->
                runSQL(
                    String.format(
                        "MERGE INTO %s.%s at branch %s as t using %s.%s as s on t.EXPR$0 = s.EXPR$0\n"
                            + "when matched then update set EXPR$1 = 67890",
                        DATAPLANE_PLUGIN_NAME,
                        devTable1,
                        devBranchName,
                        DATAPLANE_PLUGIN_NAME,
                        mainTable)))
        .isInstanceOf(UserRemoteException.class);
  }
}
