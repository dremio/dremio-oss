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
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createBranchAtBranchQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createBranchAtSpecifierQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createEmptyTableQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createTagQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.dropTableQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueBranchName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueTableName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueTagName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.insertTableQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.insertTableWithValuesQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.joinedTableKey;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.selectStarQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.tablePathWithFolders;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.updateAtQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.updateAtQueryWithAtRef;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.updateByIdFromAnotherBranchQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.updateByIdQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.useBranchQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.useContextQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.useTagQuery;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.dremio.catalog.model.VersionContext;
import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.catalog.model.dataset.TableVersionType;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.exceptions.UserRemoteException;


public class ITDataplanePluginUpdate extends ITDataplanePluginTestSetup {

  @Test
  public void updateOne() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final String devBranch = generateUniqueBranchName();
    // Set context to main branch
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(tablePath));
    runSQL(insertTableQuery(tablePath));
    // Verify with select
    assertTableHasExpectedNumRows(tablePath, 3);
    // Create dev branch
    runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));
    // Switch to dev
    runSQL(useBranchQuery(devBranch));

    // Act
    runSQL(updateByIdQuery(tablePath));

    // Assert
    assertTableHasExpectedNumRows(tablePath, 3);
    // Select
    testBuilder()
      .sqlQuery(selectStarQuery(tablePath))
      .unOrdered()
      .baselineColumns("id", "name", "distance")
      .baselineValues(1, "first row", new BigDecimal("1000.000"))
      .baselineValues(2, "second row", new BigDecimal("2000.000"))
      .baselineValues(3, "third row", new BigDecimal("30000.000"))
      .go();

    //Check that main context still has the table
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    // Assert
    assertTableHasExpectedNumRows(tablePath, 3);
    // Select
    testBuilder()
      .sqlQuery(selectStarQuery(tablePath))
      .unOrdered()
      .baselineColumns("id", "name", "distance")
      .baselineValues(1, "first row", new BigDecimal("1000.000"))
      .baselineValues(2, "second row", new BigDecimal("2000.000"))
      .baselineValues(3, "third row", new BigDecimal("3000.000"))
      .go();

    assertCommitLogTail(
      VersionContext.ofBranch(DEFAULT_BRANCH_NAME),
      String.format("CREATE TABLE %s", joinedTableKey(tablePath)),
      String.format("INSERT on TABLE %s", joinedTableKey(tablePath))
    );

    assertCommitLogTail(
      VersionContext.ofBranch(devBranch),
      String.format("CREATE TABLE %s", joinedTableKey(tablePath)),
      String.format("INSERT on TABLE %s", joinedTableKey(tablePath)),
      String.format("UPDATE on TABLE %s", joinedTableKey(tablePath))
    );

    //cleanup
    runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void updateOneFromAnotherBranch() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final String devBranch = generateUniqueBranchName();
    // Set context to main branch
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(tablePath));
    runSQL(insertTableQuery(tablePath));
    long mtime1 = getMtimeForTable(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME),this);
    // Create dev branch
    runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));
    // Switch to dev
    runSQL(useBranchQuery(devBranch));
    // Insert more data
    runSQL(insertTableWithValuesQuery(tablePath,
      Collections.singletonList("(4, CAST('fourth row' AS VARCHAR(65536)), CAST(4000 AS DECIMAL(38,3)))")));
    // Assert
    assertTableHasExpectedNumRows(tablePath, 4);
    // Switch to main
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));

    // Act
    runSQL(updateByIdFromAnotherBranchQuery(tablePath, devBranch));
    long mtime2 = getMtimeForTable(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), this);
    // Assert
    assertTableHasExpectedNumRows(tablePath, 3);
    assertThat(mtime2 > mtime1).isTrue();
    // Select
    testBuilder()
      .sqlQuery(selectStarQuery(tablePath))
      .unOrdered()
      .baselineColumns("id", "name", "distance")
      .baselineValues(1, "first row", new BigDecimal("1000.000"))
      .baselineValues(2, "second row", new BigDecimal("2000.000"))
      .baselineValues(3, "third row", new BigDecimal("4000.000"))
      .go();

    //cleanup
    runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void updateAgnosticOfSourceBucket() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(tablePath));
    runSQL(insertTableQuery(tablePath));

    // Act
    runWithAlternateSourcePath(updateByIdQuery(tablePath));
    assertAllFilesAreInBaseBucket(tablePath);

    // Assert
    assertTableHasExpectedNumRows(tablePath, 3);
    // Select
    testBuilder()
      .sqlQuery(selectStarQuery(tablePath))
      .unOrdered()
      .baselineColumns("id", "name", "distance")
      .baselineValues(1, "first row", new BigDecimal("1000.000"))
      .baselineValues(2, "second row", new BigDecimal("2000.000"))
      .baselineValues(3, "third row", new BigDecimal("30000.000"))
      .go();
  }

  @Test
  public void updateWithTagSet() throws Exception {
    // Arrange
    String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(tablePath));
    runSQL(insertTableQuery(tablePath));
    final String tag = generateUniqueTagName();
    // Act and Assert
    runSQL(createTagQuery(tag, DEFAULT_BRANCH_NAME));
    runSQL(useTagQuery(tag));
    assertQueryThrowsExpectedError(updateByIdQuery(tablePath),
            String.format("DDL and DML operations are only supported for branches - not on tags or commits. %s is not a branch.",
                    tag));
  }

  @Test
  public void testDmlUpdateWithAtNonVersionedSource() throws Exception {
    String nonVersionedSourceName = "dfs_test";
    String tableName = "myTable";
    runSQL(String.format("CREATE table %s.%s as select 1,2,3",nonVersionedSourceName, tableName));
    assertThatThrownBy(() -> runSQL(updateAtQuery(nonVersionedSourceName, tableName, DEFAULT_BRANCH_NAME)))
      .isInstanceOf(UserException.class)
      .hasMessageContaining(String.format("Source [%s] does not support", nonVersionedSourceName))
      .hasMessageContaining("version specification");
  }

  @Test
  public void testDmlUpdateWithAt() throws Exception {
    String branchName = "devBranch";
    String mainBranch = String.format("BRANCH %s", DEFAULT_BRANCH_NAME);
    String tableName = "myTable";
    runSQL(String.format("CREATE table %s.%s as select 1,2,3",DATAPLANE_PLUGIN_NAME, tableName));
    runSQL(createBranchAtSpecifierQuery(branchName, mainBranch));
    runSQL(updateAtQuery(DATAPLANE_PLUGIN_NAME, tableName, branchName));
    Assertions.assertThat(runSqlWithResults(String.format("select * from %s.%s at branch %s",DATAPLANE_PLUGIN_NAME, tableName, branchName)))
      .isEqualTo(Collections.singletonList(Arrays.asList("2", "2", "3")));
    Assertions.assertThat(runSqlWithResults(String.format("select * from %s.%s at branch %s",DATAPLANE_PLUGIN_NAME, tableName, DEFAULT_BRANCH_NAME)))
      .isEqualTo(Collections.singletonList(Arrays.asList("1", "2", "3")));
  }

  @Test
  public void testDmlUpdateWithRef() throws Exception {
    String branchName = "devBranch";
    String mainBranch = String.format("BRANCH %s", DEFAULT_BRANCH_NAME);
    String tableName = "myTable";
    runSQL(String.format("CREATE table %s.%s as select 1,2,3",DATAPLANE_PLUGIN_NAME, tableName));
    runSQL(createBranchAtSpecifierQuery(branchName, mainBranch));
    runSQL(updateAtQueryWithAtRef(DATAPLANE_PLUGIN_NAME, tableName, branchName));
    Assertions.assertThat(runSqlWithResults(String.format("select * from %s.%s at branch %s",DATAPLANE_PLUGIN_NAME, tableName, branchName)))
      .isEqualTo(Collections.singletonList(Arrays.asList("2", "2", "3")));
    Assertions.assertThat(runSqlWithResults(String.format("select * from %s.%s at branch %s",DATAPLANE_PLUGIN_NAME, tableName, DEFAULT_BRANCH_NAME)))
      .isEqualTo(Collections.singletonList(Arrays.asList("1", "2", "3")));
  }

  @Test
  public void testDmlUpdateWithAtWrongReferenceType() throws Exception {
    String tagName = "myTag";
    String tableName = "myTruncateTableWithAt";
    runSQL(String.format("CREATE table %s.%s as select 1,2,3",DATAPLANE_PLUGIN_NAME, tableName));
    runSQL(createTagQuery(tagName, DEFAULT_BRANCH_NAME));
    // expect error for TAG
    assertThatThrownBy(() -> runSQL(String.format("update %s.%s at TAG %s set EXPR$0 = 2", DATAPLANE_PLUGIN_NAME, tableName, tagName)))
      .isInstanceOf(UserRemoteException.class);
  }

  @Test
  public void testDmlUpdate() throws Exception {
    String tableName = "myTable";
    runSQL(String.format("CREATE table %s.%s as select 1,2,3", DATAPLANE_PLUGIN_NAME, tableName));
    runSQL(useContextQuery());
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    runSQL(String.format("update %s set EXPR$0 = 2", tableName));
    Assertions.assertThat(runSqlWithResults(String.format("select * from %s.%s at branch %s",DATAPLANE_PLUGIN_NAME, tableName, DEFAULT_BRANCH_NAME)))
      .isEqualTo(Collections.singletonList(Arrays.asList("2", "2", "3")));
  }

  @Test
  public void testDmlUpdateWithContextWithDifferentAtStatement() throws Exception {
    String tableName = "myTable";
    String devBranch = "dev";
    String mainBranch = String.format("BRANCH %s", DEFAULT_BRANCH_NAME);
    runSQL(String.format("CREATE table %s.%s as select 1,2,3", DATAPLANE_PLUGIN_NAME, tableName));
    runSQL(createBranchAtSpecifierQuery(devBranch, mainBranch));
    runSQL(useContextQuery());
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    runSQL(String.format("update %s at branch %s set EXPR$0 = 2", tableName, devBranch));
    Assertions.assertThat(runSqlWithResults(String.format("select * from %s.%s at branch %s",DATAPLANE_PLUGIN_NAME, tableName, DEFAULT_BRANCH_NAME)))
      .isEqualTo(Collections.singletonList(Arrays.asList("1", "2", "3")));
    Assertions.assertThat(runSqlWithResults(String.format("select * from %s.%s at branch %s",DATAPLANE_PLUGIN_NAME, tableName, devBranch)))
      .isEqualTo(Collections.singletonList(Arrays.asList("2", "2", "3")));
  }

  @Test
  public void testDmlUpdateWithContextWithDifferentAtStatementToNonExistentTable() throws Exception {
    String tableName = "myTable";
    String devBranch = "dev";
    String mainBranch = String.format("BRANCH %s", DEFAULT_BRANCH_NAME);
    runSQL(createBranchAtSpecifierQuery(devBranch, mainBranch));
    runSQL(useContextQuery());
    runSQL(useBranchQuery(devBranch));
    runSQL(String.format("CREATE table %s.%s as select 1,2,3", DATAPLANE_PLUGIN_NAME, tableName));
    assertThatThrownBy(() -> runSQL(String.format("update %s at branch %s set EXPR$0 = 2", tableName, DEFAULT_BRANCH_NAME)))
      .isInstanceOf(UserRemoteException.class);
  }

  @Test
  public void testUpdateOnlyExistsInNonMainBranch() throws Exception {
    String branchName = "devBranch";
    String mainBranch = String.format("BRANCH %s", DEFAULT_BRANCH_NAME);
    String tableName1 = "myTable1";
    runSQL(createBranchAtSpecifierQuery(branchName, mainBranch));
    runSQL(useContextQuery());
    runSQL(useBranchQuery(branchName));
    //table1 only exists in the dev branch
    runSQL(String.format("CREATE table %s.%s as select 1,2,3",DATAPLANE_PLUGIN_NAME, tableName1));

    runSQL(updateAtQuery(DATAPLANE_PLUGIN_NAME, tableName1, branchName));
    Assertions.assertThat(runSqlWithResults(String.format("select * from %s.%s at branch %s",DATAPLANE_PLUGIN_NAME, tableName1, branchName)))
      .isEqualTo(Collections.singletonList(Arrays.asList("2", "2", "3")));
  }

  @Test
  public void testDmlUpdateOnlySourceVersionSpecified() throws Exception {
    String mainTable = "mainTable";
    String devTable = "devTable";
    String devBranchName = "dev";
    String mainBranch = String.format("BRANCH %s", DEFAULT_BRANCH_NAME);
    runSQL(createBranchAtSpecifierQuery(devBranchName, mainBranch));

    // current session is DATAPLANE_PLUGIN_NAME, DEFAULT_BRANCH
    runSQL(useContextQuery());
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    runSQL(String.format("CREATE table %s at branch %s as select 1,2,3", mainTable, DEFAULT_BRANCH_NAME));
    runSQL(String.format("CREATE table %s at branch %s as select 1,2,3", devTable, devBranchName));

    // update mainTable using source in dev
    runSQL(String.format("update %s set EXPR$0 = s.EXPR$1 FROM %s at branch %s as s", mainTable, devTable, devBranchName));

    Assertions.assertThat(runSqlWithResults(String.format("select * from %s.%s at branch %s",DATAPLANE_PLUGIN_NAME, mainTable, DEFAULT_BRANCH_NAME)))
      .isEqualTo(Collections.singletonList(Arrays.asList("2", "2", "3")));
    Assertions.assertThat(runSqlWithResults(String.format("select * from %s.%s at branch %s",DATAPLANE_PLUGIN_NAME, devTable, devBranchName)))
      .isEqualTo(Collections.singletonList(Arrays.asList("1", "2", "3")));

    // current session is DATAPLANE_PLUGIN_NAME, dev
    runSQL(useContextQuery());
    runSQL(useBranchQuery(devBranchName));

    // update mainTable using source in dev should error since the target version was not specified and the session is dev.

    assertThatThrownBy(() -> runSQL(String.format("update %s set EXPR$1 = s.EXPR$2 FROM %s at branch %s as s", mainTable, devTable, devBranchName)))
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
    runSQL(useContextQuery());
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    runSQL(String.format("CREATE table %s at branch %s as select 1,2,3", mainTable, DEFAULT_BRANCH_NAME));
    runSQL(String.format("CREATE table %s at branch %s as select 1,2,3", devTable, devBranchName));

    // update mainTable using source in dev
    runSQL(String.format("update %s at branch %s set EXPR$0 = s.EXPR$1 FROM %s at branch %s as s", mainTable, DEFAULT_BRANCH_NAME,devTable, devBranchName));

    Assertions.assertThat(runSqlWithResults(String.format("select * from %s.%s at branch %s",DATAPLANE_PLUGIN_NAME, mainTable, DEFAULT_BRANCH_NAME)))
      .isEqualTo(Collections.singletonList(Arrays.asList("2", "2", "3")));
    Assertions.assertThat(runSqlWithResults(String.format("select * from %s.%s at branch %s",DATAPLANE_PLUGIN_NAME, devTable, devBranchName)))
      .isEqualTo(Collections.singletonList(Arrays.asList("1", "2", "3")));

    // current session is DATAPLANE_PLUGIN_NAME, dev
    runSQL(useContextQuery());
    runSQL(useBranchQuery(devBranchName));

    // update mainTable using source in dev
    runSQL(String.format("update %s at branch %s set EXPR$1 = s.EXPR$2 FROM %s at branch %s as s", mainTable, DEFAULT_BRANCH_NAME, devTable, devBranchName));

    Assertions.assertThat(runSqlWithResults(String.format("select * from %s.%s at branch %s",DATAPLANE_PLUGIN_NAME, mainTable, DEFAULT_BRANCH_NAME)))
      .isEqualTo(Collections.singletonList(Arrays.asList("2", "3", "3")));
    Assertions.assertThat(runSqlWithResults(String.format("select * from %s.%s at branch %s",DATAPLANE_PLUGIN_NAME, devTable, devBranchName)))
      .isEqualTo(Collections.singletonList(Arrays.asList("1", "2", "3")));
  }

  //TODO: When DX-69248 is fixed, this test should be modified.
  //TODO: Current behaviour: when we only have version for the target table, DML query throws an error (either you should specify both, or only source table, or no specification)
  @Test
  public void testDmlUpdateOnlyTargetVersionSpecifiedThrowsError() throws Exception {
    String mainTable = "mainTable";
    String devTable1 = "devTable1";
    String devTable2 = "devTable2";
    String devBranchName = "dev";
    String mainBranch = String.format("BRANCH %s", DEFAULT_BRANCH_NAME);
    runSQL(createBranchAtSpecifierQuery(devBranchName, mainBranch));

    // current session is DATAPLANE_PLUGIN_NAME, DEFAULT_BRANCH
    runSQL(useContextQuery());
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    runSQL(String.format("CREATE table %s at branch %s as select 1,2,3", mainTable, DEFAULT_BRANCH_NAME));
    runSQL(String.format("CREATE table %s at branch %s as select 1,2,3", devTable1, devBranchName));
    runSQL(String.format("CREATE table %s at branch %s as select 1,2,3", devTable2, devBranchName));

    // update devTable1 using source in dev
    assertThatThrownBy(() -> runSQL(String.format("update %s at branch %s set EXPR$0 = s.EXPR$1 FROM %s as s", devTable1, devBranchName, devTable2)))
      .isInstanceOf(UserRemoteException.class)
      .hasMessageContaining("When specifying the version of the table to be modified, you must also specify the version of the source table")
      .hasMessageContaining("using AT SQL syntax");

    assertThatThrownBy(() -> runSQL(String.format("update %s at branch %s set EXPR$0 = s.EXPR$1 FROM %s as s", devTable1, devBranchName, mainTable)))
      .isInstanceOf(UserRemoteException.class);

    // current session is DATAPLANE_PLUGIN_NAME, dev
    runSQL(useContextQuery());
    runSQL(useBranchQuery(devBranchName));

    // update devTable1 using source in dev
    assertThatThrownBy(() -> runSQL(String.format("update %s at branch %s set EXPR$1 = s.EXPR$2 FROM %s as s", devTable1, devBranchName, devTable2)))
      .isInstanceOf(UserRemoteException.class)
      .hasMessageContaining("When specifying the version of the table to be modified, you must also specify the version of the source table")
      .hasMessageContaining("using AT SQL syntax");

    assertThatThrownBy(() -> runSQL(String.format("update %s at branch %s set EXPR$0 = s.EXPR$1 FROM %s as s", devTable1, devBranchName, mainTable)))
      .isInstanceOf(UserRemoteException.class);
  }

  @Test
  public void testDmlUpdateOnlyTargetVersionSpecifiedWithSourceTableJoin() throws Exception {
    String devBranchName = "devBranch";
    String mainTable1 = "mainTable1";
    String mainTable2 = "mainTable2";
    String devTable1 = "devTable1";
    String devTable2 = "devTable2";
    String mainBranch = String.format("BRANCH %s", DEFAULT_BRANCH_NAME);
    runSQL(createBranchAtSpecifierQuery(devBranchName, mainBranch));

    runSQL(useContextQuery());
    runSQL(useBranchQuery(devBranchName));

    runSQL(String.format("CREATE table %s at branch %s as select 1,2,3", mainTable1, DEFAULT_BRANCH_NAME));
    runSQL(String.format("CREATE table %s at branch %s as select 1,2,3", mainTable2, DEFAULT_BRANCH_NAME));
    runSQL(String.format("CREATE table %s at branch %s as select 1,2,3", devTable1, devBranchName));
    runSQL(String.format("CREATE table %s at branch %s as select 1,2,3", devTable2, devBranchName));

    assertThatThrownBy(() -> runSQL(
      String.format(
        "update %s at branch %s set EXPR$0 = 1 \n" +
          "from %s at branch %s \n" +
          "JOIN %s at branch %s on %s.EXPR$0 = %s.EXPR$0\n" +
          "JOIN %s on %s.EXPR$0 = %s.EXPR$0"
      ,mainTable1, DEFAULT_BRANCH_NAME, devTable2, devBranchName, devTable1, devBranchName, devTable2, devTable1, mainTable2, devTable1, mainTable2
      )))
      .isInstanceOf(UserRemoteException.class)
      .hasMessageContaining("When specifying the version of the table to be modified, you must also specify the version of the source table")
      .hasMessageContaining("using AT SQL syntax");

    //fully qualified
    runSQL(
      String.format(
        "update %s at branch %s set EXPR$2 = 1 \n" +
          "from %s at branch %s \n" +
          "JOIN %s at branch %s on %s.EXPR$0 = %s.EXPR$0\n" +
          "JOIN %s at branch %s on %s.EXPR$0 = %s.EXPR$0"
        ,mainTable1, DEFAULT_BRANCH_NAME, devTable2, devBranchName, devTable1, devBranchName, devTable2, devTable1, mainTable2, DEFAULT_BRANCH_NAME, devTable1, mainTable2
      ));

    Assertions.assertThat(runSqlWithResults(String.format("select * from %s.%s at branch %s",DATAPLANE_PLUGIN_NAME, mainTable1, DEFAULT_BRANCH_NAME)))
      .isEqualTo(Collections.singletonList(Arrays.asList("1", "2", "1")));
  }

  @Test
  public void testDmlUpdateOnlyTargetVersionSpecifiedWithSourceTableUnionThrowsError() throws Exception {
    String devBranchName = "devBranch";
    String mainTable1 = "mainTable1";
    String mainTable2 = "mainTable2";
    String devTable1 = "devTable1";
    String devTable2 = "devTable2";
    String mainBranch = String.format("BRANCH %s", DEFAULT_BRANCH_NAME);
    runSQL(createBranchAtSpecifierQuery(devBranchName, mainBranch));

    runSQL(useContextQuery());
    runSQL(useBranchQuery(devBranchName));

    runSQL(String.format("CREATE table %s at branch %s as select 1,2,3", mainTable1, DEFAULT_BRANCH_NAME));
    runSQL(String.format("CREATE table %s at branch %s as select 1,2,3", mainTable2, DEFAULT_BRANCH_NAME));
    runSQL(String.format("CREATE table %s at branch %s as select 1,2,3", devTable1, devBranchName));
    runSQL(String.format("CREATE table %s at branch %s as select 1,2,3", devTable2, devBranchName));

    assertThatThrownBy(() -> runSQL(
      String.format(
        "update %s at branch %s " +
          "set EXPR$0 = (select 1 from %s at branch %s " +
          "union all select 1 from %s limit 1)",
          mainTable1, DEFAULT_BRANCH_NAME, devTable1, devBranchName, mainTable2
      )))
      .isInstanceOf(UserRemoteException.class)
      .hasMessageContaining("When specifying the version of the table to be modified, you must also specify the version of the source table")
      .hasMessageContaining("using AT SQL syntax");

    //fully qualified
    runSQL(
      String.format(
        "update %s at branch %s " +
          "set EXPR$2 = (select 1 from %s at branch %s " +
          "union all select 1 from %s at branch %s limit 1)",
        mainTable1, DEFAULT_BRANCH_NAME, devTable1, devBranchName, mainTable2, DEFAULT_BRANCH_NAME
      ));

    Assertions.assertThat(runSqlWithResults(String.format("select * from %s.%s at branch %s",DATAPLANE_PLUGIN_NAME, mainTable1, DEFAULT_BRANCH_NAME)))
      .isEqualTo(Collections.singletonList(Arrays.asList("1", "2", "1")));
  }
}
