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
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.alterTableAddColumnsQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.alterTableAddColumnsQueryWithAtSyntax;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.alterTableAddPartitionQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.alterTableAddPartitionQueryAt;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.alterTableAddPrimaryKeyQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.alterTableAddPrimaryKeyQueryWithAtSyntax;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.alterTableChangeColumnQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.alterTableChangeColumnQueryWithAtSyntax;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.alterTableDropColumnQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.alterTableDropColumnQueryWithAtSyntax;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.alterTableDropPrimaryKeyQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.alterTableDropPrimaryKeyQueryWithAtSyntax;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.alterTableReplaceSortOrder;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.alterTableSetTablePropertiesQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.alterTableUnsetTablePropertiesQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createBranchAtBranchQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createEmptyTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createEmptyTableQueryWithAt;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createTableAsQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createTableWithColDefsQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createTagQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.dropTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.dropTableQueryWithAt;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueBranchName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueTableName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueTagName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.insertTableWithValuesQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.joinedTableKey;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.quoted;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.selectStarQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.selectStarQueryWithSpecifier;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.showTablePropertiesQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.tablePathWithFolders;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.truncateTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.useBranchQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.useTagQuery;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dremio.catalog.model.VersionContext;
import com.dremio.exec.catalog.dataplane.test.ITDataplanePluginTestSetup;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.sabot.rpc.user.QueryDataBatch;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

public class ITDataplanePluginAlter extends ITDataplanePluginTestSetup {

  // Tests adding columns to existing table
  @Test
  void alterTableAddOneColumn() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    final List<String> columnDefinition = Collections.singletonList("col1 int");
    final List<String> addedColDef = Collections.singletonList("col2 int");
    final List<String> columnValuesBeforeAdd = Collections.singletonList("(1)");
    final List<String> columnValuesAfterAdd = Collections.singletonList("(2,2)");
    // Setup
    runSQL(createTableWithColDefsQuery(tablePath, columnDefinition));
    // Insert
    runSQL(insertTableWithValuesQuery(tablePath, columnValuesBeforeAdd));
    // Select
    testBuilder()
        .sqlQuery(selectStarQuery(tablePath))
        .unOrdered()
        .baselineColumns("col1")
        .baselineValues(1)
        .go();

    // Add single column
    runSQL(alterTableAddColumnsQuery(tablePath, addedColDef));
    // Select again
    testBuilder()
        .sqlQuery(selectStarQuery(tablePath))
        .unOrdered()
        .baselineColumns("col1", "col2")
        .baselineValues(1, null)
        .go();

    // Insert a new row
    runSQL(insertTableWithValuesQuery(tablePath, columnValuesAfterAdd));
    // Select
    testBuilder()
        .sqlQuery(selectStarQuery(tablePath))
        .unOrdered()
        .baselineColumns("col1", "col2")
        .baselineValues(1, null)
        .baselineValues(2, 2)
        .go();

    // cleanup
    runSQL(dropTableQuery(tablePath));

    assertCommitLogTail(
        String.format("CREATE TABLE %s", joinedTableKey(tablePath)),
        String.format("INSERT on TABLE %s", joinedTableKey(tablePath)),
        String.format("ALTER on TABLE %s", joinedTableKey(tablePath)),
        String.format("INSERT on TABLE %s", joinedTableKey(tablePath)),
        String.format("DROP TABLE %s", joinedTableKey(tablePath)));
  }

  @Test
  void alterTableAddMultipleColumns() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final List<String> columnDefinition = Collections.singletonList("col1 int");
    final List<String> addedColDef1 = Arrays.asList("col2 int", "col3 int", "col4 varchar");
    final List<String> columnValuesBeforeAdd = Collections.singletonList("(1)");
    final List<String> columnValuesAfterAdd = Arrays.asList("(2,2,2,'two')", "(3,3,3,'three')");

    // Setup
    runSQL(createTableWithColDefsQuery(tablePath, columnDefinition));
    // Insert
    runSQL(insertTableWithValuesQuery(tablePath, columnValuesBeforeAdd));

    // Act
    // Add 3 columns
    runSQL(alterTableAddColumnsQuery(tablePath, addedColDef1));

    // Assert
    // Insert a new row
    test(insertTableWithValuesQuery(tablePath, columnValuesAfterAdd));
    // Select the new rows from new columns
    testBuilder()
        .sqlQuery(selectStarQuery(tablePath))
        .unOrdered()
        .baselineColumns("col1", "col2", "col3", "col4")
        .baselineValues(1, null, null, null)
        .baselineValues(2, 2, 2, "two")
        .baselineValues(3, 3, 3, "three")
        .go();

    // cleanup
    test(dropTableQuery(tablePath));
  }

  @Test
  void alterTableInMainWithAtSyntax() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = Collections.singletonList(tableName);
    final List<String> columnDefinition = Collections.singletonList("col1 int");
    final List<String> addedColDef = Collections.singletonList("col2 int");
    final List<String> columnValuesBeforeAdd = Collections.singletonList("(1)");
    final List<String> columnValuesAfterAdd = Collections.singletonList("(2,2)");

    // Setup
    runSQL(createTableWithColDefsQuery(tablePath, columnDefinition));
    // Insert
    runSQL(insertTableWithValuesQuery(tablePath, columnValuesBeforeAdd));

    // Add single column
    runSQL(alterTableAddColumnsQueryWithAtSyntax(tablePath, addedColDef, DEFAULT_BRANCH_NAME));

    // Insert a new row
    runSQL(insertTableWithValuesQuery(tablePath, columnValuesAfterAdd));

    testBuilder()
        .sqlQuery(selectStarQueryWithSpecifier(tablePath, "BRANCH " + DEFAULT_BRANCH_NAME))
        .unOrdered()
        .baselineColumns("col1", "col2")
        .baselineValues(1, null)
        .baselineValues(2, 2)
        .go();

    // cleanup
    runSQL(dropTableQueryWithAt(tablePath, DEFAULT_BRANCH_NAME));
  }

  @Test
  void alterTableInDevWithAtSyntax() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = Collections.singletonList(tableName);
    final String devBranch = generateUniqueBranchName();
    final List<String> columnDefinition = Collections.singletonList("col1 int");
    final List<String> addedColDef = Collections.singletonList("col2 int");
    final List<String> columnValuesBeforeAdd = Collections.singletonList("(1)");
    final List<String> columnValuesAfterAdd = Collections.singletonList("(2,2)");

    // Setup
    runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));
    runSQL(useBranchQuery(devBranch));
    runSQL(createTableWithColDefsQuery(tablePath, columnDefinition));
    // Insert
    runSQL(insertTableWithValuesQuery(tablePath, columnValuesBeforeAdd));

    // Add single column
    runSQL(alterTableAddColumnsQueryWithAtSyntax(tablePath, addedColDef, devBranch));

    // Insert a new row
    runSQL(insertTableWithValuesQuery(tablePath, columnValuesAfterAdd));

    testBuilder()
        .sqlQuery(selectStarQueryWithSpecifier(tablePath, "BRANCH " + devBranch))
        .unOrdered()
        .baselineColumns("col1", "col2")
        .baselineValues(1, null)
        .baselineValues(2, 2)
        .go();

    // cleanup
    runSQL(dropTableQueryWithAt(tablePath, devBranch));
  }

  @Test
  void alterTableInDevWithAtSyntaxAndUseStatement() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = Collections.singletonList(tableName);
    final String devBranch = generateUniqueBranchName();
    final List<String> columnDefinition = Collections.singletonList("col1 int");
    final List<String> addedColDef = Collections.singletonList("col2 int");
    final List<String> columnValuesBeforeAdd = Collections.singletonList("(1)");
    final List<String> columnValuesAfterAdd = Collections.singletonList("(2,2)");

    // Setup
    runSQL(createTableWithColDefsQuery(tablePath, columnDefinition));
    runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    // Add single column to dev table
    runSQL(alterTableAddColumnsQueryWithAtSyntax(tablePath, addedColDef, devBranch));
    // insert into main table
    runSQL(insertTableWithValuesQuery(tablePath, columnValuesBeforeAdd));
    // insert into dev table
    runSQL(useBranchQuery(devBranch));
    runSQL(insertTableWithValuesQuery(tablePath, columnValuesAfterAdd));

    // assert
    testBuilder()
        .sqlQuery(selectStarQueryWithSpecifier(tablePath, "BRANCH " + devBranch))
        .unOrdered()
        .baselineColumns("col1", "col2")
        .baselineValues(2, 2)
        .go();
    testBuilder()
        .sqlQuery(selectStarQueryWithSpecifier(tablePath, "BRANCH " + DEFAULT_BRANCH_NAME))
        .unOrdered()
        .baselineColumns("col1")
        .baselineValues(1)
        .go();
    // cleanup
    runSQL(dropTableQueryWithAt(tablePath, devBranch));
  }

  @Test
  void alterTableWithInvalidReference() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = Collections.singletonList(tableName);
    final List<String> columnDefinition = Collections.singletonList("col1 int");
    final List<String> addedColDef = Collections.singletonList("col2 int");
    // create empty table and get hash
    runSQL(createTableWithColDefsQuery(tablePath, columnDefinition));
    final String commitHashTemporaryBranch = getCommitHashForBranch(DEFAULT_BRANCH_NAME);

    final String query =
        String.format(
            "ALTER TABLE %s.%s AT COMMIT %s add columns %s",
            DATAPLANE_PLUGIN_NAME,
            String.join(".", tablePath),
            quoted(commitHashTemporaryBranch),
            "(" + String.join(",", addedColDef) + ")");

    assertQueryThrowsExpectedError(
        query,
        "DDL and DML operations are only supported for branches - not on tags or commits. DETACHED is not a branch. ");

    runSQL(dropTableQueryWithAt(tablePath, DEFAULT_BRANCH_NAME));
  }

  @Test
  void alterTableWithAtSyntaxAndModify() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = Collections.singletonList(tableName);
    final String devBranch = generateUniqueBranchName();

    final List<String> columnDefinition = Arrays.asList("col1 int", "col2 float");
    final List<String> changeColDef = Collections.singletonList("col2 col3 double");
    final List<String> columnValuesBeforeChange =
        Collections.singletonList("(1, cast(1.0 as float))");
    final List<String> columnValuesAfterChange =
        Collections.singletonList("(2,cast(2.0 as double))");

    // create table in dev branch
    runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));
    runSQL(useBranchQuery(devBranch));
    runSQL(createTableWithColDefsQuery(tablePath, columnDefinition));
    // Insert
    runSQL(insertTableWithValuesQuery(tablePath, columnValuesBeforeChange));
    // Select
    testBuilder()
        .sqlQuery(selectStarQuery(tablePath))
        .unOrdered()
        .baselineColumns("col1", "col2")
        .baselineValues(1, 1.0f)
        .go();

    // Act
    // Change  column
    runSQL(alterTableChangeColumnQueryWithAtSyntax(tablePath, changeColDef, devBranch));

    // Assert
    // Insert a new row
    runSQL(insertTableWithValuesQuery(tablePath, columnValuesAfterChange));
    // Select
    testBuilder()
        .sqlQuery(selectStarQuery(tablePath))
        .unOrdered()
        .baselineColumns("col1", "col3")
        .baselineValues(1, 1.0)
        .baselineValues(2, 2.0)
        .go();

    // cleanup
    runSQL(dropTableQuery(tablePath));
  }

  @Test
  void alterTableDropOneColumn() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    final List<String> columnDefinition =
        Arrays.asList("col1 int", "col2 int", "col3 int", "col4 varchar");
    final List<String> dropColumn = Collections.singletonList("col3");
    final List<String> columnValues = Collections.singletonList("(1,1,1,'one')");
    final List<String> columnsValuesAfterDrop = Arrays.asList("(2,2,'two')", "(3,3,'three')");
    runSQL(createTableWithColDefsQuery(tablePath, columnDefinition));
    runSQL(insertTableWithValuesQuery(tablePath, columnValues));
    testBuilder()
        .sqlQuery(selectStarQuery(tablePath))
        .unOrdered()
        .baselineColumns("col1", "col2", "col3", "col4")
        .baselineValues(1, 1, 1, "one")
        .go();

    // Act
    // Drop column
    runSQL(alterTableDropColumnQuery(tablePath, dropColumn));

    // Assert
    // select
    testBuilder()
        .sqlQuery(selectStarQuery(tablePath))
        .unOrdered()
        .baselineColumns("col1", "col2", "col4")
        .baselineValues(1, 1, "one")
        .go();
    // Insert again
    runSQL(insertTableWithValuesQuery(tablePath, columnsValuesAfterDrop));
    testBuilder()
        .sqlQuery(selectStarQuery(tablePath))
        .unOrdered()
        .baselineColumns("col1", "col2", "col4")
        .baselineValues(1, 1, "one")
        .baselineValues(2, 2, "two")
        .baselineValues(3, 3, "three")
        .go();

    // Cleanup
    runSQL(dropTableQuery(tablePath));
  }

  @Test
  void alterTableAddDropColumns() throws Exception {

    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    final List<String> columnDefinition = Arrays.asList("col1 int", "col2 int");
    final List<String> addedColDef = Arrays.asList("col3 int", "col4 int");
    final List<String> dropColumns = Collections.singletonList("col2");
    final List<String> columnValues = Collections.singletonList("(1,1)");
    final List<String> columnValuesAfterAdd = Collections.singletonList("(2,2,2,2)");
    final List<String> columnValuesAfterDrop = Arrays.asList("(3,3,3)", "(4,4,4)");
    // Setup
    runSQL(createTableWithColDefsQuery(tablePath, columnDefinition));
    test(insertTableWithValuesQuery(tablePath, columnValues));
    testBuilder()
        .sqlQuery(selectStarQuery(tablePath))
        .unOrdered()
        .baselineColumns("col1", "col2")
        .baselineValues(1, 1)
        .go();

    // Act
    // Add columns
    runSQL(alterTableAddColumnsQuery(tablePath, addedColDef));
    runSQL(insertTableWithValuesQuery(tablePath, columnValuesAfterAdd));
    // Drop columns
    runSQL(alterTableDropColumnQuery(tablePath, dropColumns));
    // Insert again
    runSQL(insertTableWithValuesQuery(tablePath, columnValuesAfterDrop));

    // Assert
    testBuilder()
        .sqlQuery(selectStarQuery(tablePath))
        .unOrdered()
        .baselineColumns("col1", "col3", "col4")
        .baselineValues(1, null, null)
        .baselineValues(2, 2, 2)
        .baselineValues(3, 3, 3)
        .baselineValues(4, 4, 4)
        .go();

    // Cleanup
    runSQL(dropTableQuery(tablePath));
  }

  @Test
  void alterTableAddDropColumnsWithAtSyntax() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = Collections.singletonList(tableName);
    final String devBranch = generateUniqueBranchName();

    final List<String> columnDefinition = Arrays.asList("col1 int", "col2 int");
    final List<String> dropColumns = Collections.singletonList("col2");
    final List<String> columnValues = Collections.singletonList("(1,1)");
    final List<String> columnValuesAfterDrop = Arrays.asList("(2)", "(3)");
    // Setup
    runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));
    runSQL(useBranchQuery(devBranch));
    runSQL(createTableWithColDefsQuery(tablePath, columnDefinition));
    test(insertTableWithValuesQuery(tablePath, columnValues));
    testBuilder()
        .sqlQuery(selectStarQuery(tablePath))
        .unOrdered()
        .baselineColumns("col1", "col2")
        .baselineValues(1, 1)
        .go();

    // Act
    // Drop columns
    runSQL(alterTableDropColumnQueryWithAtSyntax(tablePath, dropColumns, devBranch));
    // Insert again
    runSQL(insertTableWithValuesQuery(tablePath, columnValuesAfterDrop));

    // Assert
    testBuilder()
        .sqlQuery(selectStarQuery(tablePath))
        .unOrdered()
        .baselineColumns("col1")
        .baselineValues(1)
        .baselineValues(2)
        .baselineValues(3)
        .go();

    // Cleanup
    runSQL(dropTableQuery(tablePath));
  }

  @Test
  void alterTableChangeColumn() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    final List<String> columnDefinition = Arrays.asList("col1 int", "col2 float");
    final List<String> changeColDef = Collections.singletonList("col2 col3 double");
    final List<String> columnValuesBeforeChange =
        Collections.singletonList("(1, cast(1.0 as float))");
    final List<String> columnValuesAfterChange =
        Collections.singletonList("(2,cast(2.0 as double))");
    runSQL(createTableWithColDefsQuery(tablePath, columnDefinition));
    // Insert
    runSQL(insertTableWithValuesQuery(tablePath, columnValuesBeforeChange));
    // Select
    testBuilder()
        .sqlQuery(selectStarQuery(tablePath))
        .unOrdered()
        .baselineColumns("col1", "col2")
        .baselineValues(1, 1.0f)
        .go();

    // Act
    // Change  column
    runSQL(alterTableChangeColumnQuery(tablePath, changeColDef));

    // Assert
    // Insert a new row
    runSQL(insertTableWithValuesQuery(tablePath, columnValuesAfterChange));
    // Select
    testBuilder()
        .sqlQuery(selectStarQuery(tablePath))
        .unOrdered()
        .baselineColumns("col1", "col3")
        .baselineValues(1, 1.0)
        .baselineValues(2, 2.0)
        .go();

    // cleanup
    runSQL(dropTableQuery(tablePath));
  }

  @Test
  void truncateTableInDiffBranches() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final String devBranch = generateUniqueBranchName();
    // Set context to main branch
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    runSQL(createTableAsQuery(tablePath, 5));
    // Verify with select
    assertTableHasExpectedNumRows(tablePath, 5);
    // Create dev branch
    runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));
    // Switch to dev
    runSQL(useBranchQuery(devBranch));

    // Act
    runSQL(truncateTableQuery(tablePath));

    // Assert
    assertTableHasExpectedNumRows(tablePath, 0);
    // Check that main context still has the table
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    assertTableHasExpectedNumRows(tablePath, 5);

    // cleanup
    runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void testDropInvalidPrimaryKey() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    // Set context to main branch
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));

    try {
      runSQL(createEmptyTableQuery(tablePath));
      // Try to drop a primary key when there is none added.
      assertThatThrownBy(() -> runSQL(alterTableDropPrimaryKeyQuery(tablePath)))
          .hasMessageContaining("No primary key to drop");
    } finally {
      // Cleanup
      try {
        runSQL(dropTableQuery(tablePath));
      } catch (Exception ignore) {
      }
    }
  }

  @Test
  public void testAddAndDropPrimaryKey() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    // Set context to main branch
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    try {
      runSQL(createEmptyTableQuery(tablePath));
      runSQL(alterTableAddPrimaryKeyQuery(tablePath, Collections.singletonList("id")));
      runSQL(alterTableDropPrimaryKeyQuery(tablePath));
    } finally {
      // Cleanup
      try {
        runSQL(dropTableQuery(tablePath));
      } catch (Exception ignore) {
      }
    }
  }

  @Test
  public void testAddAndDropPrimaryKeyWithAtSyntax() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = Collections.singletonList(tableName);
    final String devBranch = generateUniqueBranchName();
    // Set context to main branch
    runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));
    try {
      runSQL(createEmptyTableQueryWithAt(tablePath, devBranch));
      runSQL(
          alterTableAddPrimaryKeyQueryWithAtSyntax(
              tablePath, Collections.singletonList("id"), devBranch));
      runSQL(alterTableDropPrimaryKeyQueryWithAtSyntax(tablePath, devBranch));
    } finally {
      // Cleanup
      try {
        runSQL(dropTableQueryWithAt(tablePath, devBranch));
      } catch (Exception ignore) {
      }
    }
  }

  @Test
  public void testAddInvalidPrimaryKey() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    // Set context to main branch
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));

    try {
      runSQL(createEmptyTableQuery(tablePath));
      // Try to add a non-existent primary key.
      final String primaryKey = "blah_blah";
      assertThatThrownBy(
              () ->
                  runSQL(
                      alterTableAddPrimaryKeyQuery(
                          tablePath, Collections.singletonList(primaryKey))))
          .hasMessageContaining(String.format("Column %s not found", primaryKey));
    } finally {
      // Cleanup
      try {
        runSQL(dropTableQuery(tablePath));
      } catch (Exception ignore) {
      }
    }
  }

  @Test
  public void testAddColumnAgnosticOfSourceBucket() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    final List<String> columnDefinition = Collections.singletonList("col1 int");
    final List<String> addedColDef = Collections.singletonList("col2 int");
    final List<String> columnValuesBeforeAdd = Collections.singletonList("(1)");

    // Setup
    runSQL(createTableWithColDefsQuery(tablePath, columnDefinition));

    // Insert
    runSQL(insertTableWithValuesQuery(tablePath, columnValuesBeforeAdd));

    // Select
    testBuilder()
        .sqlQuery(selectStarQuery(tablePath))
        .unOrdered()
        .baselineColumns("col1")
        .baselineValues(1)
        .go();

    // Add single column
    runWithAlternateSourcePath(alterTableAddColumnsQuery(tablePath, addedColDef));
    // Select again
    testBuilder()
        .sqlQuery(selectStarQuery(tablePath))
        .unOrdered()
        .baselineColumns("col1", "col2")
        .baselineValues(1, null)
        .go();
  }

  @Test
  public void addColumnWithTagSet() throws Exception {
    // Arrange
    String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final String tag = generateUniqueTagName();
    runSQL(createTagQuery(tag, DEFAULT_BRANCH_NAME));
    runSQL(useTagQuery(tag));

    // Act and Assert
    assertQueryThrowsExpectedError(
        createTableAsQuery(tablePath, 5),
        String.format(
            "DDL and DML operations are only supported for branches - not on tags or commits. %s is not a branch.",
            tag));
  }

  @Test
  public void changeColumnWithTagSet() throws Exception {
    // Arrange
    String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final String tag = generateUniqueTagName();
    runSQL(createTagQuery(tag, DEFAULT_BRANCH_NAME));
    runSQL(useTagQuery(tag));

    // Act and Assert
    assertQueryThrowsExpectedError(
        createTableAsQuery(tablePath, 5),
        String.format(
            "DDL and DML operations are only supported for branches - not on tags or commits. %s is not a branch.",
            tag));
  }

  @Test
  public void dropColumnWithTagSet() throws Exception {
    // Arrange
    String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final String tag = generateUniqueTagName();
    runSQL(createTagQuery(tag, DEFAULT_BRANCH_NAME));
    runSQL(useTagQuery(tag));

    // Act and Assert
    assertQueryThrowsExpectedError(
        createTableAsQuery(tablePath, 5),
        String.format(
            "DDL and DML operations are only supported for branches - not on tags or commits. %s is not a branch.",
            tag));
  }

  @Test
  void alterTableAddPartitionInDevWithAtSyntaxContextMain() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = Collections.singletonList(tableName);
    final String devBranch = generateUniqueBranchName();
    runSQL(createTableAsQuery(tablePath, 10));
    // Setup
    runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    runSQL(alterTableAddPartitionQueryAt(tablePath, "n_nationkey", devBranch));

    assertCommitLogTail(
        VersionContext.ofBranch(devBranch),
        String.format("CREATE TABLE %s", joinedTableKey(tablePath)),
        String.format("ALTER on TABLE %s", joinedTableKey(tablePath)));

    assertCommitLogTail(
        VersionContext.ofBranch(DEFAULT_BRANCH_NAME),
        String.format("CREATE TABLE %s", joinedTableKey(tablePath)));

    // cleanup
    runSQL(dropTableQueryWithAt(tablePath, devBranch));
    runSQL(dropTableQueryWithAt(tablePath, DEFAULT_BRANCH_NAME));
  }

  @Test
  void alterTableAddPartitionInDevWithAtSyntaxContextDev() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = Collections.singletonList(tableName);
    final String devBranch = generateUniqueBranchName();
    runSQL(createTableAsQuery(tablePath, 10));
    // Setup
    runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));
    runSQL(useBranchQuery(devBranch));
    runSQL(alterTableAddPartitionQueryAt(tablePath, "n_nationkey", devBranch));

    assertCommitLogTail(
        VersionContext.ofBranch(devBranch),
        String.format("CREATE TABLE %s", joinedTableKey(tablePath)),
        String.format("ALTER on TABLE %s", joinedTableKey(tablePath)));

    assertCommitLogTail(
        VersionContext.ofBranch(DEFAULT_BRANCH_NAME),
        String.format("CREATE TABLE %s", joinedTableKey(tablePath)));

    // cleanup
    runSQL(dropTableQueryWithAt(tablePath, devBranch));
    runSQL(dropTableQueryWithAt(tablePath, DEFAULT_BRANCH_NAME));
  }

  @Test
  void alterTableAddPartitionInDevWithContextDev() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = Collections.singletonList(tableName);
    final String devBranch = generateUniqueBranchName();
    runSQL(createTableAsQuery(tablePath, 10));
    // Setup
    runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));
    runSQL(useBranchQuery(devBranch));
    runSQL(alterTableAddPartitionQuery(tablePath, "n_nationkey"));

    assertCommitLogTail(
        VersionContext.ofBranch(devBranch),
        String.format("CREATE TABLE %s", joinedTableKey(tablePath)),
        String.format("ALTER on TABLE %s", joinedTableKey(tablePath)));

    assertCommitLogTail(
        VersionContext.ofBranch(DEFAULT_BRANCH_NAME),
        String.format("CREATE TABLE %s", joinedTableKey(tablePath)));

    // cleanup
    runSQL(dropTableQueryWithAt(tablePath, devBranch));
    runSQL(dropTableQueryWithAt(tablePath, DEFAULT_BRANCH_NAME));
  }

  @Test
  void alterTableSortOrder() throws Exception {
    String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    final List<String> columnDefinition =
        Arrays.asList("a BOOLEAN", "b BOOLEAN", "c BOOLEAN", "d BOOLEAN");
    final List<String> sortedColumnDefinition = Arrays.asList("d", "b", "c", "a");

    // create table
    runSQL(createTableWithColDefsQuery(tablePath, columnDefinition));

    // alter table to obtain speified sortOrder
    runSQL(alterTableReplaceSortOrder(tablePath, sortedColumnDefinition));

    // bundle values
    final List<String> insertValues =
        Collections.singletonList(
            "(true, true, true, true),"
                + "(true, true, true, false), "
                + "(true, true, false, true), "
                + "(true, true, false, false), "
                + "(true, false, true, true), "
                + "(true, false, true, false), "
                + "(true, false, false, true),"
                + "(true, false, false, false), "
                + "(false, true, true, true), "
                + "(false, true, true, false), "
                + "(false, true, false, true), "
                + "(false, true, false, false),"
                + "(false, false, true, true), "
                + "(false, false, true, false), "
                + "(false, false, false, true), "
                + "(false, false, false, false) ");

    // insert values into table
    runSQL(insertTableWithValuesQuery(tablePath, insertValues));

    // assure sort order is honored on select *
    // Will be adjusted ASAP. We need to perform a file scan of parquet files. Sort Order occurs at
    // the data-file level.
    testBuilder()
        .sqlQuery(selectStarQuery(tablePath))
        .ordered()
        .baselineColumns("a", "b", "c", "d")
        .baselineValues(false, false, false, false)
        .baselineValues(true, false, false, false)
        .baselineValues(false, false, true, false)
        .baselineValues(true, false, true, false)
        .baselineValues(false, true, false, false)
        .baselineValues(true, true, false, false)
        .baselineValues(false, true, true, false)
        .baselineValues(true, true, true, false)
        .baselineValues(false, false, false, true)
        .baselineValues(true, false, false, true)
        .baselineValues(false, false, true, true)
        .baselineValues(true, false, true, true)
        .baselineValues(false, true, false, true)
        .baselineValues(true, true, false, true)
        .baselineValues(false, true, true, true)
        .baselineValues(true, true, true, true)
        .go();
  }

  @Test
  public void testSetUnsetTableProperties() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    // Set context to main branch
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));

    runSQL(createEmptyTableQuery(tablePath));

    runSQL(alterTableSetTablePropertiesQuery(tablePath));
    // check table properties
    List<QueryDataBatch> queryDataBatches =
        testRunAndReturn(UserBitShared.QueryType.SQL, showTablePropertiesQuery(tablePath));
    String resultString = getResultString(queryDataBatches, ",", false);
    assertThat(resultString).isNotNull().contains("property_name");

    runSQL(alterTableUnsetTablePropertiesQuery(tablePath));
    // check table properties not exists
    queryDataBatches =
        testRunAndReturn(UserBitShared.QueryType.SQL, showTablePropertiesQuery(tablePath));
    resultString = getResultString(queryDataBatches, ",", false);
    assertThat(resultString != null && resultString.contains("property_name")).isFalse();
    runSQL(dropTableQuery(tablePath));
  }
}
