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

import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.DEFAULT_BRANCH_NAME;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.alterTableAddColumnsQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.alterTableAddPrimaryKeyQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.alterTableChangeColumnQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.alterTableDropColumnQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.alterTableDropPrimaryKeyQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createBranchAtBranchQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createEmptyTableQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createTableAsQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createTableWithColDefsQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createTagQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.dropTableQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueBranchName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueTableName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueTagName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.insertTableWithValuesQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.selectStarQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.tablePathWithFolders;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.truncateTableQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.useBranchQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.useTagQuery;
import static com.dremio.exec.catalog.dataplane.ITDataplanePluginTestSetup.createFolders;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.dremio.exec.catalog.VersionContext;

/**
 *
 * To run these tests, run through container class ITDataplanePlugin
 * To run all tests run {@link ITDataplanePlugin.NestedAlterTests}
 * To run single test, see instructions at the top of {@link ITDataplanePlugin}
 */

public class AlterTestCases {
  private ITDataplanePluginTestSetup base;

  AlterTestCases(ITDataplanePluginTestSetup base) {
    this.base = base;
  }

  //Tests adding columns to existing table
  @Test
  void alterTableAddOneColumn() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    final List<String> columnDefinition = Collections.singletonList("col1 int");
    final List<String> addedColDef = Collections.singletonList("col2 int");
    final List<String> columnValuesBeforeAdd = Collections.singletonList("(1)");
    final List<String> columnValuesAfterAdd = Collections.singletonList("(2,2)");
    //Setup
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createTableWithColDefsQuery(tablePath, columnDefinition));
    //Insert
    base.runSQL(insertTableWithValuesQuery(tablePath, columnValuesBeforeAdd));
    //Select
    base.testBuilder()
      .sqlQuery(selectStarQuery(tablePath))
      .unOrdered()
      .baselineColumns("col1")
      .baselineValues(1)
      .go();

    //Add single column
    base.runSQL(alterTableAddColumnsQuery(tablePath, addedColDef));
    //Select again
    base.testBuilder()
      .sqlQuery(selectStarQuery(tablePath))
      .unOrdered()
      .baselineColumns("col1", "col2")
      .baselineValues(1, null)
      .go();

    //Insert a new row
    base.runSQL(insertTableWithValuesQuery(tablePath, columnValuesAfterAdd));
    //Select
    base.testBuilder()
      .sqlQuery(selectStarQuery(tablePath))
      .unOrdered()
      .baselineColumns("col1", "col2")
      .baselineValues(1, null)
      .baselineValues(2, 2)
      .go();

    //cleanup
    base.runSQL(dropTableQuery(tablePath));
  }

  @Test
  void alterTableAddMultipleColumns() throws Exception {
    //Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final List<String> columnDefinition = Collections.singletonList("col1 int");
    final List<String> addedColDef1 = Arrays.asList("col2 int", "col3 int", "col4 varchar");
    final List<String> columnValuesBeforeAdd = Collections.singletonList("(1)");
    final List<String> columnValuesAfterAdd = Arrays.asList("(2,2,2,'two')", "(3,3,3,'three')");

    //Setup
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createTableWithColDefsQuery(tablePath, columnDefinition));
    //Insert
    base.runSQL(insertTableWithValuesQuery(tablePath, columnValuesBeforeAdd));

    //Act
    //Add 3 columns
    base.runSQL(alterTableAddColumnsQuery(tablePath, addedColDef1));

    //Assert
    //Insert a new row
    base.test(insertTableWithValuesQuery(tablePath, columnValuesAfterAdd));
    //Select the new rows from new columns
    base.testBuilder()
      .sqlQuery(selectStarQuery(tablePath))
      .unOrdered()
      .baselineColumns("col1", "col2", "col3", "col4")
      .baselineValues(1, null, null, null)
      .baselineValues(2, 2, 2, "two")
      .baselineValues(3, 3, 3, "three")
      .go();

    //cleanup
    base.test(dropTableQuery(tablePath));
  }

  @Test
  void alterTableDropOneColumn() throws Exception {
    //Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    final List<String> columnDefinition = Arrays.asList("col1 int", "col2 int", "col3 int", "col4 varchar");
    final List<String> dropColumn = Collections.singletonList("col3");
    final List<String> columnValues = Collections.singletonList("(1,1,1,'one')");
    final List<String> columnsValuesAfterDrop = Arrays.asList("(2,2,'two')", "(3,3,'three')");
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createTableWithColDefsQuery(tablePath, columnDefinition));
    base.runSQL(insertTableWithValuesQuery(tablePath, columnValues));
    base.testBuilder()
      .sqlQuery(selectStarQuery(tablePath))
      .unOrdered()
      .baselineColumns("col1", "col2", "col3", "col4")
      .baselineValues(1, 1, 1, "one")
      .go();

    //Act
    //Drop column
    base.runSQL(alterTableDropColumnQuery(tablePath, dropColumn));

    //Assert
    //select
    base.testBuilder()
      .sqlQuery(selectStarQuery(tablePath))
      .unOrdered()
      .baselineColumns("col1", "col2", "col4")
      .baselineValues(1, 1, "one")
      .go();
    //Insert again
    base.runSQL(insertTableWithValuesQuery(tablePath, columnsValuesAfterDrop));
    base.testBuilder()
      .sqlQuery(selectStarQuery(tablePath))
      .unOrdered()
      .baselineColumns("col1", "col2", "col4")
      .baselineValues(1, 1, "one")
      .baselineValues(2, 2, "two")
      .baselineValues(3, 3, "three")
      .go();

    //Cleanup
    base.runSQL(dropTableQuery(tablePath));
  }

  @Test
  void alterTableAddDropColumns() throws Exception {

    //Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    final List<String> columnDefinition = Arrays.asList("col1 int", "col2 int");
    final List<String> addedColDef = Arrays.asList("col3 int", "col4 int");
    final List<String> dropColumns = Collections.singletonList("col2");
    final List<String> columnValues = Collections.singletonList("(1,1)");
    final List<String> columnValuesAfterAdd = Collections.singletonList("(2,2,2,2)");
    final List<String> columnValuesAfterDrop = Arrays.asList("(3,3,3)", "(4,4,4)");
    //Setup
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createTableWithColDefsQuery(tablePath, columnDefinition));
    base.test(insertTableWithValuesQuery(tablePath, columnValues));
    base.testBuilder()
      .sqlQuery(selectStarQuery(tablePath))
      .unOrdered()
      .baselineColumns("col1", "col2")
      .baselineValues(1, 1)
      .go();

    //Act
    //Add columns
    base.runSQL(alterTableAddColumnsQuery(tablePath, addedColDef));
    base.runSQL(insertTableWithValuesQuery(tablePath, columnValuesAfterAdd));
    //Drop columns
    base.runSQL(alterTableDropColumnQuery(tablePath, dropColumns));
    //Insert again
    base.runSQL(insertTableWithValuesQuery(tablePath, columnValuesAfterDrop));

    //Assert
    base.testBuilder()
      .sqlQuery(selectStarQuery(tablePath))
      .unOrdered()
      .baselineColumns("col1", "col3", "col4")
      .baselineValues(1, null, null)
      .baselineValues(2, 2, 2)
      .baselineValues(3, 3, 3)
      .baselineValues(4, 4, 4)
      .go();

    //Cleanup
    base.runSQL(dropTableQuery(tablePath));
  }

  @Test
  void alterTableChangeColumn() throws Exception {
    //Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    final List<String> columnDefinition = Arrays.asList("col1 int", "col2 float");
    final List<String> changeColDef = Collections.singletonList("col2 col3 double");
    final List<String> columnValuesBeforeChange = Collections.singletonList("(1, cast(1.0 as float))");
    final List<String> columnValuesAfterChange = Collections.singletonList("(2,cast(2.0 as double))");
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createTableWithColDefsQuery(tablePath, columnDefinition));
    //Insert
    base.runSQL(insertTableWithValuesQuery(tablePath, columnValuesBeforeChange));
    //Select
    base.testBuilder()
      .sqlQuery(selectStarQuery(tablePath))
      .unOrdered()
      .baselineColumns("col1", "col2")
      .baselineValues(1, new Float("1.0"))
      .go();

    //Act
    //Change  column
    base.runSQL(alterTableChangeColumnQuery(tablePath, changeColDef));

    //Assert
    //Insert a new row
    base.runSQL(insertTableWithValuesQuery(tablePath, columnValuesAfterChange));
    //Select
    base.testBuilder()
      .sqlQuery(selectStarQuery(tablePath))
      .unOrdered()
      .baselineColumns("col1", "col3")
      .baselineValues(1, 1.0)
      .baselineValues(2, 2.0)
      .go();

    //cleanup
    base.runSQL(dropTableQuery(tablePath));
  }

  @Test
  void truncateTableInDiffBranches() throws Exception {
    //Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final String devBranch = generateUniqueBranchName();
    // Set context to main branch
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createTableAsQuery(tablePath, 5));
    // Verify with select
    base.assertTableHasExpectedNumRows(tablePath, 5);
    // Create dev branch
    base.runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));
    // Switch to dev
    base.runSQL(useBranchQuery(devBranch));

    //Act
    base.runSQL(truncateTableQuery(tablePath));

    //Assert
    base.assertTableHasExpectedNumRows(tablePath, 0);
    //Check that main context still has the table
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    base.assertTableHasExpectedNumRows(tablePath, 5);

    //cleanup
    base.runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void testDropInvalidPrimaryKey() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    // Set context to main branch
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));

    try {
      createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
      base.runSQL(createEmptyTableQuery(tablePath));
      // Try to drop a primary key when there is none added.
      assertThatThrownBy(() -> base.runSQL(alterTableDropPrimaryKeyQuery(tablePath)))
        .hasMessageContaining("No primary key to drop");
    } finally {
      // Cleanup
      try {
        base.runSQL(dropTableQuery(tablePath));
      } catch (Exception ignore) {
      }
    }
  }

  @Test
  public void testAddAndDropPrimaryKey() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    // Set context to main branch
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    try {
      createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
      base.runSQL(createEmptyTableQuery(tablePath));
      base.runSQL(alterTableAddPrimaryKeyQuery(tablePath, Collections.singletonList("id")));
      base.runSQL(alterTableDropPrimaryKeyQuery(tablePath));
    } finally {
      // Cleanup
      try {
        base.runSQL(dropTableQuery(tablePath));
      } catch (Exception ignore) {
      }
    }
  }

  @Test
  public void testAddInvalidPrimaryKey() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    // Set context to main branch
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));

    try {
      createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
      base.runSQL(createEmptyTableQuery(tablePath));
      // Try to add a non-existent primary key.
      final String primaryKey = "blah_blah";
      assertThatThrownBy(() -> base.runSQL(alterTableAddPrimaryKeyQuery(tablePath, Collections.singletonList(primaryKey))))
        .hasMessageContaining(String.format("Column %s not found", primaryKey));
    } finally {
      // Cleanup
      try {
        base.runSQL(dropTableQuery(tablePath));
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

    //Setup
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createTableWithColDefsQuery(tablePath, columnDefinition));

    //Insert
    base.runSQL(insertTableWithValuesQuery(tablePath, columnValuesBeforeAdd));

    //Select
    base.testBuilder()
      .sqlQuery(selectStarQuery(tablePath))
      .unOrdered()
      .baselineColumns("col1")
      .baselineValues(1)
      .go();

    //Add single column
    base.runWithAlternateSourcePath(alterTableAddColumnsQuery(tablePath, addedColDef));
    //Select again
    base.testBuilder()
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
    base.runSQL(createTagQuery(tag, DEFAULT_BRANCH_NAME));
    base.runSQL(useTagQuery(tag));
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));

    // Act and Assert
    base.assertQueryThrowsExpectedError(createTableAsQuery(tablePath, 5),
      String.format("DDL and DML operations are only supported for branches - not on tags or commits. %s is not a branch.",
        tag));
  }

  @Test
  public void changeColumnWithTagSet() throws Exception {
    // Arrange
    String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final String tag = generateUniqueTagName();
    base.runSQL(createTagQuery(tag, DEFAULT_BRANCH_NAME));
    base.runSQL(useTagQuery(tag));
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));

    // Act and Assert
    base.assertQueryThrowsExpectedError(createTableAsQuery(tablePath, 5),
      String.format("DDL and DML operations are only supported for branches - not on tags or commits. %s is not a branch.",
        tag));
  }

  @Test
  public void dropColumnWithTagSet() throws Exception {
    // Arrange
    String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final String tag = generateUniqueTagName();
    base.runSQL(createTagQuery(tag, DEFAULT_BRANCH_NAME));
    base.runSQL(useTagQuery(tag));
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));

    // Act and Assert
    base.assertQueryThrowsExpectedError(createTableAsQuery(tablePath, 5),
      String.format("DDL and DML operations are only supported for branches - not on tags or commits. %s is not a branch.",
        tag));
  }
}
