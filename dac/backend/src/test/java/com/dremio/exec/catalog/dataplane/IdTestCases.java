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
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.alterTableAddColumnsQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createBranchAtBranchQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createEmptyTableQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createReplaceViewQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createTableAsQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createTagQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createViewQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.deleteAllQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.dropTableQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueBranchName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueTableName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueTagName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueViewName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.insertSelectQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.insertTableQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.tablePathWithFolders;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.useBranchQuery;
import static com.dremio.exec.catalog.dataplane.ITDataplanePluginTestSetup.createFolders;
import static com.dremio.exec.catalog.dataplane.TestDataplaneAssertions.assertNessieHasCommitForTable;
import static com.dremio.exec.catalog.dataplane.TestDataplaneAssertions.assertNessieHasTable;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;
import org.projectnessie.model.Operation;

import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.TableVersionContext;
import com.dremio.exec.catalog.TableVersionType;
import com.dremio.exec.catalog.VersionContext;
import com.dremio.exec.catalog.VersionedDatasetId;
import com.dremio.service.namespace.proto.EntityId;
@Isolated
public class IdTestCases {
  private ITDataplanePluginTestSetup base;

  IdTestCases(ITDataplanePluginTestSetup base) {
    this.base = base;
  }

  /**
   * Tests content id  after create- should be unique for each create
   */
  @Test
  public void checkContentIdAfterReCreate() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));
    String contentIdBeforeDrop = base.getContentId(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), base);
    base.runSQL(dropTableQuery(tablePath));

    // Act
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));
    String contentIdAfterReCreate = base.getContentId(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), base);

    // Assert
    assertThat((contentIdBeforeDrop.equals(contentIdAfterReCreate))).isFalse();
    base.runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void checkUniqueIdAfterCreate() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));
    String uniqueIdBeforeDrop = base.getUniqueIdForTable(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), base);
    base.runSQL(dropTableQuery(tablePath));

    // Act
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));
    String uniqueIdAfterDrop = base.getUniqueIdForTable(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), base);

    // Assert
    assertThat(uniqueIdBeforeDrop.equals(uniqueIdAfterDrop)).isFalse();
    base.runSQL(dropTableQuery(tablePath));
  }

  /**
   * Tests content id of altered objects - should remain the same
   */
  @Test
  public void checkContentIdAfterAlters() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));
    String contentIdBeforeAlter = base.getContentId(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), base);
    final List<String> addedColDef = Collections.singletonList("col2 int");
    //Add single column
    base.runSQL(alterTableAddColumnsQuery(tablePath, addedColDef));
    String contentIdAfterAlter = base.getContentId(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), base);

    // Assert
    assertThat(contentIdBeforeAlter.equals(contentIdAfterAlter)).isTrue();
    base.runSQL(dropTableQuery(tablePath));
  }

  /**
   * Tests unique id of altered objects - should change
   */
  @Test
  public void checkUniqueIdAfterAlters() throws Exception {
// Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));
    String uniqueIdBeforeAlter = base.getUniqueIdForTable(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), base);
    final List<String> addedColDef = Collections.singletonList("col2 int");
    //Add single column
    base.runSQL(alterTableAddColumnsQuery(tablePath, addedColDef));
    String uniqueIdAfterAlter = base.getUniqueIdForTable(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), base);

    // Assert
    assertThat(uniqueIdBeforeAlter.equals(uniqueIdAfterAlter)).isFalse();
    base.runSQL(dropTableQuery(tablePath));
  }

  /**
   * Tests content id  after insert- should stay the same
   */
  @Test
  public void checkContentIdAfterInsert() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));
    assertNessieHasTable(tablePath, DEFAULT_BRANCH_NAME, base);
    assertNessieHasCommitForTable(tablePath, Operation.Put.class, DEFAULT_BRANCH_NAME, base);
    String contentIdBeforeInsert = base.getContentId(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), base);
    // Act
    base.runSQL(insertTableQuery(tablePath));
    String contentIdAfterInsert = base.getContentId(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), base);
    // Assert
   assertThat(contentIdBeforeInsert.equals(contentIdAfterInsert)).isTrue();

    // cleanup
    base.runSQL(dropTableQuery(tablePath));
  }

  /**
   * Tests unique id of objects after insert - should change;
   */
  @Test
  public void checkUniqueIdAfterInsert() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));
    assertNessieHasTable(tablePath, DEFAULT_BRANCH_NAME, base);
    assertNessieHasCommitForTable(tablePath, Operation.Put.class, DEFAULT_BRANCH_NAME, base);
    String uniqueIdBeforeInsert = base.getUniqueIdForTable(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), base);
    // Act
    base.runSQL(insertTableQuery(tablePath));
    String uniqueIdAfterInsert = base.getUniqueIdForTable(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), base);
    // Assert
    assertThat(uniqueIdBeforeInsert.equals(uniqueIdAfterInsert)).isFalse();

    // cleanup
    base.runSQL(dropTableQuery(tablePath));
  }

  /**
   * Tests content id  after insert- should stay the same
   */
  @Test
  public void checkContentIdAfterDeletes() throws Exception {
    //Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));
    assertNessieHasTable(tablePath, DEFAULT_BRANCH_NAME, base);
    assertNessieHasCommitForTable(tablePath, Operation.Put.class, DEFAULT_BRANCH_NAME, base);
    String contentIdAfterCreate = base.getContentId(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), base);
    base.runSQL(insertTableQuery(tablePath));
    String contentIdAfterInsert = base.getContentId(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), base);
    // Act
    base.runSQL(deleteAllQuery(tablePath));
    String contentIdAfterDelete = base.getContentId(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), base);
    // Assert
    assertThat(contentIdAfterCreate.equals(contentIdAfterInsert)).isTrue();
    assertThat(contentIdAfterDelete.equals(contentIdAfterInsert)).isTrue();
    // cleanup
    base.runSQL(dropTableQuery(tablePath));
  }

  /**
   * Tests unique id of objects after Deletes -should change
   */
  @Test
  public void checkUniqueIdAfterDeletes() throws Exception {
    //Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));
    assertNessieHasTable(tablePath, DEFAULT_BRANCH_NAME, base);
    assertNessieHasCommitForTable(tablePath, Operation.Put.class, DEFAULT_BRANCH_NAME, base);
    String uniqueIdAfterCreate = base.getUniqueIdForTable(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), base);
    base.runSQL(insertTableQuery(tablePath));
    String uniqueIdAfterInsert = base.getUniqueIdForTable(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), base);
    // Act
    base.runSQL(deleteAllQuery(tablePath));
    String uniqueIdAfterDelete = base.getUniqueIdForTable(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), base);
    // Assert
    assertThat(uniqueIdAfterCreate.equals(uniqueIdAfterInsert)).isFalse();
    assertThat(uniqueIdAfterDelete.equals(uniqueIdAfterInsert)).isFalse();

    // cleanup
    base.runSQL(dropTableQuery(tablePath));
  }


  /**
   * Tests content id  after branching- should stay the same
   */
  @Test
  public void checkContentIdInNewBranch() throws Exception {
    // Arrange
    final String newBranchName = generateUniqueBranchName();
    final String tableName = generateUniqueTableName();
    final List<String>  tablePath = tablePathWithFolders(tableName);
    //Create table in main branch
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));
    base.runSQL(insertTableQuery(tablePath));
    base.runSQL(createBranchAtBranchQuery(newBranchName, DEFAULT_BRANCH_NAME));
    String contentIdInMainBranch = base.getContentId(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), base);

    //Act
    base.runSQL(useBranchQuery(newBranchName));
    String contentIdInNewBranch = base.getContentId(tablePath, new TableVersionContext(TableVersionType.BRANCH, newBranchName), base);

    //Assert
    assertThat(contentIdInNewBranch.equals(contentIdInMainBranch)).isTrue();
  }

  /**
   * Tests unique id of objects after branching - should stay the same
   */
  @Test
  public void checkUniqueIdAfterDMLInBranch() throws Exception {
    // Arrange
    final String newBranchName = generateUniqueBranchName();
    final String tableName = generateUniqueTableName();
    final List<String>  tablePath = tablePathWithFolders(tableName);
    //Create table in main branch
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));
    base.runSQL(createBranchAtBranchQuery(newBranchName, DEFAULT_BRANCH_NAME));
    //insert in main branch
    base.runSQL(insertTableQuery(tablePath));
    String uniqueIdBeforeBranching = base.getUniqueIdForTable(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), base);

    //Act
    base.runSQL(useBranchQuery(newBranchName));
    String uniqueIdAfterBranching = base.getUniqueIdForTable(tablePath, new TableVersionContext(TableVersionType.BRANCH, newBranchName), base);

    //Assert
    assertThat(uniqueIdAfterBranching.equals(uniqueIdBeforeBranching)).isFalse();
  }

  /**
   * Tests content id  after assign- should stay the same
   */
  @Test
  public void checkContentIdAfterAssignBranch() throws Exception {
    // Arrange
    final String branchName = generateUniqueBranchName();
    final String mainTableName = generateUniqueTableName();
    final List<String> mainTablePath = tablePathWithFolders(mainTableName);

    base.runSQL(createBranchAtBranchQuery(branchName, DEFAULT_BRANCH_NAME));
    createFolders(mainTablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(mainTablePath));
    base.runSQL(insertTableQuery(mainTablePath));
    String contentIdBeforeAssign = base.getContentId(mainTablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), base);
    // Act
    base.runSQL(alterBranchAssignBranchQuery(branchName, DEFAULT_BRANCH_NAME));
    base.runSQL(useBranchQuery(branchName));
    String contentIdAfterAssign = base.getContentId(mainTablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), base);
    // ASSERT
    assertThat(contentIdBeforeAssign.equals(contentIdBeforeAssign)).isTrue();
    // Drop tables
    base.runSQL(dropTableQuery(mainTablePath));
  }

  /**
   * Tests unique id of objects after assign- should stay the same
   */
  @Test
  public void checkUniqueIdAfterAssignBranch() throws Exception {
    // Arrange
    final String branchName = generateUniqueBranchName();
    final String mainTableName = generateUniqueTableName();
    final List<String> mainTablePath = tablePathWithFolders(mainTableName);

    base.runSQL(createBranchAtBranchQuery(branchName, DEFAULT_BRANCH_NAME));
    createFolders(mainTablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(mainTablePath));
    base.runSQL(insertTableQuery(mainTablePath));
    String uniqueIdBeforeAssign = base.getUniqueIdForTable(mainTablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), base);
    // Act
    base.runSQL(alterBranchAssignBranchQuery(branchName, DEFAULT_BRANCH_NAME));
    base.runSQL(useBranchQuery(branchName));
    String uniqueIdAfterAssign = base.getUniqueIdForTable(mainTablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), base);
    // ASSERT
    assertThat(uniqueIdBeforeAssign.equals(uniqueIdAfterAssign)).isTrue();
    // Drop tables
    base.runSQL(dropTableQuery(mainTablePath));
  }

  @Test
  public void checkContentIdAfterAlterView() throws Exception {
    //Arrange
    String tableName1 = generateUniqueTableName();
    final List<String> tablePath1 = tablePathWithFolders(tableName1);
    String tableName2 = generateUniqueTableName();
    final List<String> tablePath2 = tablePathWithFolders(tableName2);

    // Create table1 with 10 rows
    createFolders(tablePath1, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createTableAsQuery(tablePath1, 10));
    final String viewName = generateUniqueViewName();
    List<String> viewKey = tablePathWithFolders(viewName);
    createFolders(viewKey, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createViewQuery(viewKey, tablePath1));
    String contentIdAfterCreate = base.getContentId(viewKey, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), base);

    base.assertViewHasExpectedNumRows(viewKey, 10);
    //Create table2 with 20 rows.
    createFolders(tablePath2, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createTableAsQuery(tablePath2, 20));

    //Act
    base.runSQL(createReplaceViewQuery(viewKey, tablePath2));
    String contentIdAfterUpdate = base.getContentId(viewKey, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), base);
    assertThat(contentIdAfterCreate.equals(contentIdAfterUpdate)).isTrue();
    base.runSQL(dropTableQuery(tablePath1));
    base.runSQL(dropTableQuery(tablePath2));
  }

  @Test
  public void checkUniqueIdAfterAlterView() throws Exception {
    //Arrange
    String tableName1 = generateUniqueTableName();
    final List<String> tablePath1 = tablePathWithFolders(tableName1);
    String tableName2 = generateUniqueTableName();
    final List<String> tablePath2 = tablePathWithFolders(tableName2);

    // Create table1 with 10 rows
    createFolders(tablePath1, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createTableAsQuery(tablePath1, 10));
    final String viewName = generateUniqueViewName();
    List<String> viewKey = tablePathWithFolders(viewName);
    createFolders(viewKey, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createViewQuery(viewKey, tablePath1));
    String uniqueIdAfterCreate = base.getUniqueIdForView(viewKey, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), base);

    base.assertViewHasExpectedNumRows(viewKey, 10);
    //Create table2 with 20 rows.
    createFolders(tablePath2, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createTableAsQuery(tablePath2, 20));

    //Act
    base.runSQL(createReplaceViewQuery(viewKey, tablePath2));
    String uniqueIdAfterUpdate = base.getUniqueIdForView(viewKey, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), base);
    assertThat(uniqueIdAfterCreate.equals(uniqueIdAfterUpdate)).isFalse();
    base.runSQL(dropTableQuery(tablePath1));
    base.runSQL(dropTableQuery(tablePath2));
  }

  /**
   * Tests dataset id  after branching- should change
   */
  @Test
  public void checkDatasetIdInNewBranch() throws Exception {
    // Arrange
    final String newBranchName = generateUniqueBranchName();
    final String tableName = generateUniqueTableName();
    final List<String>  tablePath = tablePathWithFolders(tableName);
    //Create table in main branch
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));
    base.runSQL(insertTableQuery(tablePath));
    base.runSQL(createBranchAtBranchQuery(newBranchName, DEFAULT_BRANCH_NAME));
    String datasetIdInMainBranch = base.getVersionedDatatsetId(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), base);

    //Act
    base.runSQL(useBranchQuery(newBranchName));
    String datasetIdInNewBranch = base.getVersionedDatatsetId(tablePath, new TableVersionContext(TableVersionType.BRANCH, newBranchName), base);

    //Assert
    assertThat(datasetIdInMainBranch.equals(datasetIdInNewBranch)).isFalse();

  }

  /**
   * Tests dataset id of objects after branching - should change
   */
  @Test
  public void checkDatasetIdAfterDMLInBranch() throws Exception {
    // Arrange
    final String newBranchName = generateUniqueBranchName();
    final String tableName = generateUniqueTableName();
    final List<String>  tablePath = tablePathWithFolders(tableName);
    //Create table in main branch
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));
    base.runSQL(createBranchAtBranchQuery(newBranchName, DEFAULT_BRANCH_NAME));
    //insert in main branch
    base.runSQL(insertTableQuery(tablePath));
    String datasetIdBeforeBranching = base.getVersionedDatatsetId(tablePath,new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), base);

    //Act
    base.runSQL(useBranchQuery(newBranchName));
    String datasetIdIdAfterBranching = base.getVersionedDatatsetId(tablePath,new TableVersionContext(TableVersionType.BRANCH, newBranchName), base);

    //Assert
    assertThat(datasetIdBeforeBranching.equals(datasetIdIdAfterBranching)).isFalse();
  }

  @Test
  public void getTableWithId() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));
    String datasetIdAfterCreate = base.getVersionedDatatsetId(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), base);
    DremioTable dremioTableAfterCreate = base.getTableFromId(datasetIdAfterCreate, base);
    String  schema = dremioTableAfterCreate.getSchema().toJSONString();
    assertThat(schema.contains("id")).isTrue();
    assertThat(schema.contains("name")).isTrue();
    assertThat(schema.contains("distance")).isTrue();
    base.runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void getTableWithIdSnapshot() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));
    base.runSQL(insertTableQuery(tablePath));
    String getSnapshotString = String.format("SELECT snapshot_id FROM table(table_snapshot('%s.%s'))",
      DATAPLANE_PLUGIN_NAME,
      String.join(".", tablePath));
    List<String> results = base.runSqlWithResults(getSnapshotString).get(0);


    String datasetIdFromSnapshot = base.getVersionedDatatsetIdForTimeTravel(tablePath, new TableVersionContext(TableVersionType.SNAPSHOT_ID, results.get(0)), base);
    DremioTable dremioTable = base.getTableFromId(datasetIdFromSnapshot, base);
    String  schema = dremioTable.getSchema().toJSONString();
    assertThat(schema.contains("id")).isTrue();
    assertThat(schema.contains("name")).isTrue();
    assertThat(schema.contains("distance")).isTrue();
    base.runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void getTableWithIdTimestamp() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));
    base.runSQL(insertTableQuery(tablePath));
    long ts1 = System.currentTimeMillis();
    String datasetIdWithTS = base.getVersionedDatatsetIdForTimeTravel(tablePath, new TableVersionContext(TableVersionType.TIMESTAMP, ts1), base);
    DremioTable dremioTableWithTS = base.getTableFromId(datasetIdWithTS, base);
    String  schema = dremioTableWithTS.getSchema().toJSONString();
    assertThat(schema.contains("id")).isTrue();
    assertThat(schema.contains("name")).isTrue();
    assertThat(schema.contains("distance")).isTrue();
    base.runSQL(dropTableQuery(tablePath));
  }


  @Test
  public void getTableWithIdAfterAlter() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));
    String datasetIdAfterCreate = base.getVersionedDatatsetId(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), base);
    final List<String> addedColDef = Collections.singletonList("col2 int");
    //Add single column
    base.runSQL(alterTableAddColumnsQuery(tablePath, addedColDef));
    String datasetIdAfterAlter = base.getVersionedDatatsetId(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), base);
    DremioTable dremioTableAfterAlter = base.getTableFromId(datasetIdAfterAlter, base);

    // Assert
    assertThat(datasetIdAfterCreate.equals(datasetIdAfterAlter)).isTrue();
    String  schemaAfterAlter = dremioTableAfterAlter.getSchema().toJSONString();
    assertThat(schemaAfterAlter.contains("col2")).isTrue();
    base.runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void getTableWithIdAfterReCreate() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));
    String datasetIdAfterFirstCreate = base.getVersionedDatatsetId(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), base);

    base.runSQL(dropTableQuery(tablePath));
    base.runSQL(createTableAsQuery(tablePath, 5));
    String datasetIdAfterRecreate = base.getVersionedDatatsetId(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), base);

    //Act
    //Lookup with the new id.
    DremioTable dremioTableAfterRecreate = base.getTableFromId(datasetIdAfterRecreate, base);
    DremioTable dremioTableAfterFirstCreate = base.getTableFromId(datasetIdAfterFirstCreate, base);

    // Assert
    assertThat(datasetIdAfterFirstCreate.equals(datasetIdAfterRecreate)).isFalse();
    assertThat(dremioTableAfterRecreate.getSchema().toJSONString().contains("n_nationkey")).isTrue();
    assertThat(dremioTableAfterFirstCreate).isNull();

    base.runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void getTableWithInvalidId() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));
    String datasetIdAfterCreate = base.getVersionedDatatsetId(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), base);
    String invalidDatasetId = StringUtils.replace(datasetIdAfterCreate, "contentId" , "invalidContentIdToken");
    DremioTable dremioTableFromWrongId = base.getTableFromId(invalidDatasetId, base);

    assertThat(dremioTableFromWrongId).isNull();
    base.runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void getTableWithIdFromNewBranch() throws Exception {
// Arrange
    final String newBranchName = generateUniqueBranchName();
    final String tableName = generateUniqueTableName();
    final List<String>  tablePath = tablePathWithFolders(tableName);
    //Create table in main branch
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));
    //insert in main branch
    base.runSQL(insertTableQuery(tablePath));
    base.runSQL(createBranchAtBranchQuery(newBranchName, DEFAULT_BRANCH_NAME));

    String datasetIdFromMain = base.getVersionedDatatsetId(tablePath,new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), base);

    //Act
    base.runSQL(useBranchQuery(newBranchName));
    String datasetIdIdFromBranch = base.getVersionedDatatsetId(tablePath,new TableVersionContext(TableVersionType.BRANCH, newBranchName), base);

    DremioTable tableFromMain = base.getTableFromId(datasetIdFromMain, base);
    DremioTable tableFromBranch = base.getTableFromId(datasetIdIdFromBranch, base);
    VersionedDatasetId versionedDatasetIdFromMain = VersionedDatasetId.fromString(tableFromMain.getDatasetConfig().getId().getId());
    VersionedDatasetId versionedDatasetIdFromBranch = VersionedDatasetId.fromString(tableFromBranch.getDatasetConfig().getId().getId());
    String contentIdFromMain = versionedDatasetIdFromMain.getContentId();
    String contentIdFromBranch = versionedDatasetIdFromBranch.getContentId();

    String uniqueIdFromMain = tableFromMain.getDatasetConfig().getPhysicalDataset().getIcebergMetadata().getTableUuid();
    String uniqueIdFromBranch = tableFromBranch.getDatasetConfig().getPhysicalDataset().getIcebergMetadata().getTableUuid();

    // Assert
    // The uniqueId, contentId should be the same for the table in both branches.
    // The datasetId would be different (version context part)
    assertThat (contentIdFromBranch.equals(contentIdFromMain)).isTrue();
    assertThat(uniqueIdFromBranch.equals(uniqueIdFromMain)).isTrue();
    assertThat(datasetIdFromMain.equals(datasetIdIdFromBranch)).isFalse();
  }

  @Test
  public void getTableWithIdFromTag() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final String firstTag = generateUniqueTagName();

    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));
    base.runSQL(createTagQuery(firstTag, DEFAULT_BRANCH_NAME));

    // Act
    base.runSQL(insertTableQuery(tablePath));

    String versionAtTag = base.getVersionedDatatsetId(tablePath, new TableVersionContext(TableVersionType.TAG, firstTag), base);
    String versionAtBranchTip = base.getVersionedDatatsetId(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), base);
    DremioTable tableAtTag = base.getTableFromId(versionAtTag, base);
    DremioTable tableAtBranchTip = base.getTableFromId(versionAtBranchTip, base);

    String uniqueIdAtTag = tableAtTag.getDatasetConfig().getPhysicalDataset().getIcebergMetadata().getTableUuid();
    String uniqueIdAtBranchTip = tableAtBranchTip.getDatasetConfig().getPhysicalDataset().getIcebergMetadata().getTableUuid();

    EntityId versionedDatasetIdAtTag = tableAtTag.getDatasetConfig().getId();
    EntityId versionedDatasetIdAtBranchTip = tableAtBranchTip.getDatasetConfig().getId();
    // Assert

    assertThat(uniqueIdAtTag.equals(uniqueIdAtBranchTip)).isFalse();
    assertThat(versionedDatasetIdAtTag.equals(versionedDatasetIdAtBranchTip)).isFalse();

    // cleanup
    base.runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void getTableWithIdFromInvalidTag() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final String invalidTag = generateUniqueTagName();
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));

    // Act
    base.runSQL(insertTableQuery(tablePath));
    String versionAtTag = base.getVersionedDatatsetId(tablePath, new TableVersionContext(TableVersionType.TAG, invalidTag), base);

    // Assert
    assertThat(versionAtTag).isNull();

    // cleanup
    base.runSQL(dropTableQuery(tablePath));
  }


  @Test
  public void getTableWithIdFromCommit() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));
    String commitHashBeforeInsert = base.getCommitHashForBranch(DEFAULT_BRANCH_NAME);

    // Act
    base.runSQL(insertTableQuery(tablePath));

    String versionAtCommitHash = base.getVersionedDatatsetId(tablePath, new TableVersionContext(TableVersionType.COMMIT_HASH_ONLY, commitHashBeforeInsert), base);
    String versionAtBranchTip = base.getVersionedDatatsetId(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), base);
    DremioTable tableAtCommitHash = base.getTableFromId(versionAtCommitHash, base);
    DremioTable tableAtBranchTip = base.getTableFromId(versionAtBranchTip, base);

    String uniqueIdAtTag = tableAtCommitHash.getDatasetConfig().getPhysicalDataset().getIcebergMetadata().getTableUuid();
    String uniqueIdAtBranchTip = tableAtBranchTip.getDatasetConfig().getPhysicalDataset().getIcebergMetadata().getTableUuid();

    EntityId versionedDatasetIdAtTag = tableAtCommitHash.getDatasetConfig().getId();
    EntityId versionedDatasetIdAtBranchTip = tableAtBranchTip.getDatasetConfig().getId();
    // Assert

    assertThat(uniqueIdAtTag.equals(uniqueIdAtBranchTip)).isFalse();
    assertThat(versionedDatasetIdAtTag.equals(versionedDatasetIdAtBranchTip)).isFalse();

    // cleanup
    base.runSQL(dropTableQuery(tablePath));
  }

  /**
   * Tests content id  after insert- should stay the same
   */
  @Test
  public void checkContentIdWithAt() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final String tag1 = generateUniqueTagName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createTableAsQuery(tablePath, 5));
    assertNessieHasTable(tablePath, DEFAULT_BRANCH_NAME, base);
    assertNessieHasCommitForTable(tablePath, Operation.Put.class, DEFAULT_BRANCH_NAME, base);

    //Act
    base.runSQL(insertSelectQuery(tablePath, 5));
    base.runSQL(createTagQuery(tag1, DEFAULT_BRANCH_NAME));
    base.runSQL(insertSelectQuery(tablePath, 5));

    String contentIdWithSelectAtTag = base.getContentIdForTableAtRef(tablePath, new TableVersionContext(TableVersionType.TAG, tag1), base);
    String contentIdWithSelectAtMain = base.getContentId(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), base);
    // Assert
    assertThat(contentIdWithSelectAtMain.equals(contentIdWithSelectAtTag)).isTrue();

    // cleanup
    base.runSQL(dropTableQuery(tablePath));
  }

  /**
   * Tests content id  after insert- should stay the same
   */
  @Test
  public void checkUniqueIdWithAt() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final String tag1 = generateUniqueTagName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createTableAsQuery(tablePath, 5));
    assertNessieHasTable(tablePath, DEFAULT_BRANCH_NAME, base);
    assertNessieHasCommitForTable(tablePath, Operation.Put.class, DEFAULT_BRANCH_NAME, base);

    //Act
    base.runSQL(insertSelectQuery(tablePath, 5));
    base.runSQL(createTagQuery(tag1, DEFAULT_BRANCH_NAME));
    base.runSQL(insertSelectQuery(tablePath, 5));

    String uniqueIdWithSelectAtTag = base.getUniqueIdForTableAtRef(tablePath, new TableVersionContext(TableVersionType.TAG, tag1), base);
    String uniqueIdWithSelectAtMain = base.getUniqueIdForTable(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), base);
    // Assert
    assertThat(uniqueIdWithSelectAtMain.equals(uniqueIdWithSelectAtTag)).isFalse();

    // cleanup
    base.runSQL(dropTableQuery(tablePath));
  }

}
