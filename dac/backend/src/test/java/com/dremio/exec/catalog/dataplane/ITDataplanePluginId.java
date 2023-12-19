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
import static com.dremio.exec.catalog.dataplane.TestDataplaneAssertions.assertNessieHasCommitForTable;
import static com.dremio.exec.catalog.dataplane.TestDataplaneAssertions.assertNessieHasTable;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;
import org.projectnessie.model.Operation;

import com.dremio.catalog.model.VersionContext;
import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.catalog.model.dataset.TableVersionType;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.VersionedDatasetId;
import com.dremio.service.namespace.proto.EntityId;


@Isolated
public class ITDataplanePluginId extends ITDataplanePluginTestSetup {

  /**
   * Tests content id  after create - should be unique for each create
   */
  @Test
  public void checkContentIdAfterReCreate() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(tablePath));
    String contentIdBeforeDrop = getContentId(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), this);
    runSQL(dropTableQuery(tablePath));

    // Act
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(tablePath));
    String contentIdAfterReCreate = getContentId(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), this);

    // Assert
    assertThat((contentIdBeforeDrop.equals(contentIdAfterReCreate))).isFalse();
    runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void checkUniqueIdAfterCreate() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(tablePath));
    String uniqueIdBeforeDrop = getUniqueIdForTable(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), this);
    runSQL(dropTableQuery(tablePath));

    // Act
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(tablePath));
    String uniqueIdAfterDrop = getUniqueIdForTable(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), this);

    // Assert
    assertThat(uniqueIdBeforeDrop.equals(uniqueIdAfterDrop)).isFalse();
    runSQL(dropTableQuery(tablePath));
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
    runSQL(createEmptyTableQuery(tablePath));
    String contentIdBeforeAlter = getContentId(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), this);
    final List<String> addedColDef = Collections.singletonList("col2 int");
    //Add single column
    runSQL(alterTableAddColumnsQuery(tablePath, addedColDef));
    String contentIdAfterAlter = getContentId(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), this);

    // Assert
    assertThat(contentIdBeforeAlter.equals(contentIdAfterAlter)).isTrue();
    runSQL(dropTableQuery(tablePath));
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
    runSQL(createEmptyTableQuery(tablePath));
    String uniqueIdBeforeAlter = getUniqueIdForTable(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), this);
    final List<String> addedColDef = Collections.singletonList("col2 int");
    //Add single column
    runSQL(alterTableAddColumnsQuery(tablePath, addedColDef));
    String uniqueIdAfterAlter = getUniqueIdForTable(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), this);

    // Assert
    assertThat(uniqueIdBeforeAlter.equals(uniqueIdAfterAlter)).isFalse();
    runSQL(dropTableQuery(tablePath));
  }

  /**
   * Tests content id  after insert - should stay the same
   */
  @Test
  public void checkContentIdAfterInsert() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(tablePath));
    assertNessieHasTable(tablePath, DEFAULT_BRANCH_NAME, this);
    assertNessieHasCommitForTable(tablePath, Operation.Put.class, DEFAULT_BRANCH_NAME, this);
    String contentIdBeforeInsert = getContentId(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), this);
    // Act
    runSQL(insertTableQuery(tablePath));
    String contentIdAfterInsert = getContentId(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), this);
    // Assert
   assertThat(contentIdBeforeInsert.equals(contentIdAfterInsert)).isTrue();

    // cleanup
    runSQL(dropTableQuery(tablePath));
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
    runSQL(createEmptyTableQuery(tablePath));
    assertNessieHasTable(tablePath, DEFAULT_BRANCH_NAME, this);
    assertNessieHasCommitForTable(tablePath, Operation.Put.class, DEFAULT_BRANCH_NAME, this);
    String uniqueIdBeforeInsert = getUniqueIdForTable(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), this);
    // Act
    runSQL(insertTableQuery(tablePath));
    String uniqueIdAfterInsert = getUniqueIdForTable(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), this);
    // Assert
    assertThat(uniqueIdBeforeInsert.equals(uniqueIdAfterInsert)).isFalse();

    // cleanup
    runSQL(dropTableQuery(tablePath));
  }

  /**
   * Tests content id  after insert - should stay the same
   */
  @Test
  public void checkContentIdAfterDeletes() throws Exception {
    //Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(tablePath));
    assertNessieHasTable(tablePath, DEFAULT_BRANCH_NAME, this);
    assertNessieHasCommitForTable(tablePath, Operation.Put.class, DEFAULT_BRANCH_NAME, this);
    String contentIdAfterCreate = getContentId(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), this);
    runSQL(insertTableQuery(tablePath));
    String contentIdAfterInsert = getContentId(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), this);
    // Act
    runSQL(deleteAllQuery(tablePath));
    String contentIdAfterDelete = getContentId(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), this);
    // Assert
    assertThat(contentIdAfterCreate.equals(contentIdAfterInsert)).isTrue();
    assertThat(contentIdAfterDelete.equals(contentIdAfterInsert)).isTrue();
    // cleanup
    runSQL(dropTableQuery(tablePath));
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
    runSQL(createEmptyTableQuery(tablePath));
    assertNessieHasTable(tablePath, DEFAULT_BRANCH_NAME, this);
    assertNessieHasCommitForTable(tablePath, Operation.Put.class, DEFAULT_BRANCH_NAME, this);
    String uniqueIdAfterCreate = getUniqueIdForTable(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), this);
    runSQL(insertTableQuery(tablePath));
    String uniqueIdAfterInsert = getUniqueIdForTable(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), this);
    // Act
    runSQL(deleteAllQuery(tablePath));
    String uniqueIdAfterDelete = getUniqueIdForTable(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), this);
    // Assert
    assertThat(uniqueIdAfterCreate.equals(uniqueIdAfterInsert)).isFalse();
    assertThat(uniqueIdAfterDelete.equals(uniqueIdAfterInsert)).isFalse();

    // cleanup
    runSQL(dropTableQuery(tablePath));
  }


  /**
   * Tests content id  after branching - should stay the same
   */
  @Test
  public void checkContentIdInNewBranch() throws Exception {
    // Arrange
    final String newBranchName = generateUniqueBranchName();
    final String tableName = generateUniqueTableName();
    final List<String>  tablePath = tablePathWithFolders(tableName);
    //Create table in main branch
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(tablePath));
    runSQL(insertTableQuery(tablePath));
    runSQL(createBranchAtBranchQuery(newBranchName, DEFAULT_BRANCH_NAME));
    String contentIdInMainBranch = getContentId(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), this);

    //Act
    runSQL(useBranchQuery(newBranchName));
    String contentIdInNewBranch = getContentId(tablePath, new TableVersionContext(TableVersionType.BRANCH, newBranchName), this);

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
    runSQL(createEmptyTableQuery(tablePath));
    runSQL(createBranchAtBranchQuery(newBranchName, DEFAULT_BRANCH_NAME));
    //insert in main branch
    runSQL(insertTableQuery(tablePath));
    String uniqueIdBeforeBranching = getUniqueIdForTable(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), this);

    //Act
    runSQL(useBranchQuery(newBranchName));
    String uniqueIdAfterBranching = getUniqueIdForTable(tablePath, new TableVersionContext(TableVersionType.BRANCH, newBranchName), this);

    //Assert
    assertThat(uniqueIdAfterBranching.equals(uniqueIdBeforeBranching)).isFalse();
  }

  /**
   * Tests content id  after assign - should stay the same
   */
  @Test
  public void checkContentIdAfterAssignBranch() throws Exception {
    // Arrange
    final String branchName = generateUniqueBranchName();
    final String mainTableName = generateUniqueTableName();
    final List<String> mainTablePath = tablePathWithFolders(mainTableName);

    runSQL(createBranchAtBranchQuery(branchName, DEFAULT_BRANCH_NAME));
    createFolders(mainTablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(mainTablePath));
    runSQL(insertTableQuery(mainTablePath));
    String contentIdBeforeAssign = getContentId(mainTablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), this);
    // Act
    runSQL(alterBranchAssignBranchQuery(branchName, DEFAULT_BRANCH_NAME));
    runSQL(useBranchQuery(branchName));
    String contentIdAfterAssign = getContentId(mainTablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), this);
    // ASSERT
    assertThat(contentIdAfterAssign).isEqualTo(contentIdBeforeAssign);
    // Drop tables
    runSQL(dropTableQuery(mainTablePath));
  }

  /**
   * Tests unique id of objects after assign - should stay the same
   */
  @Test
  public void checkUniqueIdAfterAssignBranch() throws Exception {
    // Arrange
    final String branchName = generateUniqueBranchName();
    final String mainTableName = generateUniqueTableName();
    final List<String> mainTablePath = tablePathWithFolders(mainTableName);

    runSQL(createBranchAtBranchQuery(branchName, DEFAULT_BRANCH_NAME));
    createFolders(mainTablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(mainTablePath));
    runSQL(insertTableQuery(mainTablePath));
    String uniqueIdBeforeAssign = getUniqueIdForTable(mainTablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), this);
    // Act
    runSQL(alterBranchAssignBranchQuery(branchName, DEFAULT_BRANCH_NAME));
    runSQL(useBranchQuery(branchName));
    String uniqueIdAfterAssign = getUniqueIdForTable(mainTablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), this);
    // ASSERT
    assertThat(uniqueIdBeforeAssign.equals(uniqueIdAfterAssign)).isTrue();
    // Drop tables
    runSQL(dropTableQuery(mainTablePath));
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
    runSQL(createTableAsQuery(tablePath1, 10));
    final String viewName = generateUniqueViewName();
    List<String> viewKey = tablePathWithFolders(viewName);
    createFolders(viewKey, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    runSQL(createViewQuery(viewKey, tablePath1));
    String contentIdAfterCreate = getContentId(viewKey, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), this);

    assertViewHasExpectedNumRows(viewKey, 10);
    //Create table2 with 20 rows.
    createFolders(tablePath2, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    runSQL(createTableAsQuery(tablePath2, 20));

    //Act
    runSQL(createReplaceViewQuery(viewKey, tablePath2));
    String contentIdAfterUpdate = getContentId(viewKey, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), this);
    assertThat(contentIdAfterCreate.equals(contentIdAfterUpdate)).isTrue();
    runSQL(dropTableQuery(tablePath1));
    runSQL(dropTableQuery(tablePath2));
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
    runSQL(createTableAsQuery(tablePath1, 10));
    final String viewName = generateUniqueViewName();
    List<String> viewKey = tablePathWithFolders(viewName);
    createFolders(viewKey, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    runSQL(createViewQuery(viewKey, tablePath1));
    String uniqueIdAfterCreate = getUniqueIdForView(viewKey, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), this);

    assertViewHasExpectedNumRows(viewKey, 10);
    //Create table2 with 20 rows.
    createFolders(tablePath2, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    runSQL(createTableAsQuery(tablePath2, 20));

    //Act
    runSQL(createReplaceViewQuery(viewKey, tablePath2));
    String uniqueIdAfterUpdate = getUniqueIdForView(viewKey, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), this);
    assertThat(uniqueIdAfterCreate.equals(uniqueIdAfterUpdate)).isFalse();
    runSQL(dropTableQuery(tablePath1));
    runSQL(dropTableQuery(tablePath2));
  }

  /**
   * Tests dataset id  after branching - should change
   */
  @Test
  public void checkDatasetIdInNewBranch() throws Exception {
    // Arrange
    final String newBranchName = generateUniqueBranchName();
    final String tableName = generateUniqueTableName();
    final List<String>  tablePath = tablePathWithFolders(tableName);
    //Create table in main branch
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(tablePath));
    runSQL(insertTableQuery(tablePath));
    runSQL(createBranchAtBranchQuery(newBranchName, DEFAULT_BRANCH_NAME));
    String datasetIdInMainBranch = getVersionedDatatsetId(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), this);

    //Act
    runSQL(useBranchQuery(newBranchName));
    String datasetIdInNewBranch = getVersionedDatatsetId(tablePath, new TableVersionContext(TableVersionType.BRANCH, newBranchName), this);

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
    runSQL(createEmptyTableQuery(tablePath));
    runSQL(createBranchAtBranchQuery(newBranchName, DEFAULT_BRANCH_NAME));
    //insert in main branch
    runSQL(insertTableQuery(tablePath));
    String datasetIdBeforeBranching = getVersionedDatatsetId(tablePath,new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), this);

    //Act
    runSQL(useBranchQuery(newBranchName));
    String datasetIdIdAfterBranching = getVersionedDatatsetId(tablePath,new TableVersionContext(TableVersionType.BRANCH, newBranchName), this);

    //Assert
    assertThat(datasetIdBeforeBranching.equals(datasetIdIdAfterBranching)).isFalse();
  }

  @Test
  public void getTableWithId() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(tablePath));
    String datasetIdAfterCreate = getVersionedDatatsetId(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), this);
    DremioTable dremioTableAfterCreate = getTableFromId(datasetIdAfterCreate, this);
    String  schema = dremioTableAfterCreate.getSchema().toJSONString();
    assertThat(schema.contains("id")).isTrue();
    assertThat(schema.contains("name")).isTrue();
    assertThat(schema.contains("distance")).isTrue();
    runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void getTableWithIdSnapshot() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(tablePath));
    runSQL(insertTableQuery(tablePath));
    String getSnapshotString = String.format("SELECT snapshot_id FROM table(table_snapshot('%s.%s'))",
      DATAPLANE_PLUGIN_NAME,
      String.join(".", tablePath));
    List<String> results = runSqlWithResults(getSnapshotString).get(0);


    String datasetIdFromSnapshot = getVersionedDatatsetIdForTimeTravel(tablePath, new TableVersionContext(TableVersionType.SNAPSHOT_ID, results.get(0)), this);
    DremioTable dremioTable = getTableFromId(datasetIdFromSnapshot, this);
    String  schema = dremioTable.getSchema().toJSONString();
    assertThat(schema.contains("id")).isTrue();
    assertThat(schema.contains("name")).isTrue();
    assertThat(schema.contains("distance")).isTrue();
    runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void getTableWithIdTimestamp() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(tablePath));
    runSQL(insertTableQuery(tablePath));
    long ts1 = System.currentTimeMillis();
    String datasetIdWithTS = getVersionedDatatsetIdForTimeTravel(tablePath, new TableVersionContext(TableVersionType.TIMESTAMP, ts1), this);
    DremioTable dremioTableWithTS = getTableFromId(datasetIdWithTS, this);
    String  schema = dremioTableWithTS.getSchema().toJSONString();
    assertThat(schema.contains("id")).isTrue();
    assertThat(schema.contains("name")).isTrue();
    assertThat(schema.contains("distance")).isTrue();
    runSQL(dropTableQuery(tablePath));
  }


  @Test
  public void getTableWithIdAfterAlter() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(tablePath));
    String datasetIdAfterCreate = getVersionedDatatsetId(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), this);
    final List<String> addedColDef = Collections.singletonList("col2 int");
    //Add single column
    runSQL(alterTableAddColumnsQuery(tablePath, addedColDef));
    String datasetIdAfterAlter = getVersionedDatatsetId(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), this);
    DremioTable dremioTableAfterAlter = getTableFromId(datasetIdAfterAlter, this);

    // Assert
    assertThat(datasetIdAfterCreate.equals(datasetIdAfterAlter)).isTrue();
    String  schemaAfterAlter = dremioTableAfterAlter.getSchema().toJSONString();
    assertThat(schemaAfterAlter.contains("col2")).isTrue();
    runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void getTableWithIdAfterReCreate() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(tablePath));
    String datasetIdAfterFirstCreate = getVersionedDatatsetId(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), this);

    runSQL(dropTableQuery(tablePath));
    runSQL(createTableAsQuery(tablePath, 5));
    String datasetIdAfterRecreate = getVersionedDatatsetId(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), this);

    //Act
    //Lookup with the new id.
    DremioTable dremioTableAfterRecreate = getTableFromId(datasetIdAfterRecreate, this);
    DremioTable dremioTableAfterFirstCreate = getTableFromId(datasetIdAfterFirstCreate, this);

    // Assert
    assertThat(datasetIdAfterFirstCreate.equals(datasetIdAfterRecreate)).isFalse();
    assertThat(dremioTableAfterRecreate.getSchema().toJSONString().contains("n_nationkey")).isTrue();
    assertThat(dremioTableAfterFirstCreate).isNull();

    runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void getTableWithInvalidId() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(tablePath));
    String datasetIdAfterCreate = getVersionedDatatsetId(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), this);
    String invalidDatasetId = StringUtils.replace(datasetIdAfterCreate, "contentId" , "invalidContentIdToken");
    DremioTable dremioTableFromWrongId = getTableFromId(invalidDatasetId, this);

    assertThat(dremioTableFromWrongId).isNull();
    runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void getTableWithIdFromNewBranch() throws Exception {
// Arrange
    final String newBranchName = generateUniqueBranchName();
    final String tableName = generateUniqueTableName();
    final List<String>  tablePath = tablePathWithFolders(tableName);
    //Create table in main branch
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(tablePath));
    //insert in main branch
    runSQL(insertTableQuery(tablePath));
    runSQL(createBranchAtBranchQuery(newBranchName, DEFAULT_BRANCH_NAME));

    String datasetIdFromMain = getVersionedDatatsetId(tablePath,new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), this);

    //Act
    runSQL(useBranchQuery(newBranchName));
    String datasetIdIdFromBranch = getVersionedDatatsetId(tablePath,new TableVersionContext(TableVersionType.BRANCH, newBranchName), this);

    DremioTable tableFromMain = getTableFromId(datasetIdFromMain, this);
    DremioTable tableFromBranch = getTableFromId(datasetIdIdFromBranch, this);
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
    runSQL(createEmptyTableQuery(tablePath));
    runSQL(createTagQuery(firstTag, DEFAULT_BRANCH_NAME));

    // Act
    runSQL(insertTableQuery(tablePath));

    String versionAtTag = getVersionedDatatsetId(tablePath, new TableVersionContext(TableVersionType.TAG, firstTag), this);
    String versionAtBranchTip = getVersionedDatatsetId(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), this);
    DremioTable tableAtTag = getTableFromId(versionAtTag, this);
    DremioTable tableAtBranchTip = getTableFromId(versionAtBranchTip, this);

    String uniqueIdAtTag = tableAtTag.getDatasetConfig().getPhysicalDataset().getIcebergMetadata().getTableUuid();
    String uniqueIdAtBranchTip = tableAtBranchTip.getDatasetConfig().getPhysicalDataset().getIcebergMetadata().getTableUuid();

    EntityId versionedDatasetIdAtTag = tableAtTag.getDatasetConfig().getId();
    EntityId versionedDatasetIdAtBranchTip = tableAtBranchTip.getDatasetConfig().getId();
    // Assert

    assertThat(uniqueIdAtTag.equals(uniqueIdAtBranchTip)).isFalse();
    assertThat(versionedDatasetIdAtTag.equals(versionedDatasetIdAtBranchTip)).isFalse();

    // cleanup
    runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void getTableWithIdFromInvalidTag() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final String invalidTag = generateUniqueTagName();
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(tablePath));

    // Act
    runSQL(insertTableQuery(tablePath));
    String versionAtTag = getVersionedDatatsetId(tablePath, new TableVersionContext(TableVersionType.TAG, invalidTag), this);

    // Assert
    assertThat(versionAtTag).isNull();

    // cleanup
    runSQL(dropTableQuery(tablePath));
  }


  @Test
  public void getTableWithIdFromCommit() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(tablePath));
    String commitHashBeforeInsert = getCommitHashForBranch(DEFAULT_BRANCH_NAME);

    // Act
    runSQL(insertTableQuery(tablePath));

    String versionAtCommitHash = getVersionedDatatsetId(tablePath, new TableVersionContext(TableVersionType.COMMIT, commitHashBeforeInsert), this);
    String versionAtBranchTip = getVersionedDatatsetId(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), this);
    DremioTable tableAtCommitHash = getTableFromId(versionAtCommitHash, this);
    DremioTable tableAtBranchTip = getTableFromId(versionAtBranchTip, this);

    String uniqueIdAtTag = tableAtCommitHash.getDatasetConfig().getPhysicalDataset().getIcebergMetadata().getTableUuid();
    String uniqueIdAtBranchTip = tableAtBranchTip.getDatasetConfig().getPhysicalDataset().getIcebergMetadata().getTableUuid();

    EntityId versionedDatasetIdAtTag = tableAtCommitHash.getDatasetConfig().getId();
    EntityId versionedDatasetIdAtBranchTip = tableAtBranchTip.getDatasetConfig().getId();
    // Assert

    assertThat(uniqueIdAtTag.equals(uniqueIdAtBranchTip)).isFalse();
    assertThat(versionedDatasetIdAtTag.equals(versionedDatasetIdAtBranchTip)).isFalse();

    // cleanup
    runSQL(dropTableQuery(tablePath));
  }

  /**
   * Tests content id  after insert - should stay the same
   */
  @Test
  public void checkContentIdWithAt() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final String tag1 = generateUniqueTagName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    runSQL(createTableAsQuery(tablePath, 5));
    assertNessieHasTable(tablePath, DEFAULT_BRANCH_NAME, this);
    assertNessieHasCommitForTable(tablePath, Operation.Put.class, DEFAULT_BRANCH_NAME, this);

    //Act
    runSQL(insertSelectQuery(tablePath, 5));
    runSQL(createTagQuery(tag1, DEFAULT_BRANCH_NAME));
    runSQL(insertSelectQuery(tablePath, 5));

    String contentIdWithSelectAtTag = getContentIdForTableAtRef(tablePath, new TableVersionContext(TableVersionType.TAG, tag1), this);
    String contentIdWithSelectAtMain = getContentId(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), this);
    // Assert
    assertThat(contentIdWithSelectAtMain.equals(contentIdWithSelectAtTag)).isTrue();

    // cleanup
    runSQL(dropTableQuery(tablePath));
  }

  /**
   * Tests content id  after insert - should stay the same
   */
  @Test
  public void checkUniqueIdWithAt() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final String tag1 = generateUniqueTagName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    runSQL(createTableAsQuery(tablePath, 5));
    assertNessieHasTable(tablePath, DEFAULT_BRANCH_NAME, this);
    assertNessieHasCommitForTable(tablePath, Operation.Put.class, DEFAULT_BRANCH_NAME, this);

    //Act
    runSQL(insertSelectQuery(tablePath, 5));
    runSQL(createTagQuery(tag1, DEFAULT_BRANCH_NAME));
    runSQL(insertSelectQuery(tablePath, 5));

    String uniqueIdWithSelectAtTag = getUniqueIdForTableAtRef(tablePath, new TableVersionContext(TableVersionType.TAG, tag1), this);
    String uniqueIdWithSelectAtMain = getUniqueIdForTable(tablePath, new TableVersionContext(TableVersionType.BRANCH, DEFAULT_BRANCH_NAME), this);
    // Assert
    assertThat(uniqueIdWithSelectAtMain.equals(uniqueIdWithSelectAtTag)).isFalse();

    // cleanup
    runSQL(dropTableQuery(tablePath));
  }

}
