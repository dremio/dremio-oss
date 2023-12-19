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
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.DEFAULT_COUNT_COLUMN;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.DEFAULT_RECORD_DELIMITER;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.copyIntoTableQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.copyIntoTableQueryWithAt;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createBranchAtBranchQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createEmptyTableQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createEmptyTableQueryWithAt;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createFolderAtQueryWithIfNotExists;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createTableWithColDefsQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createTagQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.dropBranchForceQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.dropTableQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.dropTableQueryWithAt;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateFolderPath;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateSourceFiles;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueBranchName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueFolderName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueTableName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueTagName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.insertTableAtQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.insertTableWithValuesQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.joinedTableKey;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.mergeBranchQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.selectCountQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.tablePathWithFolders;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.useBranchQuery;
import static com.dremio.exec.catalog.dataplane.TestDataplaneAssertions.assertIcebergFilesExistAtSubPath;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.dremio.catalog.model.VersionContext;
import com.dremio.service.namespace.NamespaceKey;

public class ITDataplanePluginCopyInto extends ITDataplanePluginTestSetup {
  private static final String fileNameCsv =  "file1.csv";
  private static final String fileNameJson =  "file1.json";

  private static File newSourceFileCsv;
  private static File newSourceFileJson;

  private final File location = createTempLocation();;
  private final String storageLocation = "'@" + TEMP_SCHEMA_HADOOP + "/" +  location.getName()  + "'";;

  @BeforeEach
  public void createSourceFiles() throws Exception{
    newSourceFileCsv = generateSourceFiles(fileNameCsv , location);
    newSourceFileJson = generateSourceFiles(fileNameJson , location);
  }

  @AfterEach
  public void cleanupSourceFiles() {
    assertTrue(newSourceFileCsv.delete());
    assertTrue(newSourceFileJson.delete());
  }

  @Test
  public void copyIntoEmptyTable() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(tablePath));

    // Act
    runSQL(copyIntoTableQuery(tablePath, storageLocation, fileNameCsv));

    // Assert
    assertTableHasExpectedNumRows(tablePath, 3);

    // Act
    runSQL(copyIntoTableQuery(tablePath, storageLocation, fileNameJson));

    // Assert
    assertTableHasExpectedNumRows(tablePath, 6);

    // cleanup
    runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void copyIntoEmptyTableWithAtSyntax() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = Collections.singletonList(tableName);
    final String devBranch = generateUniqueBranchName();
    runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQueryWithAt(tablePath, devBranch));

    // Act
    runSQL(copyIntoTableQueryWithAt(tablePath, storageLocation, fileNameCsv, devBranch));

    // Assert
    assertTableAtBranchHasExpectedNumRows(tablePath, devBranch, 3);

    // cleanup
    runSQL(dropTableQueryWithAt(tablePath, devBranch));
  }

  @Test
  public void copyIntoNonEmptyTableWithAtSyntaxAfterInsert() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = Collections.singletonList(tableName);
    final String devBranch = generateUniqueBranchName();
    runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQueryWithAt(tablePath, devBranch));

    //create table with default 3 rows.
    runSQL(insertTableAtQuery(tablePath, devBranch));
    assertTableAtBranchHasExpectedNumRows(tablePath, devBranch, 3);


    // Act
    runSQL(copyIntoTableQueryWithAt(tablePath, storageLocation, fileNameCsv, devBranch));

    // Assert
    assertTableAtBranchHasExpectedNumRows(tablePath, devBranch, 6);

    // cleanup
    runSQL(dropTableQueryWithAt(tablePath, devBranch));
  }

  @Test
  public void copyIntoNonEmptyTableWithAtSyntaxAfterDelete() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = Collections.singletonList(tableName);
    final String devBranch = generateUniqueBranchName();
    runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQueryWithAt(tablePath, devBranch));

    //create table with default 3 rows.
    runSQL(insertTableAtQuery(tablePath, devBranch));
    assertTableAtBranchHasExpectedNumRows(tablePath, devBranch, 3);

    //delete all from table with 3 rows.
    runSQL(String.format("DELETE FROM %s.%s at branch %s", DATAPLANE_PLUGIN_NAME, tableName, devBranch));
    assertTableAtBranchHasExpectedNumRows(tablePath, devBranch, 0);

    // Act
    runSQL(copyIntoTableQueryWithAt(tablePath, storageLocation, fileNameCsv, devBranch));

    // Assert
    assertTableAtBranchHasExpectedNumRows(tablePath, devBranch, 3);

    // cleanup
    runSQL(dropTableQueryWithAt(tablePath, devBranch));
  }

  @Test
  public void copyIntoEmptyTableWithNoMatchingRows() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    runSQL(createTableWithColDefsQuery(tablePath, Arrays.asList("c1 int", "c2 int", "c3 int")));

    // Act and assert error is thrown
    assertQueryThrowsExpectedError(copyIntoTableQuery(tablePath, storageLocation, fileNameCsv), "No column name matches target schema");

    // cleanup
    runSQL(dropTableQuery(tablePath));
  }


  @Test
  public void copyIntoTableWithRows() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(tablePath));

    //Insert into table
    runSQL(insertTableWithValuesQuery(tablePath, Arrays.asList("(4,'str1',34.45)","(5,'str1',34.45)","(6,'str1',34.45)")));

    //Assert
    assertTableHasExpectedNumRows(tablePath, 3);

    // Act
    runSQL(copyIntoTableQuery(tablePath, storageLocation, fileNameCsv));

    // Assert
    assertTableHasExpectedNumRows(tablePath, 6);

    // cleanup
    runSQL(dropTableQuery(tablePath));
  }


  /**
   * Verify insert creates underlying iceberg files in the right locations
   */
  @Test
  public void copyIntoAndVerifyFolders() throws Exception {
    // Arrange
    // Create a hierarchy of 2 folders to form key of TABLE
    final List<String> tablePath = Arrays.asList("if1", "if2", generateUniqueTableName());

    // Create empty
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(tablePath));
    // Verify iceberg manifest/avro/metadata.json files on FS
    assertIcebergFilesExistAtSubPath(tablePath, 0, 1, 1, 0);

    // Do 2 separate Inserts so there are multiple data files.
    // Copy 1
    runSQL(copyIntoTableQuery(tablePath, storageLocation, fileNameCsv));
    assertTableHasExpectedNumRows(tablePath, 3);
    // Verify iceberg manifest/avro/metadata.json files on FS
    assertIcebergFilesExistAtSubPath(tablePath, 1, 2, 2, 1);

    // Copy 2
    runSQL(copyIntoTableQuery(tablePath, storageLocation, fileNameCsv));
    // Verify number of rows with select
    assertTableHasExpectedNumRows(tablePath, 6);

    // Assert
    // Verify iceberg manifest/avro/metadata.json files on FS
    assertIcebergFilesExistAtSubPath(tablePath, 2, 3, 3, 2);

    // Cleanup
    runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void copyIntoInDiffBranchesAndConflicts() throws Exception {
    // Arrange
    final List<String> tablePath = Arrays.asList("if1", "if2", generateUniqueTableName());
    final String devBranchName = generateUniqueBranchName();

    // Set context to main
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(tablePath));
    assertTableHasExpectedNumRows(tablePath, 0);

    // Create a dev branch from main
    runSQL(createBranchAtBranchQuery(devBranchName, DEFAULT_BRANCH_NAME));

    // copy into table on main branch
    runSQL(copyIntoTableQuery(tablePath, storageLocation, fileNameCsv));
    assertTableHasExpectedNumRows(tablePath, 3);

    // switch to branch dev
    runSQL(useBranchQuery(devBranchName));

    // copy into table on dev branch so there will be conflicts
    runSQL(copyIntoTableQuery(tablePath, storageLocation, fileNameCsv));
    assertTableHasExpectedNumRows(tablePath, 3);

    // Act and Assert
    assertQueryThrowsExpectedError(mergeBranchQuery(devBranchName, DEFAULT_BRANCH_NAME),
      String.format(("VALIDATION ERROR: Merge branch %s into branch %s failed due to commit conflict on source %s"),
        devBranchName, DEFAULT_BRANCH_NAME, DATAPLANE_PLUGIN_NAME));

    // Cleanup
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    runSQL(dropTableQuery(tablePath));
  }


  @Test
  public void copyIntoIntoNonExistentBranchAndThrowsException() {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePathWithRoot = Arrays.asList(DATAPLANE_PLUGIN_NAME, tableName);
    final List<String> tablePath = Collections.singletonList(tableName);
    final NamespaceKey tablePathKey = new NamespaceKey(tablePathWithRoot);
    final String nonExistentBranch = generateUniqueBranchName();

    // Act and Assert
    assertQueryThrowsExpectedError(
      copyIntoTableQueryWithAt(tablePath, storageLocation, fileNameCsv, nonExistentBranch),
      String.format("Table [%s] does not exist.", tablePathKey));
  }

  @Test
  public void copyIntoWithValidTagAndThrowsException() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = Collections.singletonList(tableName);
    final String tagName = generateUniqueTagName();
    runSQL(createEmptyTableQuery(tablePath));
    runSQL(createTagQuery(tagName, DEFAULT_BRANCH_NAME));

    // Act and Assert
    String sql = String.format("COPY INTO %s.%s AT TAG %s FROM %s FILES(\'%s\') %s",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(tablePath),
      tagName,
      storageLocation,
      fileNameCsv,
      DEFAULT_RECORD_DELIMITER);
    assertQueryThrowsExpectedError(sql,
      String.format(
        "DDL and DML operations are only supported for branches - not on tags or commits. %s is not a branch.",
        tagName));
  }

  @Test
  public void copyInDiffBranchesAndMerge() throws Exception {
    // Arrange
    final List<String> shareFolderPath = generateFolderPath(generateUniqueFolderName());
    final String mainTableName = generateUniqueTableName();
    final String devTableName = generateUniqueTableName();
    final List<String> mainTablePath = tablePathWithFolders(mainTableName);
    final List<String> devTablePath = tablePathWithFolders(devTableName);
    final String devBranchName = generateUniqueBranchName();

    // Creating an arbitrary commit to Nessie to make a common ancestor between two branches otherwise
    // those are un-related branches
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    createFolders(shareFolderPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));

    // Create a dev branch from main
    runSQL(createBranchAtBranchQuery(devBranchName, DEFAULT_BRANCH_NAME));

    // Set context to main
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    createFolders(mainTablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(mainTablePath));
    assertTableHasExpectedNumRows(mainTablePath, 0);

    // Copy into table main
    runSQL(copyIntoTableQuery(mainTablePath, storageLocation, fileNameCsv));
    assertTableHasExpectedNumRows(mainTablePath, 3);

    // switch to branch dev
    runSQL(useBranchQuery(devBranchName));
    // Check that table does not exist in Nessie in branch dev (since it was branched off before create table)
    assertQueryThrowsExpectedError(selectCountQuery(mainTablePath, DEFAULT_COUNT_COLUMN),
      String.format("VALIDATION ERROR: Object '%s' not found within '%s",
        mainTablePath.get(0),
        DATAPLANE_PLUGIN_NAME));
    createFolders(devTablePath, VersionContext.ofBranch(devBranchName));
    runSQL(createEmptyTableQuery(devTablePath));
    assertTableHasExpectedNumRows(devTablePath, 0);

    // Copy into table dev
    runSQL(copyIntoTableQuery(devTablePath, storageLocation, fileNameCsv));
    assertTableHasExpectedNumRows(devTablePath, 3);

    // switch to branch main
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));

    // Check that dev table cannot be seen in branch main
    assertQueryThrowsExpectedError(selectCountQuery(devTablePath, DEFAULT_COUNT_COLUMN),
      String.format("VALIDATION ERROR: Object '%s' not found within '%s",
        devTablePath.get(0),
        DATAPLANE_PLUGIN_NAME));

    // Act
    runSQL(mergeBranchQuery(devBranchName, DEFAULT_BRANCH_NAME));

    // Assert and checking records in both tables
    // Table must now be visible in main.
    assertTableHasExpectedNumRows(devTablePath, 3);
    assertTableHasExpectedNumRows(mainTablePath, 3);

    // Cleanup
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    runSQL(dropTableQuery(mainTablePath));
    runSQL(dropTableQuery(devTablePath));
  }

  @Test
  public void copyInDiffBranchesAndMergeWithAt() throws Exception {
    // Arrange
    final List<String> shareFolderPath = Collections.singletonList(generateUniqueFolderName());
    final String mainTableName = generateUniqueTableName();
    final String devTableName = generateUniqueTableName();
    final List<String> mainTablePath = tablePathWithFolders(mainTableName);
    final List<String> devTablePath = tablePathWithFolders(devTableName);
    final String devBranchName = generateUniqueBranchName();

    // Creating an arbitrary commit to Nessie to make a common ancestor between two branches otherwise
    // those are un-related branches
    runSQL(createFolderAtQueryWithIfNotExists(shareFolderPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME)));

    // Create a dev branch from main
    runSQL(createBranchAtBranchQuery(devBranchName, DEFAULT_BRANCH_NAME));

    // Set context to main
    createFolders(mainTablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQueryWithAt(mainTablePath, DEFAULT_BRANCH_NAME));
    assertTableHasExpectedNumRows(mainTablePath, 0);

    // Copy into table main
    runSQL(copyIntoTableQuery(mainTablePath, storageLocation, fileNameCsv));
    assertTableHasExpectedNumRows(mainTablePath, 3);

    //create table in dev.
    createFolders(devTablePath, VersionContext.ofBranch(devBranchName));
    runSQL(createEmptyTableQueryWithAt(devTablePath, devBranchName));
    assertTableAtBranchHasExpectedNumRows(devTablePath, devBranchName, 0);

    // Copy into table dev
    runSQL(copyIntoTableQueryWithAt(devTablePath, storageLocation, fileNameCsv, devBranchName));
    assertTableAtBranchHasExpectedNumRows(devTablePath, devBranchName, 3);

    // Act
    runSQL(mergeBranchQuery(devBranchName, DEFAULT_BRANCH_NAME));

    // Assert and checking records in both tables
    // Table must now be visible in main.
    assertTableHasExpectedNumRows(devTablePath, 3);
    assertTableHasExpectedNumRows(mainTablePath, 3);

    // Cleanup
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    runSQL(dropTableQuery(mainTablePath));
    runSQL(dropTableQuery(devTablePath));
  }

  /**
   * Create in main branch
   * Insert in dev branch
   * Compare row counts in each branch
   * Merge branch to main branch and compare row count again
   */
  @Test
  public void copyIntoAndCreateInDifferentBranches() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final String devBranch = generateUniqueBranchName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    // Set context to main branch
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(tablePath));

    // Verify with select
    assertTableHasExpectedNumRows(tablePath, 0);

    // Create dev branch
    runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));

    // Switch to dev
    runSQL(useBranchQuery(devBranch));

    // Insert rows using copy into
    runSQL(copyIntoTableQuery(tablePath, storageLocation, fileNameCsv));

    // Verify number of rows.
    assertTableHasExpectedNumRows(tablePath, 3);

    // Switch back to main
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));

    // Verify number of rows
    assertTableHasExpectedNumRows(tablePath, 0);

    // Act
    // Merge dev to main
    runSQL(mergeBranchQuery(devBranch, DEFAULT_BRANCH_NAME));

    // Assert
    assertTableHasExpectedNumRows(tablePath, 3);

    // Cleanup
    runSQL(dropTableQuery(tablePath));
  }

  /**
   * The inserts should write data files relative to the table base location, and agnostic of the source configuration.
   * Create a table, insert some records using copy into
   * Create a different source with a dummy bucket path as root location
   * Make further inserts, operation should succeed
   * Verify the records
   */
  @Test
  public void copyIntoAgnosticOfSourceBucket() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(tablePath));

    // Act
    runSQL(copyIntoTableQuery(tablePath, storageLocation, fileNameCsv));
    runWithAlternateSourcePath(copyIntoTableQuery(tablePath, storageLocation, fileNameCsv));

    // Assert rows from both copy into commands
    assertTableHasExpectedNumRows(tablePath, 6);
    assertAllFilesAreInBaseBucket(tablePath);

    // cleanup
    runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void copyInDifferentTablesWithSameName() throws Exception {
    // Arrange
    final List<String> shareFolderPath = generateFolderPath(generateUniqueFolderName());
    final String tableName = generateUniqueTableName();
    final String devBranch = generateUniqueBranchName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    // Creating an arbitrary commit to Nessie to make a common ancestor between two branches otherwise
    // those are un-related branches
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    createFolders(shareFolderPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));

    runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));

    // Create table with this name in the main branch, insert records using copy into
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(tablePath));
    runSQL(copyIntoTableQuery(tablePath, storageLocation, fileNameCsv));

    // Create table with this name in the dev branch, different source path, insert records using copy into
    runSQL(useBranchQuery(devBranch));
    createFolders(tablePath, VersionContext.ofBranch(devBranch));
    runWithAlternateSourcePath(createEmptyTableQuery(tablePath));
    runSQL(copyIntoTableQuery(tablePath, storageLocation, fileNameCsv));

    // Act: Assert the paths are correct in each branch
    assertAllFilesInAlternativeBucket(tablePath); // dev branch
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    assertAllFilesAreInBaseBucket(tablePath);

    // cleanup
    runSQL(useBranchQuery(devBranch));
    runSQL(dropTableQuery(tablePath));
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    runSQL(dropTableQuery(tablePath));
    runSQL(dropBranchForceQuery(devBranch));
  }
}
