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
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.copyIntoTableQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createBranchAtBranchQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createEmptyTableQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createTableWithColDefsQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.dropBranchQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.dropTableQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateFolderPath;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateSourceFiles;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueBranchName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueFolderName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueTableName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.insertTableWithValuesQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.mergeBranchQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.selectCountQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.tablePathWithFolders;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.useBranchQuery;
import static com.dremio.exec.catalog.dataplane.ITDataplanePluginTestSetup.createFolders;
import static com.dremio.exec.catalog.dataplane.TestDataplaneAssertions.assertIcebergFilesExistAtSubPath;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.dremio.exec.catalog.VersionContext;

/**
 *
 * To run these tests, run through container class OSSITDataplanePlugin
 * To run all tests run {@link com.dremio.exec.catalog.dataplane.ITDataplanePlugin.NestedCopyIntoTests}
 * To run single test, see instructions at the top of {@link com.dremio.exec.catalog.dataplane.ITDataplanePlugin}
 */
public class CopyIntoTestCases {
  private ITDataplanePluginTestSetup base;
  private static File location;
  private static String source;
  private static String fileNameCsv =  "file1.csv";
  private static String fileNameJson =  "file1.json";
  private static File newSourceFileCsv;
  private static File newSourceFileJson;
  private static String storageLocation ;

  CopyIntoTestCases(ITDataplanePluginTestSetup base, File location, String source) {
    this.base = base;
    this.location = location;
    this.source = source;
    this.storageLocation = "\'@" + source + "/" +  location.getName()  + "\'";
  }

  @BeforeEach
  public void createSourceFiles() throws Exception{
    newSourceFileCsv = generateSourceFiles(fileNameCsv , location);
    newSourceFileJson = generateSourceFiles(fileNameJson , location);
  }

  @AfterEach
  public void cleanupSourceFiles() {
    Assert.assertTrue(newSourceFileCsv.delete());
    Assert.assertTrue(newSourceFileJson.delete());
  }

  @Test
  public void copyIntoEmptyTable() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));

    // Act
    base.runSQL(copyIntoTableQuery(tablePath, storageLocation, fileNameCsv));

    // Assert
    base.assertTableHasExpectedNumRows(tablePath, 3);

    // Act
    base.runSQL(copyIntoTableQuery(tablePath, storageLocation, fileNameJson));

    // Assert
    base.assertTableHasExpectedNumRows(tablePath, 6);

    // cleanup
    base.runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void copyIntoEmptyTableWithNoMatchingRows() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createTableWithColDefsQuery(tablePath, Arrays.asList("c1 int", "c2 int", "c3 int")));

    // Act and assert error is thrown
    base.assertQueryThrowsExpectedError(copyIntoTableQuery(tablePath, storageLocation, fileNameCsv),String.format("No column name matches target schema"));

    // cleanup
    base.runSQL(dropTableQuery(tablePath));
  }


  @Test
  public void copyIntoTableWithRows() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));

    //Insert into table
    base.runSQL(insertTableWithValuesQuery(tablePath, Arrays.asList("(4,'str1',34.45)","(5,'str1',34.45)","(6,'str1',34.45)")));

    //Assert
    base.assertTableHasExpectedNumRows(tablePath, 3);

    // Act
    base.runSQL(copyIntoTableQuery(tablePath, storageLocation, fileNameCsv));

    // Assert
    base.assertTableHasExpectedNumRows(tablePath, 6);

    // cleanup
    base.runSQL(dropTableQuery(tablePath));
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
    base.runSQL(createEmptyTableQuery(tablePath));
    // Verify iceberg manifest/avro/metadata.json files on FS
    assertIcebergFilesExistAtSubPath(tablePath, 0, 1, 1, 0);

    // Do 2 separate Inserts so there are multiple data files.
    // Copy 1
    base.runSQL(copyIntoTableQuery(tablePath, storageLocation, fileNameCsv));
    base.assertTableHasExpectedNumRows(tablePath, 3);
    // Verify iceberg manifest/avro/metadata.json files on FS
    assertIcebergFilesExistAtSubPath(tablePath, 1, 2, 2, 1);

    // Copy 2
    base.runSQL(copyIntoTableQuery(tablePath, storageLocation, fileNameCsv));
    // Verify number of rows with select
    base.assertTableHasExpectedNumRows(tablePath, 6);

    // Assert
    // Verify iceberg manifest/avro/metadata.json files on FS
    assertIcebergFilesExistAtSubPath(tablePath, 2, 3, 3, 2);

    // Cleanup
    base.runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void copyIntoInDiffBranchesAndConflicts() throws Exception {
    // Arrange
    final List<String> tablePath = Arrays.asList("if1", "if2", generateUniqueTableName());
    final String devBranchName = generateUniqueBranchName();

    // Set context to main
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));
    base.assertTableHasExpectedNumRows(tablePath, 0);

    // Create a dev branch from main
    base.runSQL(createBranchAtBranchQuery(devBranchName, DEFAULT_BRANCH_NAME));

    // copy into table on main branch
    base.runSQL(copyIntoTableQuery(tablePath, storageLocation, fileNameCsv));
    base.assertTableHasExpectedNumRows(tablePath, 3);

    // switch to branch dev
    base.runSQL(useBranchQuery(devBranchName));

    // copy into table on dev branch so there will be conflicts
    base.runSQL(copyIntoTableQuery(tablePath, storageLocation, fileNameCsv));
    base.assertTableHasExpectedNumRows(tablePath, 3);

    // Act and Assert
    base.assertQueryThrowsExpectedError(mergeBranchQuery(devBranchName, DEFAULT_BRANCH_NAME),
      String.format(("VALIDATION ERROR: Merge branch %s into branch %s failed due to commit conflict on source %s"),
        devBranchName, DEFAULT_BRANCH_NAME, DATAPLANE_PLUGIN_NAME));

    // Cleanup
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    base.runSQL(dropTableQuery(tablePath));
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
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    createFolders(shareFolderPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));

    // Create a dev branch from main
    base.runSQL(createBranchAtBranchQuery(devBranchName, DEFAULT_BRANCH_NAME));

    // Set context to main
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    createFolders(mainTablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(mainTablePath));
    base.assertTableHasExpectedNumRows(mainTablePath, 0);

    // Copy into table main
    base.runSQL(copyIntoTableQuery(mainTablePath, storageLocation, fileNameCsv));
    base.assertTableHasExpectedNumRows(mainTablePath, 3);

    // switch to branch dev
    base.runSQL(useBranchQuery(devBranchName));
    // Check that table does not exist in Nessie in branch dev (since it was branched off before create table)
    base.assertQueryThrowsExpectedError(selectCountQuery(mainTablePath, DEFAULT_COUNT_COLUMN),
      String.format("VALIDATION ERROR: Object '%s' not found within '%s",
        mainTablePath.get(0),
        DATAPLANE_PLUGIN_NAME));
    createFolders(devTablePath, VersionContext.ofBranch(devBranchName));
    base.runSQL(createEmptyTableQuery(devTablePath));
    base.assertTableHasExpectedNumRows(devTablePath, 0);

    // Copy into table dev
    base.runSQL(copyIntoTableQuery(devTablePath, storageLocation, fileNameCsv));
    base.assertTableHasExpectedNumRows(devTablePath, 3);

    // switch to branch main
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));

    // Check that dev table cannot be seen in branch main
    base.assertQueryThrowsExpectedError(selectCountQuery(devTablePath, DEFAULT_COUNT_COLUMN),
      String.format("VALIDATION ERROR: Object '%s' not found within '%s",
        devTablePath.get(0),
        DATAPLANE_PLUGIN_NAME));

    // Act
    base.runSQL(mergeBranchQuery(devBranchName, DEFAULT_BRANCH_NAME));

    // Assert and checking records in both tables
    // Table must now be visible in main.
    base.assertTableHasExpectedNumRows(devTablePath, 3);
    base.assertTableHasExpectedNumRows(mainTablePath, 3);

    // Cleanup
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    base.runSQL(dropTableQuery(mainTablePath));
    base.runSQL(dropTableQuery(devTablePath));
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
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));

    // Verify with select
    base.assertTableHasExpectedNumRows(tablePath, 0);

    // Create dev branch
    base.runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));

    // Switch to dev
    base.runSQL(useBranchQuery(devBranch));

    // Insert rows using copy into
    base.runSQL(copyIntoTableQuery(tablePath, storageLocation, fileNameCsv));

    // Verify number of rows.
    base.assertTableHasExpectedNumRows(tablePath, 3);

    // Switch back to main
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));

    // Verify number of rows
    base.assertTableHasExpectedNumRows(tablePath, 0);

    // Act
    // Merge dev to main
    base.runSQL(mergeBranchQuery(devBranch, DEFAULT_BRANCH_NAME));

    // Assert
    base.assertTableHasExpectedNumRows(tablePath, 3);

    // Cleanup
    base.runSQL(dropTableQuery(tablePath));
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
    base.runSQL(createEmptyTableQuery(tablePath));

    // Act
    base.runSQL(copyIntoTableQuery(tablePath, storageLocation, fileNameCsv));
    base.runWithAlternateSourcePath(copyIntoTableQuery(tablePath, storageLocation, fileNameCsv));

    // Assert rows from both copy into commands
    base.assertTableHasExpectedNumRows(tablePath, 6);
    base.assertAllFilesAreInBaseBucket(tablePath);

    // cleanup
    base.runSQL(dropTableQuery(tablePath));
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
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    createFolders(shareFolderPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));

    base.runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));

    // Create table with this name in the main branch, insert records using copy into
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));
    base.runSQL(copyIntoTableQuery(tablePath, storageLocation, fileNameCsv));

    // Create table with this name in the dev branch, different source path, insert records using copy into
    base.runSQL(useBranchQuery(devBranch));
    createFolders(tablePath, VersionContext.ofBranch(devBranch));
    base.runWithAlternateSourcePath(createEmptyTableQuery(tablePath));
    base.runSQL(copyIntoTableQuery(tablePath, storageLocation, fileNameCsv));

    // Act: Assert the paths are correct in each branch
    base.assertAllFilesInAlternativeBucket(tablePath); // dev branch
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    base.assertAllFilesAreInBaseBucket(tablePath);

    // cleanup
    base.runSQL(useBranchQuery(devBranch));
    base.runSQL(dropTableQuery(tablePath));
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    base.runSQL(dropTableQuery(tablePath));
    base.runSQL(dropBranchQuery(devBranch));
  }
}
