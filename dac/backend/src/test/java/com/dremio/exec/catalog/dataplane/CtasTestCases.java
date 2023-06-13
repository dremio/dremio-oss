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
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createTableAsQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createTagQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.dropTableQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueTableName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueTagName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.tablePathWithFolders;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.useTagQuery;
import static com.dremio.exec.catalog.dataplane.ITDataplanePluginTestSetup.createFolders;
import static com.dremio.exec.catalog.dataplane.TestDataplaneAssertions.assertIcebergFilesExistAtSubPath;
import static com.dremio.exec.catalog.dataplane.TestDataplaneAssertions.assertIcebergTableExistsAtSubPath;
import static com.dremio.exec.catalog.dataplane.TestDataplaneAssertions.assertNessieHasCommitForTable;
import static com.dremio.exec.catalog.dataplane.TestDataplaneAssertions.assertNessieHasTable;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.projectnessie.model.Operation;

import com.dremio.exec.catalog.VersionContext;

/**
 *
 * To run these tests, run through container class ITDataplanePlugin
 * To run all tests run {@link ITDataplanePlugin.NestedCtasTests}
 * To run single test, see instructions at the top of {@link ITDataplanePlugin}
 */
public class CtasTestCases {
  private ITDataplanePluginTestSetup base;

  CtasTestCases(ITDataplanePluginTestSetup base) {
    this.base = base;
  }
  @Test
  public void ctas() throws Exception {
    // Arrange
    String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    // Act
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createTableAsQuery(tablePath, 5));

    // Assert
    assertNessieHasCommitForTable(tablePath, Operation.Put.class, DEFAULT_BRANCH_NAME, base);
    assertNessieHasTable(tablePath, DEFAULT_BRANCH_NAME, base);
    assertIcebergTableExistsAtSubPath(tablePath);
    // Verify with select also
    base.assertTableHasExpectedNumRows(tablePath, 5);

    // Cleanup
    base.runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void ctasWithTagSet() throws Exception {
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


  // Verify ctas creates underlying iceberg files in the right locations
  @Test
  void ctasTestVerifyFolders() throws Exception {
    // Arrange
    // Create a hierarchy of 2 folders to form key for TABLE1
    final String table1 = generateUniqueTableName();
    final List<String> table1Path = Arrays.asList("f1", "f2", table1);

    // Create a hierarchy of 3 folders to form key for TABLE2
    final String table2 = generateUniqueTableName();
    final List<String> table2Path = Arrays.asList("f1", "f2", "f3", table2);

    // Act 1
    createFolders(table1Path, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createTableAsQuery(table1Path, 10));
    base.assertTableHasExpectedNumRows(table1Path, 10);

    // Assert1
    // Verify iceberg manifest/avro/metadata.json files on FS
    assertIcebergFilesExistAtSubPath(table1Path, 1, 1, 1, 1);

    // Act2
    createFolders(table2Path, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createTableAsQuery(table2Path, 10));
    base.assertTableHasExpectedNumRows(table2Path, 10);

    // Assert2
    // Verify iceberg manifest/avro/metadata.json files on FS
    assertIcebergFilesExistAtSubPath(table2Path, 1, 1, 1, 1);
    // Verify that table1's files are isolated from creation of table2
    assertIcebergFilesExistAtSubPath(table1Path, 1, 1, 1, 1);

    // Cleanup
    base.runSQL(dropTableQuery(table1Path));
    base.runSQL(dropTableQuery(table2Path));
  }

  @Test
  public void ctasWithImplicitFolders() throws Exception {
    // Arrange
    String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    // Act + Assert
    base.assertQueryThrowsExpectedError(createTableAsQuery(tablePath, 5),
      String.format("VALIDATION ERROR: Namespace '%s' must exist.",
        String.join(".", tablePath.subList(0, tablePath.size()-1))));
  }
}
