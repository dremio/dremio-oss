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

import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.DEFAULT_BRANCH_NAME;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createBranchAtBranchQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createTableAsQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createTableAsQueryWithAt;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createTagQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.dropTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueBranchName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueTableName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueTagName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.tablePathWithFolders;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.useTagQuery;
import static com.dremio.exec.catalog.dataplane.test.TestDataplaneAssertions.assertIcebergFilesExistAtSubPath;
import static com.dremio.exec.catalog.dataplane.test.TestDataplaneAssertions.assertIcebergTableExistsAtSubPath;
import static com.dremio.exec.catalog.dataplane.test.TestDataplaneAssertions.assertNessieHasCommitForTable;
import static com.dremio.exec.catalog.dataplane.test.TestDataplaneAssertions.assertNessieHasTable;

import com.dremio.exec.catalog.dataplane.test.ITDataplanePluginTestSetup;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.projectnessie.model.Operation;

public class ITDataplanePluginCtas extends ITDataplanePluginTestSetup {

  @Test
  public void ctas() throws Exception {
    // Arrange
    String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    // Act
    runSQL(createTableAsQuery(tablePath, 5));

    // Assert
    assertNessieHasCommitForTable(tablePath, Operation.Put.class, DEFAULT_BRANCH_NAME, this);
    assertNessieHasTable(tablePath, DEFAULT_BRANCH_NAME, this);
    assertIcebergTableExistsAtSubPath(tablePath);
    // Verify with select also
    assertTableHasExpectedNumRows(tablePath, 5);

    // Cleanup
    runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void ctasInMainWithAtSyntax() throws Exception {
    // Arrange
    String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    // Act
    runSQL(createTableAsQueryWithAt(tablePath, 5, DEFAULT_BRANCH_NAME));

    // Assert
    assertNessieHasCommitForTable(tablePath, Operation.Put.class, DEFAULT_BRANCH_NAME, this);
    assertNessieHasTable(tablePath, DEFAULT_BRANCH_NAME, this);
    assertIcebergTableExistsAtSubPath(tablePath);
    // Verify with select also
    assertTableHasExpectedNumRows(tablePath, 5);

    // Cleanup
    runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void ctasInDevWithAtSyntax() throws Exception {
    // Arrange
    String tableName = generateUniqueTableName();
    final List<String> tablePath = Collections.singletonList(tableName);
    final String devBranch = generateUniqueBranchName();

    // Act
    runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));
    runSQL(createTableAsQueryWithAt(tablePath, 5, devBranch));

    // Assert
    assertNessieHasCommitForTable(tablePath, Operation.Put.class, devBranch, this);
    assertNessieHasTable(tablePath, devBranch, this);
    assertIcebergTableExistsAtSubPath(tablePath);
  }

  @Test
  public void ctasWithTagSet() throws Exception {
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
    runSQL(createTableAsQuery(table1Path, 10));
    assertTableHasExpectedNumRows(table1Path, 10);

    // Assert1
    // Verify iceberg manifest/avro/metadata.json files on FS
    assertIcebergFilesExistAtSubPath(table1Path, 1, 1, 1, 1);

    // Act2
    runSQL(createTableAsQuery(table2Path, 10));
    assertTableHasExpectedNumRows(table2Path, 10);

    // Assert2
    // Verify iceberg manifest/avro/metadata.json files on FS
    assertIcebergFilesExistAtSubPath(table2Path, 1, 1, 1, 1);
    // Verify that table1's files are isolated from creation of table2
    assertIcebergFilesExistAtSubPath(table1Path, 1, 1, 1, 1);

    // Cleanup
    runSQL(dropTableQuery(table1Path));
    runSQL(dropTableQuery(table2Path));
  }

  @Test
  public void ctasWithImplicitFolders() throws Exception {
    // Arrange
    String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    // Act
    runSQL(createTableAsQuery(tablePath, 5));

    // Assert
    assertNessieHasTable(tablePath, DEFAULT_BRANCH_NAME, this);
  }
}
