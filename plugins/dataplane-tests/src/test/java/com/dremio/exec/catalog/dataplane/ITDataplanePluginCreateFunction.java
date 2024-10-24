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
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createBranchFromBranchQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createComplexUdfQueryWithAt;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createEmptyTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createTableQueryWithAt;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createTabularUdfQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createTabularUdfQueryNonQualifiedTableName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createUdfQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createUdfQueryWithAt;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createUdfQueryWithAtWithinFrom;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createViewQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueBranchName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueFunctionName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueTableName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueViewName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.insertTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.joinedTableKey;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.tablePathWithFolders;
import static com.dremio.exec.catalog.dataplane.test.TestDataplaneAssertions.assertNessieDoesNotHaveEntity;
import static com.dremio.exec.catalog.dataplane.test.TestDataplaneAssertions.assertNessieHasFunction;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.Test;

public class ITDataplanePluginCreateFunction extends ITDataplanePluginFunctionBase {

  @Test
  public void createUdfInDefault() throws Exception {
    // Act / Assert
    final List<String> functionKey = generateFunctionKeyWithFunctionInFolder();
    runSQL(createUdfQueryWithAt(functionKey, DEFAULT_BRANCH_NAME));
    assertNessieHasFunction(functionKey, DEFAULT_BRANCH_NAME, this);
  }

  @Test
  public void createUdfInDefaultExistsInDefaultFails() throws Exception {
    // Set-Up
    final List<String> functionKey = generateFunctionKeyWithFunctionInFolder();
    runSQL(createUdfQueryWithAt(functionKey, DEFAULT_BRANCH_NAME));
    assertNessieHasFunction(functionKey, DEFAULT_BRANCH_NAME, this);

    // Act / Assert
    assertThatThrownBy(() -> runSQL(createUdfQueryWithAt(functionKey, DEFAULT_BRANCH_NAME)))
        .hasMessageContaining("was not created as a function with the same name already exists");
  }

  @Test
  public void createUdfInDefaultExistsInOther() throws Exception {
    // Set-Up
    final String otherBranch = generateUniqueBranchName();
    runSQL(createBranchFromBranchQuery(otherBranch, DEFAULT_BRANCH_NAME));
    final List<String> functionKey = generateFunctionKeyWithFunctionInFolder();
    runSQL(createUdfQueryWithAt(functionKey, otherBranch));
    assertNessieHasFunction(functionKey, otherBranch, this);

    // Act / Assert
    runSQL(createUdfQueryWithAt(functionKey, DEFAULT_BRANCH_NAME));
    assertNessieHasFunction(functionKey, DEFAULT_BRANCH_NAME, this);
  }

  @Test
  public void createUdfInOther() throws Exception {
    // Set-Up
    final String otherBranch = generateUniqueBranchName();
    runSQL(createBranchFromBranchQuery(otherBranch, DEFAULT_BRANCH_NAME));

    // Act / Assert
    final List<String> functionKey = generateFunctionKeyWithFunctionInFolder();
    runSQL(createUdfQueryWithAt(functionKey, otherBranch));
    assertNessieHasFunction(functionKey, otherBranch, this);
    assertNessieDoesNotHaveEntity(functionKey, DEFAULT_BRANCH_NAME, this);
  }

  @Test
  public void createUdfInOtherExistsInOtherFails() throws Exception {
    // Set-Up
    final String otherBranch = generateUniqueBranchName();
    runSQL(createBranchFromBranchQuery(otherBranch, DEFAULT_BRANCH_NAME));
    final List<String> functionKey = generateFunctionKeyWithFunctionInFolder();
    runSQL(createUdfQueryWithAt(functionKey, otherBranch));
    assertNessieHasFunction(functionKey, otherBranch, this);

    // Act / Assert
    assertThatThrownBy(() -> runSQL(createUdfQueryWithAt(functionKey, otherBranch)))
        .hasMessageContaining("was not created as a function with the same name already exists");
  }

  @Test
  public void createUdfInOtherExistsInDefault() throws Exception {
    // Set-Up
    final String otherBranch = generateUniqueBranchName();
    runSQL(createBranchFromBranchQuery(otherBranch, DEFAULT_BRANCH_NAME));
    final List<String> functionKey = generateFunctionKeyWithFunctionInFolder();
    runSQL(createUdfQueryWithAt(functionKey, DEFAULT_BRANCH_NAME));
    assertNessieHasFunction(functionKey, DEFAULT_BRANCH_NAME, this);

    // Act / Assert
    runSQL(createUdfQueryWithAt(functionKey, otherBranch));
    assertNessieHasFunction(functionKey, otherBranch, this);
  }

  @Test
  public void createOrReplaceUdfIfNotExistsFails() {
    // Act / Assert
    final List<String> functionKey = generateFunctionKeyWithFunctionInFolder();
    assertThatThrownBy(
            () ->
                runSQL(
                    String.format(
                        "CREATE OR REPLACE FUNCTION IF NOT EXISTS %s.%s () RETURNS BOOLEAN RETURN FALSE",
                        DATAPLANE_PLUGIN_NAME, joinedTableKey(functionKey))))
        .hasMessageContaining("'OR REPLACE' and 'IF NOT EXISTS' can not both be set");
  }

  @Test
  public void createTabularUdf() throws Exception {
    // Set-Up
    final List<String> tableKey = tablePathWithFolders(generateUniqueTableName());
    runSQL(createEmptyTableQuery(tableKey));

    // Act / Assert
    List<String> functionKey = generateFunctionKeyWithFunctionInFolder();
    runSQL(createTabularUdfQuery(functionKey, tableKey));
    assertNessieHasFunction(functionKey, DEFAULT_BRANCH_NAME, this);
  }

  @Test
  public void createUdfClashWithTableFails() throws Exception {
    // Set-Up
    final List<String> tableKey = tablePathWithFolders(generateUniqueTableName());
    runSQL(createEmptyTableQuery(tableKey));

    // Act / Assert
    assertThatThrownBy(() -> runSQL(createUdfQuery(tableKey)))
        .hasMessageContaining("An Entity of type ICEBERG_TABLE with given name ")
        .hasMessageContaining("already exists");
  }

  @Test
  public void createUdfClashWithViewFails() throws Exception {
    // Set-Up
    final List<String> tableKey = tablePathWithFolders(generateUniqueTableName());
    runSQL(createEmptyTableQuery(tableKey));
    final List<String> viewKey = tablePathWithFolders(generateUniqueViewName());
    runSQL(createViewQuery(viewKey, tableKey));

    // Act / Assert
    assertThatThrownBy(() -> runSQL(createUdfQuery(viewKey)))
        .hasMessageContaining("An Entity of type ICEBERG_VIEW with given name ")
        .hasMessageContaining("already exists");
  }

  @Test
  public void createUdfClashWithFolderFails() throws Exception {
    // Set-Up
    final List<String> tableKey = tablePathWithFolders(generateUniqueTableName());
    runSQL(createEmptyTableQuery(tableKey));

    // Act / Assert
    assertThatThrownBy(() -> runSQL(createUdfQuery(tableKey.subList(0, tableKey.size() - 1))))
        .hasMessageContaining("An Entity of type FOLDER with given name ")
        .hasMessageContaining("already exists");
  }

  @Test
  public void createViewClashWithUdfFails() throws Exception {
    // Set-Up
    final List<String> functionKey = tablePathWithFolders(generateUniqueFunctionName());
    runSQL(createUdfQuery(functionKey));
    final List<String> tableKey = tablePathWithFolders(generateUniqueTableName());
    runSQL(createEmptyTableQuery(tableKey));

    // Act / Assert
    assertThatThrownBy(() -> runSQL(createViewQuery(functionKey, tableKey)))
        .hasMessageContaining("An Entity of type UDF with given name ")
        .hasMessageContaining("already exists");
  }

  @Test
  public void createUdfWithSQLErrorFails() throws Exception {
    // Act / Assert
    final List<String> functionKey = tablePathWithFolders(generateUniqueFunctionName());
    assertThatThrownBy(
            () ->
                runSQL(
                    String.format(
                        "CREATE FUNCTION %s.%s (x INT, y INT) RETURNS INT RETURN SELECT a FROM xyz",
                        DATAPLANE_PLUGIN_NAME, joinedTableKey(functionKey))))
        .hasMessageContaining("SYSTEM ERROR: SqlValidatorException");
    assertNessieDoesNotHaveEntity(functionKey, DEFAULT_BRANCH_NAME, this);
  }

  @Test
  public void createTabularUdfWithSelectFromNonQualifiedInnerTableFails() throws Exception {
    // Set-Up
    final String tableName = generateUniqueTableName();
    final List<String> tableKey = tablePathWithFolders(tableName);
    runSQL(createEmptyTableQuery(tableKey));
    runSQL(insertTableQuery(tableKey));

    // Act / assert
    List<String> functionKey = generateFunctionKeyWithFunctionInFolder();
    assertThatThrownBy(
            () ->
                runSQL(
                    createTabularUdfQueryNonQualifiedTableName(
                        DATAPLANE_PLUGIN_NAME, functionKey, tableName)))
        .hasMessageContaining("Object '%s' not found", tableName);
  }

  @Test
  public void createUdfWithATFromTableInOtherBranch() throws Exception {
    // Set-Up
    final String otherBranch = generateUniqueBranchName();
    runSQL(createBranchAtBranchQuery(otherBranch, DEFAULT_BRANCH_NAME));
    final List<String> tablePath = tablePathWithFolders(generateUniqueTableName());
    runSQL(createTableQueryWithAt(tablePath, otherBranch));

    // Act / Assert
    List<String> functionKey = generateFunctionKeyWithFunctionInFolder();
    runSQL(createUdfQueryWithAtWithinFrom(functionKey, tablePath, otherBranch));
    assertNessieHasFunction(functionKey, DEFAULT_BRANCH_NAME, this);
  }

  @Test
  public void createComplexUdfInDefault() throws Exception {
    // Act / Assert
    final List<String> functionKey = generateFunctionKeyWithFunctionInFolder();
    runSQL(createComplexUdfQueryWithAt(functionKey, DEFAULT_BRANCH_NAME));
    assertNessieHasFunction(functionKey, DEFAULT_BRANCH_NAME, this);
  }
}
