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
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.DEFAULT_COLUMN_DEFINITION;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createBranchAtBranchQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createBranchFromBranchQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createComplexUdfQueryWithAt;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createEmptyTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createTabularUdfAtBranchQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createTabularUdfQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createTagQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createUdfQueryWithAt;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueBranchName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueTableName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.insertTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.joinedTableKey;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.selectTabularUdfQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.selectUdfAtBranchQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.selectUdfQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.tablePathWithFolders;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.useBranchQuery;
import static com.dremio.exec.catalog.dataplane.test.TestDataplaneAssertions.assertNessieDoesNotHaveEntity;
import static com.dremio.exec.catalog.dataplane.test.TestDataplaneAssertions.assertNessieHasFunction;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines;
import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

public class ITDataplanePluginSelectFunction extends ITDataplanePluginFunctionBase {

  @Test
  public void selectUdfInDefaultFails() throws Exception {
    // Set-Up
    final List<String> functionKey = generateFunctionKeyWithFunctionInFolder();
    assertNessieDoesNotHaveEntity(functionKey, DEFAULT_BRANCH_NAME, this);

    // Act / Assert
    assertThatThrownBy(() -> runSQL(selectUdfAtBranchQuery(functionKey, DEFAULT_BRANCH_NAME, 2, 3)))
        .hasMessageContaining(
            "No match found for function signature %s", functionKey.get(functionKey.size() - 1));
  }

  @Test
  public void selectUdfInDefaultExistsInDefault() throws Exception {
    // Set-Up
    final List<String> functionKey = generateFunctionKeyWithFunctionInFolder();
    runSQL(createUdfQueryWithAt(functionKey, DEFAULT_BRANCH_NAME));
    assertNessieHasFunction(functionKey, DEFAULT_BRANCH_NAME, this);

    // Act / Assert
    assertUdfResult(selectUdfAtBranchQuery(functionKey, DEFAULT_BRANCH_NAME, 2, 3), 6);
  }

  @Test
  public void selectUdfInDefaultExistsInOtherFails() throws Exception {
    // Set-Up
    final String otherBranch = generateUniqueBranchName();
    runSQL(createBranchFromBranchQuery(otherBranch, DEFAULT_BRANCH_NAME));
    final List<String> functionKey = generateFunctionKeyWithFunctionInFolder();
    runSQL(createUdfQueryWithAt(functionKey, otherBranch));
    assertNessieHasFunction(functionKey, otherBranch, this);

    // Act / Assert
    assertThatThrownBy(() -> runSQL(selectUdfAtBranchQuery(functionKey, DEFAULT_BRANCH_NAME, 2, 3)))
        .hasMessageContaining(
            "No match found for function signature %s", functionKey.get(functionKey.size() - 1));
  }

  @Test
  public void selectUdfInOtherFails() throws Exception {
    // Set-Up
    final String otherBranch = generateUniqueBranchName();
    runSQL(createBranchFromBranchQuery(otherBranch, DEFAULT_BRANCH_NAME));
    final List<String> functionKey = generateFunctionKeyWithFunctionInFolder();
    assertNessieDoesNotHaveEntity(functionKey, otherBranch, this);

    // Act / Assert
    assertThatThrownBy(() -> runSQL(selectUdfAtBranchQuery(functionKey, otherBranch, 2, 3)))
        .hasMessageContaining(
            "No match found for function signature %s", functionKey.get(functionKey.size() - 1));
  }

  @Test
  public void selectUdfInOtherExistsInOther() throws Exception {
    // Set-Up
    final String otherBranch = generateUniqueBranchName();
    runSQL(createBranchFromBranchQuery(otherBranch, DEFAULT_BRANCH_NAME));
    final List<String> functionKey = generateFunctionKeyWithFunctionInFolder();
    runSQL(createUdfQueryWithAt(functionKey, otherBranch));
    assertNessieHasFunction(functionKey, otherBranch, this);

    // Act / Assert
    assertUdfResult(selectUdfAtBranchQuery(functionKey, otherBranch, 2, 3), 6);
  }

  @Test
  public void selectUdfInOtherExistsInDefaultFails() throws Exception {
    // Set-Up
    final String otherBranch = generateUniqueBranchName();
    runSQL(createBranchFromBranchQuery(otherBranch, DEFAULT_BRANCH_NAME));
    final List<String> functionKey = generateFunctionKeyWithFunctionInFolder();
    runSQL(createUdfQueryWithAt(functionKey, DEFAULT_BRANCH_NAME));
    assertNessieHasFunction(functionKey, DEFAULT_BRANCH_NAME, this);

    // Act / Assert
    assertThatThrownBy(() -> runSQL(selectUdfAtBranchQuery(functionKey, otherBranch, 2, 3)))
        .hasMessageContaining(
            "No match found for function signature %s", functionKey.get(functionKey.size() - 1));
  }

  @Test
  public void selectUdfAtBranch() throws Exception {
    String otherBranch = generateUniqueBranchName();
    runSQL(createBranchAtBranchQuery(otherBranch, DEFAULT_BRANCH_NAME));

    List<String> functionKey = generateFunctionKeyWithFunctionInFolder();

    // Act
    // Create udf in otherBranch
    runSQL(createUdfQueryWithAt(functionKey, otherBranch));
    assertNessieHasFunction(functionKey, otherBranch, this);

    // Create udf with same name in testBranch
    runSQL(
        String.format(
            "CREATE FUNCTION %s.%s (x INT, y INT) AT BRANCH %s RETURNS INT RETURN SELECT x + y",
            DATAPLANE_PLUGIN_NAME, joinedTableKey(functionKey), DEFAULT_BRANCH_NAME));
    assertNessieHasFunction(functionKey, DEFAULT_BRANCH_NAME, this);

    // Assert
    // Verify calling the same udf from different branch returns different result
    assertUdfResult(selectUdfAtBranchQuery(functionKey, otherBranch, 2, 3), 6);
    assertUdfResult(selectUdfAtBranchQuery(functionKey, DEFAULT_BRANCH_NAME, 2, 3), 5);
  }

  @Test
  public void selectUdfAtDifferentBranches() throws Exception {
    String otherBranch = generateUniqueBranchName();
    runSQL(createBranchAtBranchQuery(otherBranch, DEFAULT_BRANCH_NAME));

    List<String> functionKey = generateFunctionKeyWithFunctionInFolder();

    // Act
    // Create udf in otherBranch
    runSQL(createUdfQueryWithAt(functionKey, otherBranch));
    assertNessieHasFunction(functionKey, otherBranch, this);

    // Create udf with same name in the default branch
    runSQL(
        String.format(
            "CREATE FUNCTION %s.%s (x INT, y INT) AT BRANCH %s RETURNS INT RETURN SELECT x + y",
            DATAPLANE_PLUGIN_NAME, joinedTableKey(functionKey), DEFAULT_BRANCH_NAME));
    assertNessieHasFunction(functionKey, DEFAULT_BRANCH_NAME, this);

    // Assert
    // Verify calling the same udf from different branches in the same SQL query returns different
    // results
    assertThat(
            runSqlWithResults(
                    String.format(
                        "SELECT %s.%s(10, 20) AT BRANCH %s, %s.%s (10, 20) AT BRANCH %s",
                        DATAPLANE_PLUGIN_NAME,
                        joinedTableKey(functionKey),
                        otherBranch,
                        DATAPLANE_PLUGIN_NAME,
                        joinedTableKey(functionKey),
                        DEFAULT_BRANCH_NAME))
                .stream()
                .allMatch(
                    row ->
                        (row.size() == 2)
                            && row.get(0).contains("200")
                            && row.get(1).contains("30")))
        .isTrue();
  }

  @Test
  public void selectUdfSelectInAtTag() throws Exception {
    String otherBranch = generateUniqueBranchName();
    runSQL(createBranchAtBranchQuery(otherBranch, DEFAULT_BRANCH_NAME));

    List<String> functionKey = generateFunctionKeyWithFunctionInFolder();

    // Act
    // Create udf in otherBranch
    runSQL(createUdfQueryWithAt(functionKey, otherBranch));
    assertNessieHasFunction(functionKey, otherBranch, this);
    runSQL(createTagQuery("tag1", otherBranch));

    // Update udf
    runSQL(
        String.format(
            "CREATE OR REPLACE FUNCTION %s.%s (x INT, y INT) AT BRANCH %s RETURNS INT RETURN SELECT x + y",
            DATAPLANE_PLUGIN_NAME, joinedTableKey(functionKey), otherBranch));
    runSQL(createTagQuery("tag2", otherBranch));

    // Assert
    // Verify calling the same udf from different branch returns different result
    assertUdfResult(DataplaneTestDefines.selectUdfAtTagQuery(functionKey, "tag1", 2, 3), 6);
    assertUdfResult(DataplaneTestDefines.selectUdfAtTagQuery(functionKey, "tag2", 2, 3), 5);
  }

  @Test
  public void selectUdfInWhereClause() throws Exception {
    List<String> functionKey = generateFunctionKeyWithFunctionInFolder();
    final List<String> tableKey = tablePathWithFolders(generateUniqueTableName());

    // Act
    String create100TimesUdf =
        String.format(
            "CREATE FUNCTION %s.%s (x INT) RETURNS INT RETURN SELECT x * 100",
            DATAPLANE_PLUGIN_NAME, joinedTableKey(functionKey));
    runSQL(create100TimesUdf);
    runSQL(createEmptyTableQuery(tableKey));
    runSQL(insertTableQuery(tableKey));
    // Act
    String testUdfQuery =
        String.format(
            "SELECT id from %s.%s where %s.%s(id) > 250 ",
            DATAPLANE_PLUGIN_NAME,
            joinedTableKey(tableKey),
            DATAPLANE_PLUGIN_NAME,
            joinedTableKey(functionKey));

    // Assert
    assertUdfResult(testUdfQuery, 3);
  }

  @Test
  public void selectUdfAtBranchInWhereClause() throws Exception {
    String otherBranch = generateUniqueBranchName();
    List<String> functionKey = generateFunctionKeyWithFunctionInFolder();
    final List<String> tableKey = tablePathWithFolders(generateUniqueTableName());

    // Act
    runSQL(createBranchAtBranchQuery(otherBranch, DEFAULT_BRANCH_NAME));
    String create100TimesUdf =
        String.format(
            "CREATE FUNCTION %s.%s (x INT) AT BRANCH %s RETURNS INT RETURN SELECT x * 100",
            DATAPLANE_PLUGIN_NAME, joinedTableKey(functionKey), otherBranch);
    runSQL(create100TimesUdf);
    runSQL(createEmptyTableQuery(tableKey));
    runSQL(insertTableQuery(tableKey));
    // Act
    String testUdfQuery =
        String.format(
            "SELECT id from %s.%s where %s.%s(id) AT BRANCH %s > 250 ",
            DATAPLANE_PLUGIN_NAME,
            joinedTableKey(tableKey),
            DATAPLANE_PLUGIN_NAME,
            joinedTableKey(functionKey),
            otherBranch);

    // Assert
    assertUdfResult(testUdfQuery, 3);
  }

  @Test
  public void selectUdfAtTagInWhereClause() throws Exception {
    String otherBranch = generateUniqueBranchName();
    List<String> functionKey = generateFunctionKeyWithFunctionInFolder();
    final List<String> tableKey = tablePathWithFolders(generateUniqueTableName());

    // Act
    runSQL(createBranchAtBranchQuery(otherBranch, DEFAULT_BRANCH_NAME));
    String create100TimesUdf =
        String.format(
            "CREATE FUNCTION %s.%s (x INT) AT BRANCH %s RETURNS INT RETURN SELECT x * 100",
            DATAPLANE_PLUGIN_NAME, joinedTableKey(functionKey), otherBranch);
    runSQL(create100TimesUdf);
    runSQL(createTagQuery("tag1", otherBranch));
    runSQL(createEmptyTableQuery(tableKey));
    runSQL(insertTableQuery(tableKey));
    // Act
    String testUdfQuery =
        String.format(
            "SELECT id from %s.%s where %s.%s(id) AT TAG tag1 > 250 ",
            DATAPLANE_PLUGIN_NAME,
            joinedTableKey(tableKey),
            DATAPLANE_PLUGIN_NAME,
            joinedTableKey(functionKey));

    // Assert
    assertUdfResult(testUdfQuery, 3);
  }

  @Test
  public void selectTabularUdfSelectInDefaultBranch() throws Exception {
    List<String> functionKey = generateFunctionKeyWithFunctionInFolder();
    List<String> tableKey = tablePathWithFolders(generateUniqueTableName());
    runSQL(createEmptyTableQuery(tableKey));
    runSQL(insertTableQuery(tableKey));

    // Act
    runSQL(createTabularUdfQuery(functionKey, tableKey));

    // Assert
    assertThat(runSqlWithResults(selectTabularUdfQuery(functionKey)))
        .isEqualTo(
            Arrays.asList(
                Arrays.asList("1", "first row", "1000.000"),
                Arrays.asList("2", "second row", "2000.000")));
  }

  @Test
  public void selectTabularUdfAtBranch() throws Exception {
    final String otherBranch = generateUniqueBranchName();
    runSQL(createBranchAtBranchQuery(otherBranch, DEFAULT_BRANCH_NAME));
    List<String> functionKey = generateFunctionKeyWithFunctionInFolder();
    List<String> tableKey = tablePathWithFolders(generateUniqueTableName());
    runSQL(createEmptyTableQuery(tableKey));
    runSQL(insertTableQuery(tableKey));

    // Act
    // Create different version of tabular UDFs in differeent branch
    runSQL(
        String.format(
            "CREATE FUNCTION %s.%s() AT BRANCH %s RETURNS TABLE %s RETURN SELECT * FROM %s.%s WHERE distance <= 0",
            DATAPLANE_PLUGIN_NAME,
            joinedTableKey(functionKey),
            DEFAULT_BRANCH_NAME,
            DEFAULT_COLUMN_DEFINITION,
            DATAPLANE_PLUGIN_NAME,
            joinedTableKey(tableKey)));

    runSQL(createTabularUdfAtBranchQuery(functionKey, tableKey, otherBranch));

    // Assert
    assertThat(
            runSqlWithResults(
                DataplaneTestDefines.selectTabularUdfAtBranchQuery(functionKey, otherBranch)))
        .isEqualTo(
            Arrays.asList(
                Arrays.asList("1", "first row", "1000.000"),
                Arrays.asList("2", "second row", "2000.000")));
  }

  @Test
  public void selectTabularUdfAtTag() throws Exception {
    final String otherBranch = generateUniqueBranchName();
    runSQL(createBranchAtBranchQuery(otherBranch, DEFAULT_BRANCH_NAME));
    List<String> functionKey = generateFunctionKeyWithFunctionInFolder();
    List<String> tableKey = tablePathWithFolders(generateUniqueTableName());
    runSQL(createEmptyTableQuery(tableKey));
    runSQL(insertTableQuery(tableKey));

    // Act
    runSQL(createTabularUdfAtBranchQuery(functionKey, tableKey, otherBranch));
    assertNessieHasFunction(functionKey, otherBranch, this);
    runSQL(createTagQuery("tag1", otherBranch));

    // Assert
    assertThat(
            runSqlWithResults(DataplaneTestDefines.selectTabularUdfAtTagQuery(functionKey, "tag1")))
        .isEqualTo(
            Arrays.asList(
                Arrays.asList("1", "first row", "1000.000"),
                Arrays.asList("2", "second row", "2000.000")));
  }

  @Test
  public void selectTabularUdfSelectInOtherBranchFails() throws Exception {
    final String otherBranch = generateUniqueBranchName();
    runSQL(createBranchAtBranchQuery(otherBranch, DEFAULT_BRANCH_NAME));
    runSQL(useBranchQuery(otherBranch));
    List<String> functionKey = generateFunctionKeyWithFunctionInFolder();
    List<String> tableKey = tablePathWithFolders(generateUniqueTableName());
    runSQL(createEmptyTableQuery(tableKey));
    runSQL(insertTableQuery(tableKey));
    // Act
    runSQL(createTabularUdfQuery(functionKey, tableKey));

    // Assert
    assertThat(runSqlWithResults(selectTabularUdfQuery(functionKey)))
        .isEqualTo(
            Arrays.asList(
                Arrays.asList("1", "first row", "1000.000"),
                Arrays.asList("2", "second row", "2000.000")));
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    assertThatThrownBy(() -> runSQL(selectUdfQuery(functionKey, 2, 3)))
        .hasMessageContaining("No match found for function signature");
  }

  @Test
  public void selectUdfIncorrectParamsInWhereClauseFails() throws Exception {
    List<String> functionKey = generateFunctionKeyWithFunctionInFolder();
    final List<String> tableKey = tablePathWithFolders(generateUniqueTableName());

    // Act
    String create100TimesUdf =
        String.format(
            "CREATE FUNCTION %s.%s (x INT) RETURNS INT RETURN SELECT x * 100",
            DATAPLANE_PLUGIN_NAME, joinedTableKey(functionKey));
    runSQL(create100TimesUdf);
    runSQL(createEmptyTableQuery(tableKey));
    runSQL(insertTableQuery(tableKey));
    // Act
    String testUdfQuery =
        String.format(
            "SELECT id from %s.%s where %s.%s(id, id) > 250 ",
            DATAPLANE_PLUGIN_NAME,
            joinedTableKey(tableKey),
            DATAPLANE_PLUGIN_NAME,
            joinedTableKey(functionKey));

    // Assert
    assertThatThrownBy(() -> runSQL(testUdfQuery))
        .hasMessageContaining("No match found for function signature");
  }

  @Test
  public void selectComplexUdfInDefault() throws Exception {
    // Set-Up
    final List<String> functionKey = generateFunctionKeyWithFunctionInFolder();
    runSQL(createComplexUdfQueryWithAt(functionKey, DEFAULT_BRANCH_NAME));
    assertNessieHasFunction(functionKey, DEFAULT_BRANCH_NAME, this);

    // Act / Assert
    assertThat(runSqlWithResults(selectUdfAtBranchQuery(functionKey, DEFAULT_BRANCH_NAME, 2, 3)))
        .isEqualTo(ImmutableList.of(ImmutableList.of("[2,3]")));
  }

  @Test
  public void selectSerialUdfsInDefault() throws Exception {
    List<String> childFunctionKey = generateFunctionKeyWithFunctionInFolder();
    List<String> functionKey = generateFunctionKeyWithFunctionInFolder();

    // Act
    // Create the child udf
    runSQL(
        String.format(
            "CREATE FUNCTION %s.%s (x INT, y INT) RETURNS INT RETURN SELECT x * y",
            DATAPLANE_PLUGIN_NAME, joinedTableKey(childFunctionKey)));
    assertNessieHasFunction(childFunctionKey, DEFAULT_BRANCH_NAME, this);

    // Create the main udf
    runSQL(
        String.format(
            "CREATE FUNCTION %s.%s (x INT, y INT) RETURNS INT RETURN SELECT %s.%s(x, y) + 10",
            DATAPLANE_PLUGIN_NAME,
            joinedTableKey(functionKey),
            DATAPLANE_PLUGIN_NAME,
            joinedTableKey(childFunctionKey)));
    assertNessieHasFunction(functionKey, DEFAULT_BRANCH_NAME, this);

    // Assert
    assertUdfResult(selectUdfAtBranchQuery(childFunctionKey, DEFAULT_BRANCH_NAME, 2, 3), 6);
    assertUdfResult(selectUdfAtBranchQuery(functionKey, DEFAULT_BRANCH_NAME, 2, 3), 16);
  }
}
