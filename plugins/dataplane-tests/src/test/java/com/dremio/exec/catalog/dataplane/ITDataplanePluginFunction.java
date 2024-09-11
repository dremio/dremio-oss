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
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createEmptyTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createOrReplaceUdfQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createTabularUdfQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createTabularUdfQueryNonQualifiedTableName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createUdfQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createUdfQueryWithAt;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createViewQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.dropTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.dropUdfQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.dropUdfQueryWithAt;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.dropViewQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueBranchName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueFunctionName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueTableName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueViewName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.insertTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.joinedTableKey;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.selectUdfQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.tablePathWithFolders;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.useBranchQuery;
import static com.dremio.exec.catalog.dataplane.test.TestDataplaneAssertions.assertNessieDoesNotHaveEntity;
import static com.dremio.exec.catalog.dataplane.test.TestDataplaneAssertions.assertNessieHasFunction;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dremio.exec.catalog.CatalogOptions;
import com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines;
import com.dremio.exec.catalog.dataplane.test.ITDataplanePluginTestSetup;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

public class ITDataplanePluginFunction extends ITDataplanePluginTestSetup {

  @Test
  public void createScalarUdf() throws Exception {
    enableVersionedSourceUdf();
    final String functionName = generateUniqueFunctionName();
    List<String> functionKey = tablePathWithFolders(functionName);

    // Act
    runSQL(createUdfQuery(functionKey));

    // Assert
    assertNessieHasFunction(functionKey, DEFAULT_BRANCH_NAME, this);
  }

  @Test
  public void createTabularUdf() throws Exception {
    enableVersionedSourceUdf();
    final String functionName = generateUniqueFunctionName();
    final String tableName = generateUniqueTableName();
    List<String> tableKey = tablePathWithFolders(tableName);
    List<String> functionKey = tablePathWithFolders(functionName);
    runSQL(createEmptyTableQuery(tableKey));

    // Act
    runSQL(createTabularUdfQuery(functionKey, tableKey));

    // Assert
    assertNessieHasFunction(functionKey, DEFAULT_BRANCH_NAME, this);
  }

  @Test
  public void createUdfTwice() throws Exception {
    enableVersionedSourceUdf();
    final String functionName = generateUniqueFunctionName();
    List<String> functionKey = tablePathWithFolders(functionName);

    // Act
    runSQL(createUdfQuery(functionKey));

    // Assert
    assertThatThrownBy(() -> runSQL(createUdfQuery(functionKey)))
        .hasMessageContaining("already exists");
  }

  @Test
  public void createUdfClashWithTable() throws Exception {
    enableVersionedSourceUdf();
    String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createEmptyTableQuery(tablePath));

    // Act and Assert
    assertThatThrownBy(() -> runSQL(createUdfQuery(tablePath)))
        .hasMessageContaining("An Entity of type ICEBERG_TABLE with given name ")
        .hasMessageContaining("already exists");
  }

  @Test
  public void createUdfClashWithView() throws Exception {
    enableVersionedSourceUdf();
    String tableName = generateUniqueTableName();
    List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createEmptyTableQuery(tablePath));

    String viewName = generateUniqueViewName();
    List<String> viewKey = tablePathWithFolders(viewName);
    runSQL(createViewQuery(viewKey, tablePath));

    // Act and Assert
    assertThatThrownBy(() -> runSQL(createUdfQuery(viewKey)))
        .hasMessageContaining("An Entity of type ICEBERG_VIEW with given name ")
        .hasMessageContaining("already exists");
  }

  @Test
  public void createUdfClashWithFolder() throws Exception {
    enableVersionedSourceUdf();
    String tableName = generateUniqueTableName();
    List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createEmptyTableQuery(tablePath));

    // Act and Assert
    assertThatThrownBy(() -> runSQL(createUdfQuery(tablePath.subList(0, tablePath.size() - 1))))
        .hasMessageContaining("An Entity of type FOLDER with given name ")
        .hasMessageContaining("already exists");
  }

  @Test
  public void createViewClashWithUdf() throws Exception {
    enableVersionedSourceUdf();
    String functionName = generateUniqueFunctionName();
    List<String> functionKey = tablePathWithFolders(functionName);

    runSQL(createUdfQuery(functionKey));

    String tableName = generateUniqueTableName();
    List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createEmptyTableQuery(tablePath));

    // Act and Assert
    assertThatThrownBy(() -> runSQL(createViewQuery(functionKey, tablePath)))
        .hasMessageContaining("An Entity of type UDF with given name ")
        .hasMessageContaining("already exists");
  }

  @Test
  public void createUdfWithSQLError() throws Exception {
    enableVersionedSourceUdf();
    String functionName = generateUniqueFunctionName();
    List<String> functionKey = tablePathWithFolders(functionName);

    String createUdfQueryWithSQLError =
        String.format(
            "CREATE FUNCTION %s.%s (x INT, y INT) RETURNS INT RETURN SELECT a FROM xyz",
            DATAPLANE_PLUGIN_NAME, joinedTableKey(functionKey));
    // Act and Assert
    assertThatThrownBy(() -> runSQL(createUdfQueryWithSQLError))
        .hasMessageContaining("SYSTEM ERROR: SqlValidatorException");
    assertNessieDoesNotHaveEntity(functionKey, DEFAULT_BRANCH_NAME, this);
  }

  @Test
  public void createTabularUdfWithSelectFromNonQualifiedInnerTableInDevBranch() throws Exception {
    enableVersionedSourceUdf();
    final String functionName = generateUniqueFunctionName();
    final String tableName = generateUniqueTableName();
    List<String> functionKey = tablePathWithFolders(functionName);
    List<String> tableKey = tablePathWithFolders(tableName);
    runSQL(createEmptyTableQuery(tableKey));
    runSQL(insertTableQuery(tableKey));

    // Act and assert
    assertThatThrownBy(
            () ->
                runSQL(
                    createTabularUdfQueryNonQualifiedTableName(
                        DATAPLANE_PLUGIN_NAME, functionKey, tableName)))
        .hasMessageContaining("Object '%s' not found", tableName);
  }

  @Test
  public void updateUdf() throws Exception {
    enableVersionedSourceUdf();
    final String functionName = generateUniqueFunctionName();
    List<String> functionKey = tablePathWithFolders(functionName);

    // Act
    runSQL(createUdfQuery(functionKey));

    // Assert
    runSQL(createOrReplaceUdfQuery(functionKey));
    assertNessieHasFunction(functionKey, DEFAULT_BRANCH_NAME, this);
  }

  @Test
  public void updateUdfWithSQLError() throws Exception {
    enableVersionedSourceUdf();
    String functionName = generateUniqueFunctionName();
    List<String> functionKey = tablePathWithFolders(functionName);
    runSQL(createUdfQuery(functionKey));

    String replaceUdfQueryWithSQLError =
        String.format(
            "CREATE OR REPLACE FUNCTION %s.%s (x INT, y INT) RETURNS INT RETURN SELECT a FROM xyz",
            DATAPLANE_PLUGIN_NAME, joinedTableKey(functionKey));
    // Act and Assert
    assertThatThrownBy(() -> runSQL(replaceUdfQueryWithSQLError))
        .hasMessageContaining("SYSTEM ERROR: SqlValidatorException");
    assertNessieHasFunction(functionKey, DEFAULT_BRANCH_NAME, this);
  }

  @Test
  public void dropUdf() throws Exception {
    final String functionName = generateUniqueFunctionName();
    List<String> functionKey = tablePathWithFolders(functionName);

    // Act
    runSQL(createUdfQuery(functionKey));
    assertNessieHasFunction(functionKey, DEFAULT_BRANCH_NAME, this);

    runSQL(dropUdfQuery(functionKey));

    // Assert
    assertNessieDoesNotHaveEntity(functionKey, DEFAULT_BRANCH_NAME, this);
  }

  @Test
  public void dropUdfTwice() throws Exception {
    enableVersionedSourceUdf();
    final String functionName = generateUniqueFunctionName();
    List<String> functionKey = tablePathWithFolders(functionName);

    // Act
    runSQL(createUdfQuery(functionKey));
    assertNessieHasFunction(functionKey, DEFAULT_BRANCH_NAME, this);

    runSQL(dropUdfQuery(functionKey));
    assertNessieDoesNotHaveEntity(functionKey, DEFAULT_BRANCH_NAME, this);

    // Assert
    assertThatThrownBy(() -> runSQL(dropUdfQuery(functionKey)))
        .hasMessageContaining("does not exists");
  }

  @Test
  public void dropUdfAsTable() throws Exception {
    enableVersionedSourceUdf();
    final String functionName = generateUniqueFunctionName();
    List<String> functionKey = tablePathWithFolders(functionName);

    // Act
    runSQL(createUdfQuery(functionKey));
    assertNessieHasFunction(functionKey, DEFAULT_BRANCH_NAME, this);

    // Assert
    assertThatThrownBy(() -> runSQL(dropTableQuery(functionKey)))
        .hasMessageContaining("does not exist");
  }

  @Test
  public void dropUdfAsView() throws Exception {
    enableVersionedSourceUdf();
    final String functionName = generateUniqueFunctionName();
    List<String> functionKey = tablePathWithFolders(functionName);

    // Act
    runSQL(createUdfQuery(functionKey));
    assertNessieHasFunction(functionKey, DEFAULT_BRANCH_NAME, this);

    // Assert
    assertThatThrownBy(() -> runSQL(dropViewQuery(functionKey)))
        .hasMessageContaining("VALIDATION ERROR: Unknown view");
  }

  @Test
  public void dropViewAsUdf() throws Exception {
    enableVersionedSourceUdf();
    String tableName = generateUniqueTableName();
    List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createEmptyTableQuery(tablePath));

    String viewName = generateUniqueViewName();
    List<String> viewKey = tablePathWithFolders(viewName);
    runSQL(createViewQuery(viewKey, tablePath));

    // Assert
    assertThatThrownBy(() -> runSQL(dropUdfQuery(viewKey))).hasMessageContaining("does not exist");
  }

  @Test
  public void createAndDropUdfWithATSyntaxInDevBranch() throws Exception {
    enableVersionedSourceUdf();
    String devBranch = generateUniqueBranchName();

    runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));

    String functionName = generateUniqueFunctionName();
    List<String> functionKey = tablePathWithFolders(functionName);

    // Act and Assert
    runSQL(createUdfQueryWithAt(functionKey, devBranch));
    assertNessieHasFunction(functionKey, devBranch, this);

    runSQL(dropUdfQueryWithAt(functionKey, devBranch));
    assertNessieDoesNotHaveEntity(functionKey, devBranch, this);
  }

  @Test
  public void createAndDropUdfWithSessionVersionContext() throws Exception {
    enableVersionedSourceUdf();
    // Arrange
    String functionName = generateUniqueFunctionName();
    List<String> functionKey = tablePathWithFolders(functionName);
    // Create dev branch
    String devBranch = generateUniqueBranchName();
    runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));

    // Act and Assert
    runSQL(useBranchQuery(devBranch));
    runSQL(createUdfQuery(functionKey));
    assertNessieHasFunction(functionKey, devBranch, this);
    assertNessieDoesNotHaveEntity(functionKey, DEFAULT_BRANCH_NAME, this);

    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    assertThatThrownBy(() -> runSQL(dropUdfQuery(functionKey)))
        .hasMessageContaining("does not exists");

    runSQL(useBranchQuery(devBranch));
    runSQL(dropUdfQuery(functionKey));
    assertNessieDoesNotHaveEntity(functionKey, DEFAULT_BRANCH_NAME, this);
  }

  @Test
  public void dropUdfWithContext() throws Exception {
    enableVersionedSourceUdf();
    String devBranch = generateUniqueBranchName();
    String functionName = generateUniqueFunctionName();
    List<String> functionKey = tablePathWithFolders(functionName);

    runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));
    runSQL(createUdfQueryWithAt(functionKey, devBranch));
    assertNessieHasFunction(functionKey, devBranch, this);

    // Act
    assertThatThrownBy(() -> runSQL(dropUdfQuery(functionKey)))
        .hasMessageContaining("does not exists");

    runSQL(useBranchQuery(devBranch));
    runSQL(dropUdfQuery(functionKey));

    // Assert
    assertNessieDoesNotHaveEntity(functionKey, devBranch, this);
  }

  @Test
  public void selectScalarUdfSelectInDefaultBranch() throws Exception {
    enableVersionedSourceUdf();
    final String functionName = generateUniqueFunctionName();
    List<String> functionKey = tablePathWithFolders(functionName);
    // Act
    runSQL(createUdfQuery(functionKey));

    // Assert
    assertThat(
            runSqlWithResults(selectUdfQuery(functionKey, 2, 3)).stream()
                .allMatch(row -> (row.size() == 1) && row.get(0).contains("6")))
        .isTrue();
  }

  @Test
  public void selectScalarUdfInWhereClause() throws Exception {
    enableVersionedSourceUdf();
    final String functionName = generateUniqueFunctionName();
    List<String> functionKey = tablePathWithFolders(functionName);
    final String tableName = generateUniqueTableName();
    final List<String> tableKey = tablePathWithFolders(tableName);
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
    assertThat(
            runSqlWithResults(testUdfQuery).stream()
                .allMatch(row -> (row.size() == 1) && row.get(0).contains("3")))
        .isTrue();
  }

  @Test
  public void selectScalarUdfSelectInDevBranch() throws Exception {
    enableVersionedSourceUdf();
    final String devBranch = generateUniqueBranchName();

    runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));
    runSQL(useBranchQuery(devBranch));
    final String functionName = generateUniqueFunctionName();
    List<String> functionKey = tablePathWithFolders(functionName);
    // Act
    runSQL(createUdfQuery(functionKey));

    // Assert
    assertThat(
            runSqlWithResults(selectUdfQuery(functionKey, 2, 3)).stream()
                .allMatch(row -> (row.size() == 1) && row.get(0).contains("6")))
        .isTrue();
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    assertThatThrownBy(() -> runSQL(selectUdfQuery(functionKey, 2, 3)))
        .hasMessageContaining("No match found for function signature");
  }

  @Test
  public void selectTablularUdfSelectInDefaultBranch() throws Exception {
    enableVersionedSourceUdf();
    final String functionName = generateUniqueFunctionName();
    final String tableName = generateUniqueTableName();
    List<String> functionKey = tablePathWithFolders(functionName);
    List<String> tableKey = tablePathWithFolders(tableName);
    runSQL(createEmptyTableQuery(tableKey));
    runSQL(insertTableQuery(tableKey));

    // Act
    runSQL(createTabularUdfQuery(functionKey, tableKey));

    // Assert
    assertThat(runSqlWithResults(DataplaneTestDefines.selectTabularUdfQuery(functionKey)))
        .isEqualTo(
            Arrays.asList(
                Arrays.asList("1", "first row", "1000.000"),
                Arrays.asList("2", "second row", "2000.000")));
  }

  @Test
  public void selectTablularUdfSelectInDevBranch() throws Exception {
    enableVersionedSourceUdf();
    final String devBranch = generateUniqueBranchName();

    runSQL(createBranchAtBranchQuery(devBranch, DEFAULT_BRANCH_NAME));
    runSQL(useBranchQuery(devBranch));
    final String functionName = generateUniqueFunctionName();
    final String tableName = generateUniqueTableName();
    List<String> functionKey = tablePathWithFolders(functionName);
    List<String> tableKey = tablePathWithFolders(tableName);
    runSQL(createEmptyTableQuery(tableKey));
    runSQL(insertTableQuery(tableKey));
    // Act
    runSQL(createTabularUdfQuery(functionKey, tableKey));

    // Assert
    assertThat(runSqlWithResults(DataplaneTestDefines.selectTabularUdfQuery(functionKey)))
        .isEqualTo(
            Arrays.asList(
                Arrays.asList("1", "first row", "1000.000"),
                Arrays.asList("2", "second row", "2000.000")));
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    assertThatThrownBy(() -> runSQL(selectUdfQuery(functionKey, 2, 3)))
        .hasMessageContaining("No match found for function signature");
  }

  // Negative select Udf tests
  @Test
  public void nonExistentScalarUdfSelect() throws Exception {
    enableVersionedSourceUdf();
    final String functionName = generateUniqueFunctionName();
    List<String> functionKey = tablePathWithFolders(functionName);

    assertThatThrownBy(() -> runSQL(selectUdfQuery(functionKey, 2, 3)))
        .hasMessageContaining("No match found for function signature");
  }

  @Test
  public void scalarUdfSelectIncorrectBranch() throws Exception {
    enableVersionedSourceUdf();
    final String devBranch1 = generateUniqueBranchName();
    final String devBranch2 = generateUniqueBranchName();
    final String functionName = generateUniqueFunctionName();
    List<String> functionKey = tablePathWithFolders(functionName);

    runSQL(createBranchAtBranchQuery(devBranch1, DEFAULT_BRANCH_NAME));
    runSQL(createBranchAtBranchQuery(devBranch2, DEFAULT_BRANCH_NAME));
    runSQL(useBranchQuery(devBranch1));
    runSQL(createUdfQuery(functionKey));

    runSQL(useBranchQuery(devBranch2));
    assertThatThrownBy(() -> runSQL(selectUdfQuery(functionKey, 2, 3)))
        .hasMessageContaining("No match found for function signature");
  }

  @Test
  public void selectScalarUdfIncorrectParamsInWhereClause() throws Exception {
    enableVersionedSourceUdf();
    final String functionName = generateUniqueFunctionName();
    List<String> functionKey = tablePathWithFolders(functionName);
    final String tableName = generateUniqueTableName();
    final List<String> tableKey = tablePathWithFolders(tableName);
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

  static AutoCloseable enableVersionedSourceUdf() {
    setSystemOption(CatalogOptions.VERSIONED_SOURCE_UDF_ENABLED.getOptionName(), "true");
    return () ->
        setSystemOption(
            CatalogOptions.VERSIONED_SOURCE_UDF_ENABLED.getOptionName(),
            CatalogOptions.VERSIONED_SOURCE_UDF_ENABLED.getDefault().getBoolVal().toString());
  }
}
