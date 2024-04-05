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
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.DEFAULT_COUNT_COLUMN;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.alterTableAddColumnsQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createEmptyTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createTableAsQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createViewQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.deleteAllQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.dropTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.dropViewQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueFolderName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueTableName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.insertSelectQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.insertTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.joinedTableKey;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.quoted;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.selectCountQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.tablePathWithFolders;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.updateByIdQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.updateViewSelectQuery;
import static com.dremio.exec.catalog.dataplane.test.TestDataplaneAssertions.assertNessieDoesNotHaveTable;

import com.dremio.exec.catalog.dataplane.test.DataplaneStorage;
import com.dremio.exec.catalog.dataplane.test.ITDataplanePluginTestSetup;
import com.dremio.exec.catalog.dataplane.test.SkipForStorageType;
import com.dremio.service.namespace.AbstractConnectionConf;
import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@SkipForStorageType(
    DataplaneStorage.StorageType
        .AZURE) // TODO(DX-85934) : Re enable or  rework/split the cases to enable for Azure when
// the ticket is addressed.
public class ITDataplaneTestsWithSpecialNames extends ITDataplanePluginTestSetup {
  private static final String DATAPLANE_SOURCE = "dataplane";
  private static final String ONE_DOT = ".";
  private static final String THREE_CONSECUTIVE_DOTS = "...";
  private static final String ALTERNATE_DOTS = "one.two.three";
  private static final List<String> DATAPLANE_SOURCE_NAMES_WITH_DOTS =
      generateSourceNamesWithDotCombinations();
  private static final List<String> FOLDER_NAMES_WITH_DOTS =
      generateFolderNamesWithDotCombinations();
  private static final List<String> TABLE_NAMES_WITH_DOTS = generateTableNamesWithDotCombinations();

  @BeforeAll
  public static void setupContainers() throws Exception {
    final Map<String, AbstractConnectionConf> sourceNamesWithConnectionConf = new HashMap<>();
    DATAPLANE_SOURCE_NAMES_WITH_DOTS.forEach(
        sourceName -> sourceNamesWithConnectionConf.put(sourceName, null));
    setupForCreatingSources(sourceNamesWithConnectionConf);
  }

  @ParameterizedTest
  @MethodSource("providePathCombinationsWithDots")
  /**
   * Tests basic table DDMl/DML operations using an Arctic table path that has special characters in
   * path in different positions See PathProvider to see what combinations are covered
   */
  public void testTableArcticOperations(ImmutablePair<String, List<String>> sourceTableInput)
      throws Exception {
    String sourceName = quoted(sourceTableInput.getLeft());
    List<String> tablePath = sourceTableInput.getRight();
    runSQL(createEmptyTableQuery(sourceName, tablePath));

    // Test DML
    runSQL(insertTableQuery(sourceName, tablePath));
    runSQL(updateByIdQuery(sourceName, tablePath));
    runSQL(selectCountQuery(sourceName, tablePath, DEFAULT_COUNT_COLUMN));
    assertSQLReturnsExpectedNumRows(
        selectCountQuery(sourceName, tablePath, DEFAULT_COUNT_COLUMN), DEFAULT_COUNT_COLUMN, 3);
    runSQL(deleteAllQuery(sourceName, tablePath));
    assertSQLReturnsExpectedNumRows(
        selectCountQuery(sourceName, tablePath, DEFAULT_COUNT_COLUMN), DEFAULT_COUNT_COLUMN, 0);

    // Test DDL
    final List<String> addedColDef = Collections.singletonList("col2 int");
    runSQL(alterTableAddColumnsQuery(sourceName, tablePath, addedColDef));
    assertSQLReturnsExpectedNumRows(
        selectCountQuery(sourceName, tablePath, DEFAULT_COUNT_COLUMN), DEFAULT_COUNT_COLUMN, 0);
    runSQL(dropTableQuery(sourceName, tablePath));
    assertNessieDoesNotHaveTable(tablePath, DEFAULT_BRANCH_NAME, this);

    // Test Ctas  and Insert-Select
    List<String> ctasTablePath = sourceTableInput.getRight();
    runSQL(createTableAsQuery(sourceName, ctasTablePath, 5));
    runSQL(insertSelectQuery(sourceName, ctasTablePath, 10));
    runSQL(selectCountQuery(sourceName, ctasTablePath, DEFAULT_COUNT_COLUMN));
    assertSQLReturnsExpectedNumRows(
        selectCountQuery(sourceName, ctasTablePath, DEFAULT_COUNT_COLUMN),
        DEFAULT_COUNT_COLUMN,
        15);
    runSQL(dropTableQuery(sourceName, ctasTablePath));
  }

  @ParameterizedTest
  @MethodSource("providePathCombinationsWithDots")
  /**
   * Tests basic View operations using an Arctic view path that has special characters in path in
   * different positions See PathProvider to see what combinations are covered
   */
  public void testViewArcticOperations(ImmutablePair<String, List<String>> sourceTableInput)
      throws Exception {
    String sourceName = quoted(sourceTableInput.getLeft());
    List<String> viewPath = sourceTableInput.getRight();
    List<String> tablePath = tablePathWithFolders(generateUniqueTableName());

    runSQL(createEmptyTableQuery(sourceName, tablePath));
    runSQL(insertTableQuery(sourceName, tablePath));
    runSQL(createViewQuery(sourceName, viewPath, tablePath));
    runSQL(selectCountQuery(sourceName, viewPath, DEFAULT_COUNT_COLUMN));
    assertSQLReturnsExpectedNumRows(
        selectCountQuery(sourceName, viewPath, DEFAULT_COUNT_COLUMN), DEFAULT_COUNT_COLUMN, 3);
    String viewSQL11 =
        String.format("select id+10 AS idv1 from %s.%s", sourceName, joinedTableKey(tablePath));
    runSQL(updateViewSelectQuery(sourceName, viewPath, viewSQL11));
    runSQL(dropViewQuery(sourceName, viewPath));
    assertQueryThrowsExpectedError(
        selectCountQuery(sourceName, viewPath, DEFAULT_COUNT_COLUMN),
        String.format("Object '%s' not found ", viewPath.get(0)));
  }

  private static List<String> generateNamesWithDotCombinations(String sourceplugin) {
    return ImmutableList.of(
        sourceplugin + ONE_DOT,
        sourceplugin + ALTERNATE_DOTS,
        sourceplugin + THREE_CONSECUTIVE_DOTS,
        ONE_DOT + sourceplugin,
        THREE_CONSECUTIVE_DOTS + sourceplugin);
  }

  private static List<String> generateSourceNamesWithDotCombinations() {
    return generateNamesWithDotCombinations(DATAPLANE_SOURCE);
  }

  private static List<String> generateFolderNamesWithDotCombinations() {
    return generateNamesWithDotCombinations(generateUniqueFolderName());
  }

  private static List<String> generateTableNamesWithDotCombinations() {
    return generateNamesWithDotCombinations(generateUniqueTableName());
  }

  private static List<List<String>> tablePathCombinationsWithDots() {
    ImmutableList.Builder<List<String>> tablePathWithDotsCombinationsBuilder =
        new ImmutableList.Builder<>();
    // Create path combinations of one folder and one table with dots
    FOLDER_NAMES_WITH_DOTS.forEach(
        folder -> {
          TABLE_NAMES_WITH_DOTS.forEach(
              tableName -> {
                tablePathWithDotsCombinationsBuilder.add(Arrays.asList(folder, tableName));
              });
        });
    // Add a path combination of 3 folders in path
    tablePathWithDotsCombinationsBuilder.add(
        Arrays.asList(
            generateUniqueFolderName() + ONE_DOT,
            generateUniqueFolderName() + generateUniqueFolderName() + ALTERNATE_DOTS,
            generateUniqueTableName()));
    // Add a path combination of 3 folders in path include dot in table name.
    tablePathWithDotsCombinationsBuilder.add(
        Arrays.asList(
            generateUniqueFolderName() + ONE_DOT,
            generateUniqueFolderName(),
            generateUniqueFolderName() + ALTERNATE_DOTS,
            ONE_DOT + generateUniqueTableName() + ONE_DOT));
    return tablePathWithDotsCombinationsBuilder.build();
  }

  static Stream<Arguments> providePathCombinationsWithDots() {
    ImmutableList.Builder<Arguments> sourceCombinationsBuilder = new ImmutableList.Builder<>();

    List<List<String>> tablePathCombinations = tablePathCombinationsWithDots();
    DATAPLANE_SOURCE_NAMES_WITH_DOTS.forEach(
        source -> {
          tablePathCombinations.stream()
              .forEach(
                  tablePath ->
                      sourceCombinationsBuilder.add(
                          Arguments.of(new ImmutablePair(source, tablePath))));
        });
    return sourceCombinationsBuilder.build().stream();
  }
}
