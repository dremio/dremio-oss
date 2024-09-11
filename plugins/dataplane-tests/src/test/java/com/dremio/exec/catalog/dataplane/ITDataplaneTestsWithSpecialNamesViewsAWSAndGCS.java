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

import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.DEFAULT_COUNT_COLUMN;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createEmptyTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createViewQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.dropViewQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueFolderName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueTableName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.insertTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.joinedTableKey;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.quoted;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.selectCountQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.tablePathWithFolders;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.updateViewSelectQuery;

import com.dremio.common.utils.PathUtils;
import com.dremio.exec.catalog.dataplane.test.DataplaneStorage;
import com.dremio.exec.catalog.dataplane.test.ITDataplanePluginTestSetup;
import com.dremio.exec.catalog.dataplane.test.SkipForStorageType;
import com.dremio.service.namespace.AbstractConnectionConf;
import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Skipping Azure because we use real Azure storage instead of mock which will take 40-50 seconds
 * per test. The tests here are more than 100 and it will take 90 min to run whole tests for Azure.
 * For Azure test cases please see {@link ITDataplaneTestsWithSpecialNamesTablesAzure} TODO:
 * DX-83670, DX-83691
 */
@SkipForStorageType(DataplaneStorage.StorageType.AZURE)
public class ITDataplaneTestsWithSpecialNamesViewsAWSAndGCS extends ITDataplanePluginTestSetup {
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
   * Tests basic View operations using a Dataplane view path that has special characters in path in
   * different positions See PathProvider to see what combinations are covered
   */
  public void testViewDataplaneOperations(ImmutablePair<String, List<String>> sourceTableInput)
      throws Exception {
    String sourceName = sourceTableInput.getLeft();
    String quotedSourceName = quoted(sourceTableInput.getLeft());
    List<String> viewPath = sourceTableInput.getRight();
    List<String> tablePath = tablePathWithFolders(generateUniqueTableName());

    runSQL(createEmptyTableQuery(quotedSourceName, tablePath));
    runSQL(insertTableQuery(quotedSourceName, tablePath));
    runSQL(createViewQuery(quotedSourceName, viewPath, tablePath));
    runSQL(selectCountQuery(quotedSourceName, viewPath, DEFAULT_COUNT_COLUMN));
    assertSQLReturnsExpectedNumRows(
        selectCountQuery(quotedSourceName, viewPath, DEFAULT_COUNT_COLUMN),
        DEFAULT_COUNT_COLUMN,
        3);
    String viewSQL11 =
        String.format(
            "select id+10 AS idv1 from %s.%s", quotedSourceName, joinedTableKey(tablePath));
    runSQL(updateViewSelectQuery(quotedSourceName, viewPath, viewSQL11));
    runSQL(dropViewQuery(quotedSourceName, viewPath));
    final List<String> fullPath =
        Stream.concat(viewPath.stream(), viewPath.stream().skip(1).map(PathUtils::removeQuotes))
            .collect(Collectors.toList());
    assertQueryThrowsExpectedError(
        selectCountQuery(quotedSourceName, viewPath, DEFAULT_COUNT_COLUMN),
        String.format("Object '%s' not found", viewPath.get(viewPath.size() - 1)));
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
