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

import com.dremio.exec.catalog.dataplane.test.DataplaneStorage;
import com.dremio.exec.catalog.dataplane.test.ITDataplanePluginTestSetup;
import com.dremio.exec.catalog.dataplane.test.SkipForStorageType;
import com.dremio.service.namespace.AbstractConnectionConf;
import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Skipping for AWS/GCS storage because the test cases covered in {@link
 * ITDataplaneTestsWithSpecialNamesTablesAWSAndGCS} This test uses real Azure storage to test,
 * unlike we use mock in ITDataplaneTestsWithSpecialNamesTables. Each test will take about 40-50
 * seconds so we reduced the test size in Azure tests. TODO: DX-83670, DX-83691 To run the test
 * locally, please specify AZURE_STORAGE_DATAPLANE_ACCOUNT_NAME and
 * AZURE_STORAGE_DATAPLANE_ACCOUNT_KEY in environment variable
 */
@SkipForStorageType(DataplaneStorage.StorageType.AWS_S3_MOCK)
@SkipForStorageType(DataplaneStorage.StorageType.GCS_MOCK)
public class ITDataplaneTestsWithSpecialNamesViewsAzure extends ITDataplanePluginTestSetup {
  private static final String DATAPLANE_SOURCE = "dataplane";
  private static final String ONE_DOT = ".";
  private static final String THREE_CONSECUTIVE_DOTS = "...";
  private static final String SLASH = "/";
  private static final List<String> FOLDER_NAMES_WITH_SPECIAL_CHARACTERS =
      generateFolderNamesWithSpecialCharactersCombinations();
  private static final List<String> TABLE_NAMES_WITH_SPECIAL_CHARACTERS =
      generateTableNamesWithSpecialCharactersCombinations();

  @BeforeAll
  public static void setupContainer() throws Exception {
    final Map<String, AbstractConnectionConf> sourceNamesWithConnectionConf = new HashMap<>();
    sourceNamesWithConnectionConf.put(DATAPLANE_SOURCE, null);
    setupForCreatingSources(sourceNamesWithConnectionConf);
  }

  @ParameterizedTest
  @MethodSource("providePathCombinationsWithSpecialCharacters")
  /**
   * Tests basic View operations using a Dataplane view path that has special characters in path in
   * different positions See PathProvider to see what combinations are covered
   */
  public void testViewDataplaneOperations(ImmutablePair<String, List<String>> sourceTableInput)
      throws Exception {
    String sourceName = quoted(sourceTableInput.getLeft());
    List<String> viewPath = sourceTableInput.getRight();
    List<String> tablePath = tablePathWithFolders(generateUniqueTableName());

    // Create view with special characters
    runSQL(createEmptyTableQuery(sourceName, tablePath));
    runSQL(insertTableQuery(sourceName, tablePath));
    runSQL(createViewQuery(sourceName, viewPath, tablePath));
    runSQL(selectCountQuery(sourceName, viewPath, DEFAULT_COUNT_COLUMN));
    assertSQLReturnsExpectedNumRows(
        selectCountQuery(sourceName, viewPath, DEFAULT_COUNT_COLUMN), DEFAULT_COUNT_COLUMN, 3);
    // Try select view that contains special characters in view path
    String viewSQL11 =
        String.format("select id+10 AS idv1 from %s.%s", sourceName, joinedTableKey(tablePath));
    runSQL(updateViewSelectQuery(sourceName, viewPath, viewSQL11));
    runSQL(dropViewQuery(sourceName, viewPath));
    assertQueryThrowsExpectedError(
        selectCountQuery(sourceName, viewPath, DEFAULT_COUNT_COLUMN),
        String.format("Object '%s' not found ", viewPath.get(viewPath.size() - 1)));
  }

  private static List<String> generateFolderNamesWithSpecialCharactersCombinations() {
    return generateNamesWithSpecialCharactersCombinations(generateUniqueFolderName());
  }

  private static List<String> generateTableNamesWithSpecialCharactersCombinations() {
    return generateNamesWithSpecialCharactersCombinations(generateUniqueTableName());
  }

  private static List<String> generateNamesWithSpecialCharactersCombinations(String sourceplugin) {
    return ImmutableList.of(
        sourceplugin + ONE_DOT, sourceplugin + SLASH, sourceplugin + THREE_CONSECUTIVE_DOTS);
  }

  private static List<List<String>> tablePathCombinationsWithSpecialCharacters() {
    ImmutableList.Builder<List<String>> tablePathWithSpecialCharactersCombinationsBuilder =
        new ImmutableList.Builder<>();
    // Create path combinations of one folder and one table with dots
    FOLDER_NAMES_WITH_SPECIAL_CHARACTERS.forEach(
        folder -> {
          TABLE_NAMES_WITH_SPECIAL_CHARACTERS.forEach(
              tableName -> {
                tablePathWithSpecialCharactersCombinationsBuilder.add(
                    Arrays.asList(folder, tableName));
              });
        });
    return tablePathWithSpecialCharactersCombinationsBuilder.build();
  }

  static Stream<Arguments> providePathCombinationsWithSpecialCharacters() {
    ImmutableList.Builder<Arguments> sourceCombinationsBuilder = new ImmutableList.Builder<>();

    List<List<String>> tablePathCombinations = tablePathCombinationsWithSpecialCharacters();

    tablePathCombinations.stream()
        .forEach(
            tablePath ->
                sourceCombinationsBuilder.add(
                    Arguments.of(new ImmutablePair(DATAPLANE_SOURCE, tablePath))));
    return sourceCombinationsBuilder.build().stream();
  }
}
