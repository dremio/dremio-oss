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
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.deleteAllQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.dropTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueFolderName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueTableName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.insertSelectQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.insertTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.quoted;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.selectCountQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.updateByIdQuery;
import static com.dremio.exec.catalog.dataplane.test.TestDataplaneAssertions.assertNessieDoesNotHaveEntity;

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
public class ITDataplaneTestsWithSpecialNamesTablesAzure extends ITDataplanePluginTestSetup {
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
  public void testSpecialCharacters(ImmutablePair<String, List<String>> sourceTableInput)
      throws Exception {
    String sourceName = quoted(sourceTableInput.getLeft());
    List<String> tablePath = sourceTableInput.getRight();
    runSQL(createEmptyTableQuery(sourceName, tablePath));

    testDML(tablePath);
    testDDL(tablePath);
    runSQL(dropTableQuery(sourceName, tablePath));
    assertNessieDoesNotHaveEntity(tablePath, DEFAULT_BRANCH_NAME, this);
    testCtasAndInsertSelect(tablePath);
  }

  private void testCtasAndInsertSelect(List<String> tablePath) throws Exception {
    runSQL(createTableAsQuery(DATAPLANE_SOURCE, tablePath, 5));
    runSQL(insertSelectQuery(DATAPLANE_SOURCE, tablePath, 10));
    runSQL(selectCountQuery(DATAPLANE_SOURCE, tablePath, DEFAULT_COUNT_COLUMN));
    assertSQLReturnsExpectedNumRows(
        selectCountQuery(DATAPLANE_SOURCE, tablePath, DEFAULT_COUNT_COLUMN),
        DEFAULT_COUNT_COLUMN,
        15);
    runSQL(dropTableQuery(DATAPLANE_SOURCE, tablePath));
  }

  private void testDML(List<String> tablePath) throws Exception {
    runSQL(insertTableQuery(DATAPLANE_SOURCE, tablePath));
    runSQL(updateByIdQuery(DATAPLANE_SOURCE, tablePath));
    runSQL(selectCountQuery(DATAPLANE_SOURCE, tablePath, DEFAULT_COUNT_COLUMN));
    assertSQLReturnsExpectedNumRows(
        selectCountQuery(DATAPLANE_SOURCE, tablePath, DEFAULT_COUNT_COLUMN),
        DEFAULT_COUNT_COLUMN,
        3);
    runSQL(deleteAllQuery(DATAPLANE_SOURCE, tablePath));
    assertSQLReturnsExpectedNumRows(
        selectCountQuery(DATAPLANE_SOURCE, tablePath, DEFAULT_COUNT_COLUMN),
        DEFAULT_COUNT_COLUMN,
        0);
  }

  private void testDDL(List<String> tablePath) throws Exception {
    final List<String> addedColDef = Collections.singletonList("col2 int");
    runSQL(alterTableAddColumnsQuery(DATAPLANE_SOURCE, tablePath, addedColDef));
    assertSQLReturnsExpectedNumRows(
        selectCountQuery(DATAPLANE_SOURCE, tablePath, DEFAULT_COUNT_COLUMN),
        DEFAULT_COUNT_COLUMN,
        0);
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
