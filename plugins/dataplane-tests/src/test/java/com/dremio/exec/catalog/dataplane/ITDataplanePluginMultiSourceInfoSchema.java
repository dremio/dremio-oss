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
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createEmptyTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createFolderQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createViewSelectQuery;
import static org.assertj.core.api.Assertions.assertThat;

import com.dremio.exec.catalog.dataplane.test.ContainerEntity;
import com.dremio.exec.catalog.dataplane.test.ITDataplanePluginTestSetup;
import com.dremio.exec.store.dfs.NASConf;
import com.dremio.service.namespace.AbstractConnectionConf;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Entities created under a Nessie source will be shared by other Nessie sources too as underneath
 * they are pointing to same Nessie instance Testing is around multiple sources, nested folders,
 * tables, views
 *
 * <p>Dataplane sources in this framework: dataplane_plugin, dataplane%plugin, dataplaneASDFplugin,
 * dataplaneA1S2DFplugin
 *
 * <p>dataplane_plugin (same hierarchy is being used by other dataplane sources) - tableAFirst -
 * tableBSecond - viewCThird - viewDFourth - Folder1 - tableAFirst - tableBSecond - viewCThird -
 * viewDFourth - subFolderInFolder1 - tableAFirst - tableBSecond - viewCThird - viewDFourth -
 * emptyFolder2
 *
 * <p>nas_plugin - tableAFirst - tableBSecond - Folder1 - tableAFirst - tableBSecond -
 * subFolderInFolder1 - tableAFirst - tableBSecond
 */
public class ITDataplanePluginMultiSourceInfoSchema extends ITDataplanePluginTestSetup {

  private static final String DATAPLANE_SOURCE_PREFIX = "dataplane";
  private static final String DATAPLANE_SOURCE_NAME_WITH_UNDERSCORE =
      DATAPLANE_SOURCE_PREFIX + "_plugin";
  private static final String DATAPLANE_SOURCE_NAME_WITH_PERCENT =
      DATAPLANE_SOURCE_PREFIX + "%plugin";
  private static final String DATAPLANE_SOURCE_NAME_WITH_CHARS =
      DATAPLANE_SOURCE_PREFIX + "ASDFplugin";
  private static final String DATAPLANE_SOURCE_NAME_WITH_ALPHANUMERIC =
      DATAPLANE_SOURCE_PREFIX + "A1S2DFplugin";
  private static final String NAS_SOURCE_NAME = "nas_plugin";
  private static final String EMPTY_STRING = "";

  private static final List<String> DATAPLANE_SOURCE_NAMES =
      ImmutableList.of(
          DATAPLANE_SOURCE_NAME_WITH_UNDERSCORE,
          DATAPLANE_SOURCE_NAME_WITH_PERCENT,
          DATAPLANE_SOURCE_NAME_WITH_CHARS,
          DATAPLANE_SOURCE_NAME_WITH_ALPHANUMERIC);

  private static final Map<String, AbstractConnectionConf> sourceNamesWithConnectionConf =
      new HashMap<>();

  private static final List<ContainerEntity> DATAPLANE_SOURCE_CONTAINERS = new ArrayList<>();

  private static final List<ContainerEntity> DATAPLANE_FOLDER_CONTAINERS = new ArrayList<>();
  private static final List<ContainerEntity> DATAPLANE_SUB_FOLDER_CONTAINERS = new ArrayList<>();
  private static final List<ContainerEntity> DATAPLANE_EMPTY_FOLDER_CONTAINERS = new ArrayList<>();

  private static ContainerEntity NAS_SOURCE_CONTAINER;

  private static ContainerEntity NAS_FOLDER_CONTAINER;
  private static ContainerEntity NAS_SUB_FOLDER_CONTAINER;
  @TempDir private static File temporaryDirectory;

  @BeforeAll
  public static void setupContainers() throws Exception {
    initializeDataplanePluginContainers();
    initializeNASContainer();

    // As this is multi source tests, so we have to create all sources and one external source to
    // test across platform
    final NASConf nasConf = new NASConf();
    nasConf.path = temporaryDirectory.getPath();

    sourceNamesWithConnectionConf.put(DATAPLANE_SOURCE_NAME_WITH_UNDERSCORE, null);
    sourceNamesWithConnectionConf.put(DATAPLANE_SOURCE_NAME_WITH_PERCENT, null);
    sourceNamesWithConnectionConf.put(DATAPLANE_SOURCE_NAME_WITH_CHARS, null);
    sourceNamesWithConnectionConf.put(DATAPLANE_SOURCE_NAME_WITH_ALPHANUMERIC, null);
    sourceNamesWithConnectionConf.put(NAS_SOURCE_NAME, nasConf);

    setupForCreatingSources(sourceNamesWithConnectionConf);
    createEntitiesForNASPluginContainer();
  }

  private static void initializeNASContainer() {
    NAS_SOURCE_CONTAINER =
        new ContainerEntity(
            NAS_SOURCE_NAME,
            ContainerEntity.Type.SOURCE,
            ContainerEntity.Contains.FOLDERS_AND_TABLES,
            Collections.emptyList());

    NAS_FOLDER_CONTAINER =
        new ContainerEntity(
            "Folder1",
            ContainerEntity.Type.EXPLICIT_FOLDER,
            ContainerEntity.Contains.FOLDERS_AND_TABLES,
            NAS_SOURCE_CONTAINER.getFullPath());

    NAS_SUB_FOLDER_CONTAINER =
        new ContainerEntity(
            "subFolderInFolder1",
            ContainerEntity.Type.EXPLICIT_FOLDER,
            ContainerEntity.Contains.TABLES_ONLY,
            NAS_FOLDER_CONTAINER.getFullPath());
  }

  private static void initializeDataplanePluginContainers() {
    DATAPLANE_SOURCE_NAMES.forEach(
        sourceName ->
            DATAPLANE_SOURCE_CONTAINERS.add(
                new ContainerEntity(
                    sourceName,
                    ContainerEntity.Type.SOURCE,
                    ContainerEntity.Contains.FOLDERS_TABLES_AND_VIEWS,
                    Collections.emptyList())));

    DATAPLANE_SOURCE_CONTAINERS.forEach(
        containerEntity ->
            DATAPLANE_FOLDER_CONTAINERS.add(
                new ContainerEntity(
                    "Folder1",
                    ContainerEntity.Type.EXPLICIT_FOLDER,
                    ContainerEntity.Contains.FOLDERS_TABLES_AND_VIEWS,
                    containerEntity.getFullPath())));

    DATAPLANE_FOLDER_CONTAINERS.forEach(
        containerEntity ->
            DATAPLANE_SUB_FOLDER_CONTAINERS.add(
                new ContainerEntity(
                    "subFolderInFolder1",
                    ContainerEntity.Type.EXPLICIT_FOLDER,
                    ContainerEntity.Contains.FOLDERS_TABLES_AND_VIEWS,
                    containerEntity.getFullPath())));

    DATAPLANE_SOURCE_CONTAINERS.forEach(
        containerEntity ->
            DATAPLANE_EMPTY_FOLDER_CONTAINERS.add(
                new ContainerEntity(
                    "emptyFolder2",
                    ContainerEntity.Type.EXPLICIT_FOLDER,
                    ContainerEntity.Contains.EMPTY,
                    containerEntity.getFullPath())));
  }

  // Had to initiate the Nessie entities here as Nessie instance removes entities before each test
  // case.
  // TODO: Can we change this behavior so that we can move it to @BeforeAll?
  @BeforeEach
  public void createFoldersTablesViews() throws Exception {
    // Entities created under a Nessie source will be shared by other Nessie sources too as
    // underneath they are pointing to same Nessie instance
    createEntitiesForDataplanePluginContainer(DATAPLANE_SOURCE_CONTAINERS.get(0));
    createEntitiesForDataplanePluginContainer(DATAPLANE_FOLDER_CONTAINERS.get(0));
    createEntitiesForDataplanePluginContainer(DATAPLANE_SUB_FOLDER_CONTAINERS.get(0));
    createEntitiesForDataplanePluginContainer(DATAPLANE_EMPTY_FOLDER_CONTAINERS.get(0));
  }

  private static void createEntitiesForNASPluginContainer() throws Exception {
    createEntitiesForNASPluginContainer(NAS_SOURCE_CONTAINER);
    createEntitiesForNASPluginContainer(NAS_FOLDER_CONTAINER);
    createEntitiesForNASPluginContainer(NAS_SUB_FOLDER_CONTAINER);
  }

  private void createEntitiesForDataplanePluginContainer(ContainerEntity container)
      throws Exception {
    switch (container.getType()) {
      case SOURCE:
        // Source creation is already done so intentional break through
        break;
      case EXPLICIT_FOLDER:
        runSQL(createFolderQuery(DATAPLANE_PLUGIN_NAME, container.getPathWithoutRoot()));
        break;
      default:
        throw new IllegalStateException("Unexpected value: " + container.getType());
    }

    switch (container.getContains()) {
      case FOLDERS_TABLES_AND_VIEWS:
        createTablesAndViewsInDataplanePluginContainer(container);
        break;
      case EMPTY:
        break;
      default:
        throw new IllegalStateException("Unexpected value: " + container.getContains());
    }
  }

  private static void createEntitiesForNASPluginContainer(ContainerEntity container)
      throws Exception {
    switch (container.getContains()) {
      case FOLDERS_AND_TABLES:
      case TABLES_ONLY:
        createTablesInNASPluginContainer(container);
        break;
      case EMPTY:
        break;
      default:
        throw new IllegalStateException("Unexpected value: " + container.getContains());
    }
  }

  private static void createTablesAndViewsInDataplanePluginContainer(ContainerEntity container)
      throws Exception {
    runSQL(createEmptyTableQuery(container.getChildPathWithoutRoot("tableAFirst")));
    runSQL(createEmptyTableQuery(container.getChildPathWithoutRoot("tableBSecond")));
    String sourceName =
        container.getParentPath().isEmpty()
            ? container.getName()
            : container.getParentPath().get(0);
    runSQL(
        createViewSelectQuery(
            sourceName, container.getChildPathWithoutRoot("viewCThird"), "SELECT 1"));
    runSQL(
        createViewSelectQuery(
            sourceName, container.getChildPathWithoutRoot("viewDFourth"), "SELECT 1"));
  }

  private static void createTablesInNASPluginContainer(ContainerEntity container) throws Exception {
    String sourceName =
        container.getParentPath().isEmpty()
            ? container.getName()
            : container.getParentPath().get(0);
    runSQL(createEmptyTableQuery(sourceName, container.getChildPathWithoutRoot("tableAFirst")));
    runSQL(createEmptyTableQuery(sourceName, container.getChildPathWithoutRoot("tableBSecond")));
    // Sources except Nessie based sources do not store views within it
  }

  private static Stream<Arguments> schemataQueryArguments() {
    List<List<String>> expectedResult1 = new ArrayList<>();
    DATAPLANE_SOURCE_CONTAINERS.forEach(
        containerEntity -> expectedResult1.add(containerEntity.getExpectedSchemata()));

    List<List<String>> expectedResult2 = new ArrayList<>();
    expectedResult2.add(DATAPLANE_SOURCE_CONTAINERS.get(1).getExpectedSchemata());

    List<List<String>> expectedResult3 = new ArrayList<>();
    DATAPLANE_SOURCE_CONTAINERS.forEach(
        containerEntity -> expectedResult3.add(containerEntity.getExpectedSchemata()));
    DATAPLANE_FOLDER_CONTAINERS.forEach(
        containerEntity -> expectedResult3.add(containerEntity.getExpectedSchemata()));
    DATAPLANE_SUB_FOLDER_CONTAINERS.forEach(
        containerEntity -> expectedResult3.add(containerEntity.getExpectedSchemata()));
    DATAPLANE_EMPTY_FOLDER_CONTAINERS.forEach(
        containerEntity -> expectedResult3.add(containerEntity.getExpectedSchemata()));

    List<List<String>> expectedResult4 = new ArrayList<>();
    expectedResult4.add(NAS_SOURCE_CONTAINER.getExpectedSchemata());
    expectedResult4.add(NAS_FOLDER_CONTAINER.getExpectedSchemata());
    expectedResult4.add(NAS_SUB_FOLDER_CONTAINER.getExpectedSchemata());

    List<List<String>> expectedResult5 = new ArrayList<>();
    expectedResult5.add(NAS_FOLDER_CONTAINER.getExpectedSchemata());
    expectedResult5.add(NAS_SUB_FOLDER_CONTAINER.getExpectedSchemata());

    List<List<String>> expectedResult6 = new ArrayList<>();
    expectedResult6.add(DATAPLANE_SOURCE_CONTAINERS.get(0).getExpectedSchemata());
    expectedResult6.add(DATAPLANE_SOURCE_CONTAINERS.get(1).getExpectedSchemata());

    return Stream.of(
        Arguments.of("dataplane%plugin", 4, expectedResult1),
        Arguments.of("dataplane\\%plugin", 1, expectedResult2),
        Arguments.of("dataplane%plugin%", 16, expectedResult3),
        Arguments.of("nas%plugin%", 3, expectedResult4),
        Arguments.of("nas_plugin%Folder_", 2, expectedResult5),
        Arguments.of("dataplane_plugin", 2, expectedResult6),
        Arguments.of(
            "dataplane\\_plugin",
            1,
            Collections.singletonList(DATAPLANE_SOURCE_CONTAINERS.get(0).getExpectedSchemata())),
        Arguments.of(
            "dataplane\\_plugin%subFolderInFolder_",
            1,
            Collections.singletonList(
                DATAPLANE_SUB_FOLDER_CONTAINERS.get(0).getExpectedSchemata())),
        Arguments.of("nas_plugin%Folder\\_", 0, Collections.emptyList()));
  }

  private static Stream<Arguments> schemataQueryWithEscapeKeywordArguments() {
    return Stream.of(
        Arguments.of(
            "dataplane_%plugin",
            "%",
            2,
            Arrays.asList(
                DATAPLANE_SOURCE_CONTAINERS.get(0).getExpectedSchemata(),
                DATAPLANE_SOURCE_CONTAINERS.get(1).getExpectedSchemata())),
        Arguments.of(
            "nas_%plugin",
            "%", 1, Collections.singletonList(NAS_SOURCE_CONTAINER.getExpectedSchemata())),
        Arguments.of("dataplane_plugin", "_", 0, Collections.emptyList()),
        Arguments.of("dataplane\\_plugin", "_", 0, Collections.emptyList()));
  }

  private static Stream<Arguments> tablesQueryWithEscapeKeywordArguments() {
    return Stream.of(
        Arguments.of(
            "dataplane_%plugin",
            "%",
            8,
            Streams.concat(
                    DATAPLANE_SOURCE_CONTAINERS.get(0).getExpectedTablesIncludingViews().stream(),
                    DATAPLANE_SOURCE_CONTAINERS.get(1).getExpectedTablesIncludingViews().stream())
                .collect(Collectors.toList())),
        Arguments.of(
            "nas_%plugin",
            "%",
            2,
            // NAS and other sources except Nessie source do not store views
            NAS_SOURCE_CONTAINER.getExpectedTablesForNonNessieContainers()),
        Arguments.of("dataplane_plugin", "_", 0, Collections.emptyList()),
        Arguments.of("dataplane\\_plugin", "_", 0, Collections.emptyList()));
  }

  private static Stream<Arguments> tablesQueryArguments() {
    List<List<String>> expectedResult3 = new ArrayList<>();
    DATAPLANE_FOLDER_CONTAINERS.forEach(
        containerEntity ->
            expectedResult3.addAll(containerEntity.getExpectedTablesIncludingViews()));
    DATAPLANE_SUB_FOLDER_CONTAINERS.forEach(
        containerEntity ->
            expectedResult3.addAll(containerEntity.getExpectedTablesIncludingViews()));

    List<List<String>> expectedResult5 = new ArrayList<>();
    DATAPLANE_SOURCE_CONTAINERS.forEach(
        containerEntity ->
            expectedResult5.addAll(containerEntity.getExpectedTablesIncludingViews()));
    expectedResult5.addAll(NAS_SOURCE_CONTAINER.getExpectedTablesForNonNessieContainers());

    return Stream.of(
        Arguments.of(
            "dataplane\\%plugin",
            4, DATAPLANE_SOURCE_CONTAINERS.get(1).getExpectedTablesIncludingViews()),
        Arguments.of(
            "dataplane\\_plugin",
            4,
            DATAPLANE_SOURCE_CONTAINERS.get(0).getExpectedTablesIncludingViews()),
        Arguments.of("dataplane%plugin%Folder%", 32, expectedResult3),
        Arguments.of("dataplane_\\%plugin", 0, Collections.emptyList()),
        Arguments.of("%_plugin", 18, expectedResult5),
        Arguments.of(
            "dataplane_plugin",
            8,
            Streams.concat(
                    DATAPLANE_SOURCE_CONTAINERS.get(0).getExpectedTablesIncludingViews().stream(),
                    DATAPLANE_SOURCE_CONTAINERS.get(1).getExpectedTablesIncludingViews().stream())
                .collect(Collectors.toList())),
        Arguments.of(
            "dataplane\\_plugin%subFolderInFolder_",
            4, DATAPLANE_SUB_FOLDER_CONTAINERS.get(0).getExpectedTablesIncludingViews()),
        Arguments.of(
            "nas%plugin%Folder%",
            4,
            Streams.concat(
                    // NAS and other sources except Nessie source do not store views
                    NAS_FOLDER_CONTAINER.getExpectedTablesForNonNessieContainers().stream(),
                    NAS_SUB_FOLDER_CONTAINER.getExpectedTablesForNonNessieContainers().stream())
                .collect(Collectors.toList())),
        Arguments.of(
            "nas_plugin.Folder_",
            2,
            NAS_FOLDER_CONTAINER.getExpectedTablesForNonNessieContainers()),
        Arguments.of("nas_plugin.Folder\\_", 0, Collections.emptyList()));
  }

  private static Stream<Arguments> columnsQueryArguments() {
    return Stream.of(
        Arguments.of("dataplane\\%plugin", 8, DATAPLANE_SOURCE_NAME_WITH_PERCENT),
        Arguments.of("dataplane\\_plugin", 8, DATAPLANE_SOURCE_NAME_WITH_UNDERSCORE),
        Arguments.of("dataplane_plugin", 16, DATAPLANE_SOURCE_PREFIX),
        Arguments.of("dataplane%plugin%Folder%", 64, DATAPLANE_SOURCE_PREFIX),
        Arguments.of("nas%plugin%Folder%", 12, NAS_SOURCE_NAME),
        Arguments.of("nas_plugin.Folder_", 6, NAS_SOURCE_NAME),
        Arguments.of("nas_plugin.Folder\\_", 0, EMPTY_STRING),
        Arguments.of("dataplane_\\%plugin", 0, EMPTY_STRING),
        Arguments.of(
            "dataplane\\_plugin%subFolderInFolder_", 8, DATAPLANE_SOURCE_NAME_WITH_UNDERSCORE));
  }

  private static Stream<Arguments> viewsQueryArguments() {
    List<List<String>> expectedResult3 = new ArrayList<>();
    DATAPLANE_FOLDER_CONTAINERS.forEach(
        containerEntity ->
            expectedResult3.addAll(containerEntity.getExpectedViewsForSelect1Query()));
    DATAPLANE_SUB_FOLDER_CONTAINERS.forEach(
        containerEntity ->
            expectedResult3.addAll(containerEntity.getExpectedViewsForSelect1Query()));

    List<List<String>> expectedResult6 = new ArrayList<>();
    expectedResult6.addAll(DATAPLANE_SOURCE_CONTAINERS.get(0).getExpectedViewsForSelect1Query());
    expectedResult6.addAll(DATAPLANE_SOURCE_CONTAINERS.get(1).getExpectedViewsForSelect1Query());

    return Stream.of(
        Arguments.of(
            "dataplane\\%plugin",
            2, DATAPLANE_SOURCE_CONTAINERS.get(1).getExpectedViewsForSelect1Query()),
        Arguments.of(
            "dataplane\\_plugin",
            2,
            DATAPLANE_SOURCE_CONTAINERS.get(0).getExpectedViewsForSelect1Query()),
        Arguments.of("dataplane%plugin%Folder%", 16, expectedResult3),
        Arguments.of(
            "nas%plugin%",
            0,
            // NAS and other sources except Nessie source do not store views
            Collections.emptyList()),
        Arguments.of("dataplane_\\%plugin", 0, Collections.emptyList()),
        Arguments.of("dataplane_plugin", 4, expectedResult6),
        Arguments.of(
            "dataplane\\_plugin%subFolderInFolder_",
            2, DATAPLANE_SUB_FOLDER_CONTAINERS.get(0).getExpectedViewsForSelect1Query()));
  }

  private static Stream<Arguments> tablesQueryForTableTypeConditionArguments() {
    List<List<String>> expectedResult1 = new ArrayList<>();
    DATAPLANE_SOURCE_CONTAINERS.forEach(
        containerEntity -> expectedResult1.addAll(containerEntity.getExpectedTablesWithoutViews()));
    DATAPLANE_FOLDER_CONTAINERS.forEach(
        containerEntity -> expectedResult1.addAll(containerEntity.getExpectedTablesWithoutViews()));
    DATAPLANE_SUB_FOLDER_CONTAINERS.forEach(
        containerEntity -> expectedResult1.addAll(containerEntity.getExpectedTablesWithoutViews()));
    expectedResult1.addAll(NAS_SOURCE_CONTAINER.getExpectedTablesForNonNessieContainers());
    expectedResult1.addAll(NAS_FOLDER_CONTAINER.getExpectedTablesForNonNessieContainers());
    expectedResult1.addAll(NAS_SUB_FOLDER_CONTAINER.getExpectedTablesForNonNessieContainers());

    List<List<String>> expectedResult2 = new ArrayList<>();
    expectedResult2.addAll(NAS_SOURCE_CONTAINER.getExpectedTablesForNonNessieContainers());
    expectedResult2.addAll(NAS_FOLDER_CONTAINER.getExpectedTablesForNonNessieContainers());
    expectedResult2.addAll(NAS_SUB_FOLDER_CONTAINER.getExpectedTablesForNonNessieContainers());

    return Stream.of(
        Arguments.of("%plugin%", 30, expectedResult1),
        Arguments.of("nas%plugin%", 6, expectedResult2),
        Arguments.of("dataplane_\\%plugin", 0, Collections.emptyList()));
  }

  @ParameterizedTest(name = "{index} {0} {1}")
  @MethodSource("schemataQueryArguments")
  public void testSchemataQuery(String regex, int expectedRows, List<List<String>> expectedSchemata)
      throws Exception {
    // Test SQL: SELECT * from INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME LIKE 'dataplane%plugin'
    List<List<String>> rows =
        runSqlWithResults(
            String.format(
                "SELECT * FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME LIKE '%s'", regex));
    assertThat(rows.size()).isEqualTo(expectedRows);
    assertThat(rows).containsExactlyInAnyOrderElementsOf(expectedSchemata);
  }

  @ParameterizedTest(name = "{index} {0} {1} {2}")
  @MethodSource("schemataQueryWithEscapeKeywordArguments")
  public void testSchemataQueryWithEscape(
      String regex, String escapeKeyword, int expectedRows, List<List<String>> expectedSchemata)
      throws Exception {
    // Test SQL: SELECT * from INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME LIKE
    // 'dataplane_%plugin' ESCAPE '%'
    List<List<String>> rows =
        runSqlWithResults(
            String.format(
                "SELECT * FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME LIKE '%s' ESCAPE '%s'",
                regex, escapeKeyword));
    assertThat(rows.size()).isEqualTo(expectedRows);
    assertThat(rows).containsExactlyInAnyOrderElementsOf(expectedSchemata);
  }

  @ParameterizedTest(name = "{index} {0} {1} {2}")
  @MethodSource("tablesQueryWithEscapeKeywordArguments")
  public void testTablesQueryWithEscape(
      String regex, String escapeKeyword, int expectedRows, List<List<String>> expectedSchemata)
      throws Exception {
    // Test SQL: SELECT * from INFORMATION_SCHEMA."TABLES" WHERE TABLE_SCHEMA LIKE
    // 'dataplane_%plugin' ESCAPE '%'
    List<List<String>> rows =
        runSqlWithResults(
            String.format(
                "SELECT * FROM INFORMATION_SCHEMA.\"TABLES\" WHERE TABLE_SCHEMA LIKE '%s' ESCAPE '%s'",
                regex, escapeKeyword));
    assertThat(rows.size()).isEqualTo(expectedRows);
    assertThat(rows).containsExactlyInAnyOrderElementsOf(expectedSchemata);
  }

  @ParameterizedTest(name = "{index} {0} {1}")
  @MethodSource("tablesQueryArguments")
  public void testTablesQuery(String regex, int expectedRows, List<List<String>> expectedSchemata)
      throws Exception {
    // Test SQL: SELECT * from INFORMATION_SCHEMA."TABLES" WHERE TABLE_SCHEMA LIKE
    // 'dataplane\%plugin'
    List<List<String>> rows =
        runSqlWithResults(
            String.format(
                "SELECT * FROM INFORMATION_SCHEMA.\"TABLES\" WHERE TABLE_SCHEMA LIKE '%s'", regex));
    assertThat(rows.size()).isEqualTo(expectedRows);
    assertThat(rows).containsExactlyInAnyOrderElementsOf(expectedSchemata);
  }

  @ParameterizedTest(name = "{index} {0} {1}")
  @MethodSource("columnsQueryArguments")
  public void testColumnsQuery(String regex, int expectedRows, String expectedSourceName)
      throws Exception {
    // Test SQL: SELECT * from INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA LIKE
    // 'dataplane\%plugin'
    List<List<String>> rows =
        runSqlWithResults(
            String.format(
                "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA LIKE '%s'", regex));
    assertThat(rows.size()).isEqualTo(expectedRows);
    assertThat(rows).doesNotHaveDuplicates();
    if (!EMPTY_STRING.equals(expectedSourceName)) {
      assertThat(rows).anyMatch(row -> row.get(1).startsWith(expectedSourceName));
    }
  }

  @ParameterizedTest(name = "{index} {0} {1}")
  @MethodSource("viewsQueryArguments")
  public void testViewsQuery(String regex, int expectedRows, List<List<String>> expectedSchemata)
      throws Exception {
    // Test SQL: SELECT * from INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA LIKE 'dataplane\%plugin'
    List<List<String>> rows =
        runSqlWithResults(
            String.format(
                "SELECT * FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA LIKE '%s'", regex));
    assertThat(rows.size()).isEqualTo(expectedRows);
    assertThat(rows).containsExactlyInAnyOrderElementsOf(expectedSchemata);
  }

  @ParameterizedTest(name = "{index} {0} {1}")
  @MethodSource("tablesQueryForTableTypeConditionArguments")
  public void testTablesQueryForTableTypeCondition(
      String regex, int expectedRows, List<List<String>> expectedSchemata) throws Exception {
    // Test SQL: SELECT * from INFORMATION_SCHEMA."TABLES" WHERE TABLE_TYPE = 'TABLE'
    List<List<String>> rows =
        runSqlWithResults(
            String.format(
                "SELECT * FROM INFORMATION_SCHEMA.\"TABLES\" WHERE "
                    + "TABLE_TYPE = 'TABLE' AND TABLE_SCHEMA LIKE '%s'",
                regex));
    assertThat(rows.size()).isEqualTo(expectedRows);
    assertThat(rows).containsExactlyInAnyOrderElementsOf(expectedSchemata);
  }

  @AfterAll
  public static void cleanup() {
    cleanupForDeletingSources(sourceNamesWithConnectionConf);
  }
}
