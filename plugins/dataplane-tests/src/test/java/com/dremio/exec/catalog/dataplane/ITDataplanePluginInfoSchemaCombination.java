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
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createViewQuery;
import static org.assertj.core.api.Assertions.assertThat;

import com.dremio.exec.catalog.dataplane.test.ContainerEntity;
import com.dremio.exec.catalog.dataplane.test.ITDataplanePluginTestSetup;
import com.google.common.collect.Streams;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.model.Namespace;

/**
 *
 *
 * <pre>
 * For this combined test, we construct a series of tables and views. It is
 * important to test these together to make sure that things don't leak
 * outside where they are expected. Isolated tests cannot capture this
 * easily. We have combinations of tables and views at various levels: at
 * the root level of the source, inside a folder at the root level, and also
 * in sub-folders. Combinations of explicit and implicit folders are also
 * used.
 *
 * Structure of source, folders, tables, and views:
 *
 * Note: Table and view names are longer (e.g. tableAFirst vs. just tableA) than
 * you might initially expect. This is so that we can do tests with "names
 * contains" tests like "... WHERE table_name LIKE '%ablA%'". Table and view
 * names are also intentionally camelCased for case-sensitivity testing.
 *
 * TODO: DX-58674 other folder/table/view names incl special characters \ * ? . / and more
 * TODO: DX-58674 also add tests that explicitly account for case-sensitivity
 *
 * dataplane_test
 *  - tableAFirst
 *  - tableBSecond
 *  - viewCThird
 *  - viewDFourth
 *  - explicitFolder1
 *     - tableA
 *     - tableBSecond
 *     - viewCThird
 *     - viewDFourth
 *     - explicitFolderInExplicitParent3
 *        - tableAFirst
 *        - tableBSecond
 *        - viewCThird
 *        - viewDFourth
 *  - emptyExplicitFolder7
 * </pre>
 */
public class ITDataplanePluginInfoSchemaCombination extends ITDataplanePluginTestSetup {

  private static final ContainerEntity sourceRoot =
      new ContainerEntity(
          DATAPLANE_PLUGIN_NAME,
          ContainerEntity.Type.SOURCE,
          ContainerEntity.Contains.FOLDERS_AND_VIEWS,
          Collections.emptyList());
  private static final ContainerEntity explicitFolder1 =
      new ContainerEntity(
          "explicitFolder1",
          ContainerEntity.Type.EXPLICIT_FOLDER,
          ContainerEntity.Contains.FOLDERS_AND_VIEWS,
          sourceRoot.getFullPath());
  private static final ContainerEntity explicitFolderInExplicitParent3 =
      new ContainerEntity(
          "explicitFolderInExplicitParent3",
          ContainerEntity.Type.EXPLICIT_FOLDER,
          ContainerEntity.Contains.FOLDERS_AND_VIEWS,
          explicitFolder1.getFullPath());
  private static final ContainerEntity emptyExplicitFolder7 =
      new ContainerEntity(
          "emptyExplicitFolder7",
          ContainerEntity.Type.EXPLICIT_FOLDER,
          ContainerEntity.Contains.EMPTY,
          sourceRoot.getFullPath());

  // Had to initiate the Nessie entities here as Nessie instance removes entities before each test
  // case.
  // TODO: Can we change this behavior so that we can move it to @BeforeAll?
  @BeforeEach
  public void createFoldersTablesViews() throws Exception {
    createEntitiesForContainer(sourceRoot);
    createEntitiesForContainer(explicitFolder1);
    createEntitiesForContainer(explicitFolderInExplicitParent3);
    createEntitiesForContainer(emptyExplicitFolder7);
  }

  private void createEntitiesForContainer(ContainerEntity container) throws Exception {
    switch (container.getType()) {
      case SOURCE:
        // Intentional fallthrough
      case IMPLICIT_FOLDER:
        break;
      case EXPLICIT_FOLDER:
        getNessieApi()
            .createNamespace()
            .namespace(Namespace.of(container.getPathWithoutRoot()))
            .refName("main")
            .create();
        break;
      default:
        throw new IllegalStateException("Unexpected value: " + container.getType());
    }

    switch (container.getContains()) {
      case FOLDERS_AND_VIEWS:
        createTablesAndViewsInContainer(container);
        break;
      case EMPTY:
        break;
      default:
        throw new IllegalStateException("Unexpected value: " + container.getContains());
    }
  }

  private static void createTablesAndViewsInContainer(ContainerEntity container) throws Exception {
    runSQL(createEmptyTableQuery(container.getChildPathWithoutRoot("tableAFirst")));
    runSQL(createEmptyTableQuery(container.getChildPathWithoutRoot("tableBSecond")));
    runSQL(
        createViewQuery(
            container.getChildPathWithoutRoot("viewCThird"),
            container.getChildPathWithoutRoot("tableAFirst")));
    runSQL(
        createViewQuery(
            container.getChildPathWithoutRoot("viewDFourth"),
            container.getChildPathWithoutRoot("tableBSecond")));
  }

  @Test
  public void tablesAll() throws Exception {
    assertThat(
            runSqlWithResults(
                String.format(
                    "select * from INFORMATION_SCHEMA.\"TABLES\" WHERE table_schema LIKE '%s%%'",
                    sourceRoot.asSqlIdentifier())))
        .containsExactlyInAnyOrderElementsOf(
            Streams.concat(
                    sourceRoot.getExpectedTablesIncludingViews().stream(),
                    explicitFolder1.getExpectedTablesIncludingViews().stream(),
                    explicitFolderInExplicitParent3.getExpectedTablesIncludingViews().stream()
                    // No tables expected for emptyExplicitFolder7
                    )
                .collect(Collectors.toList()));
  }

  private static Stream<Arguments> tablesWhereSchemaLikeTopLevelFolderArguments() {
    return Stream.of(
        Arguments.of(
            explicitFolder1,
            Streams.concat(
                    explicitFolder1.getExpectedTablesIncludingViews().stream(),
                    explicitFolderInExplicitParent3.getExpectedTablesIncludingViews().stream())
                .collect(Collectors.toList())),
        Arguments.of(emptyExplicitFolder7, Collections.emptyList()));
  }

  @ParameterizedTest(name = "{index} {0}")
  @MethodSource("tablesWhereSchemaLikeTopLevelFolderArguments")
  public void tablesWhereSchemaLikeTopLevelFolder(
      ContainerEntity container, List<List<String>> expectedTables) throws Exception {
    assertThat(
            runSqlWithResults(
                String.format(
                    "select * from INFORMATION_SCHEMA.\"TABLES\" WHERE table_schema LIKE '%s%%'",
                    container.asSqlIdentifier())))
        .containsExactlyInAnyOrderElementsOf(expectedTables);
  }

  private static Stream<Arguments> tablesWhereSchemaLikeSubfolderArguments() {
    return Stream.of(Arguments.of(explicitFolderInExplicitParent3));
  }

  @ParameterizedTest(name = "{index} {0}")
  @MethodSource("tablesWhereSchemaLikeSubfolderArguments")
  public void tablesWhereSchemaLikeSubfolder(ContainerEntity container) throws Exception {
    assertThat(
            runSqlWithResults(
                String.format(
                    "select * from INFORMATION_SCHEMA.\"TABLES\" WHERE table_schema LIKE '%%%s%%'",
                    container.getName())))
        .containsExactlyInAnyOrderElementsOf(container.getExpectedTablesIncludingViews());
  }

  @Test
  public void tablesWhereSchemaContains() throws Exception {
    assertThat(
            runSqlWithResults(
                "select * from INFORMATION_SCHEMA.\"TABLES\" WHERE table_schema LIKE '%FolderIn%'"))
        .containsExactlyInAnyOrderElementsOf(
            Streams.concat(
                    explicitFolderInExplicitParent3.getExpectedTablesIncludingViews().stream())
                .collect(Collectors.toList()));
  }

  @Test
  public void tablesWhereNameStartsWith() throws Exception {
    assertThat(
            runSqlWithResults(
                String.format(
                    "select * from INFORMATION_SCHEMA.\"TABLES\" WHERE table_schema LIKE '%s%%' AND table_name LIKE 'tab%%'",
                    sourceRoot.asSqlIdentifier())))
        .containsExactlyInAnyOrderElementsOf(
            Streams.concat(
                    sourceRoot.getExpectedTablesWithoutViews().stream(),
                    explicitFolder1.getExpectedTablesWithoutViews().stream(),
                    explicitFolderInExplicitParent3.getExpectedTablesWithoutViews().stream())
                .collect(Collectors.toList()));
  }

  @Test
  public void tablesWhereNameStartsWithTableA() throws Exception {
    assertThat(
            runSqlWithResults(
                String.format(
                    "select * from INFORMATION_SCHEMA.\"TABLES\" WHERE table_schema LIKE '%s%%' AND table_name LIKE 'tableA%%'",
                    sourceRoot.asSqlIdentifier())))
        .containsExactlyInAnyOrderElementsOf(
            Streams.concat(
                    sourceRoot.getTableAOnly().stream(),
                    explicitFolder1.getTableAOnly().stream(),
                    explicitFolderInExplicitParent3.getTableAOnly().stream())
                .collect(Collectors.toList()));
  }

  @Test
  public void tablesWhereNameContains() throws Exception {
    assertThat(
            runSqlWithResults(
                "select * from INFORMATION_SCHEMA.\"TABLES\" WHERE table_name LIKE '%bleB%'"))
        .containsExactlyInAnyOrderElementsOf(
            Streams.concat(
                    sourceRoot.getTableBOnly().stream(),
                    explicitFolder1.getTableBOnly().stream(),
                    explicitFolderInExplicitParent3.getTableBOnly().stream())
                .collect(Collectors.toList()));
  }

  @Test
  public void viewsAll() throws Exception {
    assertThat(
            runSqlWithResults(
                String.format(
                    "select * from INFORMATION_SCHEMA.VIEWS WHERE table_schema LIKE '%s%%'",
                    sourceRoot.asSqlIdentifier())))
        .containsExactlyInAnyOrderElementsOf(
            Streams.concat(
                    sourceRoot.getExpectedViews().stream(),
                    explicitFolder1.getExpectedViews().stream(),
                    explicitFolderInExplicitParent3.getExpectedViews().stream()
                    // No views expected for emptyExplicitFolder7
                    )
                .collect(Collectors.toList()));
  }

  private static Stream<Arguments> viewsWhereSchemaLikeTopLevelFolderArguments() {
    return Stream.of(
        Arguments.of(
            explicitFolder1,
            Streams.concat(
                    explicitFolder1.getExpectedViews().stream(),
                    explicitFolderInExplicitParent3.getExpectedViews().stream())
                .collect(Collectors.toList())),
        Arguments.of(emptyExplicitFolder7, Collections.emptyList()));
  }

  @ParameterizedTest(name = "{index} {0}")
  @MethodSource("viewsWhereSchemaLikeTopLevelFolderArguments")
  public void viewsWhereSchemaLikeTopLevelFolder(
      ContainerEntity container, List<List<String>> expectedViews) throws Exception {
    assertThat(
            runSqlWithResults(
                String.format(
                    "select * from INFORMATION_SCHEMA.VIEWS WHERE table_schema LIKE '%s%%'",
                    container.asSqlIdentifier())))
        .containsExactlyInAnyOrderElementsOf(expectedViews);
  }

  private static Stream<Arguments> viewsWhereSchemaLikeSubfolderArguments() {
    return Stream.of(Arguments.of(explicitFolderInExplicitParent3));
  }

  @ParameterizedTest(name = "{index} {0}")
  @MethodSource("viewsWhereSchemaLikeSubfolderArguments")
  public void viewsWhereSchemaLikeSubfolder(ContainerEntity container) throws Exception {
    assertThat(
            runSqlWithResults(
                String.format(
                    "select * from INFORMATION_SCHEMA.VIEWS WHERE table_schema LIKE '%%%s%%'",
                    container.asSqlIdentifier())))
        .containsExactlyInAnyOrderElementsOf(container.getExpectedViews());
  }

  @Test
  public void viewsWhereSchemaContains() throws Exception {
    assertThat(
            runSqlWithResults(
                "select * from INFORMATION_SCHEMA.VIEWS WHERE table_schema LIKE '%FolderIn%'"))
        .containsExactlyInAnyOrderElementsOf(
            Streams.concat(explicitFolderInExplicitParent3.getExpectedViews().stream())
                .collect(Collectors.toList()));
  }

  @Test
  public void viewsWhereNameStartsWith() throws Exception {
    assertThat(
            runSqlWithResults(
                String.format(
                    "select * from INFORMATION_SCHEMA.VIEWS WHERE table_schema LIKE '%s%%' AND table_name LIKE 'vie%%'",
                    sourceRoot.asSqlIdentifier())))
        .containsExactlyInAnyOrderElementsOf(
            Streams.concat(
                    sourceRoot.getExpectedViews().stream(),
                    explicitFolder1.getExpectedViews().stream(),
                    explicitFolderInExplicitParent3.getExpectedViews().stream())
                .collect(Collectors.toList()));
  }

  @Test
  public void viewsWhereNameStartsWithViewC() throws Exception {
    assertThat(
            runSqlWithResults(
                String.format(
                    "select * from INFORMATION_SCHEMA.VIEWS WHERE table_schema LIKE '%s%%' AND table_name LIKE 'viewC%%'",
                    sourceRoot.asSqlIdentifier())))
        .containsExactlyInAnyOrderElementsOf(
            Streams.concat(
                    sourceRoot.getViewCOnly().stream(),
                    explicitFolder1.getViewCOnly().stream(),
                    explicitFolderInExplicitParent3.getViewCOnly().stream())
                .collect(Collectors.toList()));
  }

  @Test
  public void viewsWhereNameContains() throws Exception {
    assertThat(
            runSqlWithResults(
                "select * from INFORMATION_SCHEMA.VIEWS WHERE table_name LIKE '%ewD%'"))
        .containsExactlyInAnyOrderElementsOf(
            Streams.concat(
                    sourceRoot.getViewDOnly().stream(),
                    explicitFolder1.getViewDOnly().stream(),
                    explicitFolderInExplicitParent3.getViewDOnly().stream())
                .collect(Collectors.toList()));
  }

  @Test
  public void schemataAll() throws Exception {
    assertThat(
            runSqlWithResults(
                String.format(
                    "select * from INFORMATION_SCHEMA.SCHEMATA WHERE schema_name LIKE '%s%%'",
                    sourceRoot.asSqlIdentifier())))
        .containsExactlyInAnyOrder(
            sourceRoot.getExpectedSchemata(),
            explicitFolder1.getExpectedSchemata(),
            explicitFolderInExplicitParent3.getExpectedSchemata(),
            emptyExplicitFolder7.getExpectedSchemata());
  }

  private static Stream<Arguments> schemataWhereSchemaLikeTopLevelFolderArguments() {
    return Stream.of(
        Arguments.of(
            explicitFolder1,
            Arrays.asList(
                explicitFolder1.getExpectedSchemata(),
                explicitFolderInExplicitParent3.getExpectedSchemata())),
        Arguments.of(
            emptyExplicitFolder7,
            Collections.singletonList(emptyExplicitFolder7.getExpectedSchemata())));
  }

  @ParameterizedTest(name = "{index} {0}")
  @MethodSource("schemataWhereSchemaLikeTopLevelFolderArguments")
  public void schemataWhereSchemaLikeTopLevelFolder(
      ContainerEntity container, List<List<String>> expectedSchemata) throws Exception {
    assertThat(
            runSqlWithResults(
                String.format(
                    "select * from INFORMATION_SCHEMA.SCHEMATA WHERE schema_name LIKE '%s%%'",
                    container.asSqlIdentifier())))
        .containsExactlyInAnyOrderElementsOf(expectedSchemata);
  }

  private static Stream<Arguments> schemataWhereSchemaLikeSubfolderArguments() {
    return Stream.of(
        Arguments.of(
            explicitFolderInExplicitParent3,
            Collections.singletonList(explicitFolderInExplicitParent3.getExpectedSchemata())));
  }

  @ParameterizedTest(name = "{index} {0}")
  @MethodSource("schemataWhereSchemaLikeSubfolderArguments")
  public void schemataWhereSchemaLikeSubfolder(
      ContainerEntity container, List<List<String>> expectedSchemata) throws Exception {
    assertThat(
            runSqlWithResults(
                String.format(
                    "select * from INFORMATION_SCHEMA.SCHEMATA WHERE schema_name LIKE '%%%s%%'",
                    container.asSqlIdentifier())))
        .containsExactlyInAnyOrderElementsOf(expectedSchemata);
  }

  @Test
  public void schemataWhereSchemaContains() throws Exception {
    assertThat(
            runSqlWithResults(
                String.format(
                    "select * from INFORMATION_SCHEMA.SCHEMATA WHERE schema_name LIKE '%s%%' AND schema_name LIKE '%%Fold%%'",
                    sourceRoot.asSqlIdentifier())))
        .containsExactlyInAnyOrder(
            // sourceRoot doesn't match the filter
            explicitFolder1.getExpectedSchemata(),
            explicitFolderInExplicitParent3.getExpectedSchemata(),
            emptyExplicitFolder7.getExpectedSchemata());
  }

  // TODO columns tests here too? Covered by InfoSchemaTestCases?
}
