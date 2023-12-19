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

import static com.dremio.exec.catalog.dataplane.ContainerEntity.tableAFirst;
import static com.dremio.exec.catalog.dataplane.ContainerEntity.tableBSecond;
import static com.dremio.exec.catalog.dataplane.ContainerEntity.viewCThird;
import static com.dremio.exec.catalog.dataplane.ContainerEntity.viewDFourth;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.DATAPLANE_PLUGIN_NAME;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createEmptyTableQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createViewQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.folderA;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.folderB;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.tableA;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.projectnessie.model.Namespace;

import com.dremio.catalog.model.VersionContext;
import com.dremio.plugins.ExternalNamespaceEntry;
import com.google.common.collect.Streams;

/**
 * <pre>
 * Test Dataplane listEntries and listEntriesWithNested. In Nessie API V2, there are no implicit namespaces.
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
 *  - maxDepthExplicitFolder8
 *    - maxDepthExplicitFolder9
 *      - maxDepthExplicitFolder10
 *        - maxDepthExplicitFolder11
 *          - maxDepthExplicitFolder12
 *            - maxDepthExplicitFolder13
 *              - maxDepthExplicitFolder14
 *                - maxDepthExplicitFolder15
 *                  - maxDepthExplicitFolder16
 *                    - maxDepthExplicitFolder17
 *                      - maxDepthExplicitFolder18
 *                        - maxDepthExplicitFolder19
 *                          - maxDepthExplicitFolder20
 *                            - maxDepthExplicitFolder21
 *                              - maxDepthExplicitFolder22
 *                                - maxDepthExplicitFolder23
 *                                  - maxDepthExplicitFolder24
 *                                    - maxDepthExplicitFolder25
 *                                      - maxDepthExplicitFolder26
 *                                        - tableWithFortySixCharacterssssssssssssssssssss
 *  - folderA
 *    - folderB
 *      - tableA
 *  - folderB
 * </pre>
 */
public class ITDataplanePluginFolder extends ITDataplanePluginTestSetup {

  private static final VersionContext MAIN = VersionContext.ofBranch("main");

  private static final String longTableNameForMaxDepthTest = "tableWithFortySixCharacterssssssssssssssssssss";

  private static final ContainerEntity sourceRoot = new ContainerEntity(
    DATAPLANE_PLUGIN_NAME,
    ContainerEntity.Type.SOURCE,
    ContainerEntity.Contains.FOLDERS_AND_VIEWS,
    Collections.emptyList());
  private static final ContainerEntity explicitFolder1 = new ContainerEntity(
    "explicitFolder1",
    ContainerEntity.Type.EXPLICIT_FOLDER,
    ContainerEntity.Contains.FOLDERS_AND_VIEWS,
    sourceRoot.getFullPath());
  private static final ContainerEntity explicitFolderInExplicitParent3 = new ContainerEntity(
    "explicitFolderInExplicitParent3",
    ContainerEntity.Type.EXPLICIT_FOLDER,
    ContainerEntity.Contains.FOLDERS_AND_VIEWS,
    explicitFolder1.getFullPath());
  private static final ContainerEntity emptyExplicitFolder7 = new ContainerEntity(
    "emptyExplicitFolder7",
    ContainerEntity.Type.EXPLICIT_FOLDER,
    ContainerEntity.Contains.EMPTY,
    sourceRoot.getFullPath());

  private static final ContainerEntity maxDepthExplicitFolder8 = new ContainerEntity(
    "maxDepthExplicitFolder8", //23
    ContainerEntity.Type.EXPLICIT_FOLDER,
    ContainerEntity.Contains.EMPTY,
    sourceRoot.getFullPath());

  private static final ContainerEntity maxDepthExplicitFolder9 = new ContainerEntity(
    "maxDepthExplicitFolder9", //23
    ContainerEntity.Type.EXPLICIT_FOLDER,
    ContainerEntity.Contains.EMPTY,
    maxDepthExplicitFolder8.getFullPath());

  private static final ContainerEntity maxDepthExplicitFolder10 = new ContainerEntity(
    "maxDepthExplicitFolder10", //24
    ContainerEntity.Type.EXPLICIT_FOLDER,
    ContainerEntity.Contains.EMPTY,
    maxDepthExplicitFolder9.getFullPath());

  private static final ContainerEntity maxDepthExplicitFolder11 = new ContainerEntity(
    "maxDepthExplicitFolder11",
    ContainerEntity.Type.EXPLICIT_FOLDER,
    ContainerEntity.Contains.EMPTY,
    maxDepthExplicitFolder10.getFullPath());

  private static final ContainerEntity maxDepthExplicitFolder12 = new ContainerEntity(
    "maxDepthExplicitFolder12",
    ContainerEntity.Type.EXPLICIT_FOLDER,
    ContainerEntity.Contains.EMPTY,
    maxDepthExplicitFolder11.getFullPath());

  private static final ContainerEntity maxDepthExplicitFolder13 = new ContainerEntity(
    "maxDepthExplicitFolder13",
    ContainerEntity.Type.EXPLICIT_FOLDER,
    ContainerEntity.Contains.EMPTY,
    maxDepthExplicitFolder12.getFullPath());

  private static final ContainerEntity maxDepthExplicitFolder14 = new ContainerEntity(
    "maxDepthExplicitFolder14",
    ContainerEntity.Type.EXPLICIT_FOLDER,
    ContainerEntity.Contains.EMPTY,
    maxDepthExplicitFolder13.getFullPath());

  private static final ContainerEntity maxDepthExplicitFolder15 = new ContainerEntity(
    "maxDepthExplicitFolder15",
    ContainerEntity.Type.EXPLICIT_FOLDER,
    ContainerEntity.Contains.EMPTY,
    maxDepthExplicitFolder14.getFullPath());

  private static final ContainerEntity maxDepthExplicitFolder16 = new ContainerEntity(
    "maxDepthExplicitFolder16",
    ContainerEntity.Type.EXPLICIT_FOLDER,
    ContainerEntity.Contains.EMPTY,
    maxDepthExplicitFolder15.getFullPath());

  private static final ContainerEntity maxDepthExplicitFolder17 = new ContainerEntity(
    "maxDepthExplicitFolder17",
    ContainerEntity.Type.EXPLICIT_FOLDER,
    ContainerEntity.Contains.EMPTY,
    maxDepthExplicitFolder16.getFullPath());

  private static final ContainerEntity maxDepthExplicitFolder18 = new ContainerEntity(
    "maxDepthExplicitFolder18",
    ContainerEntity.Type.EXPLICIT_FOLDER,
    ContainerEntity.Contains.EMPTY,
    maxDepthExplicitFolder17.getFullPath());

  private static final ContainerEntity maxDepthExplicitFolder19 = new ContainerEntity(
    "maxDepthExplicitFolder19",
    ContainerEntity.Type.EXPLICIT_FOLDER,
    ContainerEntity.Contains.EMPTY,
    maxDepthExplicitFolder18.getFullPath());

  private static final ContainerEntity maxDepthExplicitFolder20 = new ContainerEntity(
    "maxDepthExplicitFolder20",
    ContainerEntity.Type.EXPLICIT_FOLDER,
    ContainerEntity.Contains.EMPTY,
    maxDepthExplicitFolder19.getFullPath());

  private static final ContainerEntity maxDepthExplicitFolder21 = new ContainerEntity(
    "maxDepthExplicitFolder21",
    ContainerEntity.Type.EXPLICIT_FOLDER,
    ContainerEntity.Contains.EMPTY,
    maxDepthExplicitFolder20.getFullPath());

  private static final ContainerEntity maxDepthExplicitFolder22 = new ContainerEntity(
    "maxDepthExplicitFolder22",
    ContainerEntity.Type.EXPLICIT_FOLDER,
    ContainerEntity.Contains.EMPTY,
    maxDepthExplicitFolder21.getFullPath());

  private static final ContainerEntity maxDepthExplicitFolder23 = new ContainerEntity(
    "maxDepthExplicitFolder23",
    ContainerEntity.Type.EXPLICIT_FOLDER,
    ContainerEntity.Contains.EMPTY,
    maxDepthExplicitFolder22.getFullPath());

  private static final ContainerEntity maxDepthExplicitFolder24 = new ContainerEntity(
    "maxDepthExplicitFolder24",
    ContainerEntity.Type.EXPLICIT_FOLDER,
    ContainerEntity.Contains.EMPTY,
    maxDepthExplicitFolder23.getFullPath());

  private static final ContainerEntity maxDepthExplicitFolder25 = new ContainerEntity(
    "maxDepthExplicitFolder25",
    ContainerEntity.Type.EXPLICIT_FOLDER,
    ContainerEntity.Contains.EMPTY,
    maxDepthExplicitFolder24.getFullPath());

  private static final ContainerEntity maxDepthExplicitFolder26 = new ContainerEntity(
    "maxDepthExplicitFolder26",
    ContainerEntity.Type.EXPLICIT_FOLDER,
    ContainerEntity.Contains.MAX_KEY_TABLE,
    maxDepthExplicitFolder25.getFullPath());
  private static final ContainerEntity explicitFolderA = new ContainerEntity(
    "folderA",
    ContainerEntity.Type.EXPLICIT_FOLDER,
    ContainerEntity.Contains.FOLDERS_ONLY,
    sourceRoot.getFullPath());

  private static final ContainerEntity explicitFolderB = new ContainerEntity(
    "folderB",
    ContainerEntity.Type.EXPLICIT_FOLDER,
    ContainerEntity.Contains.TABLES_ONLY,
    explicitFolderA.getFullPath());

  private static final ContainerEntity explicitFolderBUnderRoot = new ContainerEntity(
    "folderB",
    ContainerEntity.Type.EXPLICIT_FOLDER,
    ContainerEntity.Contains.EMPTY,
    sourceRoot.getFullPath());
  private void createEntitiesForContainer(ContainerEntity container) throws Exception {
    switch(container.getType()) {
      case SOURCE:
        // Intentional fallthrough
        break;
      case IMPLICIT_FOLDER:
        break;
      case EXPLICIT_FOLDER:
        getNessieClient().createNamespace()
          .namespace(Namespace.of(container.getPathWithoutRoot()))
          .refName("main")
          .create();
        break;
      default:
        throw new IllegalStateException("Unexpected value: " + container.getType());
    }

    switch(container.getContains()) {
      case FOLDERS_AND_VIEWS:
        createTablesAndViewsInContainer(container);
        break;
      case MAX_KEY_TABLE:
        createTablesAndViewsInContainerForMaxDepthTestCases(container);
        break;
      case TABLES_ONLY:
        createSingleTable(container, tableA);
        break;
      case EMPTY:
        // Intentional fallthrough
      case FOLDERS_ONLY:
        //we are manually creating folder. other than creating here. so we just break.
        break;
      default:
        throw new IllegalStateException("Unexpected value: " + container.getContains());
    }
  }

  private static void createTablesAndViewsInContainer(ContainerEntity container) throws Exception {
    runSQL(createEmptyTableQuery(container.getChildPathWithoutRoot("tableAFirst")));
    runSQL(createEmptyTableQuery(container.getChildPathWithoutRoot("tableBSecond")));
    runSQL(createViewQuery(
      container.getChildPathWithoutRoot("viewCThird"),
      container.getChildPathWithoutRoot("tableAFirst")));
    runSQL(createViewQuery(
      container.getChildPathWithoutRoot("viewDFourth"),
      container.getChildPathWithoutRoot("tableBSecond")));
  }

  private static void createTablesAndViewsInContainerForMaxDepthTestCases(ContainerEntity container) throws Exception {
    // the key length before the last part is 454 so create tables that has length of 46 to hit the max key length
    runSQL(createEmptyTableQuery(container.getChildPathWithoutRoot(longTableNameForMaxDepthTest)));
  }

  private static void createSingleTable(ContainerEntity container, String tableName) throws Exception {
    runSQL(createEmptyTableQuery(container.getChildPathWithoutRoot(tableName)));
  }

  @BeforeEach
  public void createFoldersTablesViews() throws Exception {
    createEntitiesForContainer(sourceRoot);
    createEntitiesForContainer(explicitFolder1);
    createEntitiesForContainer(explicitFolderInExplicitParent3);
    createEntitiesForContainer(emptyExplicitFolder7);
    createEntitiesForContainer(maxDepthExplicitFolder8);
    createEntitiesForContainer(maxDepthExplicitFolder9);
    createEntitiesForContainer(maxDepthExplicitFolder10);
    createEntitiesForContainer(maxDepthExplicitFolder11);
    createEntitiesForContainer(maxDepthExplicitFolder12);
    createEntitiesForContainer(maxDepthExplicitFolder13);
    createEntitiesForContainer(maxDepthExplicitFolder14);
    createEntitiesForContainer(maxDepthExplicitFolder15);
    createEntitiesForContainer(maxDepthExplicitFolder16);
    createEntitiesForContainer(maxDepthExplicitFolder17);
    createEntitiesForContainer(maxDepthExplicitFolder18);
    createEntitiesForContainer(maxDepthExplicitFolder19);
    createEntitiesForContainer(maxDepthExplicitFolder20);
    createEntitiesForContainer(maxDepthExplicitFolder21);
    createEntitiesForContainer(maxDepthExplicitFolder22);
    createEntitiesForContainer(maxDepthExplicitFolder23);
    createEntitiesForContainer(maxDepthExplicitFolder24);
    createEntitiesForContainer(maxDepthExplicitFolder25);
    createEntitiesForContainer(maxDepthExplicitFolder26);
    createEntitiesForContainer(explicitFolderA);
    createEntitiesForContainer(explicitFolderB);
    createEntitiesForContainer(explicitFolderBUnderRoot);
  }

  private Stream<List<String>> listEntries(ContainerEntity container) {
    return getDataplanePlugin().
      listEntries(container.getPathWithoutRoot(), MAIN)
      .map(ExternalNamespaceEntry::getNameElements);
  }

  private Stream<List<String>> listEntriesIncludeNested(ContainerEntity container) {
    return getDataplanePlugin().
      listEntriesIncludeNested(container.getPathWithoutRoot(), MAIN)
      .map(ExternalNamespaceEntry::getNameElements);
  }

  @Test
  public void listEntriesForExplicitFolder() {
    assertThat(listEntries(explicitFolder1))
      .containsExactlyInAnyOrderElementsOf(
        Streams.concat(
          getFullPath(explicitFolderInExplicitParent3),
          getFullPath(explicitFolder1, tableAFirst),
          getFullPath(explicitFolder1, tableBSecond),
          getFullPath(explicitFolder1, viewCThird),
          getFullPath(explicitFolder1, viewDFourth)
        ).collect(Collectors.toList()));
  }

  @Test
  public void listNestedEntriesForExplicitFolder() {
    assertThat(listEntriesIncludeNested(explicitFolder1))
      .containsExactlyInAnyOrderElementsOf(
        Streams.concat(
          getFullPath(explicitFolderInExplicitParent3),
          getFullPath(explicitFolder1, tableAFirst),
          getFullPath(explicitFolder1, tableBSecond),
          getFullPath(explicitFolder1, viewCThird),
          getFullPath(explicitFolder1, viewDFourth),
          getFullPath(explicitFolderInExplicitParent3, tableAFirst),
          getFullPath(explicitFolderInExplicitParent3, tableBSecond),
          getFullPath(explicitFolderInExplicitParent3, viewCThird),
          getFullPath(explicitFolderInExplicitParent3, viewDFourth)
        ).collect(Collectors.toList()));
  }

  @Test
  public void listEntriesInRoot() {
    assertThat(listEntries(sourceRoot))
      .containsExactlyInAnyOrderElementsOf(
        Streams.concat(
          getFullPath(explicitFolder1),
          getFullPath(emptyExplicitFolder7),
          getFullPath(maxDepthExplicitFolder8),
          getFullPath(sourceRoot, tableAFirst),
          getFullPath(sourceRoot, tableBSecond),
          getFullPath(sourceRoot, viewCThird),
          getFullPath(sourceRoot, viewDFourth),
          getFullPath(sourceRoot, folderA),
          getFullPath(sourceRoot, folderB)
        ).collect(Collectors.toList()));
  }

  @Test
  public void listNestedEntriesInRoot() {
    assertThat(listEntriesIncludeNested(sourceRoot))
      .containsExactlyInAnyOrderElementsOf(
        Streams.concat(
          getFullPath(explicitFolder1),
          getFullPath(explicitFolderInExplicitParent3),
          getFullPath(emptyExplicitFolder7),
          getFullPath(maxDepthExplicitFolder8),
          getFullPath(maxDepthExplicitFolder9),
          getFullPath(maxDepthExplicitFolder10),
          getFullPath(maxDepthExplicitFolder11),
          getFullPath(maxDepthExplicitFolder12),
          getFullPath(maxDepthExplicitFolder13),
          getFullPath(maxDepthExplicitFolder14),
          getFullPath(maxDepthExplicitFolder15),
          getFullPath(maxDepthExplicitFolder16),
          getFullPath(maxDepthExplicitFolder17),
          getFullPath(maxDepthExplicitFolder18),
          getFullPath(maxDepthExplicitFolder19),
          getFullPath(maxDepthExplicitFolder20),
          getFullPath(maxDepthExplicitFolder21),
          getFullPath(maxDepthExplicitFolder22),
          getFullPath(maxDepthExplicitFolder23),
          getFullPath(maxDepthExplicitFolder24),
          getFullPath(maxDepthExplicitFolder25),
          getFullPath(maxDepthExplicitFolder26),
          getFullPath(sourceRoot, tableAFirst),
          getFullPath(sourceRoot, tableBSecond),
          getFullPath(sourceRoot, viewCThird),
          getFullPath(sourceRoot, viewDFourth),
          getFullPath(explicitFolder1, tableAFirst),
          getFullPath(explicitFolder1, tableBSecond),
          getFullPath(explicitFolder1, viewCThird),
          getFullPath(explicitFolder1, viewDFourth),
          getFullPath(explicitFolderInExplicitParent3, tableAFirst),
          getFullPath(explicitFolderInExplicitParent3, tableBSecond),
          getFullPath(explicitFolderInExplicitParent3, viewCThird),
          getFullPath(explicitFolderInExplicitParent3, viewDFourth),
          getFullPath(maxDepthExplicitFolder26, longTableNameForMaxDepthTest),
          getFullPath(sourceRoot, folderA),
          getFullPath(sourceRoot, folderB),
          getFullPath(explicitFolderA, folderB),
          getFullPath(explicitFolderB, tableA)
          ).collect(Collectors.toList()));
  }

  @Test
  public void listEntriesInExplicitFolderInExplicitParent() {
    assertThat(listEntries(explicitFolderInExplicitParent3))
      .containsExactlyInAnyOrderElementsOf(
        Streams.concat(
          getFullPath(explicitFolderInExplicitParent3, tableAFirst),
          getFullPath(explicitFolderInExplicitParent3, tableBSecond),
          getFullPath(explicitFolderInExplicitParent3, viewCThird),
          getFullPath(explicitFolderInExplicitParent3, viewDFourth)
        ).collect(Collectors.toList()));
  }

  @Test
  public void listEntriesEmptyExplicitFolders() {
    assertThat(listEntries(emptyExplicitFolder7))
      .isEmpty();
  }

  @Test
  public void testMaxDepthFolders() {
    assertThat(listEntriesIncludeNested(maxDepthExplicitFolder8))
      .containsExactlyInAnyOrderElementsOf(
        Streams.concat(
          getFullPath(maxDepthExplicitFolder8, maxDepthExplicitFolder9.getName()),
          getFullPath(maxDepthExplicitFolder9, maxDepthExplicitFolder10.getName()),
          getFullPath(maxDepthExplicitFolder10, maxDepthExplicitFolder11.getName()),
          getFullPath(maxDepthExplicitFolder11, maxDepthExplicitFolder12.getName()),
          getFullPath(maxDepthExplicitFolder12, maxDepthExplicitFolder13.getName()),
          getFullPath(maxDepthExplicitFolder13, maxDepthExplicitFolder14.getName()),
          getFullPath(maxDepthExplicitFolder14, maxDepthExplicitFolder15.getName()),
          getFullPath(maxDepthExplicitFolder15, maxDepthExplicitFolder16.getName()),
          getFullPath(maxDepthExplicitFolder16, maxDepthExplicitFolder17.getName()),
          getFullPath(maxDepthExplicitFolder17, maxDepthExplicitFolder18.getName()),
          getFullPath(maxDepthExplicitFolder18, maxDepthExplicitFolder19.getName()),
          getFullPath(maxDepthExplicitFolder19, maxDepthExplicitFolder20.getName()),
          getFullPath(maxDepthExplicitFolder20, maxDepthExplicitFolder21.getName()),
          getFullPath(maxDepthExplicitFolder21, maxDepthExplicitFolder22.getName()),
          getFullPath(maxDepthExplicitFolder22, maxDepthExplicitFolder23.getName()),
          getFullPath(maxDepthExplicitFolder23, maxDepthExplicitFolder24.getName()),
          getFullPath(maxDepthExplicitFolder24, maxDepthExplicitFolder25.getName()),
          getFullPath(maxDepthExplicitFolder25, maxDepthExplicitFolder26.getName()),
          getFullPath(maxDepthExplicitFolder26, longTableNameForMaxDepthTest)
          ).collect(Collectors.toList()));
  }

  @Test
  public void testFolderAccessHavingInvalidEntries() {
    //Assert that dataplane_test.folderA.folderB has entry that has name of tableA
    assertThat(listEntries(explicitFolderB))
      .containsExactlyInAnyOrderElementsOf(
        Streams.concat(
          getFullPath(explicitFolderB, tableA)
        ).collect(Collectors.toList()));

    //Assert that dataplane_test.folderB.folderB does not have entry that has name of tableA
    List<String> incorrectFullPath = Arrays.asList(folderB, folderB);
    assertThat(getDataplanePlugin().listEntries(incorrectFullPath, MAIN))
      .map(ExternalNamespaceEntry::getNameElements)
      .isEmpty();
  }

  public Stream<List<String>> getFullPath(ContainerEntity parent) {
    return Stream.of(parent.getPathWithoutRoot());
  }
  public Stream<List<String>>  getFullPath(ContainerEntity parent, String nameElement) {
    if (parent.getType() == ContainerEntity.Type.SOURCE) {
      return Stream.of(Collections.singletonList(nameElement));
    }
    return Stream.of(Streams.concat(parent.getPathWithoutRoot().stream(), Stream.of(nameElement))
      .collect(Collectors.toList()));
  }
}
