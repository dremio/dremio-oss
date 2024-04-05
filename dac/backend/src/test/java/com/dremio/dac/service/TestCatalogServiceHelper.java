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
package com.dremio.dac.service;

import static com.dremio.exec.ExecConstants.VERSIONED_VIEW_ENABLED;
import static com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType.UNSUPPORTED_OPERATION;
import static com.dremio.service.namespace.dataset.proto.DatasetType.PHYSICAL_DATASET;
import static com.dremio.service.namespace.dataset.proto.DatasetType.VIRTUAL_DATASET;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dremio.BaseTestQuery;
import com.dremio.catalog.model.VersionContext;
import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.common.collections.Tuple;
import com.dremio.dac.api.CatalogEntity;
import com.dremio.dac.api.CatalogItem;
import com.dremio.dac.api.Dataset;
import com.dremio.dac.api.File;
import com.dremio.dac.api.Folder;
import com.dremio.dac.api.Home;
import com.dremio.dac.api.Source;
import com.dremio.dac.api.Space;
import com.dremio.dac.homefiles.HomeFileTool;
import com.dremio.dac.model.spaces.HomeName;
import com.dremio.dac.model.spaces.HomePath;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.service.catalog.CatalogServiceHelper;
import com.dremio.dac.service.datasets.DatasetVersionMutator;
import com.dremio.dac.service.reflection.ReflectionServiceHelper;
import com.dremio.dac.service.search.SearchService;
import com.dremio.dac.service.source.SourceService;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.catalog.ConnectionReader;
import com.dremio.exec.catalog.DatasetMetadataState;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.VersionedDatasetId;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.SchemaEntity;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.options.OptionManager;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.AccelerationSettings;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.dataset.proto.VirtualDataset;
import com.dremio.service.namespace.file.proto.JsonFileConfig;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.source.proto.LegacySourceType;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.space.proto.FolderConfig;
import com.dremio.service.namespace.space.proto.HomeConfig;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import com.dremio.service.reflection.ReflectionSettings;
import com.dremio.test.UserExceptionAssert;
import com.google.common.collect.ImmutableList;
import io.protostuff.ByteString;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import javax.ws.rs.core.SecurityContext;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/** Test for CatalogServiceHelper */
public class TestCatalogServiceHelper extends BaseTestServer {

  private CatalogServiceHelper catalogServiceHelper;
  private Catalog catalog;
  private SourceService sourceService;
  private NamespaceService namespaceService;
  private ReflectionServiceHelper reflectionServiceHelper;
  private OptionManager optionManager;

  @Before
  public void setup() {
    SabotContext sabotContext = getSabotContext();
    CatalogService catalogService = mock(CatalogService.class);
    SecurityContext securityContext = mock(SecurityContext.class);
    DatasetVersionMutator datasetVersionMutator = mock(DatasetVersionMutator.class);
    HomeFileTool homeFileTool = mock(HomeFileTool.class);
    SearchService searchService = mock(SearchService.class);

    catalog = mock(Catalog.class);
    sourceService = mock(SourceService.class);
    namespaceService = mock(NamespaceService.class);
    reflectionServiceHelper = mock(ReflectionServiceHelper.class);
    optionManager = mock(OptionManager.class);

    Principal principal = mock(Principal.class);
    when(principal.getName()).thenReturn("user");
    when(securityContext.getUserPrincipal()).thenReturn(principal);
    when(catalogService.getCatalog(any())).thenReturn(catalog);

    catalogServiceHelper =
        new CatalogServiceHelper(
            catalogService,
            securityContext,
            sourceService,
            namespaceService,
            sabotContext,
            reflectionServiceHelper,
            homeFileTool,
            datasetVersionMutator,
            searchService,
            optionManager);
  }

  @Test
  public void testGetTopLevelCatalogItems() throws Exception {
    HomeConfig homeConfig = new HomeConfig();
    homeConfig.setOwner("user");
    homeConfig.setId(new EntityId("home-id"));
    when(namespaceService.getHome(new HomePath(HomeName.getUserHomePath("user")).toNamespaceKey()))
        .thenReturn(homeConfig);

    SpaceConfig spaceConfig = new SpaceConfig();
    spaceConfig.setName("mySpace");
    spaceConfig.setId(new EntityId("space-id"));
    when(namespaceService.getSpaces()).thenReturn(Arrays.asList(spaceConfig));

    SourceConfig sourceConfig = new SourceConfig();
    sourceConfig.setName("mySource");
    sourceConfig.setId(new EntityId("source-id"));
    when(sourceService.getSources()).thenReturn(Arrays.asList(sourceConfig));

    List<CatalogItem> topLevelCatalogItems =
        catalogServiceHelper.getTopLevelCatalogItems(Collections.EMPTY_LIST);
    assertEquals(topLevelCatalogItems.size(), 3);

    int homeCount = 0;
    int spaceCount = 0;
    int sourceCount = 0;

    for (CatalogItem item : topLevelCatalogItems) {
      if (item.getType() == CatalogItem.CatalogItemType.CONTAINER) {
        if (item.getContainerType() == CatalogItem.ContainerSubType.HOME) {
          homeCount++;
          assertEquals(item.getId(), homeConfig.getId().getId());
        }

        if (item.getContainerType() == CatalogItem.ContainerSubType.SPACE) {
          spaceCount++;
          assertEquals(item.getId(), spaceConfig.getId().getId());
        }

        if (item.getContainerType() == CatalogItem.ContainerSubType.SOURCE) {
          sourceCount++;
          assertEquals(item.getId(), sourceConfig.getId().getId());
        }
      }
    }

    assertEquals(homeCount, 1);
    assertEquals(spaceCount, 1);
    assertEquals(sourceCount, 1);
  }

  @Test
  public void testGetDatasetCatalogEntityById() throws Exception {
    Tuple<DatasetConfig, BatchSchema> configAndSchema =
        initializeDataset(
            DatasetMetadataState.builder().setIsComplete(true).setIsExpired(false).build(), true);
    DatasetConfig datasetConfig = configAndSchema.first;
    BatchSchema schema = configAndSchema.second;

    Optional<CatalogEntity> entity =
        catalogServiceHelper.getCatalogEntityById(
            datasetConfig.getId().getId(), ImmutableList.of(), ImmutableList.of());

    assertTrue(entity.isPresent());

    CatalogEntity catalogEntity = entity.get();
    assertTrue(catalogEntity instanceof Dataset);

    Dataset dataset = (Dataset) catalogEntity;
    assertEquals(datasetConfig.getId().getId(), dataset.getId());

    assertFalse(dataset.getIsMetadataExpired());
    assertNull(dataset.getLastMetadataRefreshAtMillis());

    List<Field> fields = dataset.getFields();
    assertNotNull(fields);
    assertEquals(schema.getFields(), fields);
  }

  @Test
  public void testGetDatasetCatalogEntityById_lastValidityCheckTime() throws Exception {
    Tuple<DatasetConfig, BatchSchema> configAndSchema =
        initializeDataset(
            DatasetMetadataState.builder()
                .setIsComplete(true)
                .setIsExpired(true)
                .setLastRefreshTimeMillis(123L)
                .build(),
            true);
    DatasetConfig datasetConfig = configAndSchema.first;

    Optional<CatalogEntity> entity =
        catalogServiceHelper.getCatalogEntityById(
            datasetConfig.getId().getId(), ImmutableList.of(), ImmutableList.of());

    Dataset dataset = (Dataset) entity.get();
    assertTrue(dataset.getIsMetadataExpired());
    assertEquals(123L, (long) dataset.getLastMetadataRefreshAtMillis());
  }

  @Test
  public void testGetDatasetCatalogEntityByPath() throws Exception {
    Tuple<DatasetConfig, BatchSchema> configAndSchema =
        initializeDataset(
            DatasetMetadataState.builder().setIsComplete(true).setIsExpired(false).build(), false);
    DatasetConfig datasetConfig = configAndSchema.first;
    BatchSchema schema = configAndSchema.second;

    Optional<CatalogEntity> entity =
        catalogServiceHelper.getCatalogEntityByPath(
            datasetConfig.getFullPathList(), new ArrayList<>(), new ArrayList<>());

    assertTrue(entity.isPresent());

    CatalogEntity catalogEntity = entity.get();
    assertTrue(catalogEntity instanceof Dataset);

    Dataset dataset = (Dataset) catalogEntity;
    assertEquals(datasetConfig.getFullPathList(), dataset.getPath());

    List<Field> fields = dataset.getFields();
    assertNotNull(fields);
    assertEquals(schema.getFields(), fields);
  }

  private Tuple<DatasetConfig, BatchSchema> initializeDataset(
      DatasetMetadataState metadataState, boolean byIdOrPath) throws Exception {
    DatasetConfig datasetConfig = new DatasetConfig();
    String datasetId = "dataset-id";
    List<String> pathList = Collections.singletonList("path");
    datasetConfig.setId(new EntityId(datasetId));
    datasetConfig.setFullPathList(pathList);
    datasetConfig.setType(VIRTUAL_DATASET);

    BatchSchema schema =
        BatchSchema.newBuilder()
            .addField(Field.nullablePrimitive("id", new ArrowType.Int(64, true)))
            .addField(Field.nullablePrimitive("name", new ArrowType.Utf8()))
            .build();

    datasetConfig.setRecordSchema(ByteString.copyFrom(schema.serialize()));

    VirtualDataset virtualDataset = new VirtualDataset();
    virtualDataset.setSql("");
    datasetConfig.setVirtualDataset(virtualDataset);

    NameSpaceContainer namespaceContainer = new NameSpaceContainer();
    namespaceContainer.setType(NameSpaceContainer.Type.DATASET);
    namespaceContainer.setDataset(datasetConfig);

    if (byIdOrPath) {
      when(namespaceService.getEntityById(datasetConfig.getId().getId()))
          .thenReturn(namespaceContainer);
    } else {
      List<NamespaceKey> namespaceKeyList = Collections.singletonList(new NamespaceKey(pathList));
      List<NameSpaceContainer> namespaceContainerList =
          Collections.singletonList(namespaceContainer);
      when(namespaceService.getEntities(namespaceKeyList)).thenReturn(namespaceContainerList);
    }

    ReflectionSettings reflectionSettings = mock(ReflectionSettings.class);
    when(reflectionSettings.getStoredReflectionSettings(any(NamespaceKey.class)))
        .thenReturn(Optional.empty());
    when(reflectionServiceHelper.getReflectionSettings()).thenReturn(reflectionSettings);

    DremioTable dremioTable = mock(DremioTable.class);
    when(dremioTable.getDatasetConfig()).thenReturn(datasetConfig);
    when(dremioTable.getDatasetMetadataState()).thenReturn(metadataState);
    when(catalog.getTable(any(String.class))).thenReturn(dremioTable);

    return Tuple.of(datasetConfig, schema);
  }

  @Test
  public void testGetSpaceCatalogEntityById() throws Exception {
    SpaceConfig spaceConfig = new SpaceConfig();
    spaceConfig.setId(new EntityId("space-id"));
    spaceConfig.setName("mySpace");

    NameSpaceContainer namespaceContainer = new NameSpaceContainer();
    namespaceContainer.setType(NameSpaceContainer.Type.SPACE);
    namespaceContainer.setSpace(spaceConfig);

    when(namespaceService.getEntityById(spaceConfig.getId().getId()))
        .thenReturn(namespaceContainer);
    when(namespaceService.getEntities(
            Collections.singletonList(new NamespaceKey(spaceConfig.getName()))))
        .thenReturn(Collections.singletonList(namespaceContainer));
    // for children listing, we just send the space back to keep it simple
    when(namespaceService.list(new NamespaceKey(spaceConfig.getName())))
        .thenReturn(Collections.singletonList(namespaceContainer));

    Optional<CatalogEntity> catalogEntityById =
        catalogServiceHelper.getCatalogEntityById(
            spaceConfig.getId().getId(), ImmutableList.of(), ImmutableList.of());

    assertTrue(catalogEntityById.isPresent());

    CatalogEntity catalogEntity = catalogEntityById.get();
    assertTrue(catalogEntity instanceof Space);

    Space space = (Space) catalogEntity;
    assertEquals(space.getId(), spaceConfig.getId().getId());
    assertEquals(space.getChildren().size(), 1);
    assertEquals(space.getName(), spaceConfig.getName());
  }

  /**
   * Tests {@link CatalogServiceHelper#getCatalogEntityById} with children from {@link
   * FileSystemPlugin}.
   *
   * <p>Call to {@link NamespaceService#exists} takes a lot of time when called sequentially on many
   * sub-folders, make sure it's not called.
   */
  @Test
  public void testGetSpaceCatalogEntityById_FileSystem() throws Exception {
    SourceConfig sourceConfig = new SourceConfig();
    sourceConfig.setId(new EntityId("source-id"));
    sourceConfig.setName("source");
    sourceConfig.setLegacySourceTypeEnum(LegacySourceType.HDFS);

    NameSpaceContainer namespaceContainer = new NameSpaceContainer();
    namespaceContainer.setType(NameSpaceContainer.Type.SOURCE);
    namespaceContainer.setFullPathList(ImmutableList.of("root"));
    namespaceContainer.setSource(sourceConfig);

    when(namespaceService.getEntityById(sourceConfig.getId().getId()))
        .thenReturn(namespaceContainer);
    when(namespaceService.getEntities(Collections.singletonList(new NamespaceKey("root"))))
        .thenReturn(Collections.singletonList(namespaceContainer));

    // For children listing, set up FileSystemPlugin.
    FileSystemPlugin fileSystemPlugin = mock(FileSystemPlugin.class);
    when(catalog.getSource(eq(sourceConfig.getName()))).thenReturn(fileSystemPlugin);
    when(fileSystemPlugin.isWrapperFor(any())).thenReturn(false);
    when(fileSystemPlugin.list(eq(ImmutableList.of("root")), anyString()))
        .thenReturn(
            ImmutableList.of(
                new SchemaEntity("folder1", SchemaEntity.SchemaEntityType.FOLDER, "owner"),
                new SchemaEntity("folder2", SchemaEntity.SchemaEntityType.FOLDER, "owner"),
                new SchemaEntity(
                    "folder_table", SchemaEntity.SchemaEntityType.FOLDER_TABLE, "owner"),
                new SchemaEntity("file", SchemaEntity.SchemaEntityType.FILE, "owner"),
                new SchemaEntity("file_table", SchemaEntity.SchemaEntityType.FILE_TABLE, "owner")));

    // Dataset config for tables.
    when(namespaceService.getDataset(any()))
        .thenReturn(new DatasetConfig().setId(new EntityId().setId("id")));

    // Make folder2 part of KV store.
    when(namespaceService.list(eq(new NamespaceKey("root"))))
        .thenReturn(
            ImmutableList.of(
                new NameSpaceContainer()
                    .setType(NameSpaceContainer.Type.FOLDER)
                    .setFullPathList(ImmutableList.of("root", "folder2"))));
    final String folder2Id = "folder2-id";
    FolderConfig folder2Config = new FolderConfig().setId(new EntityId(folder2Id));
    when(namespaceService.getFolder(eq(new NamespaceKey(ImmutableList.of("root", "folder2")))))
        .thenReturn(folder2Config);

    // Resulting source.
    doAnswer(
            (invocation) -> {
              Object[] args = invocation.getRawArguments();
              return new Source(
                  (SourceConfig) args[0],
                  mock(AccelerationSettings.class),
                  mock(ConnectionReader.class),
                  (List<CatalogItem>) args[1]);
            })
        .when(sourceService)
        .fromSourceConfig(
            eq(sourceConfig),
            ArgumentMatchers.argThat(
                (items) ->
                    // Check number of children.
                    items.size() == 5));

    Optional<CatalogEntity> catalogEntityById =
        catalogServiceHelper.getCatalogEntityById(
            sourceConfig.getId().getId(), ImmutableList.of(), ImmutableList.of());

    // Verify exists was not called.
    verify(namespaceService, times(0)).exists(any());

    // Verify returned entity.
    assertTrue(catalogEntityById.isPresent());
    CatalogEntity catalogEntity = catalogEntityById.get();
    assertTrue(catalogEntity instanceof Source);
    Source actualSource = (Source) catalogEntity;
    assertEquals(5, actualSource.getChildren().size());

    // Verify that id was assigned from the call to namespace service.
    Optional<CatalogItem> actualFolder2 =
        actualSource.getChildren().stream()
            .filter(e -> e.getPath().equals(ImmutableList.of("root", "folder2")))
            .findFirst();
    assertTrue(actualFolder2.isPresent());
    assertEquals(folder2Id, actualFolder2.get().getId());
  }

  @Test
  public void testGetSpaceCatalogEntityByPath() throws Exception {
    SpaceConfig spaceConfig = new SpaceConfig();
    List<String> pathList = Collections.singletonList("path");
    spaceConfig.setId(new EntityId("space-id"));
    spaceConfig.setName("mySpace");

    NameSpaceContainer namespaceContainer = new NameSpaceContainer();
    namespaceContainer.setType(NameSpaceContainer.Type.SPACE);
    namespaceContainer.setSpace(spaceConfig);
    List<NameSpaceContainer> namespaceContainerList = Arrays.asList(namespaceContainer);

    List<NamespaceKey> namespaceKeyListFromPath =
        Collections.singletonList(new NamespaceKey(pathList));
    when(namespaceService.getEntities(namespaceKeyListFromPath)).thenReturn(namespaceContainerList);
    List<NamespaceKey> namespaceKeyListFromName =
        Collections.singletonList(new NamespaceKey(spaceConfig.getName()));
    when(namespaceService.getEntities(namespaceKeyListFromName)).thenReturn(namespaceContainerList);

    // for children listing, we just send the space back to keep it simple
    when(namespaceService.list(new NamespaceKey(spaceConfig.getName())))
        .thenReturn(Collections.singletonList(namespaceContainer));

    Optional<CatalogEntity> entity =
        catalogServiceHelper.getCatalogEntityByPath(pathList, new ArrayList<>(), new ArrayList<>());

    assertTrue(entity.isPresent());

    CatalogEntity catalogEntity = entity.get();
    assertTrue(catalogEntity instanceof Space);

    Space space = (Space) catalogEntity;
    assertEquals(space.getId(), spaceConfig.getId().getId());
    assertEquals(space.getChildren().size(), 1);
    assertEquals(space.getName(), spaceConfig.getName());
  }

  @Test
  public void testCreatePDSShouldFail() {
    // can only create VDS
    Dataset dataset =
        new Dataset(
            null,
            Dataset.DatasetType.PHYSICAL_DATASET,
            Arrays.asList("source", "path"),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null);
    assertThatThrownBy(() -> catalogServiceHelper.createCatalogItem(dataset))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testCreateVDSWithoutSQLShouldFail() {
    // VDS needs sql
    Dataset dataset =
        new Dataset(
            null,
            Dataset.DatasetType.VIRTUAL_DATASET,
            Arrays.asList("source", "path"),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null);
    assertThatThrownBy(() -> catalogServiceHelper.createCatalogItem(dataset))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testCreateVDSWithInvalidSQLShouldFail() {
    // VDS needs valid sql
    Dataset dataset =
        new Dataset(
            null,
            Dataset.DatasetType.VIRTUAL_DATASET,
            Arrays.asList("source", "path"),
            null,
            null,
            null,
            null,
            "CREATE VIEW foo AS SELECT 1",
            null,
            null,
            null);
    UserExceptionAssert.assertThatThrownBy(() -> catalogServiceHelper.createCatalogItem(dataset))
        .hasErrorType(UNSUPPORTED_OPERATION)
        .hasMessage("Cannot create view on statement of this type");
  }

  @Test
  public void testCreateVDSWithIdShouldFail() {
    // new VDS can't have an id
    Dataset dataset =
        new Dataset(
            "dataset-id",
            Dataset.DatasetType.VIRTUAL_DATASET,
            Arrays.asList("source", "path"),
            null,
            null,
            null,
            null,
            "sql",
            null,
            null,
            null);
    assertThatThrownBy(() -> catalogServiceHelper.createCatalogItem(dataset))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testCreateVDSWithEmptyPathShouldFail() {
    Dataset dataset =
        new Dataset(
            null,
            Dataset.DatasetType.VIRTUAL_DATASET,
            Collections.EMPTY_LIST,
            null,
            null,
            null,
            null,
            "SELECT 1",
            null,
            null,
            null);
    assertThatThrownBy(() -> catalogServiceHelper.createCatalogItem(dataset))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testCreateVDSWithInvalidPathShouldFail() {
    Dataset dataset =
        new Dataset(
            null,
            Dataset.DatasetType.VIRTUAL_DATASET,
            Arrays.asList("space"),
            null,
            null,
            null,
            null,
            "SELECT 1",
            null,
            null,
            null);
    assertThatThrownBy(() -> catalogServiceHelper.createCatalogItem(dataset))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testCreateVDSInNonVersionedSourceShouldFail() throws Exception {
    when(namespaceService.exists(any(NamespaceKey.class))).thenReturn(true);

    // provide a source container
    NameSpaceContainer namespaceContainer = new NameSpaceContainer();
    namespaceContainer.setType(NameSpaceContainer.Type.SOURCE);

    when(namespaceService.getEntities(anyList()))
        .thenReturn(Collections.singletonList(namespaceContainer));

    // VDS needs sql
    Dataset dataset =
        new Dataset(
            null,
            Dataset.DatasetType.VIRTUAL_DATASET,
            Arrays.asList("source", "path"),
            null,
            null,
            null,
            null,
            "sql",
            null,
            null,
            null);
    assertThatThrownBy(() -> catalogServiceHelper.createCatalogItem(dataset))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Virtual datasets can only be saved into spaces, home space or versioned sources.");
  }

  @Test
  public void testCreateVDSInVersionedSourceWithVersionedViewDisabledShouldFail() throws Exception {
    when(namespaceService.exists(any(NamespaceKey.class))).thenReturn(true);

    // provide a source container
    NameSpaceContainer namespaceContainer = new NameSpaceContainer();
    namespaceContainer.setType(NameSpaceContainer.Type.SOURCE);
    when(namespaceService.getEntities(anyList()))
        .thenReturn(Collections.singletonList(namespaceContainer));

    // versioned source meanwhile versioned view is disabled
    when(optionManager.getOption(VERSIONED_VIEW_ENABLED)).thenReturn(false);
    try (MockedStatic<CatalogUtil> mockedCatalogUtil = mockStatic(CatalogUtil.class)) {
      final List<String> tableKey = Arrays.asList("source", "path");
      final NamespaceKey namespaceKey = new NamespaceKey(tableKey);
      mockedCatalogUtil
          .when(() -> CatalogUtil.requestedPluginSupportsVersionedTables(namespaceKey, catalog))
          .thenReturn(true);

      Dataset dataset =
          new Dataset(
              null,
              Dataset.DatasetType.VIRTUAL_DATASET,
              tableKey,
              null,
              null,
              null,
              null,
              "sql",
              null,
              null,
              null);
      UserExceptionAssert.assertThatThrownBy(() -> catalogServiceHelper.createCatalogItem(dataset))
          .hasErrorType(UNSUPPORTED_OPERATION)
          .hasMessage("Versioned view is not enabled");
    }
  }

  @Test
  public void testCreateSpaceWithIdShouldFail() {
    Space space = new Space("space-id", "mySpace", null, 0L, null);
    assertThatThrownBy(() -> catalogServiceHelper.createCatalogItem(space))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testCreateSpaceWithoutNameShouldFail() throws Exception {
    Space space = new Space(null, null, null, 0L, null);
    assertThatThrownBy(() -> catalogServiceHelper.createCatalogItem(space))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testCreateSpaceWithEmptyNameShouldFail() {
    Space space = new Space(null, "", null, 0L, null);
    assertThatThrownBy(() -> catalogServiceHelper.createCatalogItem(space))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testCreateSpaceWithConflict() {
    when(namespaceService.exists(any(NamespaceKey.class), eq(NameSpaceContainer.Type.SPACE)))
        .thenReturn(true);

    // VDS
    Space space = new Space(null, "mySpace", null, 0L, null);

    assertThatThrownBy(() -> catalogServiceHelper.createCatalogItem(space))
        .isInstanceOf(ConcurrentModificationException.class);
  }

  @Test
  public void testCreateSpace() throws Exception {
    // VDS
    Space space = new Space(null, "mySpace", null, 0L, null);

    NameSpaceContainer container = new NameSpaceContainer();
    container.setType(NameSpaceContainer.Type.SPACE);
    container.setSpace(catalogServiceHelper.getSpaceConfig(space));

    when(namespaceService.getEntityByPath(new NamespaceKey(space.getName()))).thenReturn(container);

    CatalogEntity catalogItem = catalogServiceHelper.createCatalogItem(space);
    assertTrue(catalogItem instanceof Space);

    Space createdSpace = (Space) catalogItem;
    assertEquals(createdSpace.getName(), space.getName());
  }

  @Test
  public void testCreateFolderWithEmptyPathShouldFail() {
    Folder folder = new Folder("id", Collections.EMPTY_LIST, "1", null);
    assertThatThrownBy(() -> catalogServiceHelper.createCatalogItem(folder))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testCreateFolderWithInvalidPathShouldFail() {
    Folder folder = new Folder("id", Arrays.asList("space"), "1", null);
    assertThatThrownBy(() -> catalogServiceHelper.createCatalogItem(folder))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testUpdateDatasetWithMismatchingIdShouldFail() throws Exception {
    Dataset dataset =
        new Dataset(
            "dataset-id",
            Dataset.DatasetType.VIRTUAL_DATASET,
            Arrays.asList("source", "path"),
            null,
            0L,
            "1",
            null,
            "sql",
            null,
            null,
            null);

    assertThatThrownBy(() -> catalogServiceHelper.updateCatalogItem(dataset, "bad-id"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testUpdateVersionedDatasetWithMismatchingIdShouldFail() throws Exception {
    List<String> path = Arrays.asList("source", "path");
    final VersionedDatasetId id =
        VersionedDatasetId.newBuilder()
            .setTableVersionContext(TableVersionContext.of(VersionContext.ofBranch("main")))
            .setContentId(UUID.randomUUID().toString())
            .setTableKey(path)
            .build();

    Dataset dataset =
        new Dataset(
            id.asString(),
            Dataset.DatasetType.VIRTUAL_DATASET,
            path,
            null,
            0L,
            "1",
            null,
            "sql",
            null,
            null,
            null);

    assertThatThrownBy(() -> catalogServiceHelper.updateCatalogItem(dataset, "bad-id"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testUpdateVDSWithChangedTypeShouldFail() throws Exception {
    Dataset dataset =
        new Dataset(
            "dataset-id",
            Dataset.DatasetType.VIRTUAL_DATASET,
            Arrays.asList("source", "path"),
            null,
            0L,
            "1",
            null,
            "sql",
            null,
            null,
            null);

    DatasetConfig config = new DatasetConfig();
    config.setId(new EntityId(dataset.getId()));
    config.setFullPathList(dataset.getPath());
    config.setName(dataset.getPath().get(dataset.getPath().size() - 1));
    config.setTag(dataset.getTag());
    config.setCreatedAt(dataset.getCreatedAt());
    VirtualDataset virtualDataset = new VirtualDataset();
    virtualDataset.setSql(dataset.getSql());
    virtualDataset.setContextList(dataset.getSqlContext());
    config.setType(PHYSICAL_DATASET);
    config.setVirtualDataset(virtualDataset);

    when(namespaceService.getEntityById(dataset.getId()))
        .thenReturn(new NameSpaceContainer().setDataset(config));

    DremioTable dremioTable = mock(DremioTable.class);
    when(dremioTable.getDatasetConfig()).thenReturn(config);
    when(catalog.getTable(any(String.class))).thenReturn(dremioTable);

    assertThatThrownBy(() -> catalogServiceHelper.updateCatalogItem(dataset, dataset.getId()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testUpdateVDSWithoutSqlShouldFail() throws Exception {
    Dataset dataset =
        new Dataset(
            "dataset-id",
            Dataset.DatasetType.VIRTUAL_DATASET,
            Arrays.asList("source", "path"),
            null,
            0L,
            "1",
            null,
            null,
            null,
            null,
            null);

    DatasetConfig config = new DatasetConfig();
    config.setId(new EntityId(dataset.getId()));
    config.setFullPathList(dataset.getPath());
    config.setName(dataset.getPath().get(dataset.getPath().size() - 1));
    config.setTag(dataset.getTag());
    config.setCreatedAt(dataset.getCreatedAt());
    VirtualDataset virtualDataset = new VirtualDataset();
    virtualDataset.setSql(dataset.getSql());
    virtualDataset.setContextList(dataset.getSqlContext());
    config.setType(VIRTUAL_DATASET);
    config.setVirtualDataset(virtualDataset);

    when(namespaceService.getEntityById(dataset.getId()))
        .thenReturn(new NameSpaceContainer().setDataset(config));

    assertThatThrownBy(() -> catalogServiceHelper.updateCatalogItem(dataset, dataset.getId()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testUpdateVDSWithoutIdShouldFail() throws Exception {
    Dataset dataset =
        new Dataset(
            null,
            Dataset.DatasetType.VIRTUAL_DATASET,
            Arrays.asList("source", "path"),
            null,
            0L,
            "1",
            null,
            "SELECT 1",
            null,
            null,
            null);

    assertThatThrownBy(() -> catalogServiceHelper.updateCatalogItem(dataset, "dataset-id"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testUpdateVDS() throws Exception {
    Dataset dataset =
        new Dataset(
            "dataset-id",
            Dataset.DatasetType.VIRTUAL_DATASET,
            Arrays.asList("source", "path"),
            null,
            0L,
            "1",
            null,
            "sql",
            null,
            null,
            null);

    DatasetConfig config = new DatasetConfig();
    config.setId(new EntityId(dataset.getId()));
    config.setFullPathList(dataset.getPath());
    config.setName(dataset.getPath().get(dataset.getPath().size() - 1));
    config.setTag(dataset.getTag());
    config.setCreatedAt(dataset.getCreatedAt());
    VirtualDataset virtualDataset = new VirtualDataset();
    virtualDataset.setSql(dataset.getSql());
    virtualDataset.setContextList(dataset.getSqlContext());
    config.setType(VIRTUAL_DATASET);
    config.setVirtualDataset(virtualDataset);
    when(namespaceService.findDatasetByUUID(dataset.getId())).thenReturn(config);

    NameSpaceContainer namespaceContainer = new NameSpaceContainer();
    namespaceContainer.setType(NameSpaceContainer.Type.DATASET);
    namespaceContainer.setDataset(config);
    when(namespaceService.getEntityById(dataset.getId())).thenReturn(namespaceContainer);

    ReflectionSettings reflectionSettings = mock(ReflectionSettings.class);
    when(reflectionSettings.getStoredReflectionSettings(any(NamespaceKey.class)))
        .thenReturn(Optional.empty());
    when(reflectionServiceHelper.getReflectionSettings()).thenReturn(reflectionSettings);

    DremioTable dremioTable = mock(DremioTable.class);
    when(dremioTable.getDatasetConfig()).thenReturn(config);
    when(catalog.getTable(any(String.class))).thenReturn(dremioTable);

    catalogServiceHelper.updateCatalogItem(dataset, dataset.getId());
  }

  @Test
  public void testUpdatePDSWithChangedTypeShouldFail() throws Exception {
    Dataset dataset =
        new Dataset(
            "dataset-id",
            Dataset.DatasetType.PHYSICAL_DATASET,
            Arrays.asList("source", "path"),
            null,
            0L,
            "1",
            null,
            null,
            null,
            null,
            null);

    DatasetConfig config = new DatasetConfig();
    config.setId(new EntityId(dataset.getId()));
    config.setFullPathList(dataset.getPath());
    config.setName(dataset.getPath().get(dataset.getPath().size() - 1));
    config.setTag(dataset.getTag());
    config.setCreatedAt(dataset.getCreatedAt());
    VirtualDataset virtualDataset = new VirtualDataset();
    virtualDataset.setSql(dataset.getSql());
    virtualDataset.setContextList(dataset.getSqlContext());
    config.setType(VIRTUAL_DATASET);
    config.setVirtualDataset(virtualDataset);

    when(namespaceService.getEntityById(dataset.getId()))
        .thenReturn(new NameSpaceContainer().setDataset(config));

    assertThatThrownBy(() -> catalogServiceHelper.updateCatalogItem(dataset, dataset.getId()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testUpdatePDS() throws Exception {
    Dataset dataset =
        new Dataset(
            "dataset-id",
            Dataset.DatasetType.PHYSICAL_DATASET,
            Arrays.asList("source", "path"),
            null,
            0L,
            "1",
            null,
            null,
            null,
            new JsonFileConfig(),
            null);

    DatasetConfig config = new DatasetConfig();
    config.setId(new EntityId(dataset.getId()));
    config.setFullPathList(dataset.getPath());
    config.setName(dataset.getPath().get(dataset.getPath().size() - 1));
    config.setTag(dataset.getTag());
    config.setCreatedAt(dataset.getCreatedAt());
    PhysicalDataset physicalDataset = new PhysicalDataset();
    physicalDataset.setFormatSettings(new JsonFileConfig().asFileConfig());
    config.setType(PHYSICAL_DATASET);
    config.setPhysicalDataset(physicalDataset);
    when(namespaceService.findDatasetByUUID(dataset.getId())).thenReturn(config);

    NameSpaceContainer namespaceContainer = new NameSpaceContainer();
    namespaceContainer.setType(NameSpaceContainer.Type.DATASET);
    namespaceContainer.setDataset(config);
    when(namespaceService.getEntityById(dataset.getId())).thenReturn(namespaceContainer);

    ReflectionSettings reflectionSettings = mock(ReflectionSettings.class);
    when(reflectionSettings.getStoredReflectionSettings(any(NamespaceKey.class)))
        .thenReturn(Optional.empty());
    when(reflectionServiceHelper.getReflectionSettings()).thenReturn(reflectionSettings);

    DremioTable dremioTable = mock(DremioTable.class);
    when(dremioTable.getDatasetConfig()).thenReturn(config);
    when(catalog.getTable(any(String.class))).thenReturn(dremioTable);

    catalogServiceHelper.updateCatalogItem(dataset, dataset.getId());
    verify(catalog, times(1))
        .createOrUpdateDataset(
            eq(namespaceService),
            eq(new NamespaceKey("source")),
            eq(new NamespaceKey(dataset.getPath())),
            any(DatasetConfig.class));
  }

  @Test
  public void testUpdateFileShouldFail() {
    File file = new File("file-id", Collections.singletonList("file"));

    assertThatThrownBy(() -> catalogServiceHelper.updateCatalogItem(file, file.getId()))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void testUpdateHomeShouldFail() {
    Home home = new Home("home-id", "home", null, null);

    assertThatThrownBy(() -> catalogServiceHelper.updateCatalogItem(home, home.getId()))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void testDeleteDataset() throws Exception {
    Dataset dataset =
        new Dataset(
            "dataset-id",
            Dataset.DatasetType.VIRTUAL_DATASET,
            Arrays.asList("source", "path"),
            null,
            0L,
            "1",
            null,
            "sql",
            null,
            null,
            null);

    NameSpaceContainer namespaceContainer = new NameSpaceContainer();
    namespaceContainer.setType(NameSpaceContainer.Type.DATASET);

    DatasetConfig datasetConfig = new DatasetConfig();
    datasetConfig.setId(new EntityId(dataset.getId()));
    datasetConfig.setType(VIRTUAL_DATASET);
    datasetConfig.setFullPathList(dataset.getPath());
    datasetConfig.setTag(dataset.getTag());
    namespaceContainer.setDataset(datasetConfig);
    when(namespaceService.getEntityById(dataset.getId())).thenReturn(namespaceContainer);

    DremioTable dremioTable = mock(DremioTable.class);
    when(dremioTable.getDatasetConfig()).thenReturn(datasetConfig);
    when(catalog.getTable(any(String.class))).thenReturn(dremioTable);

    catalogServiceHelper.deleteCatalogItem(dataset.getId(), "1");
    verify(namespaceService, times(1))
        .deleteDataset(new NamespaceKey(dataset.getPath()), datasetConfig.getTag());
  }

  @Test
  public void testDeleteSource() throws Exception {
    SourceConfig sourceConfig = new SourceConfig();
    sourceConfig.setName("mySource");
    sourceConfig.setId(new EntityId("source-id"));
    sourceConfig.setTag("1");
    sourceConfig.setType("NAS");

    AccelerationSettings settings = new AccelerationSettings();
    settings.setGracePeriod(1L);
    settings.setRefreshPeriod(1L);

    Source source = new Source(sourceConfig, settings, mock(ConnectionReader.class), null);

    NameSpaceContainer namespaceContainer = new NameSpaceContainer();
    namespaceContainer.setType(NameSpaceContainer.Type.SOURCE);
    namespaceContainer.setSource(sourceConfig);
    when(namespaceService.getEntityById(source.getId())).thenReturn(namespaceContainer);

    catalogServiceHelper.deleteCatalogItem(source.getId(), "1");
    verify(sourceService, times(1)).deleteSource(sourceConfig);
  }

  @Test
  public void testDeleteSpace() throws Exception {
    Space space = new Space("space-id", "mySpace", "1", 0L, null);

    NameSpaceContainer namespaceContainer = new NameSpaceContainer();
    namespaceContainer.setSpace(catalogServiceHelper.getSpaceConfig(space));
    namespaceContainer.setType(NameSpaceContainer.Type.SPACE);
    when(namespaceService.getEntityById(space.getId())).thenReturn(namespaceContainer);

    catalogServiceHelper.deleteCatalogItem(space.getId(), space.getTag());
    verify(namespaceService, times(1))
        .deleteSpace(new NamespaceKey(space.getName()), space.getTag());
  }

  @Test
  public void testDeleteFolder() throws Exception {
    Folder folder = new Folder("folder-id", Arrays.asList("source", "folder"), "1", null);

    NameSpaceContainer namespaceContainer = new NameSpaceContainer();
    namespaceContainer.setFolder(CatalogServiceHelper.getFolderConfig(folder));
    namespaceContainer.setType(NameSpaceContainer.Type.FOLDER);
    when(namespaceService.getEntityById(folder.getId())).thenReturn(namespaceContainer);

    catalogServiceHelper.deleteCatalogItem(folder.getId(), folder.getTag());
    verify(namespaceService, times(1))
        .deleteFolder(new NamespaceKey(folder.getPath()), folder.getTag());
  }

  @Test
  public void testSourceListing() throws Exception {
    SourceConfig sourceConfig = new SourceConfig();
    sourceConfig.setName("mySource");
    sourceConfig.setId(new EntityId("source-id"));
    sourceConfig.setType("");

    StoragePlugin storagePlugin = mock(StoragePlugin.class);

    when(catalog.getSource(sourceConfig.getName())).thenReturn(storagePlugin);

    NamespaceKey namespaceKey = new NamespaceKey(sourceConfig.getName());

    // mock getting entities
    NameSpaceContainer namespaceContainer = new NameSpaceContainer();
    namespaceContainer.setSource(sourceConfig);
    namespaceContainer.setType(NameSpaceContainer.Type.SOURCE);
    when(namespaceService.getEntities(Collections.singletonList(namespaceKey)))
        .thenReturn(Collections.singletonList(namespaceContainer));

    // mock listing
    List<NameSpaceContainer> containerList = new ArrayList<>();

    // folder 1
    FolderConfig folderConfig1 = new FolderConfig();
    folderConfig1.setId(new EntityId("folder-id-1"));
    folderConfig1.setName("folder1");
    folderConfig1.setFullPathList(Arrays.asList(sourceConfig.getName(), folderConfig1.getName()));
    NameSpaceContainer folder = new NameSpaceContainer();
    folder.setFolder(folderConfig1);
    folder.setType(NameSpaceContainer.Type.FOLDER);
    containerList.add(folder);

    // folder 2
    FolderConfig folderConfig2 = new FolderConfig();
    folderConfig2.setId(new EntityId("folder-id-2"));
    folderConfig2.setName("folder2");
    folderConfig2.setFullPathList(Arrays.asList(sourceConfig.getName(), folderConfig2.getName()));
    folder = new NameSpaceContainer();
    folder.setFolder(folderConfig2);
    folder.setType(NameSpaceContainer.Type.FOLDER);
    containerList.add(folder);

    when(namespaceService.list(namespaceKey)).thenReturn(containerList);

    // folder1/folder11
    List<NameSpaceContainer> containerList2 = new ArrayList<>();
    FolderConfig folderConfig11 = new FolderConfig();
    folderConfig11.setId(new EntityId("folder-id-11"));
    folderConfig11.setName("folder11");
    folderConfig11.setFullPathList(
        Arrays.asList(sourceConfig.getName(), folderConfig1.getName(), folderConfig11.getName()));
    folder = new NameSpaceContainer();
    folder.setFolder(folderConfig11);
    folder.setType(NameSpaceContainer.Type.FOLDER);
    containerList2.add(folder);
    when(namespaceService.list(new NamespaceKey(folderConfig1.getFullPathList())))
        .thenReturn(containerList2);

    List<CatalogItem> childrenForPath = catalogServiceHelper.getChildrenForPath(namespaceKey);
    assertEquals(childrenForPath.size(), 2);

    childrenForPath =
        catalogServiceHelper.getChildrenForPath(new NamespaceKey(folderConfig1.getFullPathList()));
    assertEquals(childrenForPath.size(), 1);
  }

  @Test
  public void testFileBaseSourceListing() throws Exception {
    SourceConfig sourceConfig = new SourceConfig();
    sourceConfig.setName("mySource");
    sourceConfig.setId(new EntityId("source-id"));
    sourceConfig.setType("");

    // mock getting entities
    NameSpaceContainer namespaceContainer = new NameSpaceContainer();
    namespaceContainer.setSource(sourceConfig);
    namespaceContainer.setType(NameSpaceContainer.Type.SOURCE);
    when(namespaceService.getEntities(
            Collections.singletonList(new NamespaceKey(sourceConfig.getName()))))
        .thenReturn(Collections.singletonList(namespaceContainer));

    FileSystemPlugin storagePlugin = BaseTestQuery.getMockedFileSystemPlugin();

    when(catalog.getSource(sourceConfig.getName())).thenReturn(storagePlugin);

    // folder 1
    FolderConfig folderConfig1 = new FolderConfig();
    folderConfig1.setId(new EntityId("folder-id-1"));
    folderConfig1.setName("folder1");
    folderConfig1.setFullPathList(Arrays.asList(sourceConfig.getName(), folderConfig1.getName()));
    NameSpaceContainer folder = new NameSpaceContainer();
    folder.setFolder(folderConfig1);
    folder.setType(NameSpaceContainer.Type.FOLDER);

    // folder 2
    FolderConfig folderConfig2 = new FolderConfig();
    folderConfig2.setId(new EntityId("folder-id-2"));
    folderConfig2.setName("folder2");
    folderConfig2.setFullPathList(Arrays.asList(sourceConfig.getName(), folderConfig2.getName()));
    folder = new NameSpaceContainer();
    folder.setFolder(folderConfig2);
    folder.setType(NameSpaceContainer.Type.FOLDER);

    // mock getting entities
    SchemaEntity schemaEntityFolder1 =
        new SchemaEntity(
            com.dremio.common.utils.PathUtils.constructFullPath(folderConfig1.getFullPathList()),
            SchemaEntity.SchemaEntityType.FOLDER,
            "testuser");
    SchemaEntity schemaEntityFolder2 =
        new SchemaEntity(
            com.dremio.common.utils.PathUtils.constructFullPath(folderConfig2.getFullPathList()),
            SchemaEntity.SchemaEntityType.FOLDER,
            "testuser");

    when(storagePlugin.list(eq(Arrays.asList(sourceConfig.getName())), any(String.class)))
        .thenReturn(Arrays.asList(schemaEntityFolder1, schemaEntityFolder2));

    List<CatalogItem> childrenForPath =
        catalogServiceHelper.getChildrenForPath(new NamespaceKey(sourceConfig.getName()));
    assertEquals(childrenForPath.size(), 2);
  }

  @Test
  public void testUpdateVersionedDatasetWithoutVersionedDatasetIdShouldFail() throws Exception {
    try (MockedStatic<CatalogUtil> mockedCatalogUtil = mockStatic(CatalogUtil.class)) {
      final List<String> tableKey = Arrays.asList("source", "path");
      final NamespaceKey namespaceKey = new NamespaceKey(tableKey);
      mockedCatalogUtil
          .when(() -> CatalogUtil.requestedPluginSupportsVersionedTables(namespaceKey, catalog))
          .thenReturn(true);

      Dataset dataset =
          new Dataset(
              "id",
              Dataset.DatasetType.VIRTUAL_DATASET,
              tableKey,
              null,
              null,
              null,
              null,
              "sql",
              null,
              null,
              null);

      assertThatThrownBy(() -> catalogServiceHelper.updateCatalogItem(dataset, "id"))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("Versioned Dataset Id must be provided for updating versioned dataset.");
    }
  }

  @Test
  public void testUpdateVersionedDatasetWithVersionedViewDisabledShouldFail() throws Exception {
    try (MockedStatic<CatalogUtil> mockedCatalogUtil = mockStatic(CatalogUtil.class)) {
      final List<String> tableKey = Arrays.asList("source", "path");
      final NamespaceKey namespaceKey = new NamespaceKey(tableKey);
      mockedCatalogUtil
          .when(() -> CatalogUtil.requestedPluginSupportsVersionedTables(namespaceKey, catalog))
          .thenReturn(true);

      final VersionedDatasetId id =
          VersionedDatasetId.newBuilder()
              .setTableVersionContext(TableVersionContext.of(VersionContext.ofBranch("main")))
              .setContentId(UUID.randomUUID().toString())
              .setTableKey(tableKey)
              .build();

      Dataset dataset =
          new Dataset(
              id.asString(),
              Dataset.DatasetType.VIRTUAL_DATASET,
              tableKey,
              null,
              null,
              null,
              null,
              "sql",
              null,
              null,
              null);

      when(optionManager.getOption(VERSIONED_VIEW_ENABLED)).thenReturn(false);
      UserExceptionAssert.assertThatThrownBy(
              () -> catalogServiceHelper.updateCatalogItem(dataset, id.asString()))
          .hasErrorType(UNSUPPORTED_OPERATION)
          .hasMessage("Versioned view is not enabled");
    }
  }

  @Test
  public void testUpdateVersionedDatasetWithTimeTravelQueryShouldFail() throws Exception {
    try (MockedStatic<CatalogUtil> mockedCatalogUtil = mockStatic(CatalogUtil.class)) {
      final List<String> tableKey = Arrays.asList("source", "path");
      final NamespaceKey namespaceKey = new NamespaceKey(tableKey);
      mockedCatalogUtil
          .when(() -> CatalogUtil.requestedPluginSupportsVersionedTables(namespaceKey, catalog))
          .thenReturn(true);

      final VersionedDatasetId id =
          VersionedDatasetId.newBuilder()
              .setTableVersionContext(TableVersionContext.of(VersionContext.ofBranch("main")))
              .setContentId(UUID.randomUUID().toString())
              .setTableKey(tableKey)
              .build();

      Dataset dataset =
          new Dataset(
              id.asString(),
              Dataset.DatasetType.VIRTUAL_DATASET,
              tableKey,
              null,
              null,
              null,
              null,
              "SELECT * FROM foo AT SNAPSHOT '1'",
              null,
              null,
              null);

      when(optionManager.getOption(VERSIONED_VIEW_ENABLED)).thenReturn(true);
      UserExceptionAssert.assertThatThrownBy(
              () -> catalogServiceHelper.updateCatalogItem(dataset, id.asString()))
          .hasErrorType(UNSUPPORTED_OPERATION)
          .hasMessage(
              "Versioned views not supported for time travel queries. Please use AT TAG or AT COMMIT instead");
    }
  }

  @Test
  public void testUpdateNonExistentVersionedDatasetShouldFail() throws Exception {
    try (MockedStatic<CatalogUtil> mockedCatalogUtil = mockStatic(CatalogUtil.class)) {
      final List<String> tableKey = Arrays.asList("source", "path");
      final NamespaceKey namespaceKey = new NamespaceKey(tableKey);
      mockedCatalogUtil
          .when(() -> CatalogUtil.requestedPluginSupportsVersionedTables(namespaceKey, catalog))
          .thenReturn(true);

      final VersionedDatasetId id =
          VersionedDatasetId.newBuilder()
              .setTableVersionContext(TableVersionContext.of(VersionContext.ofBranch("main")))
              .setContentId(UUID.randomUUID().toString())
              .setTableKey(tableKey)
              .build();

      Dataset dataset =
          new Dataset(
              id.asString(),
              Dataset.DatasetType.VIRTUAL_DATASET,
              tableKey,
              null,
              null,
              null,
              null,
              "SELECT 1",
              null,
              null,
              null);

      when(optionManager.getOption(VERSIONED_VIEW_ENABLED)).thenReturn(true);
      when(catalog.getTable(dataset.getId())).thenReturn(null);
      assertThatThrownBy(() -> catalogServiceHelper.updateCatalogItem(dataset, id.asString()))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage(String.format("Could not find dataset with id [%s]", dataset.getId()));
    }
  }

  @Test
  public void testUpdateVersionedDatasetWithDifferentNameShouldFail() throws Exception {
    try (MockedStatic<CatalogUtil> mockedCatalogUtil = mockStatic(CatalogUtil.class)) {
      final List<String> tableKey = Arrays.asList("source", "path");
      final NamespaceKey namespaceKey = new NamespaceKey(tableKey);
      mockedCatalogUtil
          .when(() -> CatalogUtil.requestedPluginSupportsVersionedTables(namespaceKey, catalog))
          .thenReturn(true);

      final VersionedDatasetId id =
          VersionedDatasetId.newBuilder()
              .setTableVersionContext(TableVersionContext.of(VersionContext.ofBranch("main")))
              .setContentId(UUID.randomUUID().toString())
              .setTableKey(tableKey)
              .build();

      Dataset dataset =
          new Dataset(
              id.asString(),
              Dataset.DatasetType.VIRTUAL_DATASET,
              tableKey,
              null,
              null,
              null,
              null,
              "SELECT 1",
              null,
              null,
              null);

      when(optionManager.getOption(VERSIONED_VIEW_ENABLED)).thenReturn(true);

      NamespaceKey currentViewPath = new NamespaceKey(Arrays.asList("source", "foo"));
      DremioTable dremioTable = mock(ViewTable.class);
      when(dremioTable.getPath()).thenReturn(currentViewPath);
      when(catalog.getTable(dataset.getId())).thenReturn(dremioTable);
      UserExceptionAssert.assertThatThrownBy(
              () -> catalogServiceHelper.updateCatalogItem(dataset, id.asString()))
          .hasErrorType(UNSUPPORTED_OPERATION)
          .hasMessage("Renaming/moving a versioned view is not supported yet.");
    }
  }

  /**
   * Verifies using {@link CatalogServiceHelper#refreshCatalogItem(String)} to request refresh
   * reflections for given dataset, {@link
   * ReflectionServiceHelper#refreshReflectionsForDataset(String)} should be called.
   *
   * @throws Exception
   */
  @Test
  public void testRefreshCatalogItem() throws Exception {
    DatasetConfig datasetConfig = new DatasetConfig();
    String datasetId = "dataset-id";
    datasetConfig.setId(new EntityId(datasetId));
    datasetConfig.setFullPathList(Collections.singletonList("path"));
    datasetConfig.setType(VIRTUAL_DATASET);

    DremioTable dremioTable = mock(DremioTable.class);
    when(dremioTable.getDatasetConfig()).thenReturn(datasetConfig);
    when(catalog.getTable(datasetId)).thenReturn(dremioTable);

    catalogServiceHelper.refreshCatalogItem(datasetId);

    verify(reflectionServiceHelper, Mockito.times(1)).refreshReflectionsForDataset("dataset-id");
  }
}
