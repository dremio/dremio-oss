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
import static com.dremio.exec.catalog.CatalogOptions.SUPPORT_UDF_API;
import static com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType.UNSUPPORTED_OPERATION;
import static com.dremio.service.namespace.dataset.proto.DatasetType.PHYSICAL_DATASET;
import static com.dremio.service.namespace.dataset.proto.DatasetType.VIRTUAL_DATASET;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dremio.BaseTestQuery;
import com.dremio.catalog.model.VersionContext;
import com.dremio.catalog.model.VersionedDatasetId;
import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.common.collections.Tuple;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.dac.api.CatalogEntity;
import com.dremio.dac.api.CatalogItem;
import com.dremio.dac.api.CatalogPageToken;
import com.dremio.dac.api.Dataset;
import com.dremio.dac.api.File;
import com.dremio.dac.api.Folder;
import com.dremio.dac.api.Function;
import com.dremio.dac.api.Home;
import com.dremio.dac.api.Source;
import com.dremio.dac.api.Space;
import com.dremio.dac.homefiles.HomeFileTool;
import com.dremio.dac.model.spaces.HomeName;
import com.dremio.dac.model.spaces.HomePath;
import com.dremio.dac.service.catalog.CatalogListingResult;
import com.dremio.dac.service.catalog.CatalogServiceHelper;
import com.dremio.dac.service.datasets.DatasetVersionMutator;
import com.dremio.dac.service.reflection.ReflectionServiceHelper;
import com.dremio.dac.service.search.SearchService;
import com.dremio.dac.service.source.SourceService;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.catalog.ConnectionReader;
import com.dremio.exec.catalog.DatasetMetadataState;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.ImmutableVersionedListResponsePage;
import com.dremio.exec.catalog.VersionedListOptions;
import com.dremio.exec.catalog.VersionedPlugin;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.SimpleJobRunner;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.SchemaEntity;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.options.OptionManager;
import com.dremio.plugins.ExternalNamespaceEntry;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.NamespaceServiceImpl;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.catalogpubsub.CatalogEventMessagePublisherProvider;
import com.dremio.service.namespace.catalogstatusevents.CatalogStatusEventsImpl;
import com.dremio.service.namespace.dataset.proto.AccelerationSettings;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.IcebergMetadata;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.dataset.proto.VirtualDataset;
import com.dremio.service.namespace.file.FileFormat;
import com.dremio.service.namespace.file.proto.FileType;
import com.dremio.service.namespace.file.proto.JsonFileConfig;
import com.dremio.service.namespace.file.proto.ParquetFileConfig;
import com.dremio.service.namespace.function.proto.FunctionBody;
import com.dremio.service.namespace.function.proto.FunctionConfig;
import com.dremio.service.namespace.function.proto.FunctionDefinition;
import com.dremio.service.namespace.function.proto.ReturnType;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.source.proto.LegacySourceType;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.space.proto.FolderConfig;
import com.dremio.service.namespace.space.proto.HomeConfig;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import com.dremio.service.reflection.ReflectionSettings;
import com.dremio.test.DremioTest;
import com.dremio.test.UserExceptionAssert;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.protostuff.ByteString;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.inject.Provider;
import javax.ws.rs.core.SecurityContext;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/** Test for CatalogServiceHelper */
public class TestCatalogServiceHelper {

  private LocalKVStoreProvider kvStoreProvider;
  private CatalogServiceHelper catalogServiceHelper;
  private Catalog catalog;
  private SourceService sourceService;
  private NamespaceService namespaceService;
  private ReflectionServiceHelper reflectionServiceHelper;
  private OptionManager optionManager;

  private NamespaceService mockNamespaceService;
  private CatalogServiceHelper catalogServiceHelperWithMockNs;
  private SimpleJobRunner simpleJobRunner;

  @BeforeEach
  public void setup() throws Exception {
    SabotContext sabotContext = mock(SabotContext.class); // getSabotContext();
    CatalogService catalogService = mock(CatalogService.class);
    SecurityContext securityContext = mock(SecurityContext.class);
    DatasetVersionMutator datasetVersionMutator = mock(DatasetVersionMutator.class);
    HomeFileTool homeFileTool = mock(HomeFileTool.class);
    SearchService searchService = mock(SearchService.class);

    kvStoreProvider = new LocalKVStoreProvider(DremioTest.CLASSPATH_SCAN_RESULT, null, true, false);
    kvStoreProvider.start();

    catalog = mock(Catalog.class);
    sourceService = mock(SourceService.class);
    namespaceService =
        new NamespaceServiceImpl(
            kvStoreProvider,
            new CatalogStatusEventsImpl(),
            CatalogEventMessagePublisherProvider.NO_OP);
    mockNamespaceService = mock(NamespaceService.class);
    reflectionServiceHelper = mock(ReflectionServiceHelper.class);
    optionManager = mock(OptionManager.class);

    Principal principal = mock(Principal.class);
    when(principal.getName()).thenReturn("user");
    when(securityContext.getUserPrincipal()).thenReturn(principal);
    when(catalogService.getCatalog(any())).thenReturn(catalog);

    simpleJobRunner = mock(SimpleJobRunner.class);
    Provider<SimpleJobRunner> simpleJobRunnerProvider =
        new Provider<SimpleJobRunner>() {
          @Override
          public SimpleJobRunner get() {
            return simpleJobRunner;
          }
        };
    when(sabotContext.getJobsRunner()).thenReturn(simpleJobRunnerProvider);

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
    catalogServiceHelperWithMockNs =
        new CatalogServiceHelper(
            catalogService,
            securityContext,
            sourceService,
            mockNamespaceService,
            sabotContext,
            reflectionServiceHelper,
            homeFileTool,
            datasetVersionMutator,
            searchService,
            optionManager);
  }

  @AfterEach
  public void tearDown() throws Exception {
    kvStoreProvider.close();
  }

  @Test
  public void testGetTopLevelCatalogItems() throws Exception {
    HomeConfig homeConfig = new HomeConfig();
    homeConfig.setOwner("user");
    homeConfig.setId(new EntityId("home-id"));
    namespaceService.addOrUpdateHome(
        new HomePath(HomeName.getUserHomePath("user")).toNamespaceKey(), homeConfig);

    SpaceConfig spaceConfig = new SpaceConfig();
    spaceConfig.setName("mySpace");
    spaceConfig.setId(new EntityId("space-id"));
    namespaceService.addOrUpdateSpace(new NamespaceKey("mySpace"), spaceConfig);

    SourceConfig sourceConfig = new SourceConfig();
    sourceConfig.setName("mySource");
    sourceConfig.setId(new EntityId("source-id"));
    when(sourceService.getSources()).thenReturn(Arrays.asList(sourceConfig));

    FunctionConfig functionConfig = new FunctionConfig();
    functionConfig.setName("myFunction");
    functionConfig.setFullPathList(Lists.newArrayList("myFunction"));
    functionConfig.setId(new EntityId("function-id"));
    namespaceService.addOrUpdateFunction(new NamespaceKey("myFunction"), functionConfig);

    List<CatalogItem> topLevelCatalogItems =
        catalogServiceHelper.getTopLevelCatalogItems(Collections.EMPTY_LIST);
    assertEquals(4, topLevelCatalogItems.size());

    int homeCount = 0;
    int spaceCount = 0;
    int sourceCount = 0;
    int functionCount = 0;

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

        if (item.getContainerType() == CatalogItem.ContainerSubType.FUNCTION) {
          functionCount++;
          assertEquals(item.getId(), functionConfig.getId().getId());
        }
      }
    }

    assertEquals(homeCount, 1);
    assertEquals(spaceCount, 1);
    assertEquals(sourceCount, 1);
    assertEquals(functionCount, 1);
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
            datasetConfig.getId().getId(), ImmutableList.of(), ImmutableList.of(), null, null);

    assertTrue(entity.isPresent());

    CatalogEntity catalogEntity = entity.get();
    assertInstanceOf(Dataset.class, catalogEntity);

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
            datasetConfig.getId().getId(), ImmutableList.of(), ImmutableList.of(), null, null);

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

    namespaceService.addOrUpdateDataset(new NamespaceKey(pathList), datasetConfig);

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
    namespaceService.addOrUpdateSpace(new NamespaceKey(spaceConfig.getName()), spaceConfig);

    FolderConfig folderConfig = new FolderConfig();
    folderConfig.setId(new EntityId("folder-id-1"));
    folderConfig.setName("folder1");
    folderConfig.setFullPathList(Arrays.asList(spaceConfig.getName(), folderConfig.getName()));
    namespaceService.addOrUpdateFolder(
        new NamespaceKey(folderConfig.getFullPathList()), folderConfig);

    Optional<CatalogEntity> catalogEntityById =
        catalogServiceHelper.getCatalogEntityById(
            spaceConfig.getId().getId(), ImmutableList.of(), ImmutableList.of(), null, null);

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

    when(mockNamespaceService.getEntityById(sourceConfig.getId()))
        .thenReturn(Optional.of(namespaceContainer));
    when(mockNamespaceService.getEntities(Collections.singletonList(new NamespaceKey("root"))))
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
    when(mockNamespaceService.getDataset(any()))
        .thenReturn(new DatasetConfig().setId(new EntityId().setId("id")));

    // Make folder2 part of KV store.
    when(mockNamespaceService.list(eq(new NamespaceKey("root")), any(), anyInt()))
        .thenReturn(
            ImmutableList.of(
                new NameSpaceContainer()
                    .setType(NameSpaceContainer.Type.FOLDER)
                    .setFullPathList(ImmutableList.of("root", "folder2"))));
    final String folder2Id = "folder2-id";
    FolderConfig folder2Config = new FolderConfig().setId(new EntityId(folder2Id));
    when(mockNamespaceService.getFolder(eq(new NamespaceKey(ImmutableList.of("root", "folder2")))))
        .thenReturn(folder2Config);

    // Resulting source.
    doAnswer(
            (invocation) -> {
              Object[] args = invocation.getRawArguments();
              return new Source(
                  (SourceConfig) args[0],
                  mock(AccelerationSettings.class),
                  mock(ConnectionReader.class),
                  (List<CatalogItem>) args[1],
                  null,
                  SourceState.GOOD);
            })
        .when(sourceService)
        .fromSourceConfig(
            eq(sourceConfig),
            ArgumentMatchers.argThat(
                (items) ->
                    // Check number of children.
                    items.size() == 5),
            any());

    Optional<CatalogEntity> catalogEntityById =
        catalogServiceHelperWithMockNs.getCatalogEntityById(
            sourceConfig.getId().getId(), ImmutableList.of(), ImmutableList.of(), null, null);

    // Verify exists was not called.
    verify(mockNamespaceService, times(0)).exists(any());

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
    List<String> pathList = Collections.singletonList("mySpace");
    spaceConfig.setId(new EntityId("space-id"));
    spaceConfig.setName("mySpace");
    namespaceService.addOrUpdateSpace(new NamespaceKey(spaceConfig.getName()), spaceConfig);

    FolderConfig folderConfig = new FolderConfig();
    folderConfig.setId(new EntityId("folder-id"));
    folderConfig.setName("folder");
    folderConfig.setFullPathList(Arrays.asList(spaceConfig.getName(), folderConfig.getName()));
    namespaceService.addOrUpdateFolder(
        new NamespaceKey(folderConfig.getFullPathList()), folderConfig);

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
  public void testGetFunctionCatalogEntityById() throws Exception {
    SourceConfig sourceConfig = new SourceConfig();
    sourceConfig.setName("source");
    sourceConfig.setId(new EntityId("source-id"));
    namespaceService.addOrUpdateSource(new NamespaceKey(sourceConfig.getName()), sourceConfig);

    when(optionManager.getOption(SUPPORT_UDF_API)).thenReturn(true);
    List<String> path = Arrays.asList(sourceConfig.getName(), "myFunction");
    FunctionConfig functionConfig = new FunctionConfig();
    functionConfig.setId(new EntityId("function-id"));
    functionConfig.setFullPathList(path);
    functionConfig.setName(Iterables.getLast(path));
    functionConfig.setReturnType(
        new ReturnType().setRawDataType(ByteString.copyFrom(CompleteType.INT.serialize())));

    FunctionDefinition functionDefinition = new FunctionDefinition();
    functionDefinition.setFunctionBody(new FunctionBody().setRawBody("SELECT 1"));
    functionConfig.setFunctionDefinitionsList(ImmutableList.of(functionDefinition));

    NameSpaceContainer namespaceContainer = new NameSpaceContainer();
    namespaceContainer.setType(NameSpaceContainer.Type.FUNCTION);
    namespaceContainer.setFunction(functionConfig);
    namespaceService.addOrUpdateFunction(
        new NamespaceKey(functionConfig.getFullPathList()), functionConfig);

    Optional<CatalogEntity> catalogEntityById =
        catalogServiceHelper.getCatalogEntityById(
            functionConfig.getId().getId(), ImmutableList.of(), ImmutableList.of(), null, null);

    assertTrue(catalogEntityById.isPresent());

    CatalogEntity catalogEntity = catalogEntityById.get();
    assertTrue(catalogEntity instanceof Function);

    Function function = (Function) catalogEntity;
    assertEquals(function.getId(), functionConfig.getId().getId());
    assertEquals(function.getName(), functionConfig.getName());
  }

  @Test
  public void testGetFunctionCatalogEntityByPath() throws Exception {
    SourceConfig sourceConfig = new SourceConfig();
    sourceConfig.setName("source");
    sourceConfig.setId(new EntityId("source-id"));
    namespaceService.addOrUpdateSource(new NamespaceKey(sourceConfig.getName()), sourceConfig);

    when(optionManager.getOption(SUPPORT_UDF_API)).thenReturn(true);
    List<String> path = Arrays.asList(sourceConfig.getName(), "myFunction");
    FunctionConfig functionConfig = new FunctionConfig();
    functionConfig.setId(new EntityId("function-id"));
    functionConfig.setFullPathList(path);
    functionConfig.setName(Iterables.getLast(path));
    functionConfig.setReturnType(
        new ReturnType().setRawDataType(ByteString.copyFrom(CompleteType.INT.serialize())));

    FunctionDefinition functionDefinition = new FunctionDefinition();
    functionDefinition.setFunctionBody(new FunctionBody().setRawBody("SELECT 1"));
    functionConfig.setFunctionDefinitionsList(ImmutableList.of(functionDefinition));

    NameSpaceContainer namespaceContainer = new NameSpaceContainer();
    namespaceContainer.setType(NameSpaceContainer.Type.FUNCTION);
    namespaceContainer.setFunction(functionConfig);
    namespaceService.addOrUpdateFunction(
        new NamespaceKey(functionConfig.getFullPathList()), functionConfig);

    Optional<CatalogEntity> catalogEntityByPath =
        catalogServiceHelper.getCatalogEntityByPath(path, ImmutableList.of(), ImmutableList.of());

    assertTrue(catalogEntityByPath.isPresent());

    CatalogEntity catalogEntity = catalogEntityByPath.get();
    assertTrue(catalogEntity instanceof Function);

    Function function = (Function) catalogEntity;
    assertEquals(function.getId(), functionConfig.getId().getId());
    assertEquals(function.getName(), functionConfig.getName());
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
  public void testCreateVDSWithNonExistentSpaceShouldFail() {
    Dataset dataset =
        new Dataset(
            null,
            Dataset.DatasetType.VIRTUAL_DATASET,
            Arrays.asList("NonExistentSpace", "dataset"),
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
    SourceConfig sourceConfig = new SourceConfig();
    sourceConfig.setName("source");
    sourceConfig.setId(new EntityId("source-id"));
    sourceConfig.setType("");
    NameSpaceContainer namespaceContainer = new NameSpaceContainer();
    namespaceContainer.setSource(sourceConfig);
    namespaceContainer.setType(NameSpaceContainer.Type.SOURCE);
    namespaceService.addOrUpdateSource(new NamespaceKey(sourceConfig.getName()), sourceConfig);

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
  public void testCreateSpaceWithConflict() throws Exception {
    SpaceConfig spaceConfig = new SpaceConfig();
    spaceConfig.setName("mySpace");
    spaceConfig.setId(new EntityId("space-id"));
    namespaceService.addOrUpdateSpace(new NamespaceKey("mySpace"), spaceConfig);

    // VDS
    Space space = new Space(null, "mySpace", null, 0L, null);

    assertThatThrownBy(() -> catalogServiceHelper.createCatalogItem(space))
        .isInstanceOf(ConcurrentModificationException.class);
  }

  @Test
  public void testCreateSpace() throws Exception {
    // VDS
    Space space = new Space(null, "mySpace", null, 0L, null);

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
  public void testCreateFunctionWithEmptyPathShouldFail() {
    when(optionManager.getOption(SUPPORT_UDF_API)).thenReturn(true);

    Function function =
        new Function(null, Collections.EMPTY_LIST, "", null, null, true, null, null, null);

    assertThatThrownBy(() -> catalogServiceHelper.createCatalogItem(function))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testCreateFunctionWithEmptyNameShouldFail() {
    when(optionManager.getOption(SUPPORT_UDF_API)).thenReturn(true);

    Function function =
        new Function(null, ImmutableList.of(""), "", null, null, true, null, null, null);

    assertThatThrownBy(() -> catalogServiceHelper.createCatalogItem(function))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testCreateFunctionWithIdShouldFail() {
    when(optionManager.getOption(SUPPORT_UDF_API)).thenReturn(true);

    Function function =
        new Function(
            "id", ImmutableList.of("space", "myFunction"), "", null, null, true, null, null, null);

    assertThatThrownBy(() -> catalogServiceHelper.createCatalogItem(function))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testCreateFunctionWithTagShouldFail() {
    when(optionManager.getOption(SUPPORT_UDF_API)).thenReturn(true);

    Function function =
        new Function(
            null,
            ImmutableList.of("space", "myFunction"),
            "tag",
            null,
            null,
            true,
            null,
            null,
            null);

    assertThatThrownBy(() -> catalogServiceHelper.createCatalogItem(function))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testCreateFunctionWithNonExistentParentShouldFail() {
    when(optionManager.getOption(SUPPORT_UDF_API)).thenReturn(true);

    Function function =
        new Function(
            null,
            ImmutableList.of("NonExistentSpace", "myFunction"),
            null,
            null,
            null,
            true,
            null,
            null,
            null);

    assertThatThrownBy(() -> catalogServiceHelper.createCatalogItem(function))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testCreateFunctionWithEmptyReturnTypeShouldFail() {
    when(optionManager.getOption(SUPPORT_UDF_API)).thenReturn(true);

    Function function =
        new Function(
            null, ImmutableList.of("myFunction"), null, null, null, true, null, "SELECT 1", null);

    assertThatThrownBy(() -> catalogServiceHelper.createCatalogItem(function))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testCreateFunctionWithEmptyBodyShouldFail() {
    when(optionManager.getOption(SUPPORT_UDF_API)).thenReturn(true);

    Function function =
        new Function(
            null, ImmutableList.of("myFunction"), null, null, null, true, null, null, "INT");

    assertThatThrownBy(() -> catalogServiceHelper.createCatalogItem(function))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testCreateFunctionWithoutScalarTypeShouldFail() {
    when(optionManager.getOption(SUPPORT_UDF_API)).thenReturn(true);

    Function function =
        new Function(
            null, ImmutableList.of("myFunction"), null, null, null, null, null, "SELECT 1", "INT");

    assertThatThrownBy(() -> catalogServiceHelper.createCatalogItem(function))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testCreateFunctionAtExistingEntityShouldFail() throws Exception {
    Space space = new Space(null, "mySpace", null, 0L, null);
    CatalogEntity spaceItem = catalogServiceHelper.createCatalogItem(space);
    assertTrue(spaceItem instanceof Space);

    Folder folder = new Folder(null, ImmutableList.of("mySpace", "myFolder"), null, null);
    CatalogEntity folderItem = catalogServiceHelper.createCatalogItem(folder);
    assertTrue(folderItem instanceof Folder);

    when(optionManager.getOption(SUPPORT_UDF_API)).thenReturn(true);

    Function function =
        new Function(
            null,
            ImmutableList.of("mySpace", "myFolder"),
            null,
            null,
            null,
            true,
            null,
            "SELECT 1",
            "INT");

    assertThatThrownBy(() -> catalogServiceHelper.createCatalogItem(function))
        .isInstanceOf(ConcurrentModificationException.class);
  }

  @Test
  public void testCreateFunctionWithSyntaxErrorShouldFailButBeingCleanedUp() throws Exception {
    when(optionManager.getOption(SUPPORT_UDF_API)).thenReturn(true);

    Space space = new Space(null, "mySpace", null, 0L, null);
    CatalogEntity spaceItem = catalogServiceHelper.createCatalogItem(space);
    assertTrue(spaceItem instanceof Space);

    List<String> path = ImmutableList.of("mySpace", "myFunction");
    NamespaceKey key = new NamespaceKey(path);
    Function function =
        new Function(
            null,
            path,
            null,
            null,
            null,
            true,
            null,
            "SELECT 1",
            "IMT"); // syntax error will fail creating function definition

    doThrow(new IllegalStateException(UserException.parseError().buildSilently()))
        .when(simpleJobRunner)
        .runQueryAsJob(any(), any(), any(), any());

    assertThatThrownBy(() -> catalogServiceHelper.createCatalogItem(function))
        .isInstanceOf(UserException.class);
    assertThatThrownBy(() -> namespaceService.getFunction(key))
        .isInstanceOf(NamespaceNotFoundException.class);
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
    SourceConfig sourceConfig = new SourceConfig();
    sourceConfig.setName("source");
    sourceConfig.setId(new EntityId("source-id"));
    sourceConfig.setType("");
    NameSpaceContainer namespaceContainer = new NameSpaceContainer();
    namespaceContainer.setSource(sourceConfig);
    namespaceContainer.setType(NameSpaceContainer.Type.SOURCE);
    namespaceService.addOrUpdateSource(new NamespaceKey(sourceConfig.getName()), sourceConfig);

    List<String> path = Arrays.asList("source", "path");
    String id = "dataset-id";
    DatasetConfig config = new DatasetConfig();
    config.setId(new EntityId(id));
    config.setFullPathList(path);
    config.setName(path.get(path.size() - 1));
    config.setCreatedAt(100L);
    VirtualDataset virtualDataset = new VirtualDataset();
    virtualDataset.setSql("sql");
    virtualDataset.setContextList(null);
    config.setType(PHYSICAL_DATASET);
    config.setVirtualDataset(virtualDataset);
    namespaceService.addOrUpdateDataset(new NamespaceKey(path), config);

    DremioTable dremioTable = mock(DremioTable.class);
    config = namespaceService.getDataset(new NamespaceKey(path));
    when(dremioTable.getDatasetConfig()).thenReturn(config);
    when(catalog.getTable(any(String.class))).thenReturn(dremioTable);

    Dataset dataset =
        new Dataset(
            id,
            Dataset.DatasetType.VIRTUAL_DATASET,
            path,
            null,
            0L,
            config.getTag(),
            null,
            "sql",
            null,
            null,
            null);
    assertThatThrownBy(() -> catalogServiceHelper.updateCatalogItem(dataset, dataset.getId()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testUpdateVDSWithoutSqlShouldFail() throws Exception {
    SourceConfig sourceConfig = new SourceConfig();
    sourceConfig.setName("source");
    sourceConfig.setId(new EntityId("source-id"));
    sourceConfig.setType("");
    NameSpaceContainer namespaceContainer = new NameSpaceContainer();
    namespaceContainer.setSource(sourceConfig);
    namespaceContainer.setType(NameSpaceContainer.Type.SOURCE);
    namespaceService.addOrUpdateSource(new NamespaceKey(sourceConfig.getName()), sourceConfig);

    List<String> path = Arrays.asList("source", "path");
    String id = "dataset-id";
    DatasetConfig config = new DatasetConfig();
    config.setId(new EntityId(id));
    config.setFullPathList(path);
    config.setName(path.get(path.size() - 1));
    config.setCreatedAt(100L);
    VirtualDataset virtualDataset = new VirtualDataset();
    virtualDataset.setSql("sql");
    virtualDataset.setContextList(null);
    config.setType(PHYSICAL_DATASET);
    config.setVirtualDataset(virtualDataset);
    namespaceService.addOrUpdateDataset(new NamespaceKey(path), config);

    DremioTable dremioTable = mock(DremioTable.class);
    config = namespaceService.getDataset(new NamespaceKey(path));
    when(dremioTable.getDatasetConfig()).thenReturn(config);
    when(catalog.getTable(any(String.class))).thenReturn(dremioTable);

    Dataset dataset =
        new Dataset(
            id,
            Dataset.DatasetType.VIRTUAL_DATASET,
            path,
            null,
            0L,
            config.getTag(),
            null,
            null,
            null,
            null,
            null);
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
    SourceConfig sourceConfig = new SourceConfig();
    sourceConfig.setName("source");
    sourceConfig.setId(new EntityId("source-id"));
    sourceConfig.setType("");
    NameSpaceContainer namespaceContainer = new NameSpaceContainer();
    namespaceContainer.setSource(sourceConfig);
    namespaceContainer.setType(NameSpaceContainer.Type.SOURCE);
    namespaceService.addOrUpdateSource(new NamespaceKey(sourceConfig.getName()), sourceConfig);

    List<String> path = Arrays.asList("source", "path");
    String id = "dataset-id";
    DatasetConfig config = new DatasetConfig();
    config.setId(new EntityId(id));
    config.setFullPathList(path);
    config.setName(path.get(path.size() - 1));
    config.setCreatedAt(100L);
    VirtualDataset virtualDataset = new VirtualDataset();
    virtualDataset.setSql("sql");
    virtualDataset.setContextList(null);
    config.setType(VIRTUAL_DATASET);
    config.setVirtualDataset(virtualDataset);
    namespaceService.addOrUpdateDataset(new NamespaceKey(path), config);

    Dataset dataset =
        new Dataset(
            id,
            Dataset.DatasetType.VIRTUAL_DATASET,
            path,
            null,
            0L,
            namespaceService.getEntityById(new EntityId(id)).get().getDataset().getTag(),
            null,
            "sql",
            null,
            null,
            null);

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
    SourceConfig sourceConfig = new SourceConfig();
    sourceConfig.setName("source");
    sourceConfig.setId(new EntityId("source-id"));
    sourceConfig.setType("");
    NameSpaceContainer namespaceContainer = new NameSpaceContainer();
    namespaceContainer.setSource(sourceConfig);
    namespaceContainer.setType(NameSpaceContainer.Type.SOURCE);
    namespaceService.addOrUpdateSource(new NamespaceKey(sourceConfig.getName()), sourceConfig);

    String id = "dataset-id";
    List<String> path = Arrays.asList("source", "path");

    DatasetConfig config = new DatasetConfig();
    config.setId(new EntityId(id));
    config.setFullPathList(path);
    config.setName(path.get(path.size() - 1));
    config.setCreatedAt(100L);
    VirtualDataset virtualDataset = new VirtualDataset();
    virtualDataset.setSql("sql");
    config.setType(VIRTUAL_DATASET);
    config.setVirtualDataset(virtualDataset);
    namespaceService.addOrUpdateDataset(new NamespaceKey(path), config);

    DatasetConfig addedDataset =
        namespaceService.getEntityById(new EntityId(id)).get().getDataset();

    Dataset dataset =
        new Dataset(
            "dataset-id",
            Dataset.DatasetType.PHYSICAL_DATASET,
            Arrays.asList("source", "path"),
            null,
            0L,
            addedDataset.getTag(),
            null,
            null,
            null,
            null,
            null);

    assertThatThrownBy(() -> catalogServiceHelper.updateCatalogItem(dataset, dataset.getId()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testUpdatePDS() throws Exception {
    SourceConfig sourceConfig = new SourceConfig();
    sourceConfig.setName("source");
    sourceConfig.setId(new EntityId("source-id"));
    sourceConfig.setType("");
    NameSpaceContainer namespaceContainer = new NameSpaceContainer();
    namespaceContainer.setSource(sourceConfig);
    namespaceContainer.setType(NameSpaceContainer.Type.SOURCE);
    namespaceService.addOrUpdateSource(new NamespaceKey(sourceConfig.getName()), sourceConfig);

    String id = "dataset-id";
    List<String> path = Arrays.asList("source", "path");

    DatasetConfig config = new DatasetConfig();
    config.setId(new EntityId(id));
    config.setFullPathList(path);
    config.setName(path.get(path.size() - 1));
    config.setCreatedAt(100L);
    config.setType(DatasetType.PHYSICAL_DATASET_SOURCE_FILE);
    PhysicalDataset physicalDataset = new PhysicalDataset();
    physicalDataset.setFormatSettings(new ParquetFileConfig().asFileConfig());
    config.setPhysicalDataset(physicalDataset);
    namespaceService.addOrUpdateDataset(new NamespaceKey(path), config);

    // Verify file type.
    DatasetConfig addedConfig = namespaceService.getEntityById(new EntityId(id)).get().getDataset();
    assertEquals(FileType.PARQUET, addedConfig.getPhysicalDataset().getFormatSettings().getType());

    // Set up new file format.
    ReflectionSettings reflectionSettings = mock(ReflectionSettings.class);
    when(reflectionSettings.getStoredReflectionSettings(any(NamespaceKey.class)))
        .thenReturn(Optional.empty());
    when(reflectionServiceHelper.getReflectionSettings()).thenReturn(reflectionSettings);

    DremioTable dremioTable = mock(DremioTable.class);
    config.getPhysicalDataset().setFormatSettings(new JsonFileConfig().asFileConfig());
    when(dremioTable.getDatasetConfig()).thenReturn(config);
    when(catalog.getTable(any(String.class))).thenReturn(dremioTable);

    Dataset dataset =
        new Dataset(
            id,
            Dataset.DatasetType.PHYSICAL_DATASET,
            path,
            null,
            0L,
            addedConfig.getTag(),
            null,
            null,
            null,
            new JsonFileConfig(),
            null);

    catalogServiceHelper.updateCatalogItem(dataset, dataset.getId());

    // Verify the catalog was called with the changed format.
    verify(catalog, times(1))
        .createOrUpdateDataset(
            eq(new NamespaceKey("source")),
            eq(new NamespaceKey(dataset.getPath())),
            argThat(
                (ds) -> ds.getPhysicalDataset().getFormatSettings().getType() == FileType.JSON));
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
  public void testUpdateFunctionWithMismatchedIdShouldFail() throws Exception {
    when(optionManager.getOption(SUPPORT_UDF_API)).thenReturn(true);

    Function function =
        new Function(
            "foo",
            ImmutableList.of("mySpace", "myFolder"),
            null,
            null,
            null,
            true,
            null,
            "SELECT 1",
            "INT");

    assertThatThrownBy(() -> catalogServiceHelper.updateCatalogItem(function, "bar"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testUpdateFunctionWithEntityBeingFolderShouldFail() throws Exception {
    Space space = new Space(null, "mySpace", null, 0L, null);
    CatalogEntity spaceItem = catalogServiceHelper.createCatalogItem(space);
    assertTrue(spaceItem instanceof Space);

    Folder folder = new Folder(null, ImmutableList.of("mySpace", "myFolder"), null, null);
    CatalogEntity folderItem = catalogServiceHelper.createCatalogItem(folder);
    assertTrue(folderItem instanceof Folder);

    when(optionManager.getOption(SUPPORT_UDF_API)).thenReturn(true);

    Function function =
        new Function(
            folderItem.getId(),
            ImmutableList.of("mySpace", "myFolder"),
            null,
            null,
            null,
            true,
            null,
            "SELECT 1",
            "INT");

    assertThatThrownBy(() -> catalogServiceHelper.updateCatalogItem(function, folderItem.getId()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testUpdateFunctionWithMismatchPathShouldFail() throws Exception {
    when(optionManager.getOption(SUPPORT_UDF_API)).thenReturn(true);
    EntityId id = new EntityId(UUID.randomUUID().toString());
    NamespaceKey key = new NamespaceKey(ImmutableList.of("mySpace", "myFunction"));
    FunctionConfig functionConfig =
        new FunctionConfig().setId(id).setFullPathList(key.getPathComponents());
    NameSpaceContainer container =
        new NameSpaceContainer()
            .setFunction(functionConfig)
            .setType(NameSpaceContainer.Type.FUNCTION)
            .setFullPathList(key.getPathComponents());
    when(mockNamespaceService.getEntityById(id)).thenReturn(Optional.of(container));

    Function function =
        new Function(
            id.getId(),
            ImmutableList.of("mySpace", "myFunction1"),
            null,
            null,
            null,
            true,
            null,
            "SELECT 1",
            "INT");

    assertThatThrownBy(() -> catalogServiceHelperWithMockNs.updateCatalogItem(function, id.getId()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testUpdateFunctionWithEmptyReturnTypeShouldFail() {
    when(optionManager.getOption(SUPPORT_UDF_API)).thenReturn(true);
    EntityId id = new EntityId(UUID.randomUUID().toString());
    NamespaceKey key = new NamespaceKey(ImmutableList.of("mySpace", "myFunction"));
    FunctionConfig functionConfig =
        new FunctionConfig().setId(id).setFullPathList(key.getPathComponents());
    NameSpaceContainer container =
        new NameSpaceContainer()
            .setFunction(functionConfig)
            .setType(NameSpaceContainer.Type.FUNCTION)
            .setFullPathList(key.getPathComponents());
    when(mockNamespaceService.getEntityById(id)).thenReturn(Optional.of(container));

    Function function =
        new Function(
            id.getId(),
            ImmutableList.of("mySpace", "myFunction"),
            null,
            null,
            null,
            true,
            null,
            "SELECT 1",
            null);

    assertThatThrownBy(() -> catalogServiceHelperWithMockNs.updateCatalogItem(function, id.getId()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testUpdateFunctionWithEmptyBodyShouldFail() {
    when(optionManager.getOption(SUPPORT_UDF_API)).thenReturn(true);
    EntityId id = new EntityId(UUID.randomUUID().toString());
    NamespaceKey key = new NamespaceKey(ImmutableList.of("mySpace", "myFunction"));
    FunctionConfig functionConfig =
        new FunctionConfig().setId(id).setFullPathList(key.getPathComponents());
    NameSpaceContainer container =
        new NameSpaceContainer()
            .setFunction(functionConfig)
            .setType(NameSpaceContainer.Type.FUNCTION)
            .setFullPathList(key.getPathComponents());
    when(mockNamespaceService.getEntityById(id)).thenReturn(Optional.of(container));

    Function function =
        new Function(
            id.getId(),
            ImmutableList.of("mySpace", "myFunction"),
            null,
            null,
            null,
            true,
            null,
            null,
            "INT");

    assertThatThrownBy(() -> catalogServiceHelperWithMockNs.updateCatalogItem(function, id.getId()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testUpdateFunctionWithoutScalarTypeShouldFail() {
    when(optionManager.getOption(SUPPORT_UDF_API)).thenReturn(true);
    EntityId id = new EntityId(UUID.randomUUID().toString());
    NamespaceKey key = new NamespaceKey(ImmutableList.of("mySpace", "myFunction"));
    FunctionConfig functionConfig =
        new FunctionConfig().setId(id).setFullPathList(key.getPathComponents());
    NameSpaceContainer container =
        new NameSpaceContainer()
            .setFunction(functionConfig)
            .setType(NameSpaceContainer.Type.FUNCTION)
            .setFullPathList(key.getPathComponents());
    when(mockNamespaceService.getEntityById(id)).thenReturn(Optional.of(container));

    Function function =
        new Function(
            id.getId(),
            ImmutableList.of("mySpace", "myFunction"),
            null,
            null,
            null,
            null,
            null,
            "SELECT 1",
            "INT");

    assertThatThrownBy(() -> catalogServiceHelperWithMockNs.updateCatalogItem(function, id.getId()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testUpdateFunctionWithWrongTagShouldFail() throws Exception {
    when(optionManager.getOption(SUPPORT_UDF_API)).thenReturn(true);

    Space space = new Space(null, "mySpace", null, 0L, null);
    CatalogEntity spaceItem = catalogServiceHelper.createCatalogItem(space);
    assertTrue(spaceItem instanceof Space);

    EntityId id = new EntityId(UUID.randomUUID().toString());
    List<String> path = ImmutableList.of("mySpace", "myFunction");
    NamespaceKey key = new NamespaceKey(path);
    ReturnType returnType =
        new ReturnType().setRawDataType(ByteString.copyFrom(CompleteType.INT.serialize()));
    FunctionDefinition functionDefinition =
        new FunctionDefinition()
            .setFunctionBody(new FunctionBody().setRawBody("SELECT 1").setSerializedPlan(null))
            .setFunctionArgList(Collections.EMPTY_LIST);
    FunctionConfig functionConfig =
        new FunctionConfig()
            .setId(id)
            .setName(key.toString())
            .setFullPathList(path)
            .setReturnType(returnType)
            .setFunctionDefinitionsList(ImmutableList.of(functionDefinition));

    namespaceService.addOrUpdateFunction(key, functionConfig);

    FunctionConfig existingFunctionConfig = namespaceService.getEntityById(id).get().getFunction();
    Function function =
        new Function(
            existingFunctionConfig.getId().getId(),
            path,
            "wrongTag",
            existingFunctionConfig.getCreatedAt(),
            null,
            true,
            null,
            "SELECT 'Hello World'",
            "VARCHAR");

    assertThatThrownBy(() -> catalogServiceHelper.updateCatalogItem(function, function.getId()))
        .isInstanceOf(ConcurrentModificationException.class);
  }

  @Test
  public void testUpdateFunctionWithSyntaxErrorShouldFailButKeepFunctionUnchanged()
      throws Exception {
    when(optionManager.getOption(SUPPORT_UDF_API)).thenReturn(true);

    Space space = new Space(null, "mySpace", null, 0L, null);
    CatalogEntity spaceItem = catalogServiceHelper.createCatalogItem(space);
    assertTrue(spaceItem instanceof Space);

    EntityId id = new EntityId(UUID.randomUUID().toString());
    List<String> path = ImmutableList.of("mySpace", "myFunction");
    NamespaceKey key = new NamespaceKey(path);
    ReturnType returnType =
        new ReturnType().setRawDataType(ByteString.copyFrom(CompleteType.INT.serialize()));
    FunctionDefinition functionDefinition =
        new FunctionDefinition()
            .setFunctionBody(new FunctionBody().setRawBody("SELECT 1").setSerializedPlan(null))
            .setFunctionArgList(Collections.EMPTY_LIST);
    FunctionConfig functionConfig =
        new FunctionConfig()
            .setId(id)
            .setName(key.toString())
            .setFullPathList(path)
            .setReturnType(returnType)
            .setFunctionDefinitionsList(ImmutableList.of(functionDefinition));

    namespaceService.addOrUpdateFunction(key, functionConfig);

    FunctionConfig existingFunctionConfig = namespaceService.getEntityById(id).get().getFunction();
    Function function =
        new Function(
            existingFunctionConfig.getId().getId(),
            path,
            existingFunctionConfig.getTag(),
            existingFunctionConfig.getCreatedAt(),
            null,
            true,
            null,
            "SELECT 'Hello World'",
            "VARCHR"); // syntax error will fail updating function definition

    doThrow(new IllegalStateException(UserException.parseError().buildSilently()))
        .when(simpleJobRunner)
        .runQueryAsJob(any(), any(), any(), any());

    assertThatThrownBy(() -> catalogServiceHelper.updateCatalogItem(function, function.getId()))
        .isInstanceOf(UserException.class);

    FunctionConfig latestFunctionConfig = namespaceService.getEntityById(id).get().getFunction();
    assertEquals(
        existingFunctionConfig.getFunctionDefinitionsList(),
        latestFunctionConfig.getFunctionDefinitionsList());
    assertEquals(existingFunctionConfig.getReturnType(), latestFunctionConfig.getReturnType());
    assertEquals(existingFunctionConfig.getName(), latestFunctionConfig.getName());
    assertEquals(existingFunctionConfig.getFullPathList(), latestFunctionConfig.getFullPathList());
  }

  @Test
  public void testDeleteDataset() throws Exception {
    SourceConfig sourceConfig = new SourceConfig();
    sourceConfig.setName("source");
    sourceConfig.setId(new EntityId("source-id"));
    sourceConfig.setType("NAS");
    namespaceService.addOrUpdateSource(new NamespaceKey(sourceConfig.getName()), sourceConfig);

    String id = "dataset-id";
    List<String> path = Arrays.asList("source", "path");

    DatasetConfig config = new DatasetConfig();
    config.setId(new EntityId(id));
    config.setFullPathList(path);
    config.setName(path.get(path.size() - 1));
    config.setCreatedAt(100L);
    config.setType(VIRTUAL_DATASET);
    config.setVirtualDataset(new VirtualDataset());
    namespaceService.addOrUpdateDataset(new NamespaceKey(path), config);

    DatasetConfig addedDataset =
        namespaceService.getEntityById(new EntityId(id)).get().getDataset();

    catalogServiceHelper.deleteCatalogItem(id, addedDataset.getTag());

    assertTrue(namespaceService.getEntityById(new EntityId(id)).isEmpty());
  }

  @Test
  public void testDeleteSource() throws Exception {
    SourceConfig sourceConfig = new SourceConfig();
    sourceConfig.setName("mySource");
    sourceConfig.setId(new EntityId("source-id"));
    sourceConfig.setType("NAS");
    namespaceService.addOrUpdateSource(new NamespaceKey(sourceConfig.getName()), sourceConfig);

    Optional<NameSpaceContainer> optionalContainer =
        namespaceService.getEntityById(sourceConfig.getId());
    assertTrue(optionalContainer.isPresent());

    catalogServiceHelper.deleteCatalogItem(
        sourceConfig.getId().getId(), optionalContainer.get().getSource().getTag());

    sourceConfig.setTag(optionalContainer.get().getSource().getTag());
    verify(sourceService, times(1)).deleteSource(sourceConfig);
  }

  @Test
  public void testDeleteSpace() throws Exception {
    SpaceConfig spaceConfig = new SpaceConfig();
    spaceConfig.setName("mySpace");
    spaceConfig.setId(new EntityId("space-id"));
    namespaceService.addOrUpdateSpace(new NamespaceKey("mySpace"), spaceConfig);

    Optional<NameSpaceContainer> optionalContainer =
        namespaceService.getEntityById(spaceConfig.getId());
    assertTrue(optionalContainer.isPresent());

    catalogServiceHelper.deleteCatalogItem(
        spaceConfig.getId().getId(), optionalContainer.get().getSpace().getTag());

    assertTrue(namespaceService.getEntityById(spaceConfig.getId()).isEmpty());
  }

  @Test
  public void testDeleteFolder() throws Exception {
    Folder folder = new Folder("folder-id", Arrays.asList("folder"), null, null);

    namespaceService.addOrUpdateFolder(
        new NamespaceKey(folder.getPath()), CatalogServiceHelper.getFolderConfig(folder));
    Optional<CatalogEntity> optionalEntity =
        catalogServiceHelper.getCatalogEntityById(
            folder.getId(), ImmutableList.of(), ImmutableList.of(), null, null);
    assertTrue(optionalEntity.isPresent());

    catalogServiceHelper.deleteCatalogItem(
        folder.getId(), ((Folder) optionalEntity.get()).getTag());

    assertTrue(namespaceService.getEntityById(new EntityId(folder.getId())).isEmpty());
  }

  @Test
  public void testDeleteFunction() throws Exception {
    when(optionManager.getOption(SUPPORT_UDF_API)).thenReturn(true);

    FunctionConfig functionConfig = new FunctionConfig();
    functionConfig.setName("myFunction");
    functionConfig.setFullPathList(Arrays.asList("myFunction"));
    functionConfig.setId(new EntityId("function-id"));
    namespaceService.addOrUpdateFunction(new NamespaceKey("myFunction"), functionConfig);

    Optional<NameSpaceContainer> optionalContainer =
        namespaceService.getEntityById(functionConfig.getId());
    assertTrue(optionalContainer.isPresent());

    catalogServiceHelper.deleteCatalogItem(functionConfig.getId().getId(), null);

    assertTrue(namespaceService.getEntityById(functionConfig.getId()).isEmpty());
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
    namespaceService.addOrUpdateSource(new NamespaceKey(sourceConfig.getName()), sourceConfig);

    // folder 1
    FolderConfig folderConfig1 = new FolderConfig();
    folderConfig1.setId(new EntityId("folder-id-1"));
    folderConfig1.setName("folder1");
    folderConfig1.setFullPathList(Arrays.asList(sourceConfig.getName(), folderConfig1.getName()));
    namespaceService.addOrUpdateFolder(
        new NamespaceKey(folderConfig1.getFullPathList()), folderConfig1);

    // folder 2
    FolderConfig folderConfig2 = new FolderConfig();
    folderConfig2.setId(new EntityId("folder-id-2"));
    folderConfig2.setName("folder2");
    folderConfig2.setFullPathList(Arrays.asList(sourceConfig.getName(), folderConfig2.getName()));
    namespaceService.addOrUpdateFolder(
        new NamespaceKey(folderConfig2.getFullPathList()), folderConfig2);

    // folder1/folder11
    List<NameSpaceContainer> containerList2 = new ArrayList<>();
    FolderConfig folderConfig11 = new FolderConfig();
    folderConfig11.setId(new EntityId("folder-id-11"));
    folderConfig11.setName("folder11");
    folderConfig11.setFullPathList(
        Arrays.asList(sourceConfig.getName(), folderConfig1.getName(), folderConfig11.getName()));
    namespaceService.addOrUpdateFolder(
        new NamespaceKey(folderConfig11.getFullPathList()), folderConfig11);

    List<CatalogItem> childrenForPath =
        catalogServiceHelper.getChildrenForPath(namespaceKey, null, 10).children();
    assertEquals(childrenForPath.size(), 2);

    childrenForPath =
        catalogServiceHelper
            .getChildrenForPath(new NamespaceKey(folderConfig1.getFullPathList()), null, 10)
            .children();
    assertEquals(childrenForPath.size(), 1);
  }

  @Test
  public void testFileBaseSourceListing() throws Exception {
    SourceConfig sourceConfig = new SourceConfig();
    sourceConfig.setName("mySource");
    sourceConfig.setId(new EntityId("source-id"));
    sourceConfig.setType("");
    namespaceService.addOrUpdateSource(new NamespaceKey(sourceConfig.getName()), sourceConfig);

    // mock getting entities
    FileSystemPlugin storagePlugin = BaseTestQuery.getMockedFileSystemPlugin();

    when(catalog.getSource(sourceConfig.getName())).thenReturn(storagePlugin);

    // folder 1
    FolderConfig folderConfig1 = new FolderConfig();
    folderConfig1.setId(new EntityId("folder-id-1"));
    folderConfig1.setName("folder1");
    folderConfig1.setFullPathList(Arrays.asList(sourceConfig.getName(), folderConfig1.getName()));
    namespaceService.addOrUpdateFolder(
        new NamespaceKey(folderConfig1.getFullPathList()), folderConfig1);

    // folder 2
    FolderConfig folderConfig2 = new FolderConfig();
    folderConfig2.setId(new EntityId("folder-id-2"));
    folderConfig2.setName("folder2");
    folderConfig2.setFullPathList(Arrays.asList(sourceConfig.getName(), folderConfig2.getName()));
    namespaceService.addOrUpdateFolder(
        new NamespaceKey(folderConfig2.getFullPathList()), folderConfig2);

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
        catalogServiceHelper
            .getChildrenForPath(new NamespaceKey(sourceConfig.getName()), null, 10)
            .children();
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

  @Test
  public void testGetCatalogChildrenForPath_namespace() throws Exception {
    // for function children
    NamespaceKey spaceKey = new NamespaceKey("s1");
    namespaceService.addOrUpdateSpace(spaceKey, newTestSpace(spaceKey.getName()));

    // Create folders in the first source.
    List<String> allExpectedChildrenPaths = new ArrayList<>();
    int numFolders = 100;
    for (int i = 0; i < numFolders; i++) {
      String folderName = String.format("folder%03d", i);
      NamespaceKey folderKey = new NamespaceKey(asList(spaceKey.getName(), folderName));
      namespaceService.addOrUpdateFolder(folderKey, newTestFolder(spaceKey, folderName));
      allExpectedChildrenPaths.add(String.format("%s.%s", spaceKey.getName(), folderName));
    }

    // Paginate over the first source.
    int maxResults = 13;
    CatalogPageToken pageToken = null;
    List<String> allChildrenPaths = new ArrayList<>();
    do {
      CatalogListingResult listingResult =
          catalogServiceHelper.getCatalogChildrenForPath(
              spaceKey.getPathComponents(), null, null, pageToken, maxResults);

      assertTrue(listingResult.children().size() <= maxResults);

      allChildrenPaths.addAll(
          listingResult.children().stream()
              .map(item -> String.join(".", item.getPath()))
              .collect(Collectors.toUnmodifiableList()));

      pageToken = null;
      if (listingResult.nextPageToken().isPresent()) {
        pageToken = listingResult.nextPageToken().get();
      }
    } while (pageToken != null);

    assertThat(allChildrenPaths).isEqualTo(allExpectedChildrenPaths);
  }

  @Test
  public void testGetCatalogChildrenForPath_versioned() throws Exception {
    NamespaceKey sourceKey = new NamespaceKey("s1");
    namespaceService.addOrUpdateSource(sourceKey, newTestSource(sourceKey.getName()));

    // Create folders in the source.
    List<String> allExpectedChildrenPaths = new ArrayList<>();
    List<ExternalNamespaceEntry> allEntities = new ArrayList<>();
    int numFolders = 100;
    for (int i = 0; i < numFolders; i++) {
      String folderName = String.format("folder%03d", i);
      allExpectedChildrenPaths.add(String.format("%s.%s", sourceKey.getName(), folderName));
      allEntities.add(
          ExternalNamespaceEntry.of(
              ExternalNamespaceEntry.Type.ICEBERG_TABLE, ImmutableList.of(folderName)));
    }

    // Mock catalog.getSource.
    VersionedPlugin versionedPluginMock = mock(VersionedPlugin.class);
    StoragePlugin storagePluginMock = mock(StoragePlugin.class);
    when(storagePluginMock.isWrapperFor(eq(VersionedPlugin.class))).thenReturn(true);
    when(storagePluginMock.unwrap(eq(VersionedPlugin.class))).thenReturn(versionedPluginMock);
    when(catalog.getSource(sourceKey.getName())).thenReturn(storagePluginMock);

    // Mock versioned plugin.
    int maxResults = 13;
    String refType = "BRANCH";
    String refValue = "main";
    VersionContext versionContext = VersionContext.ofBranch(refValue);
    doAnswer(
            (args) -> {
              List<String> argPath = args.getArgument(0);
              VersionContext argVersionContext = args.getArgument(1);
              VersionedListOptions listOptions = args.getArgument(2);
              ImmutableVersionedListResponsePage.Builder responseBuilder =
                  new ImmutableVersionedListResponsePage.Builder();
              if (argPath.isEmpty() && argVersionContext.equals(versionContext)) {
                int startIndex =
                    listOptions.pageToken() == null ? 0 : Integer.parseInt(listOptions.pageToken());
                responseBuilder.addAllEntries(
                    allEntities.subList(
                        startIndex, Math.min(allEntities.size(), startIndex + maxResults)));
                if (startIndex + maxResults < allEntities.size()) {
                  responseBuilder.setPageToken(Integer.toString(startIndex + maxResults));
                }
              }
              return responseBuilder.build();
            })
        .when(versionedPluginMock)
        .listEntriesPage(any(), any(), any());

    // Paginate over the first source.
    CatalogPageToken pageToken = null;
    List<String> allChildrenPaths = new ArrayList<>();
    do {
      CatalogListingResult listingResult =
          catalogServiceHelper.getCatalogChildrenForPath(
              sourceKey.getPathComponents(), refType, refValue, pageToken, maxResults);

      assertTrue(listingResult.children().size() <= maxResults);

      allChildrenPaths.addAll(
          listingResult.children().stream()
              .map(item -> String.join(".", item.getPath()))
              .collect(Collectors.toUnmodifiableList()));

      pageToken = null;
      if (listingResult.nextPageToken().isPresent()) {
        pageToken = listingResult.nextPageToken().get();
      }
    } while (pageToken != null);

    assertThat(allChildrenPaths).isEqualTo(allExpectedChildrenPaths);
  }

  @Test
  public void testGetCatalogChildrenForPath_versionedNoPaging() throws Exception {
    NamespaceKey sourceKey = new NamespaceKey("s1");
    namespaceService.addOrUpdateSource(sourceKey, newTestSource(sourceKey.getName()));

    // Create folders in the source.
    List<String> allExpectedChildrenPaths = new ArrayList<>();
    List<ExternalNamespaceEntry> allEntities = new ArrayList<>();
    int numFolders = 100;
    for (int i = 0; i < numFolders; i++) {
      String folderName = String.format("folder%03d", i);
      allExpectedChildrenPaths.add(String.format("%s.%s", sourceKey.getName(), folderName));
      allEntities.add(
          ExternalNamespaceEntry.of(
              ExternalNamespaceEntry.Type.ICEBERG_TABLE, ImmutableList.of(folderName)));
    }

    // Mock catalog.getSource.
    VersionedPlugin versionedPluginMock = mock(VersionedPlugin.class);
    StoragePlugin storagePluginMock = mock(StoragePlugin.class);
    when(storagePluginMock.isWrapperFor(eq(VersionedPlugin.class))).thenReturn(true);
    when(storagePluginMock.unwrap(eq(VersionedPlugin.class))).thenReturn(versionedPluginMock);
    when(catalog.getSource(sourceKey.getName())).thenReturn(storagePluginMock);
    String refType = "BRANCH";
    String refValue = "main";
    when(versionedPluginMock.listEntries(ImmutableList.of(), VersionContext.ofBranch(refValue)))
        .thenReturn(allEntities.stream());

    // Verify returned paths.
    CatalogListingResult listingResult =
        catalogServiceHelper.getCatalogChildrenForPath(
            sourceKey.getPathComponents(), refType, refValue, null, null);
    assertThat(
            listingResult.children().stream()
                .map(item -> String.join(".", item.getPath()))
                .collect(Collectors.toUnmodifiableList()))
        .isEqualTo(allExpectedChildrenPaths);
  }

  @Test
  public void testFileFormatGetForDatasetNoFormatSettingsForIceberg() {
    IcebergMetadata icebergMetadata = new IcebergMetadata().setFileType(FileType.ICEBERG);
    PhysicalDataset physicalDataset = new PhysicalDataset().setIcebergMetadata(icebergMetadata);
    DatasetConfig config =
        new DatasetConfig()
            .setPhysicalDataset(physicalDataset)
            .setFullPathList(Lists.newArrayList("myTable"))
            .setCreatedAt(123L);

    FileFormat fileFormat = FileFormat.getForDataset(config);
    assertEquals(FileType.ICEBERG, fileFormat.getFileType());
    assertEquals(Lists.newArrayList("myTable"), fileFormat.getFullPath());
    assertEquals(123L, fileFormat.getCtime());
    assertTrue(fileFormat.getIsFolder());
  }

  private static SourceConfig newTestSource(String sourceName) {
    return new SourceConfig().setName(sourceName).setType("NAS").setCtime(1000L);
  }

  private static SpaceConfig newTestSpace(String spaceName) {
    return new SpaceConfig().setName(spaceName);
  }

  private static FolderConfig newTestFolder(NamespaceKey parent, String name) {
    return new FolderConfig()
        .setName(name)
        .setFullPathList(
            ImmutableList.<String>builder().addAll(parent.getPathComponents()).add(name).build());
  }
}
