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

import static com.dremio.service.namespace.dataset.proto.DatasetType.PHYSICAL_DATASET;
import static com.dremio.service.namespace.dataset.proto.DatasetType.VIRTUAL_DATASET;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.List;

import javax.ws.rs.core.SecurityContext;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Matchers;

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
import com.dremio.dac.service.catalog.CatalogServiceHelper;
import com.dremio.dac.service.datasets.DatasetVersionMutator;
import com.dremio.dac.service.reflection.ReflectionServiceHelper;
import com.dremio.dac.service.search.SearchService;
import com.dremio.dac.service.source.SourceService;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.ConnectionReader;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.SchemaEntity;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.AccelerationSettings;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.dataset.proto.VirtualDataset;
import com.dremio.service.namespace.file.proto.JsonFileConfig;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.space.proto.FolderConfig;
import com.dremio.service.namespace.space.proto.HomeConfig;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import com.dremio.service.reflection.ReflectionSettings;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

/**
 * Test for CatalogServiceHelper
 */
public class TestCatalogServiceHelper {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private CatalogServiceHelper catalogServiceHelper;
  private Catalog catalog;
  private SecurityContext context;
  private SourceService sourceService;
  private NamespaceService namespaceService;
  private ReflectionServiceHelper reflectionServiceHelper;
  private DatasetVersionMutator datasetVersionMutator;
  private HomeFileTool homeFileTool;
  private SabotContext sabotContext;
  private SearchService searchService;

  @Before
  public void setup() {
    catalog = mock(Catalog.class);
    context = mock(SecurityContext.class);
    sourceService = mock(SourceService.class);
    namespaceService = mock(NamespaceService.class);
    sabotContext = mock(SabotContext.class);
    reflectionServiceHelper = mock(ReflectionServiceHelper.class);
    homeFileTool = mock(HomeFileTool.class);
    datasetVersionMutator = mock(DatasetVersionMutator.class);
    searchService = mock(SearchService.class);

    Principal principal = mock(Principal.class);
    when(principal.getName()).thenReturn("user");
    when(context.getUserPrincipal()).thenReturn(principal);

    catalogServiceHelper = new CatalogServiceHelper(
      catalog,
      context,
      sourceService,
      namespaceService,
      sabotContext,
      reflectionServiceHelper,
      homeFileTool,
      datasetVersionMutator,
      searchService
    );
  }

  @Test
  public void testGetTopLevelCatalogItems() throws Exception {
    HomeConfig homeConfig = new HomeConfig();
    homeConfig.setOwner("user");
    homeConfig.setId(new EntityId("home-id"));
    when(namespaceService.getHome(new HomePath(HomeName.getUserHomePath("user")).toNamespaceKey())).thenReturn(homeConfig);

    SpaceConfig spaceConfig = new SpaceConfig();
    spaceConfig.setName("mySpace");
    spaceConfig.setId(new EntityId("space-id"));
    when(namespaceService.getSpaces()).thenReturn(Arrays.asList(spaceConfig));

    SourceConfig sourceConfig = new SourceConfig();
    sourceConfig.setName("mySource");
    sourceConfig.setId(new EntityId("source-id"));
    when(sourceService.getSources()).thenReturn(Arrays.asList(sourceConfig));

    List<CatalogItem> topLevelCatalogItems = catalogServiceHelper.getTopLevelCatalogItems(Collections.EMPTY_LIST);
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
    DatasetConfig datasetConfig = new DatasetConfig();
    datasetConfig.setId(new EntityId("dataset-id"));
    datasetConfig.setFullPathList(Collections.singletonList("path"));
    datasetConfig.setType(VIRTUAL_DATASET);

    VirtualDataset virtualDataset = new VirtualDataset();
    virtualDataset.setSql("");
    datasetConfig.setVirtualDataset(virtualDataset);

    NameSpaceContainer namespaceContainer = new NameSpaceContainer();
    namespaceContainer.setType(NameSpaceContainer.Type.DATASET);
    namespaceContainer.setDataset(datasetConfig);

    when(namespaceService.getEntityById(datasetConfig.getId().getId())).thenReturn(namespaceContainer);

    ReflectionSettings reflectionSettings = mock(ReflectionSettings.class);
    when(reflectionSettings.getStoredReflectionSettings(any(NamespaceKey.class))).thenReturn(Optional.<AccelerationSettings>absent());
    when(reflectionServiceHelper.getReflectionSettings()).thenReturn(reflectionSettings);

    DremioTable dremioTable = mock(DremioTable.class);
    when(dremioTable.getDatasetConfig()).thenReturn(datasetConfig);
    when(catalog.getTable(any(String.class))).thenReturn(dremioTable);

    Optional<CatalogEntity> entity = catalogServiceHelper.getCatalogEntityById(datasetConfig.getId().getId(), ImmutableList.of(), ImmutableList.of());

    assertTrue(entity.isPresent());

    CatalogEntity catalogEntity = entity.get();
    assertTrue(catalogEntity instanceof Dataset);

    Dataset dataset = (Dataset) catalogEntity;
    assertEquals(dataset.getId(), datasetConfig.getId().getId());
  }

  @Test
  public void testGetSpaceCatalogEntityById() throws Exception {
    SpaceConfig spaceConfig = new SpaceConfig();
    spaceConfig.setId(new EntityId("space-id"));
    spaceConfig.setName("mySpace");

    NameSpaceContainer namespaceContainer = new NameSpaceContainer();
    namespaceContainer.setType(NameSpaceContainer.Type.SPACE);
    namespaceContainer.setSpace(spaceConfig);

    when(namespaceService.getEntityById(spaceConfig.getId().getId())).thenReturn(namespaceContainer);
    when(namespaceService.getEntities(Collections.singletonList(new NamespaceKey(spaceConfig.getName())))).thenReturn(Collections.singletonList(namespaceContainer));
    // for children listing, we just send the space back to keep it simple
    when(namespaceService.list(new NamespaceKey(spaceConfig.getName()))).thenReturn(Collections.singletonList(namespaceContainer));

    Optional<CatalogEntity> catalogEntityById = catalogServiceHelper.getCatalogEntityById(spaceConfig.getId().getId(), ImmutableList.of(), ImmutableList.of());

    assertTrue(catalogEntityById.isPresent());

    CatalogEntity catalogEntity = catalogEntityById.get();
    assertTrue(catalogEntity instanceof Space);

    Space space = (Space) catalogEntity;
    assertEquals(space.getId(), spaceConfig.getId().getId());
    assertEquals(space.getChildren().size(), 1);
  }

  @Test
  public void testCreatePDSShouldFail() throws Exception {
    // can only create VDS
    Dataset dataset = new Dataset(
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
      null
    );
    thrown.expect(IllegalArgumentException.class);
    catalogServiceHelper.createCatalogItem(dataset);
  }

  @Test
  public void testCreateVDSWithoutIdShouldFail() throws Exception {
    // new VDS can't have an id
    Dataset dataset = new Dataset(
      "foo",
      Dataset.DatasetType.VIRTUAL_DATASET,
      Arrays.asList("source", "path"),
      null,
      null,
      null,
      null,
      "",
      null,
      null,
      null
    );
    thrown.expect(IllegalArgumentException.class);
    catalogServiceHelper.createCatalogItem(dataset);
  }

  @Test
  public void testCreateVDSWithoutSQLShouldFail() throws Exception {
    // VDS needs sql
    Dataset dataset = new Dataset(
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
      null
    );
    thrown.expect(IllegalArgumentException.class);
    catalogServiceHelper.createCatalogItem(dataset);
  }

  @Test
  public void testCreateVDSWithIdShouldFail() throws Exception {
    // VDS needs sql
    Dataset dataset = new Dataset(
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
      null
    );
    thrown.expect(IllegalArgumentException.class);
    catalogServiceHelper.createCatalogItem(dataset);
  }

  @Test
  public void testCreateVDSInSourceShouldFail() throws Exception {
    when(namespaceService.exists(any(NamespaceKey.class))).thenReturn(true);

    // provide a source container
    NameSpaceContainer namespaceContainer = new NameSpaceContainer();
    namespaceContainer.setType(NameSpaceContainer.Type.SOURCE);

    when(namespaceService.getEntities(Matchers.anyList())).thenReturn(Collections.singletonList(namespaceContainer));

    // VDS needs sql
    Dataset dataset = new Dataset(
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
      null
    );
    thrown.expect(IllegalArgumentException.class);
    catalogServiceHelper.createCatalogItem(dataset);
  }

  @Test
  public void testCreateSpaceWithIdShouldFail() throws Exception {
    Space space = new Space(
      "space-id",
      "mySpace",
      null,
      0L,
      null
    );
    thrown.expect(IllegalArgumentException.class);
    catalogServiceHelper.createCatalogItem(space);
  }

  @Test
  public void testCreateSpaceWithoutNameShouldFail() throws Exception {
    Space space = new Space(
      null,
      null,
      null,
      0L,
      null
    );
    thrown.expect(IllegalArgumentException.class);
    catalogServiceHelper.createCatalogItem(space);
  }

  @Test
  public void testCreateSpaceWithEmptyNameShouldFail() throws Exception {
    Space space = new Space(
      null,
      "",
      null,
      0L,
      null
    );
    thrown.expect(IllegalArgumentException.class);
    catalogServiceHelper.createCatalogItem(space);
  }

  @Test
  public void testCreateSpaceWithConflict() throws Exception {
    when(namespaceService.exists(any(NamespaceKey.class), eq(NameSpaceContainer.Type.SPACE))).thenReturn(true);

    // VDS
    Space space = new Space(
      null,
      "mySpace",
      null,
      0L,
      null
    );

    thrown.expect(ConcurrentModificationException.class);
    catalogServiceHelper.createCatalogItem(space);
  }

  @Test
  public void testCreateSpace() throws Exception {
    // VDS
    Space space = new Space(
      null,
      "mySpace",
      null,
      0L,
      null
    );

    when(namespaceService.exists(any(NamespaceKey.class))).thenReturn(false);
    when(namespaceService.getSpace(any(NamespaceKey.class))).thenReturn(catalogServiceHelper.getSpaceConfig(space));

    CatalogEntity catalogItem = catalogServiceHelper.createCatalogItem(space);
    assertTrue(catalogItem instanceof Space);

    Space createdSpace = (Space) catalogItem;
    assertEquals(createdSpace.getName(), space.getName());
  }

  @Test
  public void testUpdateDatasetWithMismatchingIdShouldFail() throws Exception {
    Dataset dataset = new Dataset(
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
      null
    );

    thrown.expect(IllegalArgumentException.class);
    catalogServiceHelper.updateCatalogItem(dataset, "bad-id");
  }

  @Test
  public void testUpdateVDSWithChangedTypeShouldFail() throws Exception {
    Dataset dataset = new Dataset(
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
      null
    );

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

    when(namespaceService.findDatasetByUUID(dataset.getId())).thenReturn(config);

    DremioTable dremioTable = mock(DremioTable.class);
    when(dremioTable.getDatasetConfig()).thenReturn(config);
    when(catalog.getTable(any(String.class))).thenReturn(dremioTable);

    thrown.expect(IllegalArgumentException.class);
    catalogServiceHelper.updateCatalogItem(dataset, dataset.getId());
  }

  @Test
  public void testUpdateVDSWithoutSqlShouldFail() throws Exception {
    Dataset dataset = new Dataset(
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
      null
    );

    thrown.expect(IllegalArgumentException.class);
    catalogServiceHelper.updateCatalogItem(dataset, dataset.getId());
  }

  @Test
  public void testUpdateVDS() throws Exception {
    Dataset dataset = new Dataset(
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
      null
    );

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
    when(reflectionSettings.getStoredReflectionSettings(any(NamespaceKey.class))).thenReturn(Optional.<AccelerationSettings>absent());
    when(reflectionServiceHelper.getReflectionSettings()).thenReturn(reflectionSettings);

    DremioTable dremioTable = mock(DremioTable.class);
    when(dremioTable.getDatasetConfig()).thenReturn(config);
    when(catalog.getTable(any(String.class))).thenReturn(dremioTable);

    catalogServiceHelper.updateCatalogItem(dataset, dataset.getId());
  }

  @Test
  public void testUpdatePDSWithChangedTypeShouldFail() throws Exception {
    Dataset dataset = new Dataset(
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
      null
    );

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

    thrown.expect(IllegalArgumentException.class);
    catalogServiceHelper.updateCatalogItem(dataset, dataset.getId());
  }

  @Test
  public void testUpdatePDS() throws Exception {
    Dataset dataset = new Dataset(
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
      null
    );

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
    when(reflectionSettings.getStoredReflectionSettings(any(NamespaceKey.class))).thenReturn(Optional.<AccelerationSettings>absent());
    when(reflectionServiceHelper.getReflectionSettings()).thenReturn(reflectionSettings);

    DremioTable dremioTable = mock(DremioTable.class);
    when(dremioTable.getDatasetConfig()).thenReturn(config);
    when(catalog.getTable(any(String.class))).thenReturn(dremioTable);

    catalogServiceHelper.updateCatalogItem(dataset, dataset.getId());
    verify(catalog, times(1)).createOrUpdateDataset(
      eq(namespaceService),
      eq(new NamespaceKey("source")),
      eq(new NamespaceKey(dataset.getPath())),
      any(DatasetConfig.class)
    );
  }

  @Test
  public void testUpdateFileShouldFail() throws Exception {
    File file = new File("file-id", Collections.singletonList("file"));

    thrown.expect(UnsupportedOperationException.class);
    catalogServiceHelper.updateCatalogItem(file, file.getId());
  }

  @Test
  public void testUpdateHomeShouldFail() throws Exception {
    Home home = new Home("home-id", "home", null, null);

    thrown.expect(UnsupportedOperationException.class);
    catalogServiceHelper.updateCatalogItem(home, home.getId());
  }

  @Test
  public void testDeleteDataset() throws Exception {
    Dataset dataset = new Dataset(
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
      null
    );

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
    verify(namespaceService, times(1)).deleteDataset(new NamespaceKey(dataset.getPath()), datasetConfig.getTag());
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
    verify(namespaceService, times(1)).deleteSpace(new NamespaceKey(space.getName()), space.getTag());
  }

  @Test
  public void testDeleteFolder() throws Exception {
    Folder folder = new Folder("folder-id", Arrays.asList("source", "folder"), "1", null);

    NameSpaceContainer namespaceContainer = new NameSpaceContainer();
    namespaceContainer.setFolder(CatalogServiceHelper.getFolderConfig(folder));
    namespaceContainer.setType(NameSpaceContainer.Type.FOLDER);
    when(namespaceService.getEntityById(folder.getId())).thenReturn(namespaceContainer);

    catalogServiceHelper.deleteCatalogItem(folder.getId(), folder.getTag());
    verify(namespaceService, times(1)).deleteFolder(new NamespaceKey(folder.getPath()), folder.getTag());
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
    when(namespaceService.getEntities(Collections.singletonList(namespaceKey))).thenReturn(Collections.singletonList(namespaceContainer));

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
    folderConfig11.setFullPathList(Arrays.asList(sourceConfig.getName(), folderConfig1.getName(), folderConfig11.getName()));
    folder = new NameSpaceContainer();
    folder.setFolder(folderConfig11);
    folder.setType(NameSpaceContainer.Type.FOLDER);
    containerList2.add(folder);
    when(namespaceService.list(new NamespaceKey(folderConfig1.getFullPathList()))).thenReturn(containerList2);

    List<CatalogItem> childrenForPath = catalogServiceHelper.getChildrenForPath(namespaceKey);
    assertEquals(childrenForPath.size(), 2);

    childrenForPath = catalogServiceHelper.getChildrenForPath(new NamespaceKey(folderConfig1.getFullPathList()));
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
    when(namespaceService.getEntities(Collections.singletonList(new NamespaceKey(sourceConfig.getName())))).thenReturn(Collections.singletonList(namespaceContainer));

    FileSystemPlugin storagePlugin = mock(FileSystemPlugin.class);

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
    SchemaEntity schemaEntityFolder1 = new SchemaEntity(com.dremio.common.utils.PathUtils.constructFullPath(folderConfig1.getFullPathList()), SchemaEntity.SchemaEntityType.FOLDER, "testuser");
    SchemaEntity schemaEntityFolder2 = new SchemaEntity(com.dremio.common.utils.PathUtils.constructFullPath(folderConfig2.getFullPathList()), SchemaEntity.SchemaEntityType.FOLDER, "testuser");

    when(storagePlugin.list(eq(Arrays.asList(sourceConfig.getName())), any(String.class))).thenReturn(Arrays.asList(schemaEntityFolder1, schemaEntityFolder2));

    List<CatalogItem> childrenForPath = catalogServiceHelper.getChildrenForPath(new NamespaceKey(sourceConfig.getName()));
    assertEquals(childrenForPath.size(), 2);
  }
}
