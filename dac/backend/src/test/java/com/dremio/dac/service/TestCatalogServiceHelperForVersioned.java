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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.catalog.model.VersionContext;
import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.dac.api.CatalogEntity;
import com.dremio.dac.api.Dataset;
import com.dremio.dac.api.Folder;
import com.dremio.dac.api.Source;
import com.dremio.dac.homefiles.HomeFileTool;
import com.dremio.dac.model.folder.SourceFolderPath;
import com.dremio.dac.model.namespace.NamespaceTree;
import com.dremio.dac.model.sources.PhysicalDataset;
import com.dremio.dac.model.sources.SourceName;
import com.dremio.dac.service.catalog.CatalogServiceHelper;
import com.dremio.dac.service.datasets.DatasetVersionMutator;
import com.dremio.dac.service.errors.ClientErrorException;
import com.dremio.dac.service.reflection.ReflectionServiceHelper;
import com.dremio.dac.service.search.SearchService;
import com.dremio.dac.service.source.SourceService;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.TableMutationOptions;
import com.dremio.exec.catalog.VersionedDatasetId;
import com.dremio.exec.catalog.VersionedPlugin;
import com.dremio.exec.physical.base.ViewOptions;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.options.OptionManager;
import com.dremio.plugins.dataplane.store.DataplanePlugin;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.reflection.ReflectionSettings;
import com.dremio.test.DremioTest;
import com.google.common.collect.ImmutableList;
import java.security.Principal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import javax.ws.rs.core.SecurityContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

/** Tests for catalog service helper for versioned sources */
public class TestCatalogServiceHelperForVersioned extends DremioTest {
  @Rule public MockitoRule rule = MockitoJUnit.rule().strictness(Strictness.LENIENT);

  @Mock private ReflectionSettings reflectionSettings;
  @Mock private DataplanePlugin dataplanePlugin;
  @Mock private DremioTable dremioTable;

  @Mock private CatalogService catalogService;
  @Mock private Catalog catalog;
  @Mock private SecurityContext securityContext;
  @Mock private SourceService sourceService;
  @Mock private NamespaceService namespaceService;
  @Mock private SabotContext sabotContext;
  @Mock private ReflectionServiceHelper reflectionServiceHelper;
  @Mock private HomeFileTool homeFileTool;
  @Mock private DatasetVersionMutator datasetVersionMutator;
  @Mock private SearchService searchService;

  private static final String sourceId = UUID.randomUUID().toString();
  private static final String datasetId = UUID.randomUUID().toString();

  private SourceConfig sourceConfig;
  private NameSpaceContainer sourceContainer;
  private CatalogServiceHelper catalogServiceHelper;

  @Before
  public void setup() throws NamespaceNotFoundException {
    sourceConfig = new SourceConfig().setName("versionedSource").setId(new EntityId(sourceId));
    sourceContainer =
        new NameSpaceContainer()
            .setSource(sourceConfig)
            .setType(NameSpaceContainer.Type.SOURCE)
            .setFullPathList(Arrays.asList("versionedSource"));

    when(namespaceService.getEntityById(eq(sourceId))).thenReturn(sourceContainer);
    when(namespaceService.getEntities(
            eq(Collections.singletonList(new NamespaceKey("versionedSource")))))
        .thenReturn(Collections.singletonList(sourceContainer));
    when(catalog.getSource(anyString())).thenReturn(dataplanePlugin);
    when(dataplanePlugin.isWrapperFor(VersionedPlugin.class)).thenReturn(true);
    when(dataplanePlugin.unwrap(VersionedPlugin.class)).thenReturn(dataplanePlugin);
    when(reflectionServiceHelper.getReflectionSettings()).thenReturn(reflectionSettings);
    when(reflectionSettings.getStoredReflectionSettings(any(CatalogEntityKey.class)))
        .thenReturn(Optional.empty());
    Principal principal = mock(Principal.class);
    when(securityContext.getUserPrincipal()).thenReturn(principal);
    when(principal.getName()).thenReturn("user");
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
            mock(OptionManager.class));
  }

  @Test
  public void getCatalogSourceEntityById() throws NamespaceException {
    final NamespaceTree contents = new NamespaceTree();
    contents.addFolder(
        new com.dremio.dac.model.folder.Folder(
            VersionedDatasetId.newBuilder()
                .setTableKey(Arrays.asList("versionedSource", "myfolder"))
                .setContentId("85f4da7b-ff38-4a2e-a040-600d73e7eb9a")
                .setTableVersionContext(TableVersionContext.of(VersionContext.ofBranch("main")))
                .build()
                .asString(),
            "myfolder",
            "/source/versionedSource/folder/myfolder",
            false,
            false,
            false,
            null,
            "0",
            null,
            new NamespaceTree(),
            null,
            0));
    contents.addDataset(
        com.dremio.dac.explore.model.Dataset.newInstance(
            new SourceName("versionedSource"),
            Collections.emptyList(),
            "view",
            VersionedDatasetId.newBuilder()
                .setTableKey(Arrays.asList("versionedSource", "view"))
                .setContentId("55617bba-49df-48be-9b0f-ed4c54df30a7")
                .setTableVersionContext(TableVersionContext.of(VersionContext.ofBranch("main")))
                .build()
                .asString()));
    contents.addPhysicalDataset(
        PhysicalDataset.newInstance(
            new SourceName("versionedSource"),
            Collections.emptyList(),
            "table",
            VersionedDatasetId.newBuilder()
                .setTableKey(Arrays.asList("versionedSource", "table"))
                .setContentId("68bf9668-4e5b-4098-89e6-aeb3671e272c")
                .setTableVersionContext(TableVersionContext.of(VersionContext.ofBranch("main")))
                .build()
                .asString()));
    final Source testSource = new Source();

    when(securityContext.getUserPrincipal()).thenReturn(() -> "user123");
    when(sourceService.listSource(
            any(SourceName.class), any(SourceConfig.class), eq("user123"), eq(null), eq(null)))
        .thenReturn(contents);
    when(sourceService.fromSourceConfig(eq(sourceConfig), any(List.class))).thenReturn(testSource);

    final Optional<CatalogEntity> catalogEntity =
        catalogServiceHelper.getCatalogEntityById(sourceId, ImmutableList.of(), ImmutableList.of());

    assertThat(catalogEntity.isPresent()).isTrue();
    assertThat(catalogEntity.get()).isInstanceOf(Source.class);

    final Source source = (Source) catalogEntity.get();
    assertThat(source == testSource).isTrue();
  }

  @Test
  public void getCatalogFolderEntityById() throws NamespaceException {
    final VersionedDatasetId folderId =
        VersionedDatasetId.newBuilder()
            .setTableVersionContext(TableVersionContext.of(VersionContext.ofBranch("main")))
            .setContentId(UUID.randomUUID().toString())
            .setTableKey(Arrays.asList("versionedSource", "myfolder"))
            .build();

    final NamespaceTree contents = new NamespaceTree();
    contents.addFolder(
        new com.dremio.dac.model.folder.Folder(
            VersionedDatasetId.newBuilder()
                .setTableKey(Arrays.asList("versionedSource", "myfolder", "nested"))
                .setContentId("85f4da7b-ff38-4a2e-a040-600d73e7eb9a")
                .setTableVersionContext(TableVersionContext.of(VersionContext.ofBranch("main")))
                .build()
                .asString(),
            "nested",
            "/source/versionedSource/folder/myfolder/nested",
            false,
            false,
            false,
            null,
            "0",
            null,
            new NamespaceTree(),
            null,
            0));
    contents.addDataset(
        com.dremio.dac.explore.model.Dataset.newInstance(
            new SourceName("versionedSource"),
            Collections.singletonList("myfolder"),
            "view",
            VersionedDatasetId.newBuilder()
                .setTableKey(Arrays.asList("versionedSource", "myfolder", "view"))
                .setContentId("55617bba-49df-48be-9b0f-ed4c54df30a7")
                .setTableVersionContext(TableVersionContext.of(VersionContext.ofBranch("main")))
                .build()
                .asString()));
    contents.addPhysicalDataset(
        PhysicalDataset.newInstance(
            new SourceName("versionedSource"),
            Collections.singletonList("myfolder"),
            "table",
            VersionedDatasetId.newBuilder()
                .setTableKey(Arrays.asList("versionedSource", "myfolder", "table"))
                .setContentId("68bf9668-4e5b-4098-89e6-aeb3671e272c")
                .setTableVersionContext(TableVersionContext.of(VersionContext.ofBranch("main")))
                .build()
                .asString()));

    ResolvedVersionContext resolvedVersionContext =
        ResolvedVersionContext.ofBranch("main", "abc123");

    when(dataplanePlugin.resolveVersionContext(any(VersionContext.class)))
        .thenReturn(resolvedVersionContext);
    when(dataplanePlugin.getType(eq(Arrays.asList("myfolder")), eq(resolvedVersionContext)))
        .thenReturn(VersionedPlugin.EntityType.FOLDER);
    when(securityContext.getUserPrincipal()).thenReturn(() -> "user123");
    when(sourceService.listFolder(
            any(SourceName.class),
            any(SourceFolderPath.class),
            eq("user123"),
            eq("BRANCH"),
            eq("main")))
        .thenReturn(contents);

    final Optional<CatalogEntity> catalogEntity =
        catalogServiceHelper.getCatalogEntityById(
            folderId.asString(), ImmutableList.of(), ImmutableList.of());

    assertThat(catalogEntity.isPresent()).isTrue();
    assertThat(catalogEntity.get()).isInstanceOf(Folder.class);

    final Folder folder = (Folder) catalogEntity.get();
    assertThat(folder.getId()).isEqualTo(folderId.asString());
    assertThat(folder.getName()).isEqualTo("myfolder");
    assertThat(folder.getPath()).isEqualTo(ImmutableList.of("versionedSource", "myfolder"));
    assertThat(folder.getChildren().size()).isEqualTo(3);
  }

  @Test
  public void getCatalogTableEntityById() throws NamespaceException {
    final String tableId =
        VersionedDatasetId.newBuilder()
            .setTableVersionContext(TableVersionContext.of(VersionContext.ofBranch("main")))
            .setContentId(UUID.randomUUID().toString())
            .setTableKey(Arrays.asList("versionedSource", "table"))
            .build()
            .asString();

    ResolvedVersionContext resolvedVersionContext =
        ResolvedVersionContext.ofBranch("main", "abc123");

    when(dataplanePlugin.resolveVersionContext(any(VersionContext.class)))
        .thenReturn(resolvedVersionContext);
    when(dataplanePlugin.getType(eq(Arrays.asList("table")), eq(resolvedVersionContext)))
        .thenReturn(VersionedPlugin.EntityType.ICEBERG_TABLE);
    when(catalog.getTable(eq(tableId))).thenReturn(dremioTable);
    final DatasetConfig datasetConfig =
        new DatasetConfig()
            .setType(DatasetType.PHYSICAL_DATASET)
            .setId(new EntityId(tableId))
            .setFullPathList(Arrays.asList("versionedSource", "table"));
    when(dremioTable.getDatasetConfig()).thenReturn(datasetConfig);

    final Optional<CatalogEntity> catalogEntity =
        catalogServiceHelper.getCatalogEntityById(tableId, ImmutableList.of(), ImmutableList.of());

    assertThat(catalogEntity.isPresent()).isTrue();
    assertThat(catalogEntity.get()).isInstanceOf(Dataset.class);

    final Dataset dataset = (Dataset) catalogEntity.get();
    assertThat(dataset.getId()).isEqualTo(tableId);
    assertThat(dataset.getType()).isEqualTo(Dataset.DatasetType.PHYSICAL_DATASET);
    assertThat(dataset.getPath()).isEqualTo(ImmutableList.of("versionedSource", "table"));
  }

  @Test
  public void getCatalogViewEntityById() throws NamespaceException {
    final String viewId =
        VersionedDatasetId.newBuilder()
            .setTableVersionContext(TableVersionContext.of(VersionContext.ofBranch("main")))
            .setContentId(UUID.randomUUID().toString())
            .setTableKey(Arrays.asList("versionedSource", "view"))
            .build()
            .asString();

    ResolvedVersionContext resolvedVersionContext =
        ResolvedVersionContext.ofBranch("main", "abc123");

    when(dataplanePlugin.resolveVersionContext(any(VersionContext.class)))
        .thenReturn(resolvedVersionContext);
    when(dataplanePlugin.getType(eq(Arrays.asList("view")), eq(resolvedVersionContext)))
        .thenReturn(VersionedPlugin.EntityType.ICEBERG_VIEW);
    when(catalog.getTable(eq(viewId))).thenReturn(dremioTable);
    final DatasetConfig datasetConfig =
        new DatasetConfig()
            .setType(DatasetType.VIRTUAL_DATASET)
            .setId(new EntityId(viewId))
            .setFullPathList(Arrays.asList("versionedSource", "view"));
    when(dremioTable.getDatasetConfig()).thenReturn(datasetConfig);

    final Optional<CatalogEntity> catalogEntity =
        catalogServiceHelper.getCatalogEntityById(viewId, ImmutableList.of(), ImmutableList.of());

    assertThat(catalogEntity.isPresent()).isTrue();
    assertThat(catalogEntity.get()).isInstanceOf(Dataset.class);

    final Dataset dataset = (Dataset) catalogEntity.get();
    assertThat(dataset.getId()).isEqualTo(viewId);
    assertThat(dataset.getType()).isEqualTo(Dataset.DatasetType.VIRTUAL_DATASET);
    assertThat(dataset.getPath()).isEqualTo(ImmutableList.of("versionedSource", "view"));
  }

  @Test
  public void getCatalogEntityByPathWithoutVersionValue() throws Exception {
    assertThatThrownBy(
            () ->
                catalogServiceHelper.getCatalogEntityByPath(
                    Arrays.asList("versionedSource", "table"),
                    ImmutableList.of(),
                    ImmutableList.of(),
                    "BRANCH",
                    null))
        .isInstanceOf(ClientErrorException.class)
        .hasMessageContaining("Missing a valid versionType/versionValue");
  }

  @Test
  public void getCatalogEntityByPathWithoutVersionType() throws Exception {
    assertThatThrownBy(
            () ->
                catalogServiceHelper.getCatalogEntityByPath(
                    Arrays.asList("versionedSource", "table"),
                    ImmutableList.of(),
                    ImmutableList.of(),
                    null,
                    "main"))
        .isInstanceOf(ClientErrorException.class)
        .hasMessageContaining("Missing a valid versionType/versionValue");
  }

  @Test
  public void getCatalogEntityByPathWithoutVersionContext() throws Exception {
    final DatasetConfig datasetConfig =
        new DatasetConfig()
            .setType(DatasetType.PHYSICAL_DATASET)
            .setId(new EntityId(datasetId))
            .setFullPathList(Arrays.asList("versionedSource", "table"));
    ResolvedVersionContext resolvedVersionContext =
        ResolvedVersionContext.ofBranch("main", "abc123");

    when(dataplanePlugin.resolveVersionContext(any(VersionContext.class)))
        .thenReturn(resolvedVersionContext);
    when(dataplanePlugin.getType(eq(Arrays.asList("table")), eq(resolvedVersionContext)))
        .thenReturn(VersionedPlugin.EntityType.ICEBERG_VIEW);
    when(catalog.resolveCatalog(any(Map.class))).thenReturn(catalog);
    when(catalog.getTable(any(NamespaceKey.class))).thenReturn(dremioTable);
    when(catalog.getTable(any(CatalogEntityKey.class))).thenReturn(dremioTable);
    when(dremioTable.getDatasetConfig()).thenReturn(datasetConfig);

    final Optional<CatalogEntity> catalogEntity =
        catalogServiceHelper.getCatalogEntityByPath(
            Arrays.asList("versionedSource", "table"),
            ImmutableList.of(),
            ImmutableList.of(),
            null,
            null);

    assertThat(catalogEntity.isPresent()).isTrue();
    assertThat(catalogEntity.get()).isInstanceOf(Dataset.class);

    final Dataset dataset = (Dataset) catalogEntity.get();

    assertThat(dataset.getId()).isEqualTo(datasetId);
    assertThat(dataset.getType()).isEqualTo(Dataset.DatasetType.PHYSICAL_DATASET);
  }

  @Test
  public void getCatalogEntityByPathNotFound() throws Exception {
    when(catalog.getTableSnapshot(any(CatalogEntityKey.class))).thenReturn(null);

    final Optional<CatalogEntity> catalogEntity =
        catalogServiceHelper.getCatalogEntityByPath(
            Arrays.asList("versionedSource", "table"),
            ImmutableList.of(),
            ImmutableList.of(),
            "BRANCH",
            "main");

    assertThat(catalogEntity.isPresent()).isFalse();
  }

  @Test
  public void getCatalogEntityByPathForTable() throws Exception {
    final DatasetConfig datasetConfig =
        new DatasetConfig()
            .setType(DatasetType.PHYSICAL_DATASET)
            .setId(new EntityId(datasetId))
            .setFullPathList(Arrays.asList("versionedSource", "table"));
    ResolvedVersionContext resolvedVersionContext =
        ResolvedVersionContext.ofBranch("main", "abc123");

    when(dataplanePlugin.resolveVersionContext(any(VersionContext.class)))
        .thenReturn(resolvedVersionContext);
    when(dataplanePlugin.getType(eq(Arrays.asList("table")), eq(resolvedVersionContext)))
        .thenReturn(VersionedPlugin.EntityType.ICEBERG_TABLE);
    when(catalog.getTableSnapshot(any(CatalogEntityKey.class))).thenReturn(dremioTable);
    when(catalog.getTable(any(CatalogEntityKey.class))).thenReturn(dremioTable);
    when(dremioTable.getDatasetConfig()).thenReturn(datasetConfig);

    final Optional<CatalogEntity> catalogEntity =
        catalogServiceHelper.getCatalogEntityByPath(
            Arrays.asList("versionedSource", "table"),
            ImmutableList.of(),
            ImmutableList.of(),
            "BRANCH",
            "main");

    assertThat(catalogEntity.isPresent()).isTrue();
    assertThat(catalogEntity.get()).isInstanceOf(Dataset.class);

    final Dataset dataset = (Dataset) catalogEntity.get();

    assertThat(dataset.getId()).isEqualTo(datasetId);
    assertThat(dataset.getType()).isEqualTo(Dataset.DatasetType.PHYSICAL_DATASET);
  }

  @Test
  public void getCatalogEntityByPathForSnapshot() throws Exception {
    final DatasetConfig datasetConfig =
        new DatasetConfig()
            .setType(DatasetType.PHYSICAL_DATASET)
            .setId(new EntityId(datasetId))
            .setFullPathList(Arrays.asList("versionedSource", "table"));
    ResolvedVersionContext resolvedVersionContext = ResolvedVersionContext.ofCommit("abc123");

    when(dataplanePlugin.resolveVersionContext(any(VersionContext.class)))
        .thenReturn(resolvedVersionContext);
    when(dataplanePlugin.getType(eq(Arrays.asList("table")), eq(resolvedVersionContext)))
        .thenReturn(VersionedPlugin.EntityType.ICEBERG_TABLE);
    when(catalog.getTableSnapshot(any(CatalogEntityKey.class))).thenReturn(dremioTable);
    when(catalog.getTable(any(CatalogEntityKey.class))).thenReturn(dremioTable);
    when(dremioTable.getDatasetConfig()).thenReturn(datasetConfig);

    final Optional<CatalogEntity> catalogEntity =
        catalogServiceHelper.getCatalogEntityByPath(
            Arrays.asList("versionedSource", "table"),
            ImmutableList.of(),
            ImmutableList.of(),
            "SNAPSHOT",
            "1128544236092645872");

    assertThat(catalogEntity.isPresent()).isTrue();
    assertThat(catalogEntity.get()).isInstanceOf(Dataset.class);

    final Dataset dataset = (Dataset) catalogEntity.get();

    assertThat(dataset.getId()).isEqualTo(datasetId);
    assertThat(dataset.getType()).isEqualTo(Dataset.DatasetType.PHYSICAL_DATASET);
  }

  @Test
  public void getCatalogEntityByPathForTimestamp() throws Exception {
    final DatasetConfig datasetConfig =
        new DatasetConfig()
            .setType(DatasetType.PHYSICAL_DATASET)
            .setId(new EntityId(datasetId))
            .setFullPathList(Arrays.asList("versionedSource", "table"));
    ResolvedVersionContext resolvedVersionContext = ResolvedVersionContext.ofCommit("abc123");

    when(dataplanePlugin.resolveVersionContext(any(VersionContext.class)))
        .thenReturn(resolvedVersionContext);
    when(dataplanePlugin.getType(eq(Arrays.asList("table")), eq(resolvedVersionContext)))
        .thenReturn(VersionedPlugin.EntityType.ICEBERG_TABLE);
    when(catalog.getTableSnapshot(any(CatalogEntityKey.class))).thenReturn(dremioTable);
    when(catalog.getTable(any(CatalogEntityKey.class))).thenReturn(dremioTable);
    when(dremioTable.getDatasetConfig()).thenReturn(datasetConfig);

    final Optional<CatalogEntity> catalogEntity =
        catalogServiceHelper.getCatalogEntityByPath(
            Arrays.asList("versionedSource", "table"),
            ImmutableList.of(),
            ImmutableList.of(),
            "TIMESTAMP",
            "1679029735226");

    assertThat(catalogEntity.isPresent()).isTrue();
    assertThat(catalogEntity.get()).isInstanceOf(Dataset.class);

    final Dataset dataset = (Dataset) catalogEntity.get();

    assertThat(dataset.getId()).isEqualTo(datasetId);
    assertThat(dataset.getType()).isEqualTo(Dataset.DatasetType.PHYSICAL_DATASET);
  }

  @Test
  public void getCatalogEntityByPathForView() throws Exception {
    final DatasetConfig datasetConfig =
        new DatasetConfig()
            .setType(DatasetType.VIRTUAL_DATASET)
            .setId(new EntityId(datasetId))
            .setFullPathList(Arrays.asList("versionedSource", "view"));
    ResolvedVersionContext resolvedVersionContext =
        ResolvedVersionContext.ofBranch("main", "abc123");

    when(dataplanePlugin.resolveVersionContext(any(VersionContext.class)))
        .thenReturn(resolvedVersionContext);
    when(dataplanePlugin.getType(eq(Arrays.asList("view")), eq(resolvedVersionContext)))
        .thenReturn(VersionedPlugin.EntityType.ICEBERG_VIEW);
    when(catalog.getTableSnapshot(any(CatalogEntityKey.class))).thenReturn(dremioTable);
    when(catalog.getTable(any(CatalogEntityKey.class))).thenReturn(dremioTable);
    when(dremioTable.getDatasetConfig()).thenReturn(datasetConfig);

    final Optional<CatalogEntity> catalogEntity =
        catalogServiceHelper.getCatalogEntityByPath(
            Arrays.asList("versionedSource", "view"),
            ImmutableList.of(),
            ImmutableList.of(),
            "BRANCH",
            "main");

    assertThat(catalogEntity.isPresent()).isTrue();
    assertThat(catalogEntity.get()).isInstanceOf(Dataset.class);

    final Dataset dataset = (Dataset) catalogEntity.get();

    assertThat(dataset.getId()).isEqualTo(datasetId);
    assertThat(dataset.getType()).isEqualTo(Dataset.DatasetType.VIRTUAL_DATASET);
  }

  @Test
  public void getCatalogEntityByPathForFolder() throws Exception {
    final SourceConfig sourceConfig = new SourceConfig().setName("versionedSource");
    final NameSpaceContainer source =
        new NameSpaceContainer().setSource(sourceConfig).setType(NameSpaceContainer.Type.SOURCE);
    final ResolvedVersionContext resolvedVersionContext =
        ResolvedVersionContext.ofBranch("main", "abc123");
    final NamespaceTree contents = new NamespaceTree();
    contents.addFolder(
        new com.dremio.dac.model.folder.Folder(
            VersionedDatasetId.newBuilder()
                .setTableKey(Arrays.asList("versionedSource", "myfolder", "nested"))
                .setContentId("85f4da7b-ff38-4a2e-a040-600d73e7eb9a")
                .setTableVersionContext(TableVersionContext.of(VersionContext.ofBranch("main")))
                .build()
                .asString(),
            "nested",
            "/source/versionedSource/folder/myfolder/nested",
            false,
            false,
            false,
            null,
            "0",
            null,
            new NamespaceTree(),
            null,
            0));
    contents.addDataset(
        com.dremio.dac.explore.model.Dataset.newInstance(
            new SourceName("versionedSource"),
            Arrays.asList("myfolder"),
            "view",
            VersionedDatasetId.newBuilder()
                .setTableKey(Arrays.asList("versionedSource", "myfolder", "view"))
                .setContentId("55617bba-49df-48be-9b0f-ed4c54df30a7")
                .setTableVersionContext(TableVersionContext.of(VersionContext.ofBranch("main")))
                .build()
                .asString()));
    contents.addPhysicalDataset(
        PhysicalDataset.newInstance(
            new SourceName("versionedSource"),
            Arrays.asList("myfolder"),
            "table",
            VersionedDatasetId.newBuilder()
                .setTableKey(Arrays.asList("versionedSource", "myfolder", "table"))
                .setContentId("68bf9668-4e5b-4098-89e6-aeb3671e272c")
                .setTableVersionContext(TableVersionContext.of(VersionContext.ofBranch("main")))
                .build()
                .asString()));
    VersionedDatasetId testVersionedDatasetId =
        VersionedDatasetId.newBuilder()
            .setTableKey(Arrays.asList("versionedSource", "myfolder"))
            .setContentId("78c9e027-0678-43bb-a4eb-0419b87e86fb")
            .setTableVersionContext(TableVersionContext.of(VersionContext.ofBranch("main")))
            .build();

    when(dataplanePlugin.resolveVersionContext(any(VersionContext.class)))
        .thenReturn(resolvedVersionContext);
    when(dataplanePlugin.getType(eq(Arrays.asList("myfolder")), eq(resolvedVersionContext)))
        .thenReturn(VersionedPlugin.EntityType.FOLDER);
    when(catalog.getTableSnapshot(any(CatalogEntityKey.class))).thenReturn(dremioTable);
    when(catalog.getDatasetId(new NamespaceKey(Arrays.asList("versionedSource", "myfolder"))))
        .thenReturn(testVersionedDatasetId.asString());
    when(namespaceService.getEntities(any(List.class)))
        .thenReturn(Collections.singletonList(source));
    when(securityContext.getUserPrincipal()).thenReturn(() -> "user123");
    when(sourceService.listFolder(
            any(SourceName.class),
            any(SourceFolderPath.class),
            eq("user123"),
            eq("BRANCH"),
            eq("main")))
        .thenReturn(contents);

    final Optional<CatalogEntity> catalogEntity =
        catalogServiceHelper.getCatalogEntityByPath(
            Arrays.asList("versionedSource", "myfolder"),
            ImmutableList.of(),
            ImmutableList.of(),
            "BRANCH",
            "main");

    assertThat(catalogEntity.isPresent()).isTrue();
    assertThat(catalogEntity.get()).isInstanceOf(Folder.class);

    final Folder folder = (Folder) catalogEntity.get();

    assertThat(folder.getId()).isEqualTo(testVersionedDatasetId.asString());
    assertThat(folder.getPath()).isEqualTo(Arrays.asList("versionedSource", "myfolder"));
    assertThat(folder.getName()).isEqualTo("myfolder");
    assertThat(folder.getChildren().size()).isEqualTo(3);
  }

  @Test
  public void dropView() throws Exception {
    final String viewId =
        VersionedDatasetId.newBuilder()
            .setTableVersionContext(TableVersionContext.of(VersionContext.ofBranch("main")))
            .setContentId(UUID.randomUUID().toString())
            .setTableKey(Arrays.asList("versionedSource", "view"))
            .build()
            .asString();
    final ResolvedVersionContext resolvedVersionContext =
        ResolvedVersionContext.ofBranch("main", "abc123");

    when(dataplanePlugin.resolveVersionContext(any(VersionContext.class)))
        .thenReturn(resolvedVersionContext);
    when(dataplanePlugin.getType(eq(Arrays.asList("view")), eq(resolvedVersionContext)))
        .thenReturn(VersionedPlugin.EntityType.ICEBERG_VIEW);
    when(catalog.getTable(eq(viewId))).thenReturn(dremioTable);
    final DatasetConfig datasetConfig =
        new DatasetConfig()
            .setType(DatasetType.VIRTUAL_DATASET)
            .setId(new EntityId(viewId))
            .setFullPathList(Arrays.asList("versionedSource", "view"));
    when(dremioTable.getDatasetConfig()).thenReturn(datasetConfig);
    when(catalog.getSource("versionedSource")).thenReturn(dataplanePlugin);
    when(catalog.resolveVersionContext("versionedSource", VersionContext.ofBranch("main")))
        .thenReturn(resolvedVersionContext);

    catalogServiceHelper.deleteCatalogItem(viewId, null);

    verify(catalog)
        .dropView(
            new NamespaceKey(Arrays.asList("versionedSource", "view")),
            new ViewOptions.ViewOptionsBuilder().version(resolvedVersionContext).build());
  }

  @Test
  public void dropTable() throws Exception {
    final String tableId =
        VersionedDatasetId.newBuilder()
            .setTableVersionContext(TableVersionContext.of(VersionContext.ofBranch("main")))
            .setContentId(UUID.randomUUID().toString())
            .setTableKey(Arrays.asList("versionedSource", "table"))
            .build()
            .asString();
    final ResolvedVersionContext resolvedVersionContext =
        ResolvedVersionContext.ofBranch("main", "abc123");

    when(dataplanePlugin.resolveVersionContext(any(VersionContext.class)))
        .thenReturn(resolvedVersionContext);
    when(dataplanePlugin.getType(eq(Arrays.asList("table")), eq(resolvedVersionContext)))
        .thenReturn(VersionedPlugin.EntityType.ICEBERG_TABLE);
    when(catalog.getTable(eq(tableId))).thenReturn(dremioTable);
    final DatasetConfig datasetConfig =
        new DatasetConfig()
            .setType(DatasetType.PHYSICAL_DATASET)
            .setId(new EntityId(tableId))
            .setFullPathList(Arrays.asList("versionedSource", "table"));
    when(dremioTable.getDatasetConfig()).thenReturn(datasetConfig);
    when(catalog.getSource("versionedSource")).thenReturn(dataplanePlugin);
    when(catalog.resolveVersionContext("versionedSource", VersionContext.ofBranch("main")))
        .thenReturn(resolvedVersionContext);

    catalogServiceHelper.deleteCatalogItem(tableId, null);

    verify(catalog)
        .dropTable(
            new NamespaceKey(Arrays.asList("versionedSource", "table")),
            TableMutationOptions.newBuilder()
                .setResolvedVersionContext(resolvedVersionContext)
                .build());
  }
}
