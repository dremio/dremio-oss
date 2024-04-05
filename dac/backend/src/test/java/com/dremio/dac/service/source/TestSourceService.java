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
package com.dremio.dac.service.source;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dremio.catalog.model.VersionContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.ProtostuffUtil;
import com.dremio.dac.explore.QueryExecutor;
import com.dremio.dac.explore.model.Dataset;
import com.dremio.dac.explore.model.VersionContextReq;
import com.dremio.dac.homefiles.HomeFileConf;
import com.dremio.dac.model.folder.Folder;
import com.dremio.dac.model.folder.FolderName;
import com.dremio.dac.model.namespace.NamespaceTree;
import com.dremio.dac.model.sources.FormatTools;
import com.dremio.dac.model.sources.PhysicalDataset;
import com.dremio.dac.model.sources.SourceName;
import com.dremio.dac.resource.SourceResource;
import com.dremio.dac.server.BufferAllocatorFactory;
import com.dremio.dac.service.collaboration.CollaborationHelper;
import com.dremio.dac.service.datasets.DatasetVersionMutator;
import com.dremio.dac.service.reflection.ReflectionServiceHelper;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.ConnectionReader;
import com.dremio.exec.catalog.SourceCatalog;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.VersionedPlugin;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.server.ContextService;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.NessieNamespaceAlreadyExistsException;
import com.dremio.exec.store.ReferenceNotFoundException;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.dfs.InternalFileConf;
import com.dremio.exec.store.dfs.PDFSConf;
import com.dremio.exec.store.sys.SystemPluginConf;
import com.dremio.file.File;
import com.dremio.plugins.ExternalNamespaceEntry;
import com.dremio.plugins.ExternalNamespaceEntry.Type;
import com.dremio.plugins.dataplane.store.DataplanePlugin;
import com.dremio.service.namespace.BoundedDatasetCount;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.namespace.source.proto.MetadataPolicy;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.source.proto.UpdateMode;
import com.dremio.service.reflection.ReflectionAdministrationService;
import com.dremio.service.reflection.ReflectionSettings;
import com.google.common.collect.ImmutableList;
import java.security.Principal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.inject.Provider;
import javax.ws.rs.core.SecurityContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TestSourceService {
  private static final String SOURCE_NAME = "sourceName";
  private static final String DEFAULT_REF_TYPE =
      VersionContextReq.VersionContextType.BRANCH.toString();
  private static final String DEFAULT_BRANCH_NAME = "somebranch";
  private static final VersionContext DEFAULT_VERSION_CONTEXT =
      VersionContext.ofBranch(DEFAULT_BRANCH_NAME);
  private static final String FOLDER_NAME_1 = "folder1";
  private static final String FOLDER_NAME_2 = "folder2";
  private static final String TABLE_NAME_1 = "table1";
  private static final List<ExternalNamespaceEntry> DEFAULT_ENTRIES =
      Arrays.asList(
          ExternalNamespaceEntry.of(Type.FOLDER, Collections.singletonList(FOLDER_NAME_1)),
          ExternalNamespaceEntry.of(Type.FOLDER, Collections.singletonList(FOLDER_NAME_2)),
          ExternalNamespaceEntry.of(Type.ICEBERG_TABLE, Collections.singletonList(TABLE_NAME_1)));
  private static final SourceConfig SOURCE_CONFIG =
      new SourceConfig()
          .setId(new EntityId().setId("id"))
          .setName("test")
          .setType("s3")
          .setTag("tag")
          .setMetadataPolicy(
              new MetadataPolicy()
                  .setAuthTtlMs(100_000L)
                  .setDatasetDefinitionExpireAfterMs(300_000L)
                  .setDatasetDefinitionRefreshAfterMs(300_000L)
                  .setDatasetUpdateMode(UpdateMode.INLINE)
                  .setNamesRefreshMs(300_000L))
          .setCtime(System.currentTimeMillis());
  private static final List<ConnectionConf<?, ?>> validConnectionConfs =
      ImmutableList.of(new NonInternalConf());
  private static final List<ConnectionConf<?, ?>> invalidConnectionConfs =
      ImmutableList.of(
          new SystemPluginConf(), new HomeFileConf(), new PDFSConf(), new InternalFileConf());

  @Mock private NamespaceService namespaceService;
  @Mock private DataplanePlugin dataplanePlugin;
  @Mock private ConnectionReader connectionReader;
  @Mock private ReflectionAdministrationService.Factory reflectionService;
  @Mock private SecurityContext securityContext;
  @Mock private CatalogService catalogService;
  @Mock private ReflectionServiceHelper reflectionServiceHelper;

  private SourceService getSourceService() {
    return getSourceService(Clock.systemUTC());
  }

  private SourceService getSourceService(Clock clock) {
    return new SourceService(
        clock,
        mock(SabotContext.class),
        namespaceService,
        mock(DatasetVersionMutator.class),
        catalogService,
        reflectionServiceHelper,
        mock(CollaborationHelper.class),
        connectionReader,
        securityContext);
  }

  private void testConnectionConfs(
      List<ConnectionConf<?, ?>> validConnectionConfs, boolean isValid) {
    SourceService sourceService = getSourceService();
    for (ConnectionConf<?, ?> connectionConf : validConnectionConfs) {
      try {
        sourceService.validateConnectionConf(connectionConf);
      } catch (UserException e) {
        assertFalse(isValid);
      }
    }
  }

  @Test
  public void testCreateSource() throws Exception {
    Clock clock = Clock.fixed(Instant.now(), ZoneId.of("UTC"));
    SourceService sourceService = getSourceService(clock);

    final Principal principal = mock(Principal.class);
    when(principal.getName()).thenReturn("username");
    when(securityContext.getUserPrincipal()).thenReturn(principal);

    final Catalog catalog = mock(Catalog.class);
    when(catalogService.getCatalog(any())).thenReturn(catalog);

    final ReflectionSettings reflectionSettings = mock(ReflectionSettings.class);
    when(reflectionServiceHelper.getReflectionSettings()).thenReturn(reflectionSettings);

    SourceConfig sourceConfig =
        ProtostuffUtil.copy(SOURCE_CONFIG).setCtime(null).setId(null).setTag(null);
    when(connectionReader.getConnectionConf(sourceConfig)).thenReturn(mock(ConnectionConf.class));

    assertThat(sourceConfig.getCtime()).isNull();

    sourceService.createSource(sourceConfig);

    assertThat(sourceConfig.getCtime()).isEqualTo(clock.millis());
    verify(catalog, times(1)).createSource(eq(sourceConfig));
  }

  @ParameterizedTest
  @ValueSource(strings = {"copy", "null", "zero"})
  public void testUpdateSource(String timeSetOption) throws Exception {
    Clock clock = Clock.fixed(Instant.now().plusSeconds(1), ZoneId.of("UTC"));
    SourceService sourceService = getSourceService(clock);

    final Principal principal = mock(Principal.class);
    when(principal.getName()).thenReturn("username");
    when(securityContext.getUserPrincipal()).thenReturn(principal);

    final Catalog catalog = mock(Catalog.class);
    when(catalogService.getCatalog(any())).thenReturn(catalog);

    final ReflectionSettings reflectionSettings = mock(ReflectionSettings.class);
    when(reflectionServiceHelper.getReflectionSettings()).thenReturn(reflectionSettings);

    // Mock currently stored source.
    SourceConfig currentSourceConfig = ProtostuffUtil.copy(SOURCE_CONFIG);
    when(namespaceService.getSourceById(eq(currentSourceConfig.getId().getId())))
        .thenReturn(currentSourceConfig);

    // Mock updated source.
    SourceConfig updatedSourceConfig =
        ProtostuffUtil.copy(currentSourceConfig).setAccelerationNeverRefresh(true);
    switch (timeSetOption) {
      case "copy":
        updatedSourceConfig.setCtime(currentSourceConfig.getCtime());
        break;
      case "null":
        updatedSourceConfig.setCtime(null);
        break;
      case "zero":
        updatedSourceConfig.setCtime(0L);
        break;
      default:
        throw new RuntimeException("Unexpected case: " + timeSetOption);
    }

    when(connectionReader.getConnectionConf(updatedSourceConfig))
        .thenReturn(mock(ConnectionConf.class));

    // Expect that the times are the same and the catalog was updated.
    sourceService.updateSource(updatedSourceConfig.getId().getId(), updatedSourceConfig);
    assertThat(updatedSourceConfig.getCtime()).isEqualTo(currentSourceConfig.getCtime());
    assertThat(updatedSourceConfig.getLastModifiedAt()).isEqualTo(clock.millis());
    verify(catalog, times(1)).updateSource(eq(updatedSourceConfig));
  }

  @Test
  public void testUpdateSource_invalidCtime() throws Exception {
    SourceService sourceService = getSourceService();

    // Mock currently stored source.
    SourceConfig currentSourceConfig = ProtostuffUtil.copy(SOURCE_CONFIG);
    when(namespaceService.getSourceById(eq(currentSourceConfig.getId().getId())))
        .thenReturn(currentSourceConfig);

    // Mock updated one.
    SourceConfig updatedSourceConfig =
        ProtostuffUtil.copy(currentSourceConfig)
            .setAccelerationNeverRefresh(true)
            .setCtime(System.currentTimeMillis() + 1000);

    when(connectionReader.getConnectionConf(updatedSourceConfig))
        .thenReturn(mock(ConnectionConf.class));

    // Expect an exception.
    Exception e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                sourceService.updateSource(
                    updatedSourceConfig.getId().getId(), updatedSourceConfig));
    assertThat(e.getMessage()).contains("Creation time is immutable.");
  }

  @Test
  public void testGetSource() throws Exception {
    // Arrange
    complexMockSetup();
    when(dataplanePlugin.listEntries(any(), eq(DEFAULT_VERSION_CONTEXT)))
        .thenReturn(DEFAULT_ENTRIES.stream());
    SourceResource sourceResource = makeSourceResource();

    // Act
    NamespaceTree contents =
        sourceResource.getSource(true, DEFAULT_REF_TYPE, DEFAULT_BRANCH_NAME).getContents();

    // Assert
    assertMatchesDefaultEntries(contents);
  }

  @Test
  public void testGetFolder() throws Exception {
    // Arrange
    when(namespaceService.getDataset(any())).thenThrow(NamespaceNotFoundException.class);

    when(dataplanePlugin.listEntries(any(), eq(DEFAULT_VERSION_CONTEXT)))
        .thenReturn(DEFAULT_ENTRIES.stream());

    SourceResource sourceResource = makeSourceResource();

    // Act
    NamespaceTree contents =
        sourceResource
            .getFolder("folder", true, DEFAULT_REF_TYPE, DEFAULT_BRANCH_NAME)
            .getContents();

    // Assert
    assertMatchesDefaultEntries(contents);
  }

  @Test
  public void testCreateFolder() {
    SourceResource sourceResource = makeSourceResource();

    doNothing().when(dataplanePlugin).createNamespace(any(), eq(DEFAULT_VERSION_CONTEXT));

    Folder folder =
        sourceResource.createFolder(
            null, DEFAULT_REF_TYPE, DEFAULT_BRANCH_NAME, new FolderName(FOLDER_NAME_1));

    assertThat(folder.getName()).isEqualTo(FOLDER_NAME_1);
    assertThat(folder.getIsPhysicalDataset()).isFalse();
  }

  @Test
  public void testCreateFolderThrownNessieNamespaceAlreadyExistsException() {
    SourceResource sourceResource = makeSourceResource();

    doThrow(NessieNamespaceAlreadyExistsException.class)
        .doNothing()
        .when(dataplanePlugin)
        .createNamespace(any(), eq(DEFAULT_VERSION_CONTEXT));

    assertThatThrownBy(
            () ->
                sourceResource.createFolder(
                    null, DEFAULT_REF_TYPE, DEFAULT_BRANCH_NAME, new FolderName(FOLDER_NAME_1)))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("already exists");
  }

  @Test
  public void testCreateFolderThrownReferenceNotFoundException() {
    SourceResource sourceResource = makeSourceResource();

    doThrow(ReferenceNotFoundException.class)
        .doNothing()
        .when(dataplanePlugin)
        .createNamespace(any(), eq(DEFAULT_VERSION_CONTEXT));

    assertThatThrownBy(
            () ->
                sourceResource.createFolder(
                    null, DEFAULT_REF_TYPE, DEFAULT_BRANCH_NAME, new FolderName(FOLDER_NAME_1)))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("not found");
  }

  @Test
  public void testCreateFolderWithSpaceInFolderName() {
    final String folderNameWithSpace = "folder with space";
    SourceResource sourceResource = makeSourceResource();

    Folder folder =
        sourceResource.createFolder(
            null, DEFAULT_REF_TYPE, DEFAULT_BRANCH_NAME, new FolderName(folderNameWithSpace));

    verify(dataplanePlugin)
        .createNamespace(
            new NamespaceKey(Arrays.asList(SOURCE_NAME, folderNameWithSpace)),
            DEFAULT_VERSION_CONTEXT);
    assertThat(folder.getName()).isEqualTo(folderNameWithSpace);
    assertThat(folder.getIsPhysicalDataset()).isFalse();
  }

  @Test
  public void testCreateFolderWithSpaceInFolderNameWithinNestedFolder() {
    final String rootFolderNameWithSpace = "folder with space";
    final String leafFolderNameWithSpace = "folder with another space";
    final String path = "folder with space/";
    SourceResource sourceResource = makeSourceResource();

    Folder folder =
        sourceResource.createFolder(
            path, DEFAULT_REF_TYPE, DEFAULT_BRANCH_NAME, new FolderName(leafFolderNameWithSpace));

    verify(dataplanePlugin)
        .createNamespace(
            new NamespaceKey(
                Arrays.asList(SOURCE_NAME, rootFolderNameWithSpace, leafFolderNameWithSpace)),
            DEFAULT_VERSION_CONTEXT);
    assertThat(folder.getName()).isEqualTo(leafFolderNameWithSpace);
    assertThat(folder.getIsPhysicalDataset()).isFalse();
  }

  @Test
  public void testDeleteFolder() {
    final String rootFolder = "rootFolder";
    final String path = "";
    SourceResource sourceResource = makeSourceResource();

    sourceResource.createFolder(
        path, DEFAULT_REF_TYPE, DEFAULT_BRANCH_NAME, new FolderName(rootFolder));

    sourceResource.deleteFolder("rootFolder/", DEFAULT_REF_TYPE, DEFAULT_BRANCH_NAME);
    verify(dataplanePlugin)
        .deleteFolder(
            new NamespaceKey(Arrays.asList(SOURCE_NAME, rootFolder)), DEFAULT_VERSION_CONTEXT);
  }

  @Test
  public void testDeletePhysicalDatasetForVersionedSource() {
    when(catalogService.getSource("nessie")).thenReturn(dataplanePlugin);
    when(dataplanePlugin.isWrapperFor(VersionedPlugin.class)).thenReturn(true);
    SourceService sourceService = getSourceService();

    assertThatThrownBy(
            () -> sourceService.deletePhysicalDataset(new SourceName("nessie"), null, "0001", null))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("not allowed for Versioned source");
  }

  @Test
  public void testValidConnectionConfs() {
    testConnectionConfs(validConnectionConfs, true);
  }

  @Test
  public void testInvalidConnectionConfs() {
    testConnectionConfs(invalidConnectionConfs, false);
  }

  private void assertMatchesDefaultEntries(NamespaceTree contents) {

    List<Folder> folders = contents.getFolders();
    List<PhysicalDataset> physicalDatasets = contents.getPhysicalDatasets();
    List<File> files = contents.getFiles();
    List<Dataset> virtualDatasets = contents.getDatasets();

    assertThat(folders).hasSize(2);
    assertThat(folders.get(0).getName()).isEqualTo(FOLDER_NAME_1);
    assertThat(folders.get(0).getIsPhysicalDataset()).isFalse();

    assertThat(physicalDatasets).hasSize(1);
    assertThat(physicalDatasets.get(0).getDatasetName().getName()).isEqualTo(TABLE_NAME_1);

    assertThat(files).isEmpty();
    assertThat(virtualDatasets).isEmpty();

    assertThat(contents.totalCount()).isEqualTo(3);
  }

  private SourceResource makeSourceResource() {
    when(catalogService.getSource(anyString())).thenReturn(dataplanePlugin);
    when(dataplanePlugin.isWrapperFor(VersionedPlugin.class)).thenReturn(true);
    when(dataplanePlugin.unwrap(VersionedPlugin.class)).thenReturn(dataplanePlugin);

    final Principal principal = mock(Principal.class);
    when(principal.getName()).thenReturn("username");
    when(securityContext.getUserPrincipal()).thenReturn(principal);

    final SourceService sourceService = getSourceService();

    return new SourceResource(
        namespaceService,
        reflectionService,
        sourceService,
        new SourceName(SOURCE_NAME),
        mock(QueryExecutor.class),
        securityContext,
        connectionReader,
        mock(SourceCatalog.class),
        mock(FormatTools.class),
        mock(ContextService.class),
        mock(BufferAllocatorFactory.class));
  }

  private void complexMockSetup() throws NamespaceException {
    final SourceConfig sourceConfig =
        new SourceConfig()
            .setName(SOURCE_NAME)
            .setMetadataPolicy(CatalogService.NEVER_REFRESH_POLICY)
            .setCtime(100L)
            .setId(new EntityId().setId("1"));
    when(connectionReader.getConnectionConf(sourceConfig)).thenReturn(mock(ConnectionConf.class));
    when(namespaceService.getSource(any())).thenReturn(sourceConfig);
    when(namespaceService.getDatasetCount(any(), anyLong(), anyInt()))
        .thenReturn(new BoundedDatasetCount(0, false, false));
    when(catalogService.getSourceState(SOURCE_NAME)).thenReturn(SourceState.GOOD);

    ReflectionSettings reflectionSettings = mock(ReflectionSettings.class);
    when(reflectionSettings.getReflectionSettings((NamespaceKey) any())).thenReturn(null);

    ReflectionAdministrationService reflectionAdministrationService =
        mock(ReflectionAdministrationService.class);
    when(reflectionAdministrationService.getReflectionSettings()).thenReturn(reflectionSettings);

    when(reflectionService.get(any())).thenReturn(reflectionAdministrationService);
  }

  private static final class NonInternalConf
      extends ConnectionConf<NonInternalConf, StoragePlugin> {

    @Override
    public StoragePlugin newPlugin(
        SabotContext context, String name, Provider<StoragePluginId> pluginIdProvider) {
      return null;
    }

    @Override
    // Don't need to override, but making it explicit.
    public boolean isInternal() {
      return false;
    }
  }
}
