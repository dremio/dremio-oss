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
package com.dremio.dac.service.datasets;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.ProtostuffUtil;
import com.dremio.dac.explore.model.DatasetName;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.model.sources.SourceName;
import com.dremio.dac.proto.model.dataset.DatasetVersionOrigin;
import com.dremio.dac.proto.model.dataset.NameDatasetRef;
import com.dremio.dac.proto.model.dataset.VirtualDatasetState;
import com.dremio.dac.proto.model.dataset.VirtualDatasetUI;
import com.dremio.dac.proto.model.dataset.VirtualDatasetVersion;
import com.dremio.dac.util.DatasetsUtil;
import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.VersionedPlugin;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.options.OptionManager;
import com.dremio.plugins.dataplane.store.DataplanePlugin;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.dremio.service.namespace.dataset.proto.ViewFieldType;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TestDatasetVersionMutator {
  @Mock private OptionManager optionManager;
  @Mock private Catalog catalog;
  @Mock private CatalogService catalogService;
  @Mock private LegacyKVStoreProvider legacyKVStoreProvider;
  @Mock private JobsService jobsService;
  @Mock private SabotContext sabotContext;
  @Mock private DataplanePlugin dataplanePlugin;

  @Mock
  private LegacyKVStore<DatasetVersionMutator.VersionDatasetKey, VirtualDatasetVersion>
      datasetVersions;

  private static final String versionedSourceName = "nessie";

  private DatasetVersionMutator datasetVersionMutator;

  @BeforeEach
  public void setup() throws Exception {
    when(legacyKVStoreProvider.getStore(any())).thenReturn(datasetVersions);

    datasetVersionMutator =
        new DatasetVersionMutator(
            legacyKVStoreProvider, jobsService, catalogService, optionManager, sabotContext);
  }

  @Test
  public void testRenameDatasetForVersionedSource() throws Exception {
    setupForVersionedSource();
    DatasetPath oldDatasetPath =
        new DatasetPath(new SourceName(versionedSourceName), new DatasetName("testTable"));
    DatasetPath newDatasetPath =
        new DatasetPath(new SourceName(versionedSourceName), new DatasetName("testMoveTable"));

    assertThatThrownBy(() -> datasetVersionMutator.renameDataset(oldDatasetPath, newDatasetPath))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("not allowed in Versioned source");
  }

  @Test
  public void testCopyFromDatasetForVersionedSource() throws Exception {
    when(catalogService.getSystemUserCatalog()).thenReturn(catalog);

    DatasetPath datasetPath = new DatasetPath(new SourceName("test"), new DatasetName("testTable"));

    assertThatThrownBy(
            () -> datasetVersionMutator.createDatasetFrom(datasetPath, datasetPath, "userName"))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("already exists");
  }

  @Test
  public void testCopyFromDatasetForTheSamePath() throws Exception {
    setupForVersionedSource();
    DatasetPath datasetPath =
        new DatasetPath(new SourceName(versionedSourceName), new DatasetName("testTable"));

    assertThatThrownBy(
            () -> datasetVersionMutator.createDatasetFrom(datasetPath, datasetPath, "userName"))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("not allowed within Versioned source");
  }

  @Test
  public void testPutVersion_createdTimeSet() {
    VirtualDatasetUI uiProto =
        new VirtualDatasetUI()
            .setId("123")
            .setFullPathList(ImmutableList.of("path"))
            .setSqlFieldsList(ImmutableList.of(new ViewFieldType().setName("a").setType("int")))
            .setState(new VirtualDatasetState())
            .setVersion(new DatasetVersion("1"))
            .setCreatedAt(123L);
    datasetVersionMutator.putVersion(uiProto);
    DatasetVersionMutator.VersionDatasetKey key =
        new DatasetVersionMutator.VersionDatasetKey(
            new DatasetPath(uiProto.getFullPathList()), uiProto.getVersion());
    verify(datasetVersions, times(1))
        .put(eq(key), argThat((arg) -> arg.getDataset().getCreatedAt() != 123L));
  }

  @Test
  public void testPutVersion_createdTimeNotSet() {
    VirtualDatasetUI uiProto =
        new VirtualDatasetUI()
            .setId("123")
            .setFullPathList(ImmutableList.of("path"))
            .setSqlFieldsList(ImmutableList.of(new ViewFieldType().setName("a").setType("int")))
            .setState(new VirtualDatasetState())
            .setVersion(new DatasetVersion("1"));
    VirtualDatasetUI baseUiProto = new VirtualDatasetUI().setCreatedAt(123L);
    VirtualDatasetVersion storageProto =
        DatasetsUtil.toVirtualDatasetVersion(
            ProtostuffUtil.copy(uiProto).setCreatedAt(baseUiProto.getCreatedAt()));
    datasetVersionMutator.putVersion(uiProto, baseUiProto);
    DatasetVersionMutator.VersionDatasetKey key =
        new DatasetVersionMutator.VersionDatasetKey(
            new DatasetPath(uiProto.getFullPathList()), uiProto.getVersion());
    verify(datasetVersions, times(1)).put(eq(key), eq(storageProto));
    assertEquals(uiProto.getCreatedAt(), storageProto.getDataset().getCreatedAt());
  }

  @Test
  public void testGettingCorrectSavedVersion() {
    List<String> path = ImmutableList.of("path");
    VirtualDatasetUI first =
        new VirtualDatasetUI()
            .setId("1")
            .setFullPathList(path)
            .setSqlFieldsList(ImmutableList.of(new ViewFieldType().setName("a").setType("int")))
            .setState(new VirtualDatasetState())
            .setVersion(new DatasetVersion("1"));

    VirtualDatasetUI second =
        new VirtualDatasetUI()
            .setId("2")
            .setFullPathList(path)
            .setSqlFieldsList(ImmutableList.of(new ViewFieldType().setName("a").setType("int")))
            .setState(new VirtualDatasetState())
            .setVersion(new DatasetVersion("2"))
            .setDatasetVersionOrigin(DatasetVersionOrigin.SAVE);

    VirtualDatasetUI third =
        new VirtualDatasetUI()
            .setId("3")
            .setFullPathList(path)
            .setSqlFieldsList(ImmutableList.of(new ViewFieldType().setName("a").setType("int")))
            .setState(new VirtualDatasetState())
            .setVersion(new DatasetVersion("3"));

    NameDatasetRef firstRef =
        new NameDatasetRef()
            .setDatasetPath("path")
            .setDatasetVersion(first.getVersion().getVersion());
    second.setPreviousVersion(firstRef);

    NameDatasetRef secondRef =
        new NameDatasetRef()
            .setDatasetPath("path")
            .setDatasetVersion(second.getVersion().getVersion());
    third.setPreviousVersion(secondRef);

    datasetVersionMutator.putVersion(first);
    datasetVersionMutator.putVersion(second);
    datasetVersionMutator.putVersion(third);

    when(datasetVersions.get(
            new DatasetVersionMutator.VersionDatasetKey(
                new DatasetPath(path), second.getVersion())))
        .thenReturn(
            new VirtualDatasetVersion()
                .setPreviousVersion(firstRef)
                .setDatasetVersionOrigin(DatasetVersionOrigin.SAVE));

    when(datasetVersions.get(
            new DatasetVersionMutator.VersionDatasetKey(new DatasetPath(path), third.getVersion())))
        .thenReturn(new VirtualDatasetVersion().setPreviousVersion(secondRef));

    // the version history looks like third -> second (saved) -> first. Therefore, when we are
    // getting
    // saved version from the third, we will stop at second version and return it.
    assertEquals(
        datasetVersionMutator.getLatestVersionByOrigin(
            new DatasetPath(path), third.getVersion(), DatasetVersionOrigin.SAVE),
        second.getVersion());
  }

  @Test
  public void testGetVersionForSingleVersionReturnsNull() {
    List<String> path = ImmutableList.of("path");
    VirtualDatasetUI first =
        new VirtualDatasetUI()
            .setId("1")
            .setFullPathList(path)
            .setSqlFieldsList(ImmutableList.of(new ViewFieldType().setName("a").setType("int")))
            .setState(new VirtualDatasetState())
            .setVersion(new DatasetVersion("1"));

    datasetVersionMutator.putVersion(first);

    when(datasetVersions.get(
            new DatasetVersionMutator.VersionDatasetKey(new DatasetPath(path), first.getVersion())))
        .thenReturn(new VirtualDatasetVersion());

    assertNull(
        datasetVersionMutator.getLatestVersionByOrigin(
            new DatasetPath(path), first.getVersion(), DatasetVersionOrigin.SAVE));
  }

  private void setupForVersionedSource() throws NamespaceException {
    NameSpaceContainer nameSpaceContainer = mock(NameSpaceContainer.class);
    when(nameSpaceContainer.getType()).thenReturn(NameSpaceContainer.Type.SOURCE);
    when(catalog.getEntityByPath(eq(new NamespaceKey(versionedSourceName))))
        .thenReturn(nameSpaceContainer);
    when(catalogService.getSystemUserCatalog()).thenReturn(catalog);
    when(catalogService.getSource(versionedSourceName)).thenReturn(dataplanePlugin);
    when(dataplanePlugin.isWrapperFor(VersionedPlugin.class)).thenReturn(true);
  }
}
