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
package com.dremio.exec.catalog;

import static com.dremio.options.TypeValidators.PositiveLongValidator;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Optional;

import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.stubbing.Answer;

import com.dremio.common.AutoCloseables;
import com.dremio.connector.metadata.BytesOutput;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.DatasetNotFoundException;
import com.dremio.connector.metadata.DatasetStats;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.connector.metadata.extensions.SupportsReadSignature;
import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.cost.ScanCostFactor;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.options.OptionManager;
import com.dremio.service.namespace.DatasetMetadataSaver;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.dremio.service.namespace.source.proto.MetadataPolicy;
import com.dremio.service.scheduler.ModifiableLocalSchedulerService;
import com.dremio.service.scheduler.ModifiableSchedulerService;
import com.dremio.test.UserExceptionMatcher;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class TestSourceMetadataManager {
  private static final int MAX_COLUMNS = 800;
  private static final int MAX_NESTED_LEVELS = 16;
  private OptionManager optionManager;
  private final MetadataRefreshInfoBroadcaster broadcaster = mock(MetadataRefreshInfoBroadcaster.class);
  private ModifiableSchedulerService modifiableSchedulerService;

  @Rule
  public final ExpectedException thrownException = ExpectedException.none();

  @Before
  public void setup() {
    optionManager = mock(OptionManager.class);
    when(optionManager.getOption(eq(CatalogOptions.SPLIT_COMPRESSION_TYPE)))
      .thenAnswer((Answer) invocation -> NamespaceService.SplitCompression.SNAPPY.toString());
    doNothing().when(broadcaster).communicateChange(any());

    PositiveLongValidator option = ExecConstants.MAX_CONCURRENT_METADATA_REFRESHES;
    when(optionManager.getOption(option)).thenReturn(1L);
    modifiableSchedulerService = new ModifiableLocalSchedulerService(1, "modifiable-scheduler-",
        option, optionManager);
  }

  @After
  public void tearDown() throws Exception{
    AutoCloseables.close(modifiableSchedulerService);
  }

  @Test
  public void deleteUnavailableDataset() throws Exception {
    NamespaceService ns = mock(NamespaceService.class);
    when(ns.getDataset(any()))
        .thenReturn(
            new DatasetConfig()
                .setTag("0")
                .setReadDefinition(new ReadDefinition())
                .setFullPathList(ImmutableList.of("one", "two"))
        );

    boolean[] deleted = new boolean[] {false};
    doAnswer(invocation -> {
      deleted[0] = true;
      return null;
    }).when(ns).deleteDataset(any(), anyString());

    ExtendedStoragePlugin sp = mock(ExtendedStoragePlugin.class);
    when(sp.getDatasetHandle(any(), any(), any()))
        .thenReturn(Optional.empty());

    ManagedStoragePlugin.MetadataBridge msp = mock(ManagedStoragePlugin.MetadataBridge.class);
    when(msp.getMetadata())
        .thenReturn(sp);
    when(msp.getMetadataPolicy())
        .thenReturn(new MetadataPolicy().setDeleteUnavailableDatasets(false));
    when(msp.getMaxMetadataColumns())
      .thenReturn(MAX_COLUMNS);
    when(msp.getMaxNestedLevels())
      .thenReturn(MAX_NESTED_LEVELS);
    when(msp.getNamespaceService())
      .thenReturn(ns);

    //noinspection unchecked
    SourceMetadataManager manager = new SourceMetadataManager(
        new NamespaceKey("joker"),
        modifiableSchedulerService,
        true,
        mock(LegacyKVStore.class),
        msp,
        optionManager,
        CatalogServiceMonitor.DEFAULT,
        () -> broadcaster
      );

    assertEquals(DatasetCatalog.UpdateStatus.DELETED,
        manager.refreshDataset(new NamespaceKey(""), DatasetRetrievalOptions.DEFAULT));
    assertTrue(deleted[0]);
  }

  @Test
  public void doNotDeleteUnavailableDataset() throws Exception {
    NamespaceService ns = mock(NamespaceService.class);
    when(ns.getDataset(any()))
        .thenReturn(new DatasetConfig()
                        .setReadDefinition(new ReadDefinition())
                        .setFullPathList(ImmutableList.of("one", "two")));

    doThrow(new IllegalStateException("should not invoke deleteDataset()"))
        .when(ns)
        .deleteDataset(any(), anyString());

    ExtendedStoragePlugin sp = mock(ExtendedStoragePlugin.class);
    when(sp.getDatasetHandle(any(), any(), any()))
        .thenReturn(Optional.empty());

    ManagedStoragePlugin.MetadataBridge msp = mock(ManagedStoragePlugin.MetadataBridge.class);
    when(msp.getMetadata())
        .thenReturn(sp);
    when(msp.getMetadataPolicy())
        .thenReturn(new MetadataPolicy().setDeleteUnavailableDatasets(false));
    when(msp.getMaxMetadataColumns())
      .thenReturn(MAX_COLUMNS);
    when(msp.getMaxNestedLevels())
      .thenReturn(MAX_NESTED_LEVELS);
    when(msp.getNamespaceService())
      .thenReturn(ns);

    //noinspection unchecked
    SourceMetadataManager manager = new SourceMetadataManager(
        new NamespaceKey("joker"),
        modifiableSchedulerService,
        true,
        mock(LegacyKVStore.class),
        msp,
        optionManager,
        CatalogServiceMonitor.DEFAULT,
        () -> broadcaster
    );


    assertEquals(DatasetCatalog.UpdateStatus.UNCHANGED,
        manager.refreshDataset(new NamespaceKey(""),
            DatasetRetrievalOptions.DEFAULT.toBuilder()
                .setDeleteUnavailableDatasets(false)
                .build()));
  }

  @Test
  public void deleteUnavailableDatasetWithoutDefinition() throws Exception {
    NamespaceService ns = mock(NamespaceService.class);
    when(ns.getDataset(any()))
        .thenReturn(
            new DatasetConfig()
                .setTag("0")
                .setFullPathList(ImmutableList.of("one", "two"))
        );

    boolean[] deleted = new boolean[] {false};
    doAnswer(invocation -> {
      deleted[0] = true;
      return null;
    }).when(ns).deleteDataset(any(), anyString());

    ExtendedStoragePlugin sp = mock(ExtendedStoragePlugin.class);
    when(sp.getDatasetHandle(any(), any(), any()))
        .thenReturn(Optional.empty());

    ManagedStoragePlugin.MetadataBridge msp = mock(ManagedStoragePlugin.MetadataBridge.class);
    when(msp.getMetadata())
        .thenReturn(sp);
    when(msp.getMetadataPolicy())
        .thenReturn(new MetadataPolicy().setDeleteUnavailableDatasets(false));
    when(msp.getMaxMetadataColumns())
      .thenReturn(MAX_COLUMNS);
    when(msp.getMaxNestedLevels())
      .thenReturn(MAX_NESTED_LEVELS);
    when(msp.getNamespaceService())
      .thenReturn(ns);

    //noinspection unchecked
    SourceMetadataManager manager = new SourceMetadataManager(
        new NamespaceKey("joker"),
        modifiableSchedulerService,
        true,
        mock(LegacyKVStore.class),
        msp,
        optionManager,
        CatalogServiceMonitor.DEFAULT,
        () -> broadcaster
    );

    assertEquals(DatasetCatalog.UpdateStatus.DELETED,
        manager.refreshDataset(new NamespaceKey(""), DatasetRetrievalOptions.DEFAULT));
    assertTrue(deleted[0]);
  }

  @Test
  public void doNotDeleteUnavailableDatasetWithoutDefinition() throws Exception {
    NamespaceService ns = mock(NamespaceService.class);
    when(ns.getDataset(any())).thenReturn(new DatasetConfig()
                                                .setFullPathList(ImmutableList.of("one", "two")));

    doThrow(new IllegalStateException("should not invoke deleteDataset()"))
        .when(ns)
        .deleteDataset(any(), anyString());

    ExtendedStoragePlugin sp = mock(ExtendedStoragePlugin.class);
    when(sp.getDatasetHandle(any(), any(), any()))
        .thenReturn(Optional.empty());

    ManagedStoragePlugin.MetadataBridge msp = mock(ManagedStoragePlugin.MetadataBridge.class);
    when(msp.getMetadata())
        .thenReturn(sp);
    when(msp.getMetadataPolicy())
        .thenReturn(new MetadataPolicy().setDeleteUnavailableDatasets(false));
    when(msp.getMaxMetadataColumns())
        .thenReturn(MAX_COLUMNS);
    when(msp.getMaxNestedLevels())
      .thenReturn(MAX_NESTED_LEVELS);
    when(msp.getNamespaceService())
        .thenReturn(ns);

    //noinspection unchecked
    SourceMetadataManager manager = new SourceMetadataManager(
        new NamespaceKey("joker"),
        modifiableSchedulerService,
        true,
        mock(LegacyKVStore.class),
        msp,
        optionManager,
        CatalogServiceMonitor.DEFAULT,
        () -> broadcaster
    );

    assertEquals(DatasetCatalog.UpdateStatus.UNCHANGED,
        manager.refreshDataset(new NamespaceKey(""),
            DatasetRetrievalOptions.DEFAULT.toBuilder()
                .setDeleteUnavailableDatasets(false)
                .build()));
  }

  @Test
  public void handleUnavailableSourceDataset() throws Exception {

    NamespaceService ns = mock(NamespaceService.class);

    doThrow(new NamespaceNotFoundException(new NamespaceKey(""), "not found"))
      .when(ns)
      .getDataset(any());

    ExtendedStoragePlugin sp = mock(ExtendedStoragePlugin.class);
    when(sp.getDatasetHandle(any(), any(), any()))
      .thenReturn(Optional.empty());

    ManagedStoragePlugin.MetadataBridge msp = mock(ManagedStoragePlugin.MetadataBridge.class);
    when(msp.getMetadata())
      .thenReturn(sp);
    when(msp.getMetadataPolicy())
      .thenReturn(new MetadataPolicy().setDeleteUnavailableDatasets(false));
    when(msp.getMaxMetadataColumns())
      .thenReturn(MAX_COLUMNS);
    when(msp.getMaxNestedLevels())
      .thenReturn(MAX_NESTED_LEVELS);
    when(msp.getNamespaceService())
      .thenReturn(ns);

    SourceMetadataManager manager = new SourceMetadataManager(
      new NamespaceKey("joker"),
      modifiableSchedulerService,
      true,
      mock(LegacyKVStore.class),
      msp,
      optionManager,
      CatalogServiceMonitor.DEFAULT,
      () -> broadcaster
    );

    thrownException.expect(DatasetNotFoundException.class);
    manager.refreshDataset(new NamespaceKey("three"),
      DatasetRetrievalOptions.DEFAULT.toBuilder()
        .setForceUpdate(true)
        .setMaxMetadataLeafColumns(1)
        .build());

  }

  @Test
  public void checkForceUpdate() throws Exception {
    NamespaceService ns = mock(NamespaceService.class);
    when(ns.getDataset(any())).thenReturn(null);

    DatasetMetadataSaver saver = mock(DatasetMetadataSaver.class);
    doNothing().when(saver).saveDataset(any(), anyBoolean(), any(), any());
    when(ns.newDatasetMetadataSaver(any(), any(), any(), anyLong()))
        .thenReturn(saver);

    ExtendedStoragePlugin sp = mock(ExtendedStoragePlugin.class);
    DatasetHandle handle = () -> new EntityPath(Lists.newArrayList("one"));
    when(sp.getDatasetHandle(any(), any(), any()))
        .thenReturn(Optional.of(handle));
    when(sp.provideSignature(any(), any()))
        .thenReturn(BytesOutput.NONE);

    final boolean[] forced = new boolean[]{false};
    doAnswer(invocation -> {
      forced[0] = true;
      return DatasetMetadata.of(DatasetStats.of(0, ScanCostFactor.OTHER.getFactor()), new Schema(new ArrayList<>()));
    }).when(sp).getDatasetMetadata(any(DatasetHandle.class), any(PartitionChunkListing.class), any(), any());
    when(sp.listPartitionChunks(any(), any(), any()))
        .thenReturn(Collections::emptyIterator);
    when(sp.validateMetadata(any(), any(), any()))
        .thenReturn(SupportsReadSignature.MetadataValidity.VALID);

    ManagedStoragePlugin.MetadataBridge msp = mock(ManagedStoragePlugin.MetadataBridge.class);
    when(msp.getMetadata())
        .thenReturn(sp);
    when(msp.getMetadataPolicy())
        .thenReturn(new MetadataPolicy().setDeleteUnavailableDatasets(false));
    when(msp.getMaxMetadataColumns())
        .thenReturn(MAX_COLUMNS);
    when(msp.getMaxNestedLevels())
      .thenReturn(MAX_NESTED_LEVELS);
    when(msp.getNamespaceService()).thenReturn(ns);

    //noinspection unchecked
    SourceMetadataManager manager = new SourceMetadataManager(
        new NamespaceKey("joker"),
        modifiableSchedulerService,
        true,
        mock(LegacyKVStore.class),
        msp,
        optionManager,
        CatalogServiceMonitor.DEFAULT,
        () -> broadcaster
    );

    manager.refreshDataset(new NamespaceKey(""),
        DatasetRetrievalOptions.DEFAULT.toBuilder()
            .setForceUpdate(true)
            .build());

    assertTrue(forced[0]);
  }

  @Test
  public void dataSetPathCaseSensitivity() throws Exception {
    final String qualifier = "inspector";
    final String original = "testPath";
    final String capital = "TESTPATH";
    final ImmutableList<String> fullPathList = ImmutableList.of(qualifier, original);
    final EntityPath originalPath = new EntityPath(fullPathList);
    final EntityPath capitalPath = new EntityPath(ImmutableList.of(qualifier, capital));
    final DatasetHandle datasetHandle = () -> originalPath;
    final NamespaceKey dataSetKey = new NamespaceKey(ImmutableList.of(qualifier, capital));

    ExtendedStoragePlugin mockStoragePlugin = mock(ExtendedStoragePlugin.class);
      when(mockStoragePlugin.listDatasetHandles())
        .thenReturn(Collections::emptyIterator);
      when(mockStoragePlugin.getDatasetHandle(eq(capitalPath), any(), any()))
        .thenReturn(Optional.empty());
      when(mockStoragePlugin.getDatasetHandle(eq(originalPath), any(), any()))
        .thenReturn(Optional.of(datasetHandle));
      when(mockStoragePlugin.getState())
        .thenReturn(SourceState.GOOD);
      when(mockStoragePlugin.listPartitionChunks(any(), any(), any()))
        .thenReturn(Collections::emptyIterator);
      when(mockStoragePlugin.validateMetadata(any(), any(), any()))
        .thenReturn(SupportsReadSignature.MetadataValidity.VALID);
      when(mockStoragePlugin.provideSignature(any(), any()))
        .thenReturn(BytesOutput.NONE);
      final boolean[] forced = new boolean[]{false};
      doAnswer(invocation -> {
        forced[0] = true;
        return DatasetMetadata.of(DatasetStats.of(0, ScanCostFactor.OTHER.getFactor()), new Schema(new ArrayList<>()));
      }).when(mockStoragePlugin).getDatasetMetadata(any(DatasetHandle.class), any(PartitionChunkListing.class), any(), any());

    NamespaceService ns = mock(NamespaceService.class);
    when(ns.getDataset(any()))
      .thenReturn(MetadataObjectsUtils.newShallowConfig(datasetHandle));

    DatasetMetadataSaver saver = mock(DatasetMetadataSaver.class);
    doNothing().when(saver).saveDataset(any(), anyBoolean(), any(), any());
    when(ns.newDatasetMetadataSaver(any(), any(), any(), anyLong()))
      .thenReturn(saver);

    ManagedStoragePlugin.MetadataBridge msp = mock(ManagedStoragePlugin.MetadataBridge.class);
    when(msp.getMetadata())
      .thenReturn(mockStoragePlugin);
    when(msp.getMetadataPolicy())
      .thenReturn(new MetadataPolicy());
    when(msp.getNamespaceService())
      .thenReturn(ns);

    SourceMetadataManager manager = new SourceMetadataManager(
      dataSetKey,
      modifiableSchedulerService,
      true,
      mock(LegacyKVStore.class),
      msp,
      optionManager,
      CatalogServiceMonitor.DEFAULT,
      () -> broadcaster
    );

    assertEquals(DatasetCatalog.UpdateStatus.CHANGED,
      manager.refreshDataset(dataSetKey,
        DatasetRetrievalOptions.DEFAULT.toBuilder()
          .build())
    );
  }

  @Test
  public void exceedMaxColumnLimit() throws Exception {
    NamespaceService ns = mock(NamespaceService.class);
    when(ns.getDataset(any()))
        .thenReturn(null);

    ExtendedStoragePlugin sp = mock(ExtendedStoragePlugin.class);

    DatasetHandle handle = () -> new EntityPath(Lists.newArrayList("one"));
    when(sp.getDatasetHandle(any(), any(), any()))
        .thenReturn(Optional.of(handle));
    when(sp.listPartitionChunks(any(), any(), any()))
        .thenReturn(Collections::emptyIterator);

    when(sp.validateMetadata(any(), eq(handle), any()))
        .thenReturn(SupportsReadSignature.MetadataValidity.INVALID);
    doThrow(new ColumnCountTooLargeException(1))
        .when(sp)
        .getDatasetMetadata(eq(handle), any(PartitionChunkListing.class), any(), any());

    ManagedStoragePlugin.MetadataBridge msp = mock(ManagedStoragePlugin.MetadataBridge.class);
    when(msp.getMetadata())
        .thenReturn(sp);
    when(msp.getMetadataPolicy())
        .thenReturn(new MetadataPolicy());
    when(msp.getNamespaceService())
        .thenReturn(ns);

    //noinspection unchecked
    SourceMetadataManager manager = new SourceMetadataManager(
        new NamespaceKey("joker"),
        modifiableSchedulerService,
        true,
        mock(LegacyKVStore.class),
        msp,
        optionManager,
        CatalogServiceMonitor.DEFAULT,
        () -> broadcaster
    );

    thrownException.expect(new UserExceptionMatcher(UserBitShared.DremioPBError.ErrorType.VALIDATION,
        "exceeded the maximum number of fields of 1"));
    manager.refreshDataset(new NamespaceKey(""),
        DatasetRetrievalOptions.DEFAULT.toBuilder()
            .setForceUpdate(true)
            .setMaxMetadataLeafColumns(1)
            .build());
  }
}
