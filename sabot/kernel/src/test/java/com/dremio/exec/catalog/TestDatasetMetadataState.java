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

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.when;

import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.connector.metadata.BytesOutput;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.DatasetSplit;
import com.dremio.connector.metadata.DatasetStats;
import com.dremio.connector.metadata.PartitionChunk;
import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.VersionedDatasetHandle;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.options.OptionManager;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.IcebergMetadata;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.file.proto.FileType;
import com.dremio.service.namespace.source.proto.MetadataPolicy;
import com.dremio.service.namespace.source.proto.SourceInternalData;
import com.dremio.service.scheduler.ModifiableSchedulerService;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TestDatasetMetadataState {

  private static final String USER = "username";
  private static final List<String> TABLE = Collections.singletonList("T");

  @Mock private OptionManager optionManager;
  @Mock private StoragePluginId storagePluginId;
  @Mock private FileSystemPlugin<?> storagePlugin;
  @Mock private DatasetHandle datasetHandle;
  @Mock private VersionedDatasetHandle versionedDatasetHandle;
  @Mock private ResolvedVersionContext resolvedVersionContext;
  @Mock private DatasetMetadata datasetMetadata;
  @Mock private Schema schema;
  @Mock private Table icebergTable;
  @Mock private SchemaConfig schemaConfig;
  @Mock private MetadataRequestOptions metadataRequestOptions;
  @Mock private ManagedStoragePlugin.MetadataBridge bridge;
  @Mock private LegacyKVStore<NamespaceKey, SourceInternalData> kvStore;
  @Mock private ModifiableSchedulerService modifiableSchedulerService;
  @Mock private MetadataRefreshInfoBroadcaster broadcaster;
  @Mock private DatasetConfig datasetConfig;

  /**
   * Dataset metadata is always considered complete and up-to-date for versioned datasets. And
   * {@link DatasetMetadataState#lastRefreshTimeMillis()} should correspond to when the metadata was
   * fetched.
   */
  @Test
  public void testDatasetMetadataStateForVersionedDatasets() throws Exception {
    long ts = System.currentTimeMillis();
    final DatasetMetadataState metadataState =
        initVersionedDatasetAdapter()
            .translateIcebergTable(USER, icebergTable)
            .getDatasetMetadataState();
    assertAll(
        () -> assertTrue(metadataState.isCompleteAndValid()),
        () ->
            assertTrue(
                metadataState.lastRefreshTimeMillis().isPresent()
                    && metadataState.lastRefreshTimeMillis().get() >= ts));
  }

  /**
   * For non-Iceberg datasets, {@link SourceMetadataManager#getDatasetMetadataState} should reflect
   * when dataset was last modified.
   */
  @Test
  public void testDatasetMetadataStateForNonIcebergDatasets() {
    when(datasetConfig.getLastModified()).thenReturn(1000L);

    final SourceMetadataManager sourceMetadataManager =
        new SourceMetadataManager(
            new NamespaceKey("SOURCE"),
            modifiableSchedulerService,
            true,
            kvStore,
            bridge,
            optionManager,
            CatalogServiceMonitor.DEFAULT,
            () -> broadcaster);

    final DatasetMetadataState metadataState =
        sourceMetadataManager.getDatasetMetadataState(datasetConfig, storagePlugin);

    assertAll(
        () -> assertFalse(metadataState.isExpired()),
        () ->
            assertEquals(
                datasetConfig.getLastModified(), metadataState.lastRefreshTimeMillis().orElse(0L)));
  }

  /**
   * If no validity checks have been run on dataset yet, {@link
   * SourceMetadataManager#getDatasetMetadataState} should return an expired state.
   */
  @Test
  public void testDatasetMetadataStateWhenNoValidityCheck() {
    when(datasetConfig.getFullPathList()).thenReturn(TABLE);
    when(datasetConfig.getPhysicalDataset())
        .thenReturn(
            new PhysicalDataset()
                .setIcebergMetadata(new IcebergMetadata().setFileType(FileType.ICEBERG)));

    final SourceMetadataManager sourceMetadataManager =
        new SourceMetadataManager(
            new NamespaceKey("SOURCE"),
            modifiableSchedulerService,
            true,
            kvStore,
            bridge,
            optionManager,
            CatalogServiceMonitor.DEFAULT,
            () -> broadcaster);

    final DatasetMetadataState metadataState =
        sourceMetadataManager.getDatasetMetadataState(datasetConfig, storagePlugin);

    assertAll(
        () -> assertTrue(metadataState.isExpired()),
        () -> assertTrue(metadataState.lastRefreshTimeMillis().isEmpty()));
  }

  /**
   * If there has been a validity check run on the dataset, {@link
   * SourceMetadataManager#getDatasetMetadataState} should reflect the state of the last validity
   * check.
   */
  @Test
  public void testDatasetMetadataStateWhenValidityCheckIsPresent() {
    when(datasetConfig.getFullPathList()).thenReturn(TABLE);
    when(datasetConfig.getPhysicalDataset())
        .thenReturn(
            new PhysicalDataset()
                .setIcebergMetadata(new IcebergMetadata().setFileType(FileType.ICEBERG)));

    when(metadataRequestOptions.checkValidity()).thenReturn(true);

    MetadataPolicy metadataPolicy = new MetadataPolicy();
    metadataPolicy.setDatasetDefinitionExpireAfterMs(Long.MAX_VALUE);
    when(bridge.getMetadataPolicy()).thenReturn(metadataPolicy);

    final boolean isValid = false;
    when(storagePlugin.isMetadataValidityCheckRecentEnough(any(), any(), any())).thenReturn(false);
    when(storagePlugin.isIcebergMetadataValid(any(), any())).thenReturn(isValid);

    final SourceMetadataManager sourceMetadataManager =
        new SourceMetadataManager(
            new NamespaceKey("SOURCE"),
            modifiableSchedulerService,
            true,
            kvStore,
            bridge,
            optionManager,
            CatalogServiceMonitor.DEFAULT,
            () -> broadcaster);

    // Run validity check to update SourceMetadataManager's internal state
    final long ts = System.currentTimeMillis();
    sourceMetadataManager.isStillValid(metadataRequestOptions, datasetConfig, storagePlugin);
    final DatasetMetadataState metadataState =
        sourceMetadataManager.getDatasetMetadataState(datasetConfig, storagePlugin);

    assertAll(
        () -> assertTrue(isValid != metadataState.isExpired()),
        () -> assertTrue(metadataState.lastRefreshTimeMillis().orElse(0L) >= ts));
  }

  private VersionedDatasetAdapter initVersionedDatasetAdapter() throws Exception {
    when(resolvedVersionContext.getType()).thenReturn(ResolvedVersionContext.Type.BRANCH);
    when(resolvedVersionContext.getRefName()).thenReturn("main");

    when(versionedDatasetHandle.getType()).thenReturn(VersionedPlugin.EntityType.ICEBERG_TABLE);
    when(versionedDatasetHandle.getContentId()).thenReturn(UUID.randomUUID().toString());

    when(datasetMetadata.getRecordSchema()).thenReturn(schema);
    when(datasetMetadata.getExtraInfo()).thenReturn(BytesOutput.NONE);
    when(datasetMetadata.getDatasetStats()).thenReturn(DatasetStats.of(0.1d));

    when(storagePlugin.listPartitionChunks(any(), any()))
        .thenReturn(
            () -> Collections.singleton(PartitionChunk.of(DatasetSplit.of(0, 0))).iterator());
    when(storagePlugin.getDatasetMetadata(any(), any(), any())).thenReturn(datasetMetadata);
    when(storagePlugin.provideSignature(same(datasetHandle), isNull()))
        .thenReturn(BytesOutput.NONE);

    when(datasetHandle.unwrap(any())).thenReturn(versionedDatasetHandle);

    when(datasetConfig.getPhysicalDataset())
        .thenReturn(new PhysicalDataset().setIcebergMetadata(new IcebergMetadata()));

    return new VersionedDatasetAdapter(
        TABLE,
        resolvedVersionContext,
        storagePlugin,
        storagePluginId,
        optionManager,
        datasetHandle,
        datasetConfig);
  }
}
