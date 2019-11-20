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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
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
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.stubbing.Answer;

import com.dremio.connector.metadata.BytesOutput;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.DatasetStats;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.connector.metadata.extensions.SupportsReadSignature;
import com.dremio.datastore.KVStore;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.options.OptionManager;
import com.dremio.service.namespace.DatasetMetadataSaver;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.dremio.service.namespace.source.proto.MetadataPolicy;
import com.dremio.service.scheduler.SchedulerService;
import com.dremio.test.UserExceptionMatcher;
import com.google.common.collect.Lists;

public class TestSourceMetadataManager {
  private static final int MAX_COLUMNS = 800;
  private OptionManager optionManager;

  @Rule
  public final ExpectedException thrownException = ExpectedException.none();

  @Before
  public void setup() {
    optionManager = mock(OptionManager.class);
    when(optionManager.getOption(eq(CatalogOptions.SPLIT_COMPRESSION_TYPE)))
      .thenAnswer((Answer) invocation -> NamespaceService.SplitCompression.SNAPPY.toString());
  }

  @Test
  public void deleteUnavailableDataset() throws Exception {
    NamespaceService ns = mock(NamespaceService.class);
    when(ns.getDataset(any()))
        .thenReturn(
            new DatasetConfig()
                .setTag("0")
                .setReadDefinition(new ReadDefinition())
        );

    boolean[] deleted = new boolean[] {false};
    doAnswer(invocation -> {
      deleted[0] = true;
      return null;
    }).when(ns).deleteDataset(any(), anyString());

    ExtendedStoragePlugin sp = mock(ExtendedStoragePlugin.class);
    when(sp.getDatasetHandle(any(), any()))
        .thenReturn(Optional.empty());

    ManagedStoragePlugin.MetadataBridge msp = mock(ManagedStoragePlugin.MetadataBridge.class);
    when(msp.getMetadata())
        .thenReturn(sp);
    when(msp.getMetadataPolicy())
        .thenReturn(new MetadataPolicy().setDeleteUnavailableDatasets(false));
    when(msp.getMaxMetadataColumns())
      .thenReturn(MAX_COLUMNS);
    when(msp.getNamespaceService())
      .thenReturn(ns);

    //noinspection unchecked
    SourceMetadataManager manager = new SourceMetadataManager(
        new NamespaceKey("joker"),
        mock(SchedulerService.class),
        true,
        mock(KVStore.class),
        msp,
        optionManager,
        CatalogServiceMonitor.DEFAULT);

    assertEquals(Catalog.UpdateStatus.DELETED,
        manager.refreshDataset(new NamespaceKey(""), DatasetRetrievalOptions.DEFAULT));
    assertTrue(deleted[0]);
  }

  @Test
  public void doNotDeleteUnavailableDataset() throws Exception {
    NamespaceService ns = mock(NamespaceService.class);
    when(ns.getDataset(any()))
        .thenReturn(new DatasetConfig().setReadDefinition(new ReadDefinition()));

    doThrow(new IllegalStateException("should not invoke deleteDataset()"))
        .when(ns)
        .deleteDataset(any(), anyString());

    ExtendedStoragePlugin sp = mock(ExtendedStoragePlugin.class);
    when(sp.getDatasetHandle(any(), any()))
        .thenReturn(Optional.empty());

    ManagedStoragePlugin.MetadataBridge msp = mock(ManagedStoragePlugin.MetadataBridge.class);
    when(msp.getMetadata())
        .thenReturn(sp);
    when(msp.getMetadataPolicy())
        .thenReturn(new MetadataPolicy().setDeleteUnavailableDatasets(false));
    when(msp.getMaxMetadataColumns())
      .thenReturn(MAX_COLUMNS);
    when(msp.getNamespaceService())
      .thenReturn(ns);

    //noinspection unchecked
    SourceMetadataManager manager = new SourceMetadataManager(
        new NamespaceKey("joker"),
        mock(SchedulerService.class),
        true,
        mock(KVStore.class),
        msp,
        optionManager,
        CatalogServiceMonitor.DEFAULT);


    assertEquals(Catalog.UpdateStatus.UNCHANGED,
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
        );

    boolean[] deleted = new boolean[] {false};
    doAnswer(invocation -> {
      deleted[0] = true;
      return null;
    }).when(ns).deleteDataset(any(), anyString());

    ExtendedStoragePlugin sp = mock(ExtendedStoragePlugin.class);
    when(sp.getDatasetHandle(any(), any()))
        .thenReturn(Optional.empty());

    ManagedStoragePlugin.MetadataBridge msp = mock(ManagedStoragePlugin.MetadataBridge.class);
    when(msp.getMetadata())
        .thenReturn(sp);
    when(msp.getMetadataPolicy())
        .thenReturn(new MetadataPolicy().setDeleteUnavailableDatasets(false));
    when(msp.getMaxMetadataColumns())
      .thenReturn(MAX_COLUMNS);
    when(msp.getNamespaceService())
      .thenReturn(ns);

    //noinspection unchecked
    SourceMetadataManager manager = new SourceMetadataManager(
        new NamespaceKey("joker"),
        mock(SchedulerService.class),
        true,
        mock(KVStore.class),
        msp,
        optionManager,
        CatalogServiceMonitor.DEFAULT);

    assertEquals(Catalog.UpdateStatus.DELETED,
        manager.refreshDataset(new NamespaceKey(""), DatasetRetrievalOptions.DEFAULT));
    assertTrue(deleted[0]);
  }

  @Test
  public void doNotDeleteUnavailableDatasetWithoutDefinition() throws Exception {
    NamespaceService ns = mock(NamespaceService.class);
    when(ns.getDataset(any())).thenReturn(new DatasetConfig());

    doThrow(new IllegalStateException("should not invoke deleteDataset()"))
        .when(ns)
        .deleteDataset(any(), anyString());

    ExtendedStoragePlugin sp = mock(ExtendedStoragePlugin.class);
    when(sp.getDatasetHandle(any(), any()))
        .thenReturn(Optional.empty());

    ManagedStoragePlugin.MetadataBridge msp = mock(ManagedStoragePlugin.MetadataBridge.class);
    when(msp.getMetadata())
        .thenReturn(sp);
    when(msp.getMetadataPolicy())
        .thenReturn(new MetadataPolicy().setDeleteUnavailableDatasets(false));
    when(msp.getMaxMetadataColumns())
        .thenReturn(MAX_COLUMNS);
    when(msp.getNamespaceService())
        .thenReturn(ns);

    //noinspection unchecked
    SourceMetadataManager manager = new SourceMetadataManager(
        new NamespaceKey("joker"),
        mock(SchedulerService.class),
        true,
        mock(KVStore.class),
        msp,
        optionManager,
        CatalogServiceMonitor.DEFAULT);

    assertEquals(Catalog.UpdateStatus.UNCHANGED,
        manager.refreshDataset(new NamespaceKey(""),
            DatasetRetrievalOptions.DEFAULT.toBuilder()
                .setDeleteUnavailableDatasets(false)
                .build()));
  }

  @Test
  public void checkForceUpdate() throws Exception {
    NamespaceService ns = mock(NamespaceService.class);
    when(ns.getDataset(any())).thenReturn(null);

    DatasetMetadataSaver saver = mock(DatasetMetadataSaver.class);
    doNothing().when(saver).saveDataset(any(), anyBoolean(), any());
    when(ns.newDatasetMetadataSaver(any(), any(), any()))
        .thenReturn(saver);

    ExtendedStoragePlugin sp = mock(ExtendedStoragePlugin.class);
    DatasetHandle handle = () -> new EntityPath(Lists.newArrayList("one"));
    when(sp.getDatasetHandle(any(), any()))
        .thenReturn(Optional.of(handle));
    when(sp.provideSignature(any(), any()))
        .thenReturn(BytesOutput.NONE);

    final boolean[] forced = new boolean[]{false};
    doAnswer(invocation -> {
      forced[0] = true;
      return DatasetMetadata.of(DatasetStats.of(0, 0), new Schema(new ArrayList<>()));
    }).when(sp).getDatasetMetadata(any(DatasetHandle.class), any(PartitionChunkListing.class), any());
    when(sp.listPartitionChunks(any(), any()))
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
    when(msp.getNamespaceService()).thenReturn(ns);

    //noinspection unchecked
    SourceMetadataManager manager = new SourceMetadataManager(
        new NamespaceKey("joker"),
        mock(SchedulerService.class),
        true,
        mock(KVStore.class),
        msp,
        optionManager,
        CatalogServiceMonitor.DEFAULT
    );

    manager.refreshDataset(new NamespaceKey(""),
        DatasetRetrievalOptions.DEFAULT.toBuilder()
            .setForceUpdate(true)
            .build());

    assertTrue(forced[0]);
  }

  @Test
  public void exceedMaxColumnLimit() throws Exception {
    NamespaceService ns = mock(NamespaceService.class);
    when(ns.getDataset(any()))
        .thenReturn(null);

    ExtendedStoragePlugin sp = mock(ExtendedStoragePlugin.class);

    DatasetHandle handle = () -> new EntityPath(Lists.newArrayList("one"));
    when(sp.getDatasetHandle(any(), any()))
        .thenReturn(Optional.of(handle));
    when(sp.listPartitionChunks(any(), any()))
        .thenReturn(Collections::emptyIterator);

    when(sp.validateMetadata(any(), eq(handle), any()))
        .thenReturn(SupportsReadSignature.MetadataValidity.INVALID);
    doThrow(new ColumnCountTooLargeException(1))
        .when(sp)
        .getDatasetMetadata(eq(handle), any(PartitionChunkListing.class), any());

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
        mock(SchedulerService.class),
        true,
        mock(KVStore.class),
        msp,
        optionManager,
        CatalogServiceMonitor.DEFAULT
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
