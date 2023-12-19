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

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.iceberg.viewdepoc.Version;
import org.apache.iceberg.viewdepoc.ViewDefinition;
import org.apache.iceberg.viewdepoc.ViewVersionMetadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.BytesOutput;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.DatasetSplit;
import com.dremio.connector.metadata.DatasetStats;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.PartitionChunk;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.VersionedDatasetHandle;
import com.dremio.exec.store.iceberg.ViewHandle;
import com.dremio.options.OptionManager;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.IcebergMetadata;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.dataset.proto.VirtualDataset;

@ExtendWith(MockitoExtension.class)
public class TestVersionedDatasetAdapter {

  @Mock
  private OptionManager optionManager;

  @Test
  public void testTranslateIcebergTableOwnerWithFlagOff() throws ConnectorException {
    when(optionManager.getOption(CatalogOptions.VERSIONED_SOURCE_VIEW_DELEGATION_ENABLED)).thenReturn(false);
    assertNull(getVersionedDatasetAdapterForTable()
      .translateIcebergTable("user").getDatasetConfig().getOwner());
  }

  @Test
  public void testTranslateIcebergTableOwner() throws ConnectorException {
    when(optionManager.getOption(CatalogOptions.VERSIONED_SOURCE_VIEW_DELEGATION_ENABLED)).thenReturn(true);
    assertNull(getVersionedDatasetAdapterForTable()
      .translateIcebergTable("user").getDatasetConfig().getOwner());
  }

  private VersionedDatasetAdapter getVersionedDatasetAdapterForTable() throws ConnectorException {
    List<String> tableKey = Arrays.asList("Arctic", "mytable");

    ResolvedVersionContext resolvedVersionContext = mock(ResolvedVersionContext.class);
    when(resolvedVersionContext.getType()).thenReturn(ResolvedVersionContext.Type.BRANCH);
    when(resolvedVersionContext.getRefName()).thenReturn("mybranch");

    VersionedDatasetHandle versionedDatasetHandle = mock(VersionedDatasetHandle.class);
    when(versionedDatasetHandle.getType()).thenReturn(VersionedPlugin.EntityType.ICEBERG_TABLE);
    when(versionedDatasetHandle.getContentId()).thenReturn(UUID.randomUUID().toString());

    DatasetMetadata datasetMetadata = mock(DatasetMetadata.class);
    when(datasetMetadata.getRecordSchema()).thenReturn(mock(Schema.class));
    when(datasetMetadata.getExtraInfo()).thenReturn(BytesOutput.NONE);
    when(datasetMetadata.getDatasetStats()).thenReturn(DatasetStats.of(0.1d));

    StoragePlugin storagePlugin = mock(StoragePlugin.class);
    when(storagePlugin.listPartitionChunks(any(), any())).thenReturn(() ->
      Collections.singleton(PartitionChunk.of(DatasetSplit.of(0,0))).iterator());
    when(storagePlugin.getDatasetMetadata(any(), any(), any())).thenReturn(datasetMetadata);

    DatasetHandle datasetHandle = mock(DatasetHandle.class);
    when(datasetHandle.unwrap(any())).thenReturn(versionedDatasetHandle);

    return new VersionedDatasetAdapter(
      tableKey,
      resolvedVersionContext,
      storagePlugin,
      mock(StoragePluginId.class),
      optionManager,
      datasetHandle,
      new DatasetConfig()
        .setPhysicalDataset(new PhysicalDataset().setIcebergMetadata(new IcebergMetadata()))
        .setFullPathList(tableKey));
  }

  @Test
  public void testTranslateIcebergViewOwnerWithFlagOff() throws ConnectorException {
    when(optionManager.getOption(CatalogOptions.VERSIONED_SOURCE_VIEW_DELEGATION_ENABLED)).thenReturn(false);
    ViewTable viewTable = (ViewTable) getVersionedDatasetAdapterForView()
      .translateIcebergView("user");
    assertNull(viewTable.getViewOwner());
    assertNull(viewTable.getDatasetConfig().getOwner());
  }

  @Test
  public void testTranslateIcebergViewOwner() throws ConnectorException {
    when(optionManager.getOption(CatalogOptions.VERSIONED_SOURCE_VIEW_DELEGATION_ENABLED)).thenReturn(true);
    ViewTable viewTable = (ViewTable) getVersionedDatasetAdapterForView()
      .translateIcebergView("user");
    assertNull(viewTable.getViewOwner());
    assertNull(viewTable.getDatasetConfig().getOwner());
  }

  private VersionedDatasetAdapter getVersionedDatasetAdapterForView() throws ConnectorException {
    when(optionManager.getOption(ExecConstants.ENABLE_MAP_DATA_TYPE)).thenReturn(true);

    List<String> tableKey = Arrays.asList("Arctic", "myview");

    ResolvedVersionContext resolvedVersionContext = mock(ResolvedVersionContext.class);
    when(resolvedVersionContext.getType()).thenReturn(ResolvedVersionContext.Type.BRANCH);
    when(resolvedVersionContext.getRefName()).thenReturn("mybranch");

    ViewDefinition viewDefinition = mock(ViewDefinition.class);
    when(viewDefinition.schema()).thenReturn(new org.apache.iceberg.Schema());

    ViewVersionMetadata viewVersionMetadata = mock(ViewVersionMetadata.class);
    when(viewVersionMetadata.definition()).thenReturn(viewDefinition);
    when(viewVersionMetadata.currentVersion()).thenReturn(mock(Version.class));

    ViewHandle viewHandle = mock(ViewHandle.class);
    when(viewHandle.getType()).thenReturn(VersionedPlugin.EntityType.ICEBERG_VIEW);
    when(viewHandle.getContentId()).thenReturn(UUID.randomUUID().toString());
    when(viewHandle.getDatasetPath()).thenReturn(new EntityPath(tableKey));
    when(viewHandle.getViewVersionMetadata()).thenReturn(viewVersionMetadata);

    DatasetHandle datasetHandle = mock(DatasetHandle.class);
    when(datasetHandle.unwrap(any())).thenReturn(viewHandle);

    return new VersionedDatasetAdapter(
      tableKey,
      resolvedVersionContext,
      mock(StoragePlugin.class),
      mock(StoragePluginId.class),
      optionManager,
      datasetHandle,
      new DatasetConfig().setVirtualDataset(new VirtualDataset()));
  }
}
