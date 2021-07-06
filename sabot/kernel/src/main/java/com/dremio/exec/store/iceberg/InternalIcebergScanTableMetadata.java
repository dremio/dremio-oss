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
package com.dremio.exec.store.iceberg;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.dremio.connector.metadata.DatasetSplit;
import com.dremio.connector.metadata.PartitionChunk;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.TableMetadataImpl;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.SplitsPointer;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.dfs.FileSelection;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.io.file.Path;
import com.dremio.service.namespace.MetadataProtoUtils;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

/**
 * Internal IcebergScan table metadata, which extends TableMetadataImpl.
 * Contains table metadata information of Internal Iceberg table created from source table by Dremio.
 */
public class InternalIcebergScanTableMetadata extends TableMetadataImpl {

  private final FileSystemPlugin icebergTableStoragePlugin;
  private final IcebergExecutionDatasetAccessor datasetAccessor;

  public InternalIcebergScanTableMetadata(TableMetadata tableMetadata, FileSystemPlugin icebergTableStoragePlugin) {
    // TODO: DX-31785: Use correct Iceberg table mapping name instead of the table name
    this(tableMetadata, icebergTableStoragePlugin, tableMetadata.getDatasetConfig().getName());
  }

  public InternalIcebergScanTableMetadata(TableMetadata tableMetadata, FileSystemPlugin icebergTableStoragePlugin, String tableID) {
    super(tableMetadata.getStoragePluginId(), tableMetadata.getDatasetConfig(), tableMetadata.getUser(), (SplitsPointer) tableMetadata.getSplitsKey());
    Preconditions.checkNotNull(icebergTableStoragePlugin);
    Preconditions.checkNotNull(tableID, "tableID is required");
    this.icebergTableStoragePlugin = icebergTableStoragePlugin;
    this.datasetAccessor = deriveInternalIcebergTableDatasetAccessor(icebergTableStoragePlugin, tableMetadata.getDatasetConfig(), tableID, tableMetadata.getUser());
  }

  @Override
  public FileConfig getFormatSettings() {
    return datasetAccessor.getFileConfig();
  }

  public StoragePluginId getIcebergTableStoragePlugin() {
    return icebergTableStoragePlugin.getId();
  }

  private static IcebergExecutionDatasetAccessor deriveInternalIcebergTableDatasetAccessor(FileSystemPlugin plugin, DatasetConfig datasetConfig, String tableID, String user) {
    final IcebergFormatPlugin formatPlugin = (IcebergFormatPlugin) plugin.getFormatPlugin(new IcebergFormatConfig());
    Preconditions.checkNotNull(formatPlugin, "Unable to load format plugin for provided format config.");
    try {
      FileSelection fileSelection = generateFileSelection(plugin, tableID);
      NamespaceKey tableSchemaPath = new NamespaceKey(datasetConfig.getFullPathList());
      return new IcebergExecutionDatasetAccessor(datasetConfig.getType(), plugin.createFS(user), formatPlugin, fileSelection, plugin, tableSchemaPath);
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private static FileSelection generateFileSelection(FileSystemPlugin plugin, String tableID) throws IOException {
    Path icebergTablePath = Path.of(plugin.getConfig().getPath().toString()).resolve(tableID);
    FileSelection fileSelection = FileSelection.create(plugin.getSystemUserFS(), icebergTablePath);
    if (fileSelection == null) {
      throw new IllegalStateException("Unable to retrieve selection for path." + icebergTablePath);
    }
    return fileSelection;
  }

  public List<SplitAndPartitionInfo> getSplitAndPartitionInfo() {
    final List<SplitAndPartitionInfo> splits = new ArrayList<>();
    Iterator<? extends PartitionChunk> chunks = datasetAccessor.listPartitionChunks().iterator();
    while (chunks.hasNext()) {
      final com.dremio.connector.metadata.PartitionChunk chunk = chunks.next();
      final Iterator<? extends DatasetSplit> chunkSplits = chunk.getSplits().iterator();
      while (chunkSplits.hasNext()) {
        final DatasetSplit datasetSplit = chunkSplits.next();
        PartitionProtobuf.NormalizedPartitionInfo partitionInfo = PartitionProtobuf.NormalizedPartitionInfo.newBuilder().setId(String.valueOf(1)).build();
        PartitionProtobuf.NormalizedDatasetSplitInfo.Builder splitInfo = PartitionProtobuf.NormalizedDatasetSplitInfo
          .newBuilder()
          .setPartitionId(partitionInfo.getId())
          .setExtendedProperty(MetadataProtoUtils.toProtobuf(datasetSplit.getExtraInfo()));
        splits.add(new SplitAndPartitionInfo(partitionInfo, splitInfo.build()));
      }
    }
    return splits;
  }
}
