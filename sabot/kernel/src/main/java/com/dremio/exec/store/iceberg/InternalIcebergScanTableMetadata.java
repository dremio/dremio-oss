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

import java.util.ArrayList;
import java.util.List;

import com.dremio.connector.metadata.DatasetSplit;
import com.dremio.connector.metadata.DatasetSplitAffinity;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.TableMetadataImpl;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.SplitsPointer;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.FormatPlugin;
import com.dremio.exec.store.dfs.PhysicalDatasetUtils;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.store.easy.proto.EasyProtobuf;
import com.dremio.service.namespace.MetadataProtoUtils;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.google.common.base.Preconditions;

/**
 * Internal IcebergScan table metadata, which extends TableMetadataImpl.
 * Contains table metadata information of Internal Iceberg table created from source table by Dremio.
 */
public class InternalIcebergScanTableMetadata extends TableMetadataImpl {

  private final FileSystemPlugin<?> icebergTableStoragePlugin;
  private final BatchSchema schema;
  private final FormatPlugin formatPlugin;
  private final String tableName;

  public InternalIcebergScanTableMetadata(TableMetadata tableMetadata, FileSystemPlugin<?> icebergTableStoragePlugin, String tableName) {
    super(tableMetadata.getStoragePluginId(), tableMetadata.getDatasetConfig(), tableMetadata.getUser(), (SplitsPointer) tableMetadata.getSplitsKey());
    Preconditions.checkNotNull(icebergTableStoragePlugin);
    Preconditions.checkNotNull(tableName, "tableName is required");
    this.icebergTableStoragePlugin = icebergTableStoragePlugin;
    this.schema = tableMetadata.getSchema();
    this.tableName = tableName;
    formatPlugin = icebergTableStoragePlugin.getFormatPlugin(new IcebergFormatConfig());
    Preconditions.checkNotNull(formatPlugin, "Unable to load format plugin for provided format config.");
  }

  @Override
  public FileConfig getFormatSettings() {
    Path icebergTablePath = Path.of(icebergTableStoragePlugin.getConfig().getPath().toString()).resolve(tableName);
    return PhysicalDatasetUtils.toFileFormat(formatPlugin).asFileConfig().setLocation(Path.withoutSchemeAndAuthority(icebergTablePath).toString());
  }

  @Override
  public BatchSchema getSchema() {
    return schema;
  }

  public StoragePluginId getIcebergTableStoragePlugin() {
    return icebergTableStoragePlugin.getId();
  }

  public List<SplitAndPartitionInfo> getSplitAndPartitionInfo() {
    final List<SplitAndPartitionInfo> splits = new ArrayList<>();
    String splitPath = getDatasetConfig().getPhysicalDataset().getIcebergMetadata().getMetadataFileLocation();
    splitPath = Path.getContainerSpecificRelativePath(Path.of(splitPath));
    EasyProtobuf.EasyDatasetSplitXAttr splitExtended = EasyProtobuf.EasyDatasetSplitXAttr.newBuilder()
            .setPath(splitPath)
            .setStart(0)
            .setLength(0)
            .setUpdateKey(com.dremio.exec.store.file.proto.FileProtobuf.FileSystemCachedEntity.newBuilder()
                    .setPath(splitPath)
                    .setLastModificationTime(0))
            .build();
    List<DatasetSplitAffinity> splitAffinities = new ArrayList<>();
    DatasetSplit datasetSplit = DatasetSplit.of(
            splitAffinities, 0, 0, splitExtended::writeTo);

    PartitionProtobuf.NormalizedPartitionInfo partitionInfo = PartitionProtobuf.NormalizedPartitionInfo.newBuilder().setId(String.valueOf(1)).build();
    PartitionProtobuf.NormalizedDatasetSplitInfo.Builder splitInfo = PartitionProtobuf.NormalizedDatasetSplitInfo
      .newBuilder()
      .setPartitionId(partitionInfo.getId())
      .setExtendedProperty(MetadataProtoUtils.toProtobuf(datasetSplit.getExtraInfo()));
    splits.add(new SplitAndPartitionInfo(partitionInfo, splitInfo.build()));
    return splits;
  }
}
