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

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.TableMetadataImpl;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.SplitsPointer;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.FormatPlugin;
import com.dremio.exec.store.dfs.PhysicalDatasetUtils;
import com.dremio.io.file.Path;
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
    super(tableMetadata.getStoragePluginId(), tableMetadata.getDatasetConfig(), tableMetadata.getUser(), (SplitsPointer) tableMetadata.getSplitsKey(), tableMetadata.getPrimaryKey());
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
}
