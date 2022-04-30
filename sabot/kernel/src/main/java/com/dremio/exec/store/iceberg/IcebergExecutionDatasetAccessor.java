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
import java.util.function.Supplier;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;

import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.BytesOutput;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.exec.catalog.MutablePlugin;
import com.dremio.exec.store.dfs.FormatPlugin;
import com.dremio.exec.store.dfs.PhysicalDatasetUtils;
import com.dremio.exec.store.file.proto.FileProtobuf;
import com.dremio.io.file.FileAttributes;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.google.common.base.Throwables;

/**
 * Dataset handle for Iceberg format that supports Iceberg execution model
 */
public class IcebergExecutionDatasetAccessor extends BaseIcebergExecutionDatasetAccessor {

  private final Supplier<Table> tableSupplier;
  private final FormatPlugin formatPlugin;
  private final FileSystem fs;

  public IcebergExecutionDatasetAccessor(
      EntityPath datasetPath,
      Supplier<Table> tableSupplier,
      Configuration configuration,
      FormatPlugin formatPlugin,
      FileSystem fs,
      TableSnapshotProvider tableSnapshotProvider,
      MutablePlugin plugin
  ) {
    super(datasetPath, tableSupplier, configuration, tableSnapshotProvider, plugin);
    this.tableSupplier = tableSupplier;
    this.fs = fs;
    this.formatPlugin = formatPlugin;
  }

  private String getTableLocation() {
    return tableSupplier.get().location();
  }

  @Override
  protected FileConfig getFileConfig() {
    return PhysicalDatasetUtils.toFileFormat(formatPlugin)
      .asFileConfig()
      .setLocation(getTableLocation());
  }

  @Override
  protected String getMetadataLocation() {
    String metadataLocation = super.getMetadataLocation();
    metadataLocation = Path.getContainerSpecificRelativePath(Path.of(metadataLocation));
    return metadataLocation;
  }

  @Override
  public BytesOutput provideSignature(DatasetMetadata metadata) throws ConnectorException {
    try {
      Path metaDir = Path.of(getTableLocation()).resolve(IcebergFormatMatcher.METADATA_DIR_NAME);
      metaDir = Path.of(Path.getContainerSpecificRelativePath(metaDir));
      if (!fs.exists(metaDir) || !fs.isDirectory(metaDir)) {
        throw new IllegalStateException("missing metadata dir for iceberg table");
      }

      final FileAttributes attributes = fs.getFileAttributes(metaDir);
      final FileProtobuf.FileSystemCachedEntity cachedEntity = FileProtobuf.FileSystemCachedEntity
        .newBuilder()
        .setPath(metaDir.toString())
        .setLastModificationTime(attributes.lastModifiedTime().toMillis())
        .setLength(attributes.size())
        .build();

      return FileProtobuf.FileUpdateKey
        .newBuilder()
        .addCachedEntities(cachedEntity)
        .build()::writeTo;
    }
    catch (IOException ioe) {
      Throwables.propagateIfPossible(ioe, ConnectorException.class);
      throw new ConnectorException(ioe);
    }
  }

}
