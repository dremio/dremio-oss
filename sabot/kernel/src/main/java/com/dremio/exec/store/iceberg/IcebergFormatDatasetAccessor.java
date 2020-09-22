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
import java.util.Collections;
import java.util.List;

import org.apache.arrow.vector.types.pojo.Schema;

import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.BytesOutput;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.DatasetStats;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.GetMetadataOption;
import com.dremio.connector.metadata.ListPartitionChunkOption;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.FileConfigMetadata;
import com.dremio.exec.catalog.MetadataObjectsUtils;
import com.dremio.exec.planner.cost.ScanCostFactor;
import com.dremio.exec.store.dfs.FileDatasetHandle;
import com.dremio.exec.store.dfs.FileSelection;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.FormatPlugin;
import com.dremio.exec.store.dfs.PhysicalDatasetUtils;
import com.dremio.exec.store.dfs.PreviousDatasetInfo;
import com.dremio.io.file.FileSystem;
import com.dremio.options.Options;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.google.common.base.Throwables;

@Options
public class IcebergFormatDatasetAccessor implements FileDatasetHandle {
  private final FileSystem fs;
  private final DatasetType type;
  private final NamespaceKey tableSchemaPath;
  private final IcebergTableWrapper tableWrapper;
  private final PreviousDatasetInfo oldConfig;
  private final FileSelection fileSelection;
  private final FormatPlugin formatPlugin;

  public IcebergFormatDatasetAccessor(DatasetType type,
    FileSystem fs,
    FileSelection fileSelection,
    NamespaceKey tableSchemaPath,
    FormatPlugin formatPlugin,
    FileSystemPlugin<?> fsPlugin,
    PreviousDatasetInfo oldConfig,
    int maxLeafColumns) {

    this.type = type;
    this.fs = fs;
    this.fileSelection = fileSelection;
    this.tableSchemaPath = tableSchemaPath;
    this.formatPlugin = formatPlugin;
    this.oldConfig = oldConfig;
    this.tableWrapper = new IcebergTableWrapper(formatPlugin.getContext(), fs,
      fsPlugin.getFsConfCopy(), fileSelection.getSelectionRoot());
  }

  @Override
  public EntityPath getDatasetPath() {
    return MetadataObjectsUtils.toEntityPath(tableSchemaPath);
  }

  @Override
  public DatasetType getDatasetType() {
    return type;
  }

  @Override
  public DatasetMetadata getDatasetMetadata(GetMetadataOption... options) throws ConnectorException {
    final IcebergTableInfo tableInfo;

    if (!formatPlugin.getContext().getOptionManager().getOption(ExecConstants.ENABLE_ICEBERG)) {
      throw new UnsupportedOperationException("Please contact customer support for steps to enable " +
        "the iceberg tables feature.");
    }

    try {
      tableInfo = this.tableWrapper.getTableInfo();
    } catch (Exception ex) {
      Throwables.propagateIfPossible(ex, ConnectorException.class);
      throw new ConnectorException(ex);
    }

    return new FileConfigMetadata() {

      @Override
      public DatasetStats getDatasetStats() {
        return DatasetStats.of(tableInfo.getRecordCount(), true, ScanCostFactor.PARQUET.getFactor());
      }

      @Override
      public List<String> getSortColumns() {
        if (oldConfig == null) {
          return Collections.emptyList();
        }

        return oldConfig.getSortColumns() == null ? Collections.emptyList() : oldConfig.getSortColumns();
      }

      @Override
      public List<String> getPartitionColumns() {
        return tableInfo.getPartitionColumns();
      }

      @Override
      public BytesOutput getExtraInfo() {
        return tableInfo.getExtraInfo();
      }

      @Override
      public Schema getRecordSchema() {
        return tableInfo.getBatchSchema();
      }

      @Override
      public FileConfig getFileConfig() {
        return PhysicalDatasetUtils.toFileFormat(formatPlugin).asFileConfig().setLocation(fileSelection.getSelectionRoot());
      }
    };
  }

  @Override
  public PartitionChunkListing listPartitionChunks(ListPartitionChunkOption... options) throws ConnectorException {
    try {
      final IcebergTableInfo tableInfo = tableWrapper.getTableInfo();
      return tableInfo.getPartitionChunkListing();
    } catch (IOException e) {
      Throwables.propagateIfPossible(e, ConnectorException.class);
      throw new ConnectorException(e);
    }
  }

  @Override
  public BytesOutput provideSignature(DatasetMetadata metadata) throws ConnectorException {
    try {
      final IcebergTableInfo tableInfo = tableWrapper.getTableInfo();
      return tableInfo.provideSignature();
    } catch (IOException e) {
      Throwables.propagateIfPossible(e, ConnectorException.class);
      throw new ConnectorException(e);
    }
  }
}
