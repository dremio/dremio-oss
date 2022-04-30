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
package com.dremio.exec.planner.sql.handlers.refresh;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;

import com.dremio.common.exceptions.UserException;
import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.DatasetSplit;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.exec.catalog.DremioCatalogReader;
import com.dremio.exec.catalog.DremioPrepareTable;
import com.dremio.exec.catalog.MaterializedSplitsPointer;
import com.dremio.exec.planner.cost.ScanCostFactor;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.parser.SqlRefreshDataset;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.exec.store.NamespaceTable;
import com.dremio.exec.store.PartitionChunkListingImpl;
import com.dremio.exec.store.SplitsPointer;
import com.dremio.exec.store.dfs.FileSelection;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.metadatarefresh.RefreshExecTableMetadata;
import com.dremio.io.file.Path;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dirlist.proto.DirListInputSplitProto;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.namespace.file.proto.FileType;
import com.dremio.service.users.SystemUser;

/**
 * FilesystemFull builds plan for filesystems in case of full refresh.
 */
public class FileSystemFullRefreshPlanBuilder extends AbstractRefreshPlanBuilder {

  public FileSystemFullRefreshPlanBuilder(SqlHandlerConfig config, SqlRefreshDataset sqlRefreshDataset, UnlimitedSplitsMetadataProvider metadataProvider) {
    super(config, sqlRefreshDataset, metadataProvider);
    FileSystemPlugin fsPlugin = (FileSystemPlugin) plugin;
    Optional<FileSelection> fileSelectionOptional;
    try {
      fileSelectionOptional = fsPlugin.generateFileSelectionForPathComponents(tableNSKey, userName);
    } catch (IOException e) {
      throw UserException.ioExceptionError(e).buildSilently();
    }
    // this is required because datasetPath created in AbstractRefreshPlanBuilder may be invalid
    // for ex. when table name has DOT character
    datasetPath = Path.of(fileSelectionOptional.orElseThrow(
        () -> UserException.invalidMetadataError().message("Table %s not found", tableNSKey).buildSilently())
      .getSelectionRoot());
    readSignatureEnabled = fsPlugin.supportReadSignature(null, isFileDataset);
    logger.debug("Doing a filesystem full refresh on dataset. Dataset's full path is {}", datasetPath);
  }

  @Override
  protected void checkAndUpdateIsFileDataset() {
    try {
      this.isFileDataset = this.plugin.createFS(datasetPath.toString(), SystemUser.SYSTEM_USERNAME, null).isFile(datasetPath);
    } catch (IOException e) {
      throw new IllegalArgumentException("Failed to parse datasetPath to File.",e);
    }
  }

  @Override
  protected DatasetConfig setupDatasetConfig() {
    datasetFileType = FileType.PARQUET;
    DatasetConfig config  = super.setupDatasetConfig();
    final FileConfig format = new FileConfig();
    format.setType(datasetFileType);
    config.getPhysicalDataset().setFormatSettings(format);
    checkAndUpdateIsFileDataset();
    DatasetType datasetType = super.isFileDataset ? DatasetType.PHYSICAL_DATASET_SOURCE_FILE: DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER;
    config.setType(datasetType);
    config.getReadDefinition().getScanStats().setScanFactor(ScanCostFactor.PARQUET.getFactor());
    return config;
  }

  @Override
  public PartitionChunkListing listPartitionChunks(DatasetRetrievalOptions datasetRetrievalOptions) throws ConnectorException {
    DirListInputSplitProto.DirListInputSplit dirListInputSplit = DirListInputSplitProto.DirListInputSplit.newBuilder()
      .setRootPath(datasetPath.toString())
      .setOperatingPath(datasetPath.toString())
      .setReadSignature(Long.MAX_VALUE)
      .setIsFile(super.isFileDataset)
      .build();

    DatasetSplit split = DatasetSplit.of(Collections.emptyList(), 1, 1, dirListInputSplit::writeTo);
    PartitionChunkListingImpl partitionChunkListing = new PartitionChunkListingImpl();
    partitionChunkListing.put(Collections.emptyList(), split);
    partitionChunkListing.computePartitionChunks();
    return partitionChunkListing;
  }

  @Override
  public void setupMetadataForPlanning(PartitionChunkListing partitionChunkListing, DatasetRetrievalOptions retrievalOptions) {
    tableSchema = metadataProvider.getTableSchema();
    partitionCols = metadataProvider.getPartitionColumns();

    SplitsPointer splitsPointer = MaterializedSplitsPointer.of(0, convertToPartitionChunkMetadata(partitionChunkListing, datasetConfig), 1);

    refreshExecTableMetadata = new RefreshExecTableMetadata(storagePluginId, datasetConfig, userName, splitsPointer, tableSchema);
    final NamespaceTable nsTable = new NamespaceTable(refreshExecTableMetadata, true);
    final DremioCatalogReader catalogReader = config.getConverter().getCatalogReader();
    this.table = new DremioPrepareTable(catalogReader, JavaTypeFactoryImpl.INSTANCE, nsTable);
  }

  @Override
  public boolean updateDatasetConfigWithIcebergMetadataIfNecessary() {
    return false;
  }
}
