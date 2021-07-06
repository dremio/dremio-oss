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
package com.dremio.exec.store.iceberg.model;

import java.util.ArrayList;
import java.util.List;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.metadatarefresh.DatasetCatalogGrpcClient;
import com.dremio.exec.store.metadatarefresh.DatasetCatalogRequestBuilder;
import com.google.common.base.Preconditions;

/**
 * IcebergMetadataRefreshCommitter this committer has two update operation
 * DELETE Followed by INSERT
 */
public class IncrementalMetadataRefreshCommitter implements IcebergOpCommitter {

  private static final Logger logger = LoggerFactory.getLogger(IncrementalMetadataRefreshCommitter.class);

  private final String tableName;
  private final IcebergCommand icebergCommand;
  private final DatasetCatalogGrpcClient client;
  private List<ManifestFile> manifestFileList = new ArrayList<>();
  private List<DataFile> deleteDataFilesList = new ArrayList<>();
  private final DatasetCatalogRequestBuilder datasetCatalogRequestBuilder;

  public IncrementalMetadataRefreshCommitter(String tableName, String tableLocation, BatchSchema batchSchema, List<String> partitionColumnNames,
                                             IcebergCommand icebergCommand, DatasetCatalogGrpcClient datasetCatalogGrpcClient) {
    Preconditions.checkState(icebergCommand != null, "Unexpected state");
    Preconditions.checkNotNull(datasetCatalogGrpcClient, "Unexpected state: DatasetCatalogService client not provided");
    this.icebergCommand = icebergCommand;
    this.tableName = tableName;

    this.client = datasetCatalogGrpcClient;

    datasetCatalogRequestBuilder = DatasetCatalogRequestBuilder.forIncrementalMetadataRefresh (tableName,
      tableLocation,
      batchSchema,
      partitionColumnNames,
      datasetCatalogGrpcClient);

  }

  @Override
  public void commit() {
    this.icebergCommand.beginMetadataRefreshTransaction();

    if (deleteDataFilesList.size() > 0) {
      icebergCommand.beginDelete();
      icebergCommand.consumeDeleteDataFiles(deleteDataFilesList);
      icebergCommand.finishDelete();
    }
    if (manifestFileList.size() > 0) {
      icebergCommand.beginInsert();
      icebergCommand.consumeManifestFiles(manifestFileList);
      icebergCommand.finishInsert();
    }
    icebergCommand.endMetadataRefreshTransaction();

    logger.debug("Committed incremental metadata change of table {}. Updating Dataset Catalog store", tableName);
    client.getCatalogServiceApi().addOrUpdateDataset(datasetCatalogRequestBuilder.build());
  }

  @Override
  public void consumeManifestFile(ManifestFile manifestFile) {
    manifestFileList.add(manifestFile);
    datasetCatalogRequestBuilder.addManifest(manifestFile);
  }

  @Override
  public void consumeDeleteDataFile(DataFile icebergDeleteDatafile) {
    deleteDataFilesList.add(icebergDeleteDatafile);
    datasetCatalogRequestBuilder.deleteDatafile(icebergDeleteDatafile);
  }
}
