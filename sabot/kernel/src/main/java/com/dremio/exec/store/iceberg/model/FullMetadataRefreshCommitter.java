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

import java.util.List;

import org.apache.iceberg.ManifestFile;

import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.metadatarefresh.DatasetCatalogGrpcClient;
import com.dremio.exec.store.metadatarefresh.DatasetCatalogRequestBuilder;
import com.google.common.base.Preconditions;

/**
 * Similar to {@link IcebergTableCreationCommitter}, additionally updates Dataset Catalog with
 * new Iceberg table metadata
 */
public class FullMetadataRefreshCommitter extends IcebergTableCreationCommitter {

  private final DatasetCatalogGrpcClient client;
  private final DatasetCatalogRequestBuilder datasetCatalogRequestBuilder;

  public FullMetadataRefreshCommitter(String tableName, String tableLocation, BatchSchema batchSchema, List<String> partitionColumnNames,
                                      IcebergCommand icebergCommand, DatasetCatalogGrpcClient client) {
    super(tableName, batchSchema, partitionColumnNames, icebergCommand);

    Preconditions.checkNotNull(client, "Metadata requires DatasetCatalog service client");
    this.client = client;

    datasetCatalogRequestBuilder = DatasetCatalogRequestBuilder.forFullMetadataRefresh(tableName,
      tableLocation,
      batchSchema,
      partitionColumnNames);
  }

  @Override
  public void commit() {
    super.commit();
    client.getCatalogServiceApi().addOrUpdateDataset(datasetCatalogRequestBuilder.build());
  }

  @Override
  public void consumeManifestFile(ManifestFile icebergManifestFile) {
    super.consumeManifestFile(icebergManifestFile);
    datasetCatalogRequestBuilder.addManifest(icebergManifestFile);
  }

}
