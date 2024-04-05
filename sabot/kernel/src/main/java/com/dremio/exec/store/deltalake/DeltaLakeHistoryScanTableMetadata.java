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
package com.dremio.exec.store.deltalake;

import com.dremio.connector.metadata.DatasetSplit;
import com.dremio.exec.catalog.MaterializedSplitsPointer;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.TableMetadataImpl;
import com.dremio.exec.planner.sql.handlers.refresh.AbstractRefreshPlanBuilder;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.MFunctionCatalogMetadata;
import com.dremio.exec.store.PartitionChunkListingImpl;
import com.dremio.exec.store.SplitsPointer;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import java.util.Collections;

public class DeltaLakeHistoryScanTableMetadata extends TableMetadataImpl {
  private final BatchSchema schema;
  private final long rowCountEstimate;

  public DeltaLakeHistoryScanTableMetadata(
      StoragePluginId plugin,
      DatasetConfig config,
      String user,
      SplitsPointer splits,
      BatchSchema schema,
      long rowCountEstimate) {
    super(plugin, config, user, splits, null);
    this.schema = schema;
    this.rowCountEstimate = rowCountEstimate;
  }

  public static DeltaLakeHistoryScanTableMetadata create(
      MFunctionCatalogMetadata catalogMetadata,
      DatasetConfig config,
      String user,
      long rowCountEstimate) {
    PartitionChunkListingImpl chunkListing = new PartitionChunkListingImpl();
    chunkListing.put(Collections.emptyList(), DatasetSplit.of(1, 1));
    chunkListing.computePartitionChunks();
    SplitsPointer splits =
        MaterializedSplitsPointer.of(
            0L,
            AbstractRefreshPlanBuilder.convertToPartitionChunkMetadata(chunkListing, config),
            1);

    return new DeltaLakeHistoryScanTableMetadata(
        catalogMetadata.getStoragePluginId(),
        config,
        user,
        splits,
        catalogMetadata.getBatchSchema(),
        rowCountEstimate);
  }

  @Override
  public BatchSchema getSchema() {
    return schema;
  }

  @Override
  public long getApproximateRecordCount() {
    return rowCountEstimate;
  }
}
