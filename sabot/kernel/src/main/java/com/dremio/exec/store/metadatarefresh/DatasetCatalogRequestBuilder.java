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
package com.dremio.exec.store.metadatarefresh;

import java.util.List;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.planner.cost.ScanCostFactor;
import com.dremio.exec.record.BatchSchema;
import com.dremio.service.catalog.AddOrUpdateDatasetRequest;
import com.dremio.service.catalog.GetDatasetRequest;
import com.dremio.service.catalog.OperationType;
import com.dremio.service.catalog.UpdatableDatasetConfigFields;
import com.dremio.service.namespace.dataset.proto.DatasetCommonProtobuf;
import com.dremio.service.namespace.file.proto.FileProtobuf;
import com.google.protobuf.ByteString;

import io.grpc.Status;

/**
 * Helper class to create required fields to update metadata in dataset catalog
 */
public class DatasetCatalogRequestBuilder {

  private static final Logger logger = LoggerFactory.getLogger(DatasetCatalogRequestBuilder.class);

  private final AddOrUpdateDatasetRequest.Builder request;
  private final DatasetCommonProtobuf.ScanStats.Builder scanStatsBuilder;
  private final DatasetCommonProtobuf.ScanStats.Builder manifestStatsBuilder;

  /**
   * Helper for full metadata refresh
   * @param tableName
   * @param tableLocation
   * @param batchSchema
   * @param partitionColumns
   * @return
   */
  public static DatasetCatalogRequestBuilder forFullMetadataRefresh(String tableName, String tableLocation,
                                                                    BatchSchema batchSchema, List<String> partitionColumns) {
    return new DatasetCatalogRequestBuilder(tableName, tableLocation, batchSchema, partitionColumns);
  }

  /**
   * Gets existing dataset metadata using the passed client, updates tags, stats from previous metadata
   * @param tableName
   * @param tableLocation
   * @param batchSchema
   * @param partitionColumns
   * @param client
   * @return
   */
  public static DatasetCatalogRequestBuilder forIncrementalMetadataRefresh(String tableName, String tableLocation,
                                                                           BatchSchema batchSchema, List<String> partitionColumns,
                                                                           DatasetCatalogGrpcClient client) {
    return new DatasetCatalogRequestBuilder(tableName, tableLocation, batchSchema, partitionColumns, client);
  }

  private DatasetCatalogRequestBuilder(String tableName,
                                       String tableLocation,
                                       BatchSchema batchSchema,
                                       List<String> partitionColumns) {

    request = AddOrUpdateDatasetRequest.newBuilder()
      .addDatasetPath(tableName);

    request.setOperationType(OperationType.CREATE);

    request.getDatasetConfigBuilder()
      .setDatasetType(DatasetCommonProtobuf.DatasetType.PHYSICAL_DATASET)
      .setBatchSchema(ByteString.copyFrom(batchSchema.serialize()));

    request.getDatasetConfigBuilder().getFileFormatBuilder()
      .setType(FileProtobuf.FileType.ICEBERG)
      .setCtime(0L)
      .setExtendedConfig(ByteString.EMPTY)
      .setLocation(tableLocation);

    request.getDatasetConfigBuilder().getReadDefinitionBuilder()
      .setLastRefreshDate(System.currentTimeMillis()) // TODO: this should be start time of query execution
      .setReadSignature(ByteString.EMPTY) // TODO: read signature should be supplied to this operator
      .addAllPartitionColumns(partitionColumns)
      .setExtendedProperty(ByteString.EMPTY)
      .setSplitVersion(1L);

    scanStatsBuilder = request.getDatasetConfigBuilder().getReadDefinitionBuilder()
      .getScanStatsBuilder()
      .setType(DatasetCommonProtobuf.ScanStatsType.EXACT_ROW_COUNT)
      .setScanFactor(ScanCostFactor.PARQUET.getFactor());

    manifestStatsBuilder = request.getDatasetConfigBuilder().getReadDefinitionBuilder()
      .getManifestScanStatsBuilder()
      .setType(DatasetCommonProtobuf.ScanStatsType.EXACT_ROW_COUNT)
      .setScanFactor(ScanCostFactor.EASY.getFactor());

  }

  private DatasetCatalogRequestBuilder(String tableName, String tableLocation,
                                       BatchSchema batchSchema, List<String> partitionColumns,
                                       DatasetCatalogGrpcClient client) {
    this(tableName, tableLocation, batchSchema, partitionColumns);

    request.setOperationType(OperationType.UPDATE);

    UpdatableDatasetConfigFields existingMetadata = getPreviousMetadata(client, tableName);

    scanStatsBuilder.setRecordCount(existingMetadata.getReadDefinition().getScanStats().getRecordCount());
    manifestStatsBuilder.setRecordCount(existingMetadata.getReadDefinition().getManifestScanStats().getRecordCount());

    request.getDatasetConfigBuilder().setTag(existingMetadata.getTag());
    request.getDatasetConfigBuilder().getFileFormatBuilder().setTag(existingMetadata.getFileFormat().getTag());
  }

  /**
   * Assumes Incremental metadata refresh, throws if metadata not found
   *
   * @param client
   * @param tableName
   * @return
   */
  private UpdatableDatasetConfigFields getPreviousMetadata(DatasetCatalogGrpcClient client, String tableName) {
    try {
      logger.debug("Getting dataset config of table {} from DatasetCatalogService", tableName);
      return client.getCatalogServiceApi()
        .getDataset(GetDatasetRequest.newBuilder().addDatasetPath(tableName).build());
    } catch (Exception e) {
      Status s = Status.fromThrowable(e);

      if (s.getCode() == Status.Code.NOT_FOUND) {
        logger.error("Incremental refresh failed, previous metadata of table {} missing in Dataset Catalog store", tableName);
        throw UserException.concurrentModificationError()
          .message("Previous metadata missing")
          .buildSilently();
      } else {
        throw UserException.ioExceptionError(e)
          .message("Error while getting dataset config")
          .buildSilently();
      }
    }

  }

  public void addManifest(ManifestFile manifestFile) {
    scanStatsBuilder.setRecordCount(manifestFile.addedRowsCount() + scanStatsBuilder.getRecordCount());
    manifestStatsBuilder.setRecordCount(manifestFile.addedFilesCount() + manifestStatsBuilder.getRecordCount());
  }

  public void deleteDatafile(DataFile deletedFile) {
    scanStatsBuilder.setRecordCount(scanStatsBuilder.getRecordCount() - deletedFile.recordCount());
    manifestStatsBuilder.setRecordCount(manifestStatsBuilder.getRecordCount() - 1);
  }

  public AddOrUpdateDatasetRequest build() {
    return request.build();
  }

}
