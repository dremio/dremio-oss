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
package com.dremio.exec.store.metadatarefresh.committer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.planner.acceleration.IncrementalUpdateUtils;
import com.dremio.exec.planner.cost.ScanCostFactor;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.iceberg.IcebergSerDe;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.sabot.exec.store.easy.proto.EasyProtobuf;
import com.dremio.service.catalog.AddOrUpdateDatasetRequest;
import com.dremio.service.catalog.GetDatasetRequest;
import com.dremio.service.catalog.OperationType;
import com.dremio.service.catalog.UpdatableDatasetConfigFields;
import com.dremio.service.namespace.dataset.proto.DatasetCommonProtobuf;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.namespace.file.proto.FileProtobuf;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;

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
   * @param datasetPath
   * @param tableLocation
   * @param batchSchema
   * @param partitionColumns
   * @return
   */
  public static DatasetCatalogRequestBuilder forFullMetadataRefresh(List<String> datasetPath, String tableLocation,
                                                                    BatchSchema batchSchema, List<String> partitionColumns,
                                                                    DatasetConfig datasetConfig) {
    if (!DatasetType.PHYSICAL_DATASET.equals(datasetConfig.getType())) { // non-filesystem datasets don't need implicit columns
      batchSchema = BatchSchema.newBuilder().addFields(batchSchema.getFields())
              .addField(Field.nullable(IncrementalUpdateUtils.UPDATE_COLUMN, new ArrowType.Int(64, true))).build();
      partitionColumns = new ArrayList<>(partitionColumns);
      partitionColumns.add(IncrementalUpdateUtils.UPDATE_COLUMN);
    }
    return new DatasetCatalogRequestBuilder(datasetPath, tableLocation, batchSchema, partitionColumns, datasetConfig);
  }

  /**
   * Gets existing dataset metadata using the passed client, updates tags, stats from previous metadata
   * @param datasetPath
   * @param tableLocation
   * @param batchSchema
   * @param partitionColumns
   * @param client
   * @return
   */
  public static DatasetCatalogRequestBuilder forIncrementalMetadataRefresh(List<String> datasetPath, String tableLocation,
                                                                           BatchSchema batchSchema, List<String> partitionColumns,
                                                                           DatasetCatalogGrpcClient client, DatasetConfig datasetConfig) {
    return new DatasetCatalogRequestBuilder(datasetPath, tableLocation, batchSchema, partitionColumns, client, datasetConfig);
  }

  private DatasetCatalogRequestBuilder(List<String> datasetPath,
                                       String tableLocation,
                                       BatchSchema batchSchema,
                                       List<String> partitionColumns,
                                       DatasetConfig datasetConfig) {
    request = AddOrUpdateDatasetRequest.newBuilder()
      .addAllDatasetPath(datasetPath);

    request.setOperationType(OperationType.CREATE);

    request.getDatasetConfigBuilder()
      .setDatasetType(DatasetCommonProtobuf.DatasetType.forNumber(datasetConfig.getType().getNumber()))
      .setBatchSchema(ByteString.copyFrom(batchSchema.serialize()));

    FileConfig fileConfig = datasetConfig.getPhysicalDataset().getFormatSettings();
    if( fileConfig != null) {
      final FileProtobuf.FileType fileType = FileProtobuf.FileType.valueOf(fileConfig.getType().name());
      request.getDatasetConfigBuilder().getFileFormatBuilder()
        .setType(fileType)
        .setCtime(0L)
        .setExtendedConfig(ByteString.EMPTY)
        .setLocation(tableLocation);
    }

    request.getDatasetConfigBuilder().setIcebergMetadataEnabled(true);

    if (datasetConfig.getReadDefinition().getExtendedProperty()!=null) {
      request.getDatasetConfigBuilder().getReadDefinitionBuilder()
              .setExtendedProperty(UnsafeByteOperations.unsafeWrap(datasetConfig.getReadDefinition().getExtendedProperty().toByteArray()));
    }
    if (datasetConfig.getTag() != null) {
      request.setOperationType(OperationType.UPDATE);
      request.getDatasetConfigBuilder().setTag(datasetConfig.getTag());
      if (fileConfig != null) {
        request.getDatasetConfigBuilder().getFileFormatBuilder().setTag(datasetConfig.getTag());
      }
    }

    request.getDatasetConfigBuilder().getReadDefinitionBuilder()
      .setLastRefreshDate(System.currentTimeMillis()) // TODO: this should be start time of query execution
      .addAllPartitionColumns(partitionColumns)
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

  private DatasetCatalogRequestBuilder(List<String> datasetPath, String tableLocation,
                                       BatchSchema batchSchema, List<String> partitionColumns,
                                       DatasetCatalogGrpcClient client, DatasetConfig datasetConfig) {
    this(datasetPath, tableLocation, batchSchema, partitionColumns, datasetConfig);

    request.setOperationType(OperationType.UPDATE);

    UpdatableDatasetConfigFields existingMetadata = getPreviousMetadata(client, datasetPath);

    request.getDatasetConfigBuilder().setTag(existingMetadata.getTag());
    if (datasetConfig.getPhysicalDataset().getFormatSettings() != null) {
      request.getDatasetConfigBuilder().getFileFormatBuilder().setTag(existingMetadata.getFileFormat().getTag());
    }
  }

  /**
   * Assumes Incremental metadata refresh, throws if metadata not found
   *
   * @param client
   * @param datasetPath
   * @return
   */
  private UpdatableDatasetConfigFields getPreviousMetadata(DatasetCatalogGrpcClient client, List<String> datasetPath) {
    try {
      logger.debug("Getting dataset config of table {} from DatasetCatalogService", datasetPath);
      return client.getCatalogServiceApi()
        .getDataset(GetDatasetRequest.newBuilder().addAllDatasetPath(datasetPath).build());
    } catch (Exception e) {
      Status s = Status.fromThrowable(e);

      if (s.getCode() == Status.Code.NOT_FOUND) {
        logger.error("Incremental refresh failed, previous metadata of table {} missing in Dataset Catalog store", datasetPath);
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

  public void setNumOfRecords(long records) {
    scanStatsBuilder.setRecordCount(records);
  }

  public void setNumOfDataFiles(long dataFiles) {
    manifestStatsBuilder.setRecordCount(dataFiles);
  }

  public AddOrUpdateDatasetRequest build() {
    final String path = request.getDatasetConfig().getFileFormat().getLocation();
    final EasyProtobuf.EasyDatasetSplitXAttr splitExtended = EasyProtobuf.EasyDatasetSplitXAttr.newBuilder()
            .setPath(path)
            .setStart(0)
            .setLength(0)
            .setUpdateKey(com.dremio.exec.store.file.proto.FileProtobuf.FileSystemCachedEntity.newBuilder()
                    .setPath(path)
                    .setLastModificationTime(0))
            .build();
    final PartitionProtobuf.DatasetSplit datasetSplit = PartitionProtobuf.DatasetSplit.newBuilder()
            .setRecordCount(scanStatsBuilder.getRecordCount())
            .setSplitExtendedProperty(splitExtended.toByteString()).build();
    final PartitionProtobuf.PartitionChunk root = PartitionProtobuf.PartitionChunk.newBuilder()
            .setDatasetSplit(datasetSplit).build();
    request.getDatasetConfigBuilder().setPartitionChunk(root);
    return request.build();
  }

  public void overrideSchema(BatchSchema batchSchema) {
    request.getDatasetConfigBuilder()
            .setBatchSchema(ByteString.copyFrom(batchSchema.serialize()));
  }

  public void setIcebergMetadata(String rootPointer, String tableUuid, long snapshotId, Configuration conf, boolean isPartitioned, Map<Integer, PartitionSpec> partitionSpecMap) {
    Preconditions.checkState(request != null, "Unexpected state");
    Preconditions.checkState(request.getDatasetConfigBuilder().getIcebergMetadataEnabled(), "Unexpected state");
    byte[] specs = IcebergSerDe.serializePartitionSpecMap(partitionSpecMap);
    DatasetCommonProtobuf.IcebergMetadata.Builder metadataBuilder = DatasetCommonProtobuf.IcebergMetadata.newBuilder()
            .setMetadataFileLocation(rootPointer)
            .setTableUuid(tableUuid)
            .setSnapshotId(snapshotId)
            .setPartitionSpecs(ByteString.copyFrom(specs));
    if (isPartitioned) {
      String partitionStatsFile = IcebergUtils.getPartitionStatsFile(rootPointer, snapshotId, conf);
      if (partitionStatsFile != null) {
        metadataBuilder.setPartitionStatsFile(partitionStatsFile);
      }
    }
    request.getDatasetConfigBuilder().setIcebergMetadata(metadataBuilder.build());
  }

  public void setReadSignature(ByteString newReadSignature) {
    request.getDatasetConfigBuilder().getReadDefinitionBuilder()
      .setReadSignature(newReadSignature);
  }
}
