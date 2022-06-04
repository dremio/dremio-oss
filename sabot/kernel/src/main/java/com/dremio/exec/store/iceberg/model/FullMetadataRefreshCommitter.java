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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableProperties;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.MutablePlugin;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.metadatarefresh.committer.DatasetCatalogGrpcClient;
import com.dremio.exec.store.metadatarefresh.committer.DatasetCatalogRequestBuilder;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

/**
 * Similar to {@link IcebergTableCreationCommitter}, additionally updates Dataset Catalog with
 * new Iceberg table metadata
 */
public class FullMetadataRefreshCommitter extends IcebergTableCreationCommitter {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FullMetadataRefreshCommitter.class);
  private final DatasetCatalogGrpcClient client;
  private final DatasetCatalogRequestBuilder datasetCatalogRequestBuilder;
  private final Configuration conf;
  private final boolean isPartitioned;
  private final String tableUuid;
  private final String tableLocation;
  private final List<String> datasetPath;
  private final MutablePlugin plugin;
  private static final Map<String, String> internalIcebergTableParameter = Stream.of(new String[][] {
          { TableProperties.COMMIT_NUM_RETRIES, "0" }}).collect(Collectors.toMap(d->d[0], d->d[1]));

  public FullMetadataRefreshCommitter(String tableName, List<String> datasetPath, String tableLocation,
                                      String tableUuid, BatchSchema batchSchema,
                                      Configuration configuration, List<String> partitionColumnNames,
                                      IcebergCommand icebergCommand, DatasetCatalogGrpcClient client,
                                      DatasetConfig datasetConfig, OperatorStats operatorStats, MutablePlugin plugin) {
    super(tableName, batchSchema, partitionColumnNames, icebergCommand, internalIcebergTableParameter, operatorStats); // Full MetadataRefresh is a only way to create internal iceberg table

    Preconditions.checkNotNull(client, "Metadata requires DatasetCatalog service client");
    this.client = client;
    this.conf = configuration;
    this.tableUuid = tableUuid;
    this.tableLocation = tableLocation;
    this.isPartitioned = partitionColumnNames != null && !partitionColumnNames.isEmpty();
    this.datasetPath = datasetPath;
    datasetCatalogRequestBuilder = DatasetCatalogRequestBuilder.forFullMetadataRefresh(datasetPath,
      tableLocation,
      batchSchema,
      partitionColumnNames,
      datasetConfig
    );
    this.plugin = plugin;
  }

  @Override
  public Snapshot commit() {
    Snapshot snapshot = super.commit();

    long numRecords = Long.parseLong(snapshot.summary().getOrDefault("total-records", "0"));
    datasetCatalogRequestBuilder.setNumOfRecords(numRecords);
    long numDataFiles = Long.parseLong(snapshot.summary().getOrDefault("total-data-files", "0"));
    datasetCatalogRequestBuilder.setNumOfDataFiles(numDataFiles);
    datasetCatalogRequestBuilder.setIcebergMetadata(getRootPointer(), tableUuid, snapshot.snapshotId(), conf, isPartitioned, getCurrentSpecMap(), plugin);

    try {
      client.getCatalogServiceApi().addOrUpdateDataset(datasetCatalogRequestBuilder.build());
    } catch (StatusRuntimeException sre) {
      if (sre.getStatus().getCode() == Status.Code.ABORTED) {
        String message = "Metadata refresh failed. Dataset: "
                + Arrays.toString(datasetPath.toArray())
                + " TableLocation: " + tableLocation;
        logger.error(message);
        throw UserException.concurrentModificationError(sre).message(message).build(logger);
      }
      throw sre;
    }
    return snapshot;
  }

  @Override
  public void consumeManifestFile(ManifestFile icebergManifestFile) {
    super.consumeManifestFile(icebergManifestFile);
  }

  @Override
  public void updateReadSignature(ByteString newReadSignature) {
    logger.debug("Updating read signature.");
    datasetCatalogRequestBuilder.setReadSignature(newReadSignature);
  }
}
