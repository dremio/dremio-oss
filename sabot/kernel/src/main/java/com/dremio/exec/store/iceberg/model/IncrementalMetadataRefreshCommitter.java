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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.iceberg.SchemaConverter;
import com.dremio.exec.store.metadatarefresh.committer.DatasetCatalogGrpcClient;
import com.dremio.exec.store.metadatarefresh.committer.DatasetCatalogRequestBuilder;
import com.dremio.exec.testing.ControlsInjector;
import com.dremio.exec.testing.ControlsInjectorFactory;
import com.dremio.exec.testing.ExecutionControls;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.writer.WriterCommitterOperator;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.protobuf.ByteString;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

/**
 * IcebergMetadataRefreshCommitter this committer has two update operation
 * DELETE Followed by INSERT
 */
public class IncrementalMetadataRefreshCommitter implements IcebergOpCommitter {

  private static final Logger logger = LoggerFactory.getLogger(IncrementalMetadataRefreshCommitter.class);
  private static final ControlsInjector injector = ControlsInjectorFactory.getInjector(IncrementalMetadataRefreshCommitter.class);

  @VisibleForTesting
  public static final String INJECTOR_AFTER_ICEBERG_COMMIT_ERROR = "error-between-iceberg-commit-and-catalog-update";

  private final String tableName;
  private final String tableUuid;
  private final IcebergCommand icebergCommand;
  private final DatasetCatalogGrpcClient client;
  private final Configuration conf;
  private final boolean isPartitioned;
  private List<ManifestFile> manifestFileList = new ArrayList<>();
  private List<DataFile> deleteDataFilesList = new ArrayList<>();
  List<Types.NestedField> updatedColumnTypes = new ArrayList();
  List<Types.NestedField> newColumnTypes = new ArrayList();
  List<Types.NestedField> dropColumns = new ArrayList();
  private final DatasetCatalogRequestBuilder datasetCatalogRequestBuilder;
  private BatchSchema batchSchema;
  private boolean isFileSystem;
  private final OperatorStats operatorStats;
  private final String tableLocation;
  private final List<String> datasetPath;
  private final String prevMetadataRootPointer;
  private final ExecutionControls executionControls;

  public IncrementalMetadataRefreshCommitter(OperatorContext operatorContext, String tableName, List<String> datasetPath, String tableLocation,
                                             String tableUuid, BatchSchema batchSchema,
                                             Configuration configuration, List<String> partitionColumnNames,
                                             IcebergCommand icebergCommand, boolean isFileSystem,
                                             DatasetCatalogGrpcClient datasetCatalogGrpcClient,
                                             DatasetConfig datasetConfig) {
    Preconditions.checkState(icebergCommand != null, "Unexpected state");
    Preconditions.checkNotNull(datasetCatalogGrpcClient, "Unexpected state: DatasetCatalogService client not provided");
    Preconditions.checkNotNull(datasetConfig.getPhysicalDataset().getIcebergMetadata().getMetadataFileLocation());
    this.icebergCommand = icebergCommand;
    this.tableName = tableName;
    this.conf = configuration;
    this.tableUuid = tableUuid;
    this.client = datasetCatalogGrpcClient;
    this.isPartitioned = partitionColumnNames != null && !partitionColumnNames.isEmpty();

    datasetCatalogRequestBuilder = DatasetCatalogRequestBuilder.forIncrementalMetadataRefresh (datasetPath,
      tableLocation,
      batchSchema,
      partitionColumnNames,
      datasetCatalogGrpcClient,
      datasetConfig);
    this.prevMetadataRootPointer = datasetConfig.getPhysicalDataset().getIcebergMetadata().getMetadataFileLocation();
    this.batchSchema = batchSchema;
    this.isFileSystem = isFileSystem;
    this.operatorStats = operatorContext.getStats();
    this.tableLocation = tableLocation;
    this.datasetPath = datasetPath;
    this.executionControls = operatorContext.getExecutionControls();
  }

  @Override
  public Snapshot commit() {
    Stopwatch stopwatch = Stopwatch.createStarted();
    this.icebergCommand.beginMetadataRefreshTransaction();

    String currMetadataRootPointer = icebergCommand.getRootPointer();
    if (!prevMetadataRootPointer.equals(currMetadataRootPointer)) {
      String message = String.format("Iceberg table has updated. Expected metadataRootPointer: %s, Found metadataRootPointer: %s", prevMetadataRootPointer, currMetadataRootPointer);
      logger.error(message);
      throw UserException.concurrentModificationError().message(message).buildSilently();
    }

    if(newColumnTypes.size() > 0) {
      icebergCommand.consumeAddedColumns(newColumnTypes);
    }

    if(updatedColumnTypes.size() > 0) {
      icebergCommand.consumeUpdatedColumns(updatedColumnTypes);
    }

    if(!isFileSystem && dropColumns.size() > 0) {
      icebergCommand.consumeDroppedColumns(dropColumns);
    }

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

    Snapshot snapshot = icebergCommand.endMetadataRefreshTransaction();
    long totalCommitTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);
    operatorStats.addLongStat(WriterCommitterOperator.Metric.ICEBERG_COMMIT_TIME, totalCommitTime);

    injector.injectChecked(executionControls, INJECTOR_AFTER_ICEBERG_COMMIT_ERROR, UnsupportedOperationException.class);

    long numRecords = Long.parseLong(snapshot.summary().getOrDefault("total-records", "0"));
    datasetCatalogRequestBuilder.setNumOfRecords(numRecords);
    long numDataFiles = Long.parseLong(snapshot.summary().getOrDefault("total-data-files", "0"));
    datasetCatalogRequestBuilder.setNumOfDataFiles(numDataFiles);
    datasetCatalogRequestBuilder.setIcebergMetadata(getRootPointer(), tableUuid, snapshot.snapshotId(), conf, isPartitioned);
    logger.debug("Committed incremental metadata change of table {}. Updating Dataset Catalog store", tableName);
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
  public void consumeManifestFile(ManifestFile manifestFile) {
    manifestFileList.add(manifestFile);
  }

  @Override
  public void consumeDeleteDataFile(DataFile icebergDeleteDatafile) {
    deleteDataFilesList.add(icebergDeleteDatafile);
  }

  @Override
  public void updateSchema(BatchSchema newSchema) {
    SchemaConverter schemaConverter = new SchemaConverter(tableName);
    Schema oldIcebergSchema = schemaConverter.toIcebergSchema(batchSchema);
    Schema newIcebergSchema = schemaConverter.toIcebergSchema(newSchema);

    List<Types.NestedField> oldFields = oldIcebergSchema.columns();
    List<Types.NestedField> newFields =  newIcebergSchema.columns();

    Map<String, Types.NestedField> nameToTypeOld = oldFields.stream().collect(Collectors.toMap(x -> x.name(), x -> x));
    Map<String, Types.NestedField> nameToTypeNew = newFields.stream().collect(Collectors.toMap(x -> x.name(), x -> x));

    /*
      Collecting updated and drop columns here. Columns must not be dropped for filesystem.
    */
    for (Map.Entry<String, Types.NestedField> entry : nameToTypeOld.entrySet()) {
      Types.NestedField newType = nameToTypeNew.get(entry.getKey());
      if(newType != null && !newType.equals(entry.getValue())) {
        updatedColumnTypes.add(newType);
      }
    }

    for (Map.Entry<String, Types.NestedField> entry : nameToTypeNew.entrySet()) {
      if (!nameToTypeOld.containsKey(entry.getKey())) {
        newColumnTypes.add(entry.getValue());
      }
    }

    for (Map.Entry<String, Types.NestedField> entry : nameToTypeOld.entrySet()) {
      if(!nameToTypeNew.containsKey(entry.getKey())) {
        dropColumns.add(entry.getValue());
      }
    }

    Comparator<Types.NestedField> fieldComparator = Comparator.comparing(Types.NestedField::fieldId);
    Collections.sort(newColumnTypes, fieldComparator);
    Collections.sort(updatedColumnTypes, fieldComparator);
    Collections.sort(dropColumns, fieldComparator);
    this.batchSchema = newSchema;
    this.datasetCatalogRequestBuilder.overrideSchema(newSchema);
  }

  @Override
  public String getRootPointer() {
    return icebergCommand.getRootPointer();
  }

  @Override
  public void updateReadSignature(ByteString newReadSignature) {
    logger.debug("Updating read signature");
    datasetCatalogRequestBuilder.setReadSignature(newReadSignature);
  }
}
