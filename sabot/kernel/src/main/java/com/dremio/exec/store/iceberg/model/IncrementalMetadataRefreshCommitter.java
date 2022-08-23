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
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.types.SupportsTypeCoercionsAndUpPromotions;
import com.dremio.exec.catalog.MutablePlugin;
import com.dremio.exec.exception.NoSupportedUpPromotionOrCoercionException;
import com.dremio.exec.planner.acceleration.IncrementalUpdateUtils;
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
import com.dremio.service.catalog.GetDatasetRequest;
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
public class IncrementalMetadataRefreshCommitter implements IcebergOpCommitter, SupportsTypeCoercionsAndUpPromotions {

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
  private List<Types.NestedField> updatedColumnTypes = new ArrayList();
  private List<Types.NestedField> newColumnTypes = new ArrayList();
  private List<Types.NestedField> dropColumns = new ArrayList();
  private final DatasetCatalogRequestBuilder datasetCatalogRequestBuilder;
  private BatchSchema batchSchema;
  private boolean isFileSystem;
  private final OperatorStats operatorStats;
  private final String tableLocation;
  private final List<String> datasetPath;
  private final String prevMetadataRootPointer;
  private final ExecutionControls executionControls;
  private final DatasetConfig datasetConfig;
  private Table table;
  private final MutablePlugin plugin;

  public IncrementalMetadataRefreshCommitter(OperatorContext operatorContext, String tableName, List<String> datasetPath, String tableLocation,
                                             String tableUuid, BatchSchema batchSchema,
                                             Configuration configuration, List<String> partitionColumnNames,
                                             IcebergCommand icebergCommand, boolean isFileSystem,
                                             DatasetCatalogGrpcClient datasetCatalogGrpcClient,
                                             DatasetConfig datasetConfig, MutablePlugin plugin) {
    Preconditions.checkState(icebergCommand != null, "Unexpected state");
    Preconditions.checkNotNull(datasetCatalogGrpcClient, "Unexpected state: DatasetCatalogService client not provided");
    Preconditions.checkNotNull(datasetConfig.getPhysicalDataset().getIcebergMetadata().getMetadataFileLocation());
    this.icebergCommand = icebergCommand;
    this.tableName = tableName;
    this.conf = configuration;
    this.tableUuid = tableUuid;
    this.client = datasetCatalogGrpcClient;
    this.isPartitioned = partitionColumnNames != null && !partitionColumnNames.isEmpty();
    this.datasetConfig = datasetConfig;
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
    this.plugin = plugin;
  }

  boolean hasAnythingChanged() {
    if ((newColumnTypes.size() + updatedColumnTypes.size() + deleteDataFilesList.size() + manifestFileList.size()) > 0) {
      return true;
    }

    if (isFileSystem) {
      return false;
    }

    return (dropColumns.size() > 0);
  }

  @VisibleForTesting
  public void beginMetadataRefreshTransaction() {
    this.icebergCommand.beginTransaction();

    String currMetadataRootPointer = icebergCommand.getRootPointer();
    if (!prevMetadataRootPointer.equals(currMetadataRootPointer)) {
      String metadataFiles = String.format("Expected metadataRootPointer: %s, Found metadataRootPointer: %s",
        prevMetadataRootPointer, getRootPointer());
      logger.error(CONCURRENT_DML_OPERATION_ERROR + metadataFiles);
      throw UserException.concurrentModificationError().message(CONCURRENT_DML_OPERATION_ERROR).buildSilently();
    }
    if (hasAnythingChanged()) {
      Snapshot snapshot = icebergCommand.getCurrentSnapshot();
      Preconditions.checkArgument(snapshot != null, "Iceberg metadata does not have a snapshot");
      long snapshotId = snapshot.snapshotId();
      // Mark the transaction as a read-modify-write transaction to ensure
      // that the Iceberg table is updated only if the snapshotId is the same as the
      // one that is read as part of incremental metadata refresh
      icebergCommand.setIsReadModifyWriteTransaction(snapshotId);
    }
  }

  @VisibleForTesting
  public void performUpdates() {
    if (newColumnTypes.size() > 0) {
      icebergCommand.consumeAddedColumns(newColumnTypes);
    }

    if (updatedColumnTypes.size() > 0) {
      icebergCommand.consumeUpdatedColumns(updatedColumnTypes);
    }

    if (!isFileSystem && dropColumns.size() > 0) {
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
  }

  @VisibleForTesting
  public Snapshot endMetadataRefreshTransaction() {
    table = icebergCommand.endTransaction();
    return table.currentSnapshot();
  }

  @VisibleForTesting
  public Snapshot postCommitTransaction() {
    injector.injectChecked(executionControls, INJECTOR_AFTER_ICEBERG_COMMIT_ERROR, UnsupportedOperationException.class);
    long numRecords = Long.parseLong(table.currentSnapshot().summary().getOrDefault("total-records", "0"));
    datasetCatalogRequestBuilder.setNumOfRecords(numRecords);
    long numDataFiles = Long.parseLong(table.currentSnapshot().summary().getOrDefault("total-data-files", "0"));
    datasetCatalogRequestBuilder.setNumOfDataFiles(numDataFiles);
    datasetCatalogRequestBuilder.setIcebergMetadata(getRootPointer(), tableUuid, table.currentSnapshot().snapshotId(), conf, isPartitioned, getCurrentSpecMap(), plugin, getCurrentSchema());
    BatchSchema newSchemaFromIceberg = new SchemaConverter().fromIceberg(table.schema());
    newSchemaFromIceberg = BatchSchema.newBuilder().addFields(newSchemaFromIceberg.getFields())
      .addField(Field.nullable(IncrementalUpdateUtils.UPDATE_COLUMN, new ArrowType.Int(64, true))).build();
    datasetCatalogRequestBuilder.overrideSchema(newSchemaFromIceberg);
    logger.debug("Committed incremental metadata change of table {}. Updating Dataset Catalog store", tableName);
    try {
      client.getCatalogServiceApi().addOrUpdateDataset(datasetCatalogRequestBuilder.build());
    } catch (StatusRuntimeException sre) {
      if (sre.getStatus().getCode() == Status.Code.ABORTED) {
        logger.error("Metadata refresh failed. Dataset: " + Arrays.toString(datasetPath.toArray())
          + " TableLocation: " + tableLocation, sre);
        throw UserException.concurrentModificationError(sre)
          .message(UserException.REFRESH_METADATA_FAILED_CONCURRENT_UPDATE_MSG)
          .build(logger);
      }
      throw sre;
    }
    return table.currentSnapshot();
  }

  @Override
  public Snapshot commit() {
    Stopwatch stopwatch = Stopwatch.createStarted();
    try {
      boolean shouldCommit = hasAnythingChanged();
      Table oldTable = null;
      if (!shouldCommit) {
        oldTable = this.icebergCommand.loadTable();
        shouldCommit = oldTable.currentSnapshot().snapshotId() != client.getCatalogServiceApi()
          .getDataset(GetDatasetRequest.newBuilder().addAllDatasetPath(datasetPath).build()).getIcebergMetadata().getSnapshotId();
      }
      if(shouldCommit) {
        beginMetadataRefreshTransaction();
        performUpdates();
        endMetadataRefreshTransaction();
        return postCommitTransaction();
      } else {
        logger.debug("Nothing is changed for  table " + this.tableName + ", Skipping commit");
        return oldTable.currentSnapshot();
      }
    } finally {
      long totalCommitTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);
      operatorStats.addLongStat(WriterCommitterOperator.Metric.ICEBERG_COMMIT_TIME, totalCommitTime);
    }
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
  public void consumeDeleteDataFilePath(String icebergDeleteDatafilePath) throws UnsupportedOperationException {
    throw new UnsupportedOperationException("Deleting data file by path is not supported in metadata refresh Transaction");
  }

  @Override
  public void updateSchema(BatchSchema newSchema) {
    //handle update of columns from batch schema dropped columns and updated columns
    if(datasetConfig.getPhysicalDataset().getInternalSchemaSettings() != null) {

      if(!datasetConfig.getPhysicalDataset().getInternalSchemaSettings().getSchemaLearningEnabled()) {
        return;
      }

      List<Field> droppedColumns = new ArrayList<>();
      if(datasetConfig.getPhysicalDataset().getInternalSchemaSettings().getDroppedColumns() != null) {
        droppedColumns = BatchSchema.deserialize(datasetConfig.getPhysicalDataset().getInternalSchemaSettings().getDroppedColumns()).getFields();
      }

      List<Field> updatedColumns = new ArrayList<>();
      if(datasetConfig.getPhysicalDataset().getInternalSchemaSettings().getModifiedColumns() != null) {
        updatedColumns = BatchSchema.deserialize(datasetConfig.getPhysicalDataset().getInternalSchemaSettings().getModifiedColumns()).getFields();
      }

      for(Field field : droppedColumns) {
        newSchema = newSchema.dropField(field);
      }

      Map<String, Field> originalFieldsMap = batchSchema.getFields().stream().collect(Collectors.toMap(x -> x.getName().toLowerCase(), Function.identity()));

      for(Field field : updatedColumns) {
          if(field.getChildren().isEmpty()) {
            newSchema = newSchema.changeTypeTopLevel(field);
          } else {
            //If complex we don't want schema learning on all fields. So
            //we drop the new struct field and replace it with old struct from the original batch schema.
            Field oldField = originalFieldsMap.get(field.getName().toLowerCase());
            newSchema = newSchema.dropField(field.getName());
            try {
              newSchema = newSchema.mergeWithUpPromotion(BatchSchema.of(oldField), this);
            }catch (NoSupportedUpPromotionOrCoercionException e) {
              e.addDatasetPath(datasetPath);
              throw UserException.unsupportedError().message(e.getMessage()).build(logger);
            }
        }
      }
    }

    SchemaConverter schemaConverter = new SchemaConverter(tableName);
    Schema oldIcebergSchema = schemaConverter.toIcebergSchema(batchSchema);
    Schema newIcebergSchema = schemaConverter.toIcebergSchema(newSchema);

    List<Types.NestedField> oldFields = oldIcebergSchema.columns();
    List<Types.NestedField> newFields = newIcebergSchema.columns();

    Map<String, Types.NestedField> nameToTypeOld = oldFields.stream().collect(Collectors.toMap(x -> x.name(), x -> x));
    Map<String, Types.NestedField> nameToTypeNew = newFields.stream().collect(Collectors.toMap(x -> x.name(), x -> x));

    /*
      Collecting updated and drop columns here. Columns must not be dropped for filesystem.
    */
    for (Map.Entry<String, Types.NestedField> entry : nameToTypeOld.entrySet()) {
      Types.NestedField newType = nameToTypeNew.get(entry.getKey());
      if(newType != null && isColumnUpdated(newType, entry.getValue())) {
        updatedColumnTypes.add(newType);
      }
    }

    for (Map.Entry<String, Types.NestedField> entry : nameToTypeNew.entrySet()) {
      if (!nameToTypeOld.containsKey(entry.getKey())) {
        newColumnTypes.add(entry.getValue());
      }
    }

    for (Map.Entry<String, Types.NestedField> entry : nameToTypeOld.entrySet()) {
      if (!nameToTypeNew.containsKey(entry.getKey())) {
        if (!entry.getValue().name().equals(IncrementalUpdateUtils.UPDATE_COLUMN)) {
          dropColumns.add(entry.getValue());
        }
      }
    }

    Comparator<Types.NestedField> fieldComparator = Comparator.comparing(Types.NestedField::fieldId);
    Collections.sort(newColumnTypes, fieldComparator);
    Collections.sort(updatedColumnTypes, fieldComparator);
    Collections.sort(dropColumns, fieldComparator);
    this.batchSchema = newSchema;
    this.datasetCatalogRequestBuilder.overrideSchema(newSchema);
  }

  private boolean isColumnUpdated(Types.NestedField newField, Types.NestedField oldField) {
    if (newField.isOptional() != oldField.isOptional()) {
      return true;
    } else if (!newField.name().equals(oldField.name())) {
      return true;
    } else if (!Objects.equals(newField.doc(), oldField.doc())) {
      return true;
    }
    return !newField.type().equals(oldField.type());
  }

  @Override
  public String getRootPointer() {
    return icebergCommand.getRootPointer();
  }

  @Override
  public Map<Integer, PartitionSpec> getCurrentSpecMap() {
    return icebergCommand.getPartitionSpecMap();
  }

  @Override
  public Schema getCurrentSchema() {
    return icebergCommand.getIcebergSchema();
  }

  @Override
  public boolean isIcebergTableUpdated() {
    return !icebergCommand.getRootPointer().equals(prevMetadataRootPointer);
  }

  @Override
  public void updateReadSignature(ByteString newReadSignature) {
    logger.debug("Updating read signature");
    datasetCatalogRequestBuilder.setReadSignature(newReadSignature);
  }
}
