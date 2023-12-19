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

import static com.dremio.common.exceptions.UserException.REFRESH_METADATA_FAILED_CONCURRENT_UPDATE_MSG;
import static com.dremio.exec.store.iceberg.IcebergUtils.getPartitionStatsFiles;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionStatsFileLocations;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.types.SupportsTypeCoercionsAndUpPromotions;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.MutablePlugin;
import com.dremio.exec.exception.NoSupportedUpPromotionOrCoercionException;
import com.dremio.exec.planner.acceleration.IncrementalUpdateUtils;
import com.dremio.exec.planner.common.ImmutableDremioFileAttrs;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.exec.store.iceberg.SchemaConverter;
import com.dremio.exec.store.iceberg.SnapshotEntry;
import com.dremio.exec.store.metadatarefresh.committer.DatasetCatalogGrpcClient;
import com.dremio.exec.store.metadatarefresh.committer.DatasetCatalogRequestBuilder;
import com.dremio.exec.testing.ControlsInjector;
import com.dremio.exec.testing.ControlsInjectorFactory;
import com.dremio.exec.testing.ExecutionControls;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.writer.WriterCommitterOperator;
import com.dremio.service.catalog.GetDatasetRequest;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.file.proto.FileType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;
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
  @VisibleForTesting
  public static final int MAX_NUM_SNAPSHOTS_TO_EXPIRE = 15;

  private static final int DEFAULT_THREAD_POOL_SIZE  = 4;
  private static final ExecutorService EXECUTOR_SERVICE = ThreadPools.newWorkerPool(
    "metadata-refresh-delete", DEFAULT_THREAD_POOL_SIZE);

  private final long periodToKeepSnapshotsMs;
  private final String tableName;
  private final String tableUuid;
  private final IcebergCommand icebergCommand;
  private final DatasetCatalogGrpcClient client;
  private final Configuration conf;
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
  private final boolean isMapDataTypeEnabled;
  private final FileSystem fs;
  private final Long metadataExpireAfterMs;
  private final boolean isMetadataCleanEnabled;
  private final IcebergCommandType icebergOpType;
  private boolean enableUseDefaultPeriod = true; // For unit test purpose only
  private final FileType fileType;
  public IncrementalMetadataRefreshCommitter(OperatorContext operatorContext, String tableName, List<String> datasetPath, String tableLocation,
                                             String tableUuid, BatchSchema batchSchema,
                                             Configuration configuration, List<String> partitionColumnNames,
                                             IcebergCommand icebergCommand, boolean isFileSystem,
                                             DatasetCatalogGrpcClient datasetCatalogGrpcClient,
                                             DatasetConfig datasetConfig, MutablePlugin plugin,
                                             FileSystem fs, Long metadataExpireAfterMs, IcebergCommandType icebergOpType,
                                             FileType fileType) {
    Preconditions.checkState(icebergCommand != null, "Unexpected state");
    Preconditions.checkNotNull(datasetCatalogGrpcClient, "Unexpected state: DatasetCatalogService client not provided");
    Preconditions.checkNotNull(datasetConfig.getPhysicalDataset().getIcebergMetadata().getMetadataFileLocation());
    this.icebergCommand = icebergCommand;
    this.tableName = tableName;
    this.conf = configuration;
    this.tableUuid = tableUuid;
    this.client = datasetCatalogGrpcClient;
    this.datasetConfig = datasetConfig;
    this.fileType = fileType;
    this.datasetCatalogRequestBuilder = DatasetCatalogRequestBuilder.forIncrementalMetadataRefresh (datasetPath,
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
    this.isMapDataTypeEnabled = operatorContext.getOptions().getOption(ExecConstants.ENABLE_MAP_DATA_TYPE);
    this.fs = fs;
    this.metadataExpireAfterMs = metadataExpireAfterMs;
    this.isMetadataCleanEnabled = operatorContext.getOptions().getOption(ExecConstants.ENABLE_UNLIMITED_SPLITS_METADATA_CLEAN);
    this.icebergOpType = icebergOpType;
    this.periodToKeepSnapshotsMs = operatorContext.getOptions().getOption(ExecConstants.DEFAULT_PERIOD_TO_KEEP_SNAPSHOTS_MS);
  }

  private boolean hasAnythingChanged() {
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
    try {
      table = icebergCommand.endTransaction();
      return table.currentSnapshot();
    } catch (ValidationException e) {
      // The metadata table was updated by other incremental refresh queries. Skip this update.
      // Iceberg commit is not successful and needs to clean the orphaned manifest files.
      cleanOrphans();
      checkToThrowException(getRootPointer(), getRootPointer(), null);
      return null;
    }
  }

  @VisibleForTesting
  public Snapshot postCommitTransaction() {
    // Skip post commit, if no table instance is assigned due to CME.
    if (table == null) {
      return null;
    }

    // For the Metadata Iceberg tables, we clean the old snapshots gradually.
    if (isMetadataCleanEnabled) {
      cleanSnapshotsAndMetadataFiles(table);
    }

    injector.injectChecked(executionControls, INJECTOR_AFTER_ICEBERG_COMMIT_ERROR, UnsupportedOperationException.class);
    long numRecords = Long.parseLong(table.currentSnapshot().summary().getOrDefault("total-records", "0"));
    datasetCatalogRequestBuilder.setNumOfRecords(numRecords);
    long numDataFiles = Long.parseLong(table.currentSnapshot().summary().getOrDefault("total-data-files", "0"));
    datasetCatalogRequestBuilder.setNumOfDataFiles(numDataFiles);
    ImmutableDremioFileAttrs partitionStatsFileAttrs = IcebergUtils.getPartitionStatsFileAttrs(getRootPointer(),
        table.currentSnapshot().snapshotId(), icebergCommand.getFileIO());
    datasetCatalogRequestBuilder.setIcebergMetadata(getRootPointer(), tableUuid, table.currentSnapshot().snapshotId(),
        getCurrentSpecMap(), getCurrentSchema(), partitionStatsFileAttrs.fileName(),
        partitionStatsFileAttrs.fileLength(), fileType);
    BatchSchema newSchemaFromIceberg = SchemaConverter.getBuilder().setMapTypeEnabled(isMapDataTypeEnabled).build().fromIceberg(table.schema());
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
        // DX-84083: Iceberg commit succeeds. However, it fails to update the KV store. Because, Another concurrent
        // metadata refresh query could already succeed and update the KV store and the dataset has a new tag.
        // In this case, we don't delete consumed manifest files, as they are already used to construct the snapshot
        // in the metadata table.
        checkToThrowException(prevMetadataRootPointer, getRootPointer(), sre);
      } else {
        throw sre;
      }
    }
    return table.currentSnapshot();
  }

  private void cleanSnapshotsAndMetadataFiles(Table targetTable) {
    long periodToKeepSnapshots;
    if (enableUseDefaultPeriod) {
      // If the source has user-specified metadata expiration time, we intend to keep all snapshots are still valid.
      periodToKeepSnapshots = metadataExpireAfterMs != null
        ? Math.max(periodToKeepSnapshotsMs, metadataExpireAfterMs) : periodToKeepSnapshotsMs;
    } else {
      Preconditions.checkNotNull(metadataExpireAfterMs, "Metadata expiry time not set.");
      periodToKeepSnapshots = metadataExpireAfterMs;
    }
    cleanSnapshotsAndMetadataFiles(targetTable, periodToKeepSnapshots);
  }

  /**
   * Clean the snapshots and delete orphan manifest list file paths and manifest file paths. Don't delete data files in
   * the manifests, since data files belong to original tables.
   */
  @VisibleForTesting
  public Set<String> cleanSnapshotsAndMetadataFiles(Table targetTable, long periodToKeepSnapshots) {
    Stopwatch stopwatch = Stopwatch.createStarted();
    // Clean the metadata files through setting table properties.
    setTablePropertyToCleanOldMetadataFiles(targetTable);

    // Collect the candidate snapshots to expire.
    long timestampExpiry = System.currentTimeMillis() - periodToKeepSnapshots;
    List<SnapshotEntry>  candidateSnapshots = icebergCommand.collectExpiredSnapshots(timestampExpiry, 1);

    // The existing metadata tables might already have lots of snapshots. We don't try to expire them one time,
    // because it could dramatically increase metadata refresh query time.
    // Instead, we only expire a small number of snapshots during one metadata refresh query.
    int numSnapshotsToExpire = candidateSnapshots.size() < MAX_NUM_SNAPSHOTS_TO_EXPIRE ?
      candidateSnapshots.size() : MAX_NUM_SNAPSHOTS_TO_EXPIRE;

    int numTotalSnapshots = Iterables.size(targetTable.snapshots());
    final int numSnapshotsRetain = numTotalSnapshots - numSnapshotsToExpire;

    // Call the api again to get the exact snapshots to expire.
    List<SnapshotEntry> expiredSnapshots = icebergCommand.collectExpiredSnapshots(timestampExpiry, numSnapshotsRetain);

    operatorStats.addLongStat(WriterCommitterOperator.Metric.NUM_TOTAL_SNAPSHOTS, numTotalSnapshots);
    operatorStats.addLongStat(WriterCommitterOperator.Metric.NUM_EXPIRED_SNAPSHOTS, expiredSnapshots.size());
    if (expiredSnapshots.isEmpty()) {
      return Collections.emptySet();
    }

    // Perform the expiry operation.
    final List<SnapshotEntry> liveSnapshots = icebergCommand.expireSnapshots(timestampExpiry, numSnapshotsRetain);

    // Collect the orphan files.
    Set<String> orphanFiles = new HashSet<>();
    for (SnapshotEntry entry : expiredSnapshots) {
      Snapshot snapshot = targetTable.snapshot(entry.getSnapshotId());
      orphanFiles.addAll(collectFilesForSnapshot(targetTable, snapshot));
    }

    // Remove the files that are still used by live snapshots
    for (SnapshotEntry entry : liveSnapshots) {
      Snapshot snapshot = targetTable.snapshot(entry.getSnapshotId());
      orphanFiles.removeAll(collectFilesForSnapshot(targetTable, snapshot));
    }

    IcebergUtils.removeOrphanFiles(fs, logger, EXECUTOR_SERVICE, orphanFiles);
    long clearTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);
    operatorStats.addLongStat(WriterCommitterOperator.Metric.NUM_ORPHAN_FILES_DELETED, orphanFiles.size());
    operatorStats.addLongStat(WriterCommitterOperator.Metric.CLEAR_EXPIRE_SNAPSHOTS_TIME, clearTime);
    return orphanFiles;
  }

  private Set<String> collectFilesForSnapshot(Table targetTable, Snapshot snapshot) {
    Set<String> files = new HashSet<>();
    // Manifest list file
    files.add(snapshot.manifestListLocation());
    // Manifest files
    snapshot.dataManifests(targetTable.io()).stream().forEach(m -> files.add(m.path()));

    if (snapshot.partitionStatsMetadata() != null) {
      String partitionStatsMetadataLocation = snapshot.partitionStatsMetadata().metadataFileLocation();
      PartitionStatsFileLocations partitionStatsLocations = getPartitionStatsFiles(targetTable.io(), partitionStatsMetadataLocation);
      if (partitionStatsLocations != null) {
        // Partition stats have metadata file and partition files.
        files.add(partitionStatsMetadataLocation);
        files.addAll(partitionStatsLocations.all().entrySet().stream().map(e -> e.getValue()).collect(Collectors.toList()));
      }
    }

    return files;
  }

  /**
   *  For existing metadata tables, when they were created, those tables were not configured with the table property to
   *  delete metadata files. We enabled the table property when refreshing metadata and trigger to delete metadata files.
   *  However, the later configuration of this table property will not help to clean orphan metadata files.
   */
  private void setTablePropertyToCleanOldMetadataFiles(Table targetTable) {
    Map<String, String> tblProperties = targetTable.properties();
    if (!tblProperties.containsKey(TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED)
      || tblProperties.get(TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED).equalsIgnoreCase("false")) {
      icebergCommand.updateProperties(FullMetadataRefreshCommitter.internalIcebergTableProperties, false);
    }
  }

  @Override
  public Snapshot commit() {
    Stopwatch stopwatch = Stopwatch.createStarted();
    logger.info("Incremental refresh type: {}", icebergOpType);
    if (isIcebergTableUpdated()) {
      // The metadata table was updated by other incremental refresh queries. Skip this update.
      // Iceberg commit is not successful and needs to clean the orphaned manifest files.
      cleanOrphans();
      checkToThrowException(prevMetadataRootPointer, getRootPointer(), null);
      // Don't need to return snapshot. Since, current table was updated.
      return null;
    }
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
        // Clean the metadata table's snapshots, if needed. Even, we don't increase new commits to the table.
        if (isMetadataCleanEnabled) {
          cleanSnapshotsAndMetadataFiles(oldTable);
        }
        return oldTable.currentSnapshot();
      }
    } finally {
      long totalCommitTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);
      operatorStats.addLongStat(WriterCommitterOperator.Metric.ICEBERG_COMMIT_TIME, totalCommitTime);
    }
  }

  private void checkToThrowException(String rootPointer, String foundRootPointer, Throwable cause) {
    // The metadata table was updated by other incremental refresh queries. Skip this update.
    String metadataFiles = String.format("Expected metadataRootPointer: %s, Found metadataRootPointer: %s.",
      rootPointer, foundRootPointer);
    logger.info("Concurrent operation has updated the table." + " " + metadataFiles);
    // If the refresh query works on partitions, we should notify users the failure and re-run the query.
    if (icebergOpType == IcebergCommandType.PARTIAL_METADATA_REFRESH) {
      throw UserException.concurrentModificationError(cause)
        .message(REFRESH_METADATA_FAILED_CONCURRENT_UPDATE_MSG)
        .build(logger);
    }
  }
  private void cleanOrphans() {
    newColumnTypes.clear();
    updatedColumnTypes.clear();
    deleteDataFilesList.clear();

    // Only need to delete manifest files. The data files that plan to be deleted should be cleaned by other
    // concurrent metadata refresh queries, which made successful commits.
    logger.info("Orphan manifest files to delete: {}", manifestFileList);
    IcebergUtils.removeOrphanFiles(fs, logger, EXECUTOR_SERVICE, manifestFileList.stream().map(file -> file.path()).collect(Collectors.toSet()));
    manifestFileList.clear();
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
              throw UserException.unsupportedError(e).message(e.getMessage()).build(logger);
            }
        }
      }
    }

    SchemaConverter schemaConverter = SchemaConverter.getBuilder().setTableName(tableName).setMapTypeEnabled(isMapDataTypeEnabled).build();
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
  @VisibleForTesting
  public void disableUseDefaultPeriod () {
    enableUseDefaultPeriod = false;
  }

  @VisibleForTesting
  public void setTable(Table table) {
    this.table = table;
  }

  @VisibleForTesting
  public DatasetCatalogRequestBuilder getDatasetCatalogRequestBuilder() {
    return datasetCatalogRequestBuilder;
  }
}
