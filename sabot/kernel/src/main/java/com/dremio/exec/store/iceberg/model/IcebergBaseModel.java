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

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.AlterTableOption;
import com.dremio.exec.catalog.PartitionSpecAlterOption;
import com.dremio.exec.catalog.RollbackOption;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.dfs.ColumnOperations;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.exec.store.iceberg.SnapshotEntry;
import com.dremio.exec.store.iceberg.SupportsIcebergMutablePlugin;
import com.dremio.exec.store.metadatarefresh.committer.DatasetCatalogGrpcClient;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.writer.WriterCommitterOperator;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.file.proto.FileType;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Types;

/** Base class for common Iceberg model operations */
public abstract class IcebergBaseModel implements IcebergModel {

  protected static final String EMPTY_NAMESPACE = "";
  protected final String namespace;
  protected final Configuration configuration;
  protected final FileIO fileIO;
  protected final OperatorContext operatorContext;
  private final DatasetCatalogGrpcClient client;
  protected final SupportsIcebergMutablePlugin plugin;

  protected IcebergBaseModel(
      String namespace,
      Configuration configuration,
      FileIO fileIO,
      OperatorContext operatorContext,
      DatasetCatalogGrpcClient datasetCatalogGrpcClient,
      SupportsIcebergMutablePlugin plugin) {
    this.namespace = namespace;
    this.configuration = configuration;
    this.fileIO = Preconditions.checkNotNull(fileIO);
    this.operatorContext = operatorContext;
    this.client = datasetCatalogGrpcClient;
    this.plugin = plugin;
  }

  protected abstract IcebergCommand getIcebergCommand(
      IcebergTableIdentifier tableIdentifier, @Nullable IcebergCommitOrigin commitOrigin);

  private IcebergCommand getIcebergCommandWithMetricStat(
      IcebergTableIdentifier tableIdentifier, @Nullable IcebergCommitOrigin commitOrigin) {
    Stopwatch stopwatch = Stopwatch.createStarted();
    try {
      return getIcebergCommand(tableIdentifier, commitOrigin);
    } finally {
      long icebergCommandTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);
      if (operatorContext != null && operatorContext.getStats() != null) {
        operatorContext
            .getStats()
            .addLongStat(
                WriterCommitterOperator.Metric.ICEBERG_COMMAND_CREATE_TIME, icebergCommandTime);
      }
    }
  }

  @Override
  public IcebergOpCommitter getCreateTableCommitter(
      String tableName,
      IcebergTableIdentifier tableIdentifier,
      BatchSchema batchSchema,
      List<String> partitionColumnNames,
      OperatorStats operatorStats,
      PartitionSpec partitionSpec,
      SortOrder sortOrder,
      Map<String, String> tableProperties) {
    IcebergCommand icebergCommand =
        getIcebergCommandWithMetricStat(tableIdentifier, IcebergCommitOrigin.CREATE_TABLE);
    return new IcebergTableCreationCommitter(
        tableName,
        batchSchema,
        partitionColumnNames,
        icebergCommand,
        tableProperties,
        operatorStats,
        partitionSpec,
        sortOrder);
  }

  @Override
  public IcebergOpCommitter getInsertTableCommitter(
      IcebergTableIdentifier tableIdentifier, OperatorStats operatorStats) {
    IcebergCommand icebergCommand =
        getIcebergCommandWithMetricStat(tableIdentifier, IcebergCommitOrigin.INSERT_TABLE);
    return new IcebergInsertOperationCommitter(icebergCommand, operatorStats);
  }

  @Override
  public IcebergOpCommitter getFullMetadataRefreshCommitter(
      String tableName,
      List<String> datasetPath,
      String tableLocation,
      String tableUuid,
      IcebergTableIdentifier tableIdentifier,
      BatchSchema batchSchema,
      List<String> partitionColumnNames,
      DatasetConfig datasetConfig,
      OperatorStats operatorStats,
      PartitionSpec partitionSpec,
      FileType fileType) {
    IcebergCommand icebergCommand =
        getIcebergCommandWithMetricStat(tableIdentifier, IcebergCommitOrigin.FULL_METADATA_REFRESH);
    return new FullMetadataRefreshCommitter(
        tableName,
        datasetPath,
        tableLocation,
        tableUuid,
        batchSchema,
        configuration,
        partitionColumnNames,
        icebergCommand,
        client,
        datasetConfig,
        operatorStats,
        partitionSpec,
        plugin,
        fileType);
  }

  @Override
  public IcebergOpCommitter getIncrementalMetadataRefreshCommitter(
      OperatorContext operatorContext,
      String tableName,
      List<String> datasetPath,
      String tableLocation,
      String tableUuid,
      IcebergTableIdentifier tableIdentifier,
      BatchSchema batchSchema,
      List<String> partitionColumnNames,
      boolean isFileSystem,
      DatasetConfig datasetConfig,
      FileSystem fs,
      Long metadataExpireAfterMs,
      IcebergCommandType icebergOpType,
      FileType fileType,
      Long startingSnapshotId,
      boolean errorOnConcurrentRefresh) {
    IcebergCommand icebergCommand =
        getIcebergCommandWithMetricStat(
            tableIdentifier, IcebergCommitOrigin.INCREMENTAL_METADATA_REFRESH);
    return new IncrementalMetadataRefreshCommitter(
        operatorContext,
        tableName,
        datasetPath,
        tableLocation,
        tableUuid,
        batchSchema,
        configuration,
        partitionColumnNames,
        icebergCommand,
        isFileSystem,
        client,
        datasetConfig,
        fs,
        metadataExpireAfterMs,
        icebergOpType,
        fileType,
        startingSnapshotId,
        errorOnConcurrentRefresh);
  }

  @Override
  public IcebergOpCommitter getAlterTableCommitter(
      IcebergTableIdentifier tableIdentifier,
      ColumnOperations.AlterOperationType alterOperationType,
      BatchSchema droppedColumns,
      BatchSchema updatedColumns,
      String columnName,
      List<Field> columnTypes) {
    IcebergCommand icebergCommand =
        getIcebergCommandWithMetricStat(tableIdentifier, IcebergCommitOrigin.ALTER_TABLE);
    return new AlterTableCommitter(
        icebergCommand,
        alterOperationType,
        columnName,
        columnTypes,
        droppedColumns,
        updatedColumns);
  }

  @Override
  public IcebergOpCommitter getDmlCommitter(
      OperatorContext operatorContext,
      IcebergTableIdentifier tableIdentifier,
      DatasetConfig datasetConfig,
      IcebergCommandType commandType,
      Long startingSnapshotId,
      RowLevelOperationMode dmlWriteMode) {
    IcebergCommitOrigin commitOrigin = IcebergCommitOrigin.fromCommandType(commandType);
    IcebergCommand icebergCommand = getIcebergCommandWithMetricStat(tableIdentifier, commitOrigin);
    return new IcebergDmlOperationCommitter(
        operatorContext, icebergCommand, datasetConfig, startingSnapshotId, dmlWriteMode);
  }

  @Override
  public IcebergOpCommitter getPrimaryKeyUpdateCommitter(
      IcebergTableIdentifier tableIdentifier, List<Field> columns) {
    IcebergCommand icebergCommand =
        getIcebergCommandWithMetricStat(tableIdentifier, IcebergCommitOrigin.ALTER_TABLE);
    return new PrimaryKeyUpdateCommitter(icebergCommand, columns);
  }

  @Override
  public IcebergOpCommitter getOptimizeCommitter(
      OperatorStats operatorStats,
      IcebergTableIdentifier tableIdentifier,
      DatasetConfig datasetConfig,
      Long minInputFilesBeforeOptimize,
      Long snapshotId,
      IcebergTableProps icebergTableProps,
      FileSystem fs) {
    IcebergCommand icebergCommand =
        getIcebergCommandWithMetricStat(
            tableIdentifier, IcebergCommitOrigin.OPTIMIZE_REWRITE_DATA_TABLE);
    return new IcebergOptimizeOperationCommitter(
        icebergCommand,
        operatorStats,
        datasetConfig,
        minInputFilesBeforeOptimize,
        snapshotId,
        icebergTableProps,
        fs);
  }

  /** Utility to register an existing table to the catalog */
  @Override
  public void registerTable(IcebergTableIdentifier tableIdentifier, TableMetadata tableMetadata) {
    IcebergCommand icebergCommand =
        getIcebergCommandWithMetricStat(tableIdentifier, IcebergCommitOrigin.CREATE_TABLE);
    icebergCommand.registerTable(tableMetadata);
  }

  @Override
  public void rollbackTable(IcebergTableIdentifier tableIdentifier, RollbackOption rollbackOption) {
    IcebergCommand icebergCommand =
        getIcebergCommandWithMetricStat(tableIdentifier, IcebergCommitOrigin.ROLLBACK_TABLE);
    icebergCommand.rollback(rollbackOption);
  }

  @Override
  public List<SnapshotEntry> expireSnapshots(
      IcebergTableIdentifier tableIdentifier, long olderThanInMillis, int retainLast) {
    IcebergCommand icebergCommand =
        getIcebergCommandWithMetricStat(tableIdentifier, IcebergCommitOrigin.EXPIRE_SNAPSHOTS);
    return icebergCommand.expireSnapshots(olderThanInMillis, retainLast, false);
  }

  @Override
  public void truncateTable(IcebergTableIdentifier tableIdentifier) {
    IcebergCommand icebergCommand =
        getIcebergCommandWithMetricStat(tableIdentifier, IcebergCommitOrigin.TRUNCATE_TABLE);
    icebergCommand.truncateTable();
  }

  @Override
  public void deleteTable(IcebergTableIdentifier tableIdentifier) {
    IcebergCommand icebergCommand =
        getIcebergCommandWithMetricStat(tableIdentifier, IcebergCommitOrigin.DROP_TABLE);
    icebergCommand.deleteTable();
  }

  @Override
  public void alterTable(
      IcebergTableIdentifier tableIdentifier, AlterTableOption alterTableOption) {
    IcebergCommand icebergCommand =
        getIcebergCommandWithMetricStat(tableIdentifier, IcebergCommitOrigin.ALTER_TABLE);
    switch (alterTableOption.getType()) {
      case PARTITION_SPEC_UPDATE:
        icebergCommand.updatePartitionSpec((PartitionSpecAlterOption) alterTableOption);
        return;
      default:
        UserException.unsupportedError().message("Unsupported Operation");
    }
  }

  @Override
  public void deleteTableRootPointer(IcebergTableIdentifier tableIdentifier) {
    IcebergCommand icebergCommand =
        getIcebergCommandWithMetricStat(tableIdentifier, IcebergCommitOrigin.DROP_TABLE);
    icebergCommand.deleteTableRootPointer();
  }

  @Override
  public String addColumns(
      IcebergTableIdentifier tableIdentifier, List<Types.NestedField> columnsToAdd) {
    IcebergCommand icebergCommand =
        getIcebergCommandWithMetricStat(tableIdentifier, IcebergCommitOrigin.ALTER_TABLE);
    icebergCommand.addColumns(columnsToAdd);
    return icebergCommand.getRootPointer();
  }

  @Override
  public String dropColumn(IcebergTableIdentifier tableIdentifier, String columnToDrop) {
    IcebergCommand icebergCommand =
        getIcebergCommandWithMetricStat(tableIdentifier, IcebergCommitOrigin.ALTER_TABLE);
    icebergCommand.dropColumn(columnToDrop);
    return icebergCommand.getRootPointer();
  }

  @Override
  public String changeColumn(
      IcebergTableIdentifier tableIdentifier, String columnToChange, Field newDef) {
    IcebergCommand icebergCommand =
        getIcebergCommandWithMetricStat(tableIdentifier, IcebergCommitOrigin.ALTER_TABLE);
    icebergCommand.changeColumn(columnToChange, newDef);
    return icebergCommand.getRootPointer();
  }

  @Override
  public void replaceSortOrder(IcebergTableIdentifier tableIdentifier, List<String> sortOrder) {
    IcebergCommand icebergCommand =
        getIcebergCommandWithMetricStat(tableIdentifier, IcebergCommitOrigin.ALTER_TABLE);
    icebergCommand.replaceSortOrder(sortOrder);
  }

  @Override
  public void updateTableProperties(
      IcebergTableIdentifier tableIdentifier, Map<String, String> properties) {
    IcebergCommand icebergCommand =
        getIcebergCommandWithMetricStat(tableIdentifier, IcebergCommitOrigin.ALTER_TABLE);
    icebergCommand.updateProperties(properties);
  }

  @Override
  public void removeTableProperties(
      IcebergTableIdentifier tableIdentifier, List<String> propertyNames) {
    IcebergCommand icebergCommand =
        getIcebergCommandWithMetricStat(tableIdentifier, IcebergCommitOrigin.ALTER_TABLE);
    icebergCommand.removeProperties(propertyNames);
  }

  @Override
  public String renameColumn(IcebergTableIdentifier tableIdentifier, String name, String newName) {
    IcebergCommand icebergCommand =
        getIcebergCommandWithMetricStat(tableIdentifier, IcebergCommitOrigin.ALTER_TABLE);
    icebergCommand.renameColumn(name, newName);
    return icebergCommand.getRootPointer();
  }

  @Override
  public String updatePrimaryKey(IcebergTableIdentifier tableIdentifier, List<Field> columns) {
    IcebergCommand icebergCommand =
        getIcebergCommandWithMetricStat(tableIdentifier, IcebergCommitOrigin.ALTER_TABLE);
    icebergCommand.updatePrimaryKey(columns);
    return icebergCommand.getRootPointer();
  }

  @Override
  public Table getIcebergTable(IcebergTableIdentifier tableIdentifier) {
    // unable to use IcebergCommitOrigin.READ_ONLY because callers might perform commits
    IcebergCommand icebergCommand = getIcebergCommandWithMetricStat(tableIdentifier, null);
    return icebergCommand.loadTable();
  }

  @Override
  public IcebergTableLoader getIcebergTableLoader(IcebergTableIdentifier tableIdentifier) {
    // unable to use IcebergCommitOrigin.READ_ONLY because callers might perform commits
    IcebergCommand icebergCommand = getIcebergCommandWithMetricStat(tableIdentifier, null);
    return new IcebergTableLoader(icebergCommand);
  }

  @Override
  public long propertyAsLong(
      IcebergTableIdentifier tableIdentifier, String propertyName, long defaultValue) {
    IcebergCommand icebergCommand =
        getIcebergCommandWithMetricStat(tableIdentifier, IcebergCommitOrigin.READ_ONLY);
    return icebergCommand.propertyAsLong(propertyName, defaultValue);
  }

  protected UserBitShared.QueryId currentQueryId() {
    if (operatorContext != null && operatorContext.getFragmentHandle() != null) {
      return operatorContext.getFragmentHandle().getQueryId();
    } else {
      return null;
    }
  }
}
