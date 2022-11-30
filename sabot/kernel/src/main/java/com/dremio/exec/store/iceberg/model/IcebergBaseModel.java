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
import java.util.concurrent.TimeUnit;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.AlterTableOption;
import com.dremio.exec.catalog.MutablePlugin;
import com.dremio.exec.catalog.PartitionSpecAlterOption;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.dfs.ColumnOperations;
import com.dremio.exec.store.metadatarefresh.committer.DatasetCatalogGrpcClient;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.writer.WriterCommitterOperator;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.google.common.base.Stopwatch;

/**
 * Base class for common Iceberg model operations
 */
public abstract class IcebergBaseModel implements IcebergModel {

    protected static final String EMPTY_NAMESPACE = "";
    protected final String namespace;
    protected final Configuration configuration;
    protected final FileSystem fs; /* if fs is null it will use iceberg HadoopFileIO class instead of DremioFileIO class */
    protected final OperatorContext context;
    private final DatasetCatalogGrpcClient client;
    private final MutablePlugin plugin;

  protected IcebergBaseModel(String namespace,
                               Configuration configuration,
                               FileSystem fs, OperatorContext context,
                               DatasetCatalogGrpcClient datasetCatalogGrpcClient, MutablePlugin plugin) {
        this.namespace = namespace;
        this.configuration = configuration;
        this.fs = fs;
        this.context = context;
        this.client = datasetCatalogGrpcClient;
        this.plugin = plugin;
    }

  protected abstract IcebergCommand getIcebergCommand(IcebergTableIdentifier tableIdentifier);

  private  IcebergCommand getIcebergCommandWithMetricStat(IcebergTableIdentifier tableIdentifier) {
    Stopwatch stopwatch = Stopwatch.createStarted();
    try {
      return getIcebergCommand(tableIdentifier);
    } finally {
      long icebergCommandTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);
      if (context != null && context.getStats() != null) {
        context.getStats().addLongStat(WriterCommitterOperator.Metric.ICEBERG_COMMAND_CREATE_TIME, icebergCommandTime);
      }
    }
  }

  @Override
  public IcebergOpCommitter getCreateTableCommitter(String tableName, IcebergTableIdentifier tableIdentifier,
                                                    BatchSchema batchSchema, List<String> partitionColumnNames, OperatorStats operatorStats, PartitionSpec partitionSpec) {
    IcebergCommand icebergCommand = getIcebergCommandWithMetricStat(tableIdentifier);
    return new IcebergTableCreationCommitter(tableName, batchSchema, partitionColumnNames, icebergCommand, operatorStats, partitionSpec);
  }

  @Override
  public IcebergOpCommitter getInsertTableCommitter(IcebergTableIdentifier tableIdentifier, OperatorStats operatorStats) {
    IcebergCommand icebergCommand = getIcebergCommandWithMetricStat(tableIdentifier);
    return new IcebergInsertOperationCommitter(icebergCommand, operatorStats);
  }

  @Override
  public IcebergOpCommitter getFullMetadataRefreshCommitter(String tableName, List<String> datasetPath, String tableLocation,
                                                            String tableUuid, IcebergTableIdentifier tableIdentifier,
                                                            BatchSchema batchSchema, List<String> partitionColumnNames,
                                                            DatasetConfig datasetConfig, OperatorStats operatorStats, PartitionSpec partitionSpec) {
    IcebergCommand icebergCommand = getIcebergCommandWithMetricStat(tableIdentifier);
    return new FullMetadataRefreshCommitter(tableName, datasetPath, tableLocation, tableUuid, batchSchema, configuration,
      partitionColumnNames, icebergCommand, client, datasetConfig, operatorStats, partitionSpec, plugin);
  }

  @Override
  public IcebergOpCommitter getIncrementalMetadataRefreshCommitter(OperatorContext operatorContext, String tableName, List<String> datasetPath, String tableLocation,
                                                                   String tableUuid, IcebergTableIdentifier tableIdentifier,
                                                                   BatchSchema batchSchema, List<String> partitionColumnNames,
                                                                   boolean isFileSystem, DatasetConfig datasetConfig) {
    IcebergCommand icebergCommand = getIcebergCommandWithMetricStat(tableIdentifier);
    return new IncrementalMetadataRefreshCommitter(operatorContext, tableName, datasetPath, tableLocation, tableUuid, batchSchema, configuration,
      partitionColumnNames, icebergCommand, isFileSystem, client, datasetConfig, plugin);
  }

  @Override
  public IcebergOpCommitter getAlterTableCommitter(IcebergTableIdentifier tableIdentifier, ColumnOperations.AlterOperationType alterOperationType, BatchSchema droppedColumns, BatchSchema updatedColumns,
                                                   String columnName, List<Field> columnTypes) {
    IcebergCommand icebergCommand = getIcebergCommandWithMetricStat(tableIdentifier);
    return new AlterTableCommitter(icebergCommand, alterOperationType, columnName, columnTypes, droppedColumns, updatedColumns);
  }

  @Override
  public IcebergOpCommitter getDmlCommitter(OperatorStats operatorStats,
                                            IcebergTableIdentifier tableIdentifier,
                                            DatasetConfig datasetConfig) {
    IcebergCommand icebergCommand = getIcebergCommandWithMetricStat(tableIdentifier);
    return new IcebergDmlOperationCommitter(icebergCommand, operatorStats, datasetConfig);
  }

  @Override
  public IcebergOpCommitter getPrimaryKeyUpdateCommitter(IcebergTableIdentifier tableIdentifier, List<Field> columns) {
    IcebergCommand icebergCommand = getIcebergCommandWithMetricStat(tableIdentifier);
    return new PrimaryKeyUpdateCommitter(icebergCommand, columns);
  }

  @Override
  public void truncateTable(IcebergTableIdentifier tableIdentifier) {
    IcebergCommand icebergCommand = getIcebergCommandWithMetricStat(tableIdentifier);
    icebergCommand.truncateTable();
  }

  @Override
  public void deleteTable(IcebergTableIdentifier tableIdentifier) {
    IcebergCommand icebergCommand = getIcebergCommandWithMetricStat(tableIdentifier);
    icebergCommand.deleteTable();
  }

  @Override
  public void alterTable(IcebergTableIdentifier tableIdentifier, AlterTableOption alterTableOption) {
    IcebergCommand icebergCommand = getIcebergCommandWithMetricStat(tableIdentifier);
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
    IcebergCommand icebergCommand = getIcebergCommandWithMetricStat(tableIdentifier);
    icebergCommand.deleteTableRootPointer();
  }

  @Override
  public String addColumns(IcebergTableIdentifier tableIdentifier, List<Types.NestedField> columnsToAdd) {
    IcebergCommand icebergCommand = getIcebergCommandWithMetricStat(tableIdentifier);
    icebergCommand.addColumns(columnsToAdd);
    return icebergCommand.getRootPointer();
  }

  @Override
  public String dropColumn(IcebergTableIdentifier tableIdentifier, String columnToDrop) {
    IcebergCommand icebergCommand = getIcebergCommandWithMetricStat(tableIdentifier);
    icebergCommand.dropColumn(columnToDrop);
    return icebergCommand.getRootPointer();
  }

  @Override
  public String changeColumn(IcebergTableIdentifier tableIdentifier, String columnToChange, Field newDef) {
    IcebergCommand icebergCommand = getIcebergCommandWithMetricStat(tableIdentifier);
    icebergCommand.changeColumn(columnToChange, newDef);
    return icebergCommand.getRootPointer();
  }

  @Override
  public String renameColumn(IcebergTableIdentifier tableIdentifier, String name, String newName) {
    IcebergCommand icebergCommand = getIcebergCommand(tableIdentifier);
    icebergCommand.renameColumn(name, newName);
    return icebergCommand.getRootPointer();
  }

  @Override
  public String updatePrimaryKey(IcebergTableIdentifier tableIdentifier, List<Field> columns) {
    IcebergCommand icebergCommand = getIcebergCommandWithMetricStat(tableIdentifier);
    icebergCommand.updatePrimaryKey(columns);
    return icebergCommand.getRootPointer();
  }

  @Override
  public Table getIcebergTable(IcebergTableIdentifier tableIdentifier) {
    IcebergCommand icebergCommand = getIcebergCommandWithMetricStat(tableIdentifier);
    return icebergCommand.loadTable();
  }

  @Override
  public IcebergTableLoader getIcebergTableLoader(IcebergTableIdentifier tableIdentifier) {
    IcebergCommand icebergCommand = getIcebergCommandWithMetricStat(tableIdentifier);
    return new IcebergTableLoader(icebergCommand);
  }
}
