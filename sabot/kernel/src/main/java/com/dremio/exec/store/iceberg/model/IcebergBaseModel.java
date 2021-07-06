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

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;

import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.metadatarefresh.DatasetCatalogGrpcClient;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OperatorContext;

/**
 * Base class for common Iceberg model operations
 */
public abstract class IcebergBaseModel implements IcebergModel {

    protected static final String EMPTY_NAMESPACE = "";
    protected final String namespace;
    protected final Configuration configuration;
    protected final FileSystem fs; /* if fs is null it will use iceberg HadoopFileIO class instead of DremioFileIO class */
    protected final OperatorContext context;
    protected final List<String> dataset;
    private final DatasetCatalogGrpcClient client;

  protected IcebergBaseModel(String namespace,
                               Configuration configuration,
                               FileSystem fs, OperatorContext context, List<String> dataset,
                               DatasetCatalogGrpcClient datasetCatalogGrpcClient) {
        this.namespace = namespace;
        this.configuration = configuration;
        this.fs = fs;
        this.context = context;
        this.dataset = dataset;
        this.client = datasetCatalogGrpcClient;
    }

    protected abstract IcebergCommand getIcebergCommand(IcebergTableIdentifier tableIdentifier);

    @Override
    public IcebergOpCommitter getCreateTableCommitter(String tableName, IcebergTableIdentifier tableIdentifier,
                                                      BatchSchema batchSchema, List<String> partitionColumnNames) {
        IcebergCommand icebergCommand = getIcebergCommand(tableIdentifier);
        return new IcebergTableCreationCommitter(tableName, batchSchema, partitionColumnNames, icebergCommand);
    }

    @Override
    public IcebergOpCommitter getInsertTableCommitter(IcebergTableIdentifier tableIdentifier) {
        IcebergCommand icebergCommand = getIcebergCommand(tableIdentifier);
        return new IcebergInsertOperationCommitter(icebergCommand);
    }

  @Override
  public IcebergOpCommitter getFullMetadataRefreshCommitter(String tableName, String tableLocation, IcebergTableIdentifier tableIdentifier,
                                                            BatchSchema batchSchema, List<String> partitionColumnNames) {
    IcebergCommand icebergCommand = getIcebergCommand(tableIdentifier);
    return new FullMetadataRefreshCommitter(tableName, tableLocation, batchSchema, partitionColumnNames, icebergCommand, client);
  }

  @Override
  public IcebergOpCommitter getIncrementalMetadataRefreshCommitter(String tableName, String tableLocation, IcebergTableIdentifier tableIdentifier,
                                                                   BatchSchema batchSchema, List<String> partitionColumnNames) {
    IcebergCommand icebergCommand = getIcebergCommand(tableIdentifier);
    return new IncrementalMetadataRefreshCommitter(tableName, tableLocation, batchSchema, partitionColumnNames, icebergCommand, client);
  }

    @Override
    public void truncateTable(IcebergTableIdentifier tableIdentifier) {
        IcebergCommand icebergCommand = getIcebergCommand(tableIdentifier);
        icebergCommand.truncateTable();
    }

    @Override
    public void addColumns(IcebergTableIdentifier tableIdentifier, List<Types.NestedField> columnsToAdd) {
        IcebergCommand icebergCommand = getIcebergCommand(tableIdentifier);
        icebergCommand.addColumns(columnsToAdd);
    }

    @Override
    public void dropColumn(IcebergTableIdentifier tableIdentifier, String columnToDrop) {
        IcebergCommand icebergCommand = getIcebergCommand(tableIdentifier);
        icebergCommand.dropColumn(columnToDrop);
    }

    @Override
    public void changeColumn(IcebergTableIdentifier tableIdentifier, String columnToChange, Field newDef) {
        IcebergCommand icebergCommand = getIcebergCommand(tableIdentifier);
        icebergCommand.changeColumn(columnToChange, newDef);
    }

    @Override
    public void renameColumn(IcebergTableIdentifier tableIdentifier, String name, String newName) {
        IcebergCommand icebergCommand = getIcebergCommand(tableIdentifier);
        icebergCommand.renameColumn(name, newName);
    }

    @Override
    public Table getIcebergTable(IcebergTableIdentifier tableIdentifier) {
        IcebergCommand icebergCommand = getIcebergCommand(tableIdentifier);
        return icebergCommand.loadTable();
    }
}
