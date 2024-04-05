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
package com.dremio.exec.store.dfs.system;

import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.BytesOutput;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.exec.store.iceberg.BaseIcebergExecutionDatasetAccessor;
import com.dremio.exec.store.iceberg.TableSchemaProvider;
import com.dremio.exec.store.iceberg.TableSnapshotProvider;
import com.dremio.options.OptionResolver;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.namespace.file.proto.IcebergFileConfig;
import com.dremio.service.namespace.file.proto.ParquetFileConfig;
import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;

/**
 * A dataset handle for accessing and interacting with a "Copy Into Errors" table using the Iceberg
 * data format.
 */
public class SystemIcebergTablesExecutionDatasetAccessor
    extends BaseIcebergExecutionDatasetAccessor {

  private final Supplier<Table> tableSupplier;

  /**
   * Constructs a new {@link SystemIcebergTablesExecutionDatasetAccessor}.
   *
   * @param entityPath The entity path representing the dataset.
   * @param tableSupplier A supplier function to provide the Iceberg {@link Table} instance.
   * @param configuration The configuration for the dataset accessor.
   * @param tableSnapshotProvider The provider for table snapshots.
   * @param plugin The {@link SystemIcebergTablesStoragePlugin} instance.
   * @param tableSchemaProvider The provider for table schema.
   * @param optionResolver The option resolver for dataset options.
   */
  SystemIcebergTablesExecutionDatasetAccessor(
      EntityPath entityPath,
      Supplier<Table> tableSupplier,
      Configuration configuration,
      TableSnapshotProvider tableSnapshotProvider,
      SystemIcebergTablesStoragePlugin plugin,
      TableSchemaProvider tableSchemaProvider,
      OptionResolver optionResolver) {
    super(
        entityPath,
        tableSupplier,
        configuration,
        tableSnapshotProvider,
        plugin,
        tableSchemaProvider,
        optionResolver);
    this.tableSupplier = tableSupplier;
  }

  @Override
  public BytesOutput provideSignature(DatasetMetadata metadata) throws ConnectorException {
    return BytesOutput.NONE;
  }

  @Override
  protected FileConfig getFileConfig() {
    return new IcebergFileConfig()
        .setParquetDataFormat(new ParquetFileConfig())
        .asFileConfig()
        .setLocation(tableSupplier.get().location());
  }
}
