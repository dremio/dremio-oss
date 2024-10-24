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
package com.dremio.plugins.icebergcatalog.store;

import static com.dremio.service.namespace.dataset.proto.DatasetType.PHYSICAL_DATASET;

import com.dremio.connector.metadata.BytesOutput;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.exec.catalog.MutablePlugin;
import com.dremio.exec.store.iceberg.BaseIcebergExecutionDatasetAccessor;
import com.dremio.exec.store.iceberg.TableSchemaProvider;
import com.dremio.exec.store.iceberg.TableSnapshotProvider;
import com.dremio.options.OptionResolver;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.namespace.file.proto.IcebergFileConfig;
import com.dremio.service.namespace.file.proto.ParquetFileConfig;
import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;

public class IcebergCatalogTableProvider extends BaseIcebergExecutionDatasetAccessor {

  private final Supplier<Table> tableSupplier;

  public IcebergCatalogTableProvider(
      EntityPath datasetPath,
      Supplier<Table> tableSupplier,
      Configuration configuration,
      TableSnapshotProvider tableSnapshotProvider,
      MutablePlugin plugin,
      TableSchemaProvider tableSchemaProvider,
      OptionResolver optionResolver) {
    super(
        datasetPath,
        tableSupplier,
        configuration,
        tableSnapshotProvider,
        plugin,
        tableSchemaProvider,
        optionResolver);
    this.tableSupplier = tableSupplier;
  }

  @Override
  protected FileConfig getFileConfig() {
    return new IcebergFileConfig()
        .setParquetDataFormat(new ParquetFileConfig())
        .asFileConfig()
        .setLocation(tableSupplier.get().location());
  }

  @Override
  public BytesOutput provideSignature(DatasetMetadata metadata) {
    // metadata is not persisted
    return BytesOutput.NONE;
  }

  @Override
  public DatasetType getDatasetType() {
    return PHYSICAL_DATASET;
  }
}
