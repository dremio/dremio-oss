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
package com.dremio.plugins.dataplane.store;

import java.util.function.Supplier;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;

import com.dremio.connector.metadata.BytesOutput;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.MutablePlugin;
import com.dremio.exec.catalog.VersionedDatasetAdapter;
import com.dremio.exec.catalog.VersionedPlugin;
import com.dremio.exec.store.VersionedDatasetHandle;
import com.dremio.exec.store.iceberg.BaseIcebergExecutionDatasetAccessor;
import com.dremio.exec.store.iceberg.TableSchemaProvider;
import com.dremio.exec.store.iceberg.TableSnapshotProvider;
import com.dremio.options.OptionResolver;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.namespace.file.proto.IcebergFileConfig;
import com.dremio.service.namespace.file.proto.ParquetFileConfig;

import io.opentelemetry.instrumentation.annotations.WithSpan;

/**
 * Iceberg dataset accessor that provides metadata of an Iceberg table; the returned metadata should not be
 * persisted.
 */
public class TransientIcebergMetadataProvider extends BaseIcebergExecutionDatasetAccessor implements VersionedDatasetHandle {

  private final Supplier<Table> tableSupplier;
  private String contentId;
  private String uniqueInstanceId;

  public TransientIcebergMetadataProvider(
      EntityPath datasetPath,
      Supplier<Table> tableSupplier,
      Configuration configuration,
      TableSnapshotProvider tableSnapshotProvider,
      MutablePlugin plugin,
      TableSchemaProvider tableSchemaProvider,
      OptionResolver optionResolver,
      String contentId, // This ContentId in the Nessie ContentId and applies to iceberg tables  with root pointers in nessie. In other cases can be null or a random string
      String uniqueInstanceId // uuid extracted from the Iceberg metadata location
  ) {
    super(datasetPath, tableSupplier, configuration, tableSnapshotProvider, plugin, tableSchemaProvider, optionResolver);
    this.tableSupplier = tableSupplier;
    this.contentId = contentId;
    this.uniqueInstanceId = uniqueInstanceId;
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
    // metadata for dataplane table is not persisted
    return BytesOutput.NONE;
  }

  @Override
  public VersionedPlugin.EntityType getType() {
    return VersionedPlugin.EntityType.ICEBERG_TABLE;
  }

  @WithSpan
  @Override
  public DremioTable translateToDremioTable(VersionedDatasetAdapter vda, String accesssUserName) {
    return vda.translateIcebergTable(accesssUserName);
  }

  @Override
  public String getUniqueInstanceId() {
    return uniqueInstanceId;
  }

  @Override
  public String getContentId() {
    return contentId;
  }
}
