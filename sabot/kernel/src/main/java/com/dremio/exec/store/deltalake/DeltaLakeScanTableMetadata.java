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
package com.dremio.exec.store.deltalake;

import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_FIELD_ADD;
import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_FIELD_REMOVE;
import static com.dremio.exec.store.deltalake.DeltaConstants.SCHEMA_MODIFICATION_TIME;
import static com.dremio.exec.store.deltalake.DeltaConstants.SCHEMA_PATH;
import static com.dremio.exec.store.deltalake.DeltaConstants.SCHEMA_SIZE;

import java.util.List;

import com.dremio.common.expression.CompleteType;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.TableMetadataImpl;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.SchemaBuilder;
import com.dremio.exec.store.SplitsPointer;
import com.dremio.exec.store.TableMetadata;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;

/**
 * DeltaLakeScan table metadata, which extends TableMetadataImpl
 */
public class DeltaLakeScanTableMetadata extends TableMetadataImpl {
  private final boolean scanForAddedPaths;
  private final BatchSchema schema;

  public DeltaLakeScanTableMetadata(StoragePluginId plugin, DatasetConfig config, String user, SplitsPointer splits, List<String> primaryKeys, boolean scanForAddedPaths) {
    super(plugin, config, user, splits, primaryKeys);
    this.scanForAddedPaths = scanForAddedPaths;
    this.schema = deriveDeltaLakeScanTableSchema();
  }

  public static DeltaLakeScanTableMetadata createWithTableMetadata(TableMetadata tableMetadata, boolean scanForAddedPaths) {
    return new DeltaLakeScanTableMetadata(tableMetadata.getStoragePluginId(), tableMetadata.getDatasetConfig(),
      tableMetadata.getUser(), (SplitsPointer) tableMetadata.getSplitsKey(), tableMetadata.getPrimaryKey(), scanForAddedPaths);
  }

  @Override
  public BatchSchema getSchema() {
    return schema;
  }

  private BatchSchema deriveDeltaLakeScanTableSchema() {
    final SchemaBuilder schemaBuilder = BatchSchema.newBuilder();
    if (scanForAddedPaths) {
      schemaBuilder.addField(CompleteType.struct(
        // Schema for added paths
        CompleteType.VARCHAR.toField(SCHEMA_PATH),
        CompleteType.BIGINT.toField(SCHEMA_SIZE),
        CompleteType.BIGINT.toField(SCHEMA_MODIFICATION_TIME))
        .toField(DELTA_FIELD_ADD));
    } else {
      schemaBuilder.addField(CompleteType.struct(
        // Schema for removed paths
        CompleteType.VARCHAR.toField(SCHEMA_PATH))
        .toField(DELTA_FIELD_REMOVE));
    }
    return schemaBuilder.build();
  }
}
