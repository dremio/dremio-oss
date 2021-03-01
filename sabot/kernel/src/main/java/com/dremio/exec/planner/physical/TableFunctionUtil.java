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
package com.dremio.exec.planner.physical;

import java.util.ArrayList;
import java.util.List;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.physical.config.TableFunctionContext;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.parquet.ParquetFilterCondition;
import com.google.common.collect.ImmutableList;

/**
 * Utility functions related to table functions
 */
public class TableFunctionUtil {

  public static <E, T> TableFunctionContext getManifestScanTableFunctionContext(
    final TableMetadata tableMetadata,
    List<SchemaPath> columns,
    BatchSchema schema,
    List<ParquetFilterCondition> conditions) {
    return new TableFunctionContext(
      tableMetadata.getFormatSettings(), schema,
      tableMetadata.getSchema(),
      ImmutableList.of(tableMetadata.getName().getPathComponents()), conditions,
      tableMetadata.getStoragePluginId(),
      columns,
      tableMetadata.getReadDefinition().getPartitionColumnsList(), null,
      tableMetadata.getReadDefinition().getExtendedProperty(), false);
  }

  private static List<SchemaPath> getSplitGenSchemaColumns() {
    List<SchemaPath> schemaPathList = new ArrayList<>();
    schemaPathList.add(new SchemaPath(RecordReader.SPLIT_INFORMATION));
    return schemaPathList;
  }

  public static TableFunctionContext getParquetScanTableFunctionContext(
    final TableMetadata tableMetadata,
    List<ParquetFilterCondition> conditions,
    List<SchemaPath> columns,
    boolean arrowCachingEnabled) {
    final BatchSchema schema = tableMetadata.getSchema().maskAndReorder(columns);
    return new TableFunctionContext(
      tableMetadata.getFormatSettings(), schema,
      tableMetadata.getSchema(),
      ImmutableList.of(tableMetadata.getName().getPathComponents()), conditions,
      tableMetadata.getStoragePluginId(), columns,
      tableMetadata.getReadDefinition().getPartitionColumnsList(), null,
      tableMetadata.getReadDefinition().getExtendedProperty(),
      arrowCachingEnabled
    );
  }

  public static <E, T> TableFunctionContext getSplitProducerTableFunctionContext(
          final TableMetadata tableMetadata,
          List<ParquetFilterCondition> conditions) {
    return new TableFunctionContext(
            tableMetadata.getFormatSettings(), RecordReader.SPLIT_GEN_SCAN_SCHEMA,
            tableMetadata.getSchema(),
            ImmutableList.of(tableMetadata.getName().getPathComponents()), conditions,
            tableMetadata.getStoragePluginId(),
            getSplitGenSchemaColumns(),
            tableMetadata.getReadDefinition().getPartitionColumnsList(), null,
            tableMetadata.getReadDefinition().getExtendedProperty(), false);
  }

  public static TableFunctionConfig getParquetScanTableFunctionConfig(
    final TableMetadata tableMetadata,
    List<ParquetFilterCondition> conditions,
    List<SchemaPath> columns,
    boolean arrowCachingEnabled) {
    TableFunctionContext tableFunctionContext = getParquetScanTableFunctionContext(tableMetadata, conditions, columns, arrowCachingEnabled);
    return new TableFunctionConfig(TableFunctionConfig.FunctionType.PARQUET_DATA_SCAN, false, tableFunctionContext);
  }

  public static TableFunctionConfig getManifestScanTableFunctionConfig(
      final TableMetadata tableMetadata,
      List<SchemaPath> columns,
      BatchSchema schema,
      List<ParquetFilterCondition> conditions) {
    TableFunctionContext tableFunctionContext = getManifestScanTableFunctionContext(tableMetadata, columns, schema, conditions);
    return new TableFunctionConfig(TableFunctionConfig.FunctionType.ICEBERG_MANIFEST_SCAN, true, tableFunctionContext);
  }

  public static TableFunctionConfig getSplitGenFunctionConfig(
          final TableMetadata tableMetadata,
          List<ParquetFilterCondition> conditions) {
    TableFunctionContext tableFunctionContext = getSplitProducerTableFunctionContext(tableMetadata, conditions);
    return new TableFunctionConfig(TableFunctionConfig.FunctionType.SPLIT_GENERATION, true, tableFunctionContext);
  }
}
