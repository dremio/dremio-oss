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
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.compress.utils.Lists;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.physical.config.TableFunctionContext;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.ScanFilter;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.iceberg.InternalIcebergScanTableMetadata;
import com.dremio.exec.store.metadatarefresh.MetadataRefreshExecConstants;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.google.common.collect.ImmutableList;

/**
 * Utility functions related to table functions
 */
public class TableFunctionUtil {

  private static StoragePluginId getInternalTablePluginId(TableMetadata tableMetadata) {
      if (tableMetadata instanceof InternalIcebergScanTableMetadata) {
        return((InternalIcebergScanTableMetadata) tableMetadata).getIcebergTableStoragePlugin();
      }
      return null;
  }

  public static <E, T> TableFunctionContext getManifestScanTableFunctionContext(
    final TableMetadata tableMetadata,
    List<SchemaPath> columns,
    BatchSchema schema,
    ScanFilter scanFilter) {
    return new TableFunctionContext(
      tableMetadata.getFormatSettings(), schema,
      tableMetadata.getSchema(),
      ImmutableList.of(tableMetadata.getName().getPathComponents()), scanFilter,
      tableMetadata.getStoragePluginId(),
      getInternalTablePluginId(tableMetadata),
      columns,
      tableMetadata.getReadDefinition().getPartitionColumnsList(), null,
      tableMetadata.getReadDefinition().getExtendedProperty(), false);
  }

  public static List<SchemaPath> getSplitGenSchemaColumns() {
    List<SchemaPath> schemaPathList = new ArrayList<>();
    schemaPathList.add(new SchemaPath(RecordReader.SPLIT_IDENTITY));
    schemaPathList.add(new SchemaPath(RecordReader.SPLIT_INFORMATION));
    schemaPathList.add(new SchemaPath(RecordReader.COL_IDS));
    return schemaPathList;
  }

  public static TableFunctionContext getDataFileScanTableFunctionContext(
    final TableMetadata tableMetadata,
    ScanFilter scanFilter,
    List<SchemaPath> columns,
    boolean arrowCachingEnabled) {
    final BatchSchema schema = tableMetadata.getSchema().maskAndReorder(columns);
    return new TableFunctionContext(
      tableMetadata.getFormatSettings(), schema,
      tableMetadata.getSchema(),
      ImmutableList.of(tableMetadata.getName().getPathComponents()), scanFilter,
      tableMetadata.getStoragePluginId(), getInternalTablePluginId(tableMetadata), columns,
      tableMetadata.getReadDefinition().getPartitionColumnsList(), null,
      tableMetadata.getReadDefinition().getExtendedProperty(),
      arrowCachingEnabled
    );
  }

  public static <E, T> TableFunctionContext getSplitProducerTableFunctionContext(
    final TableMetadata tableMetadata,
    ScanFilter scanFilter) {
    return new TableFunctionContext(
      tableMetadata.getFormatSettings(), RecordReader.SPLIT_GEN_AND_COL_IDS_SCAN_SCHEMA,
      tableMetadata.getSchema(),
      ImmutableList.of(tableMetadata.getName().getPathComponents()), scanFilter,
      tableMetadata.getStoragePluginId(), getInternalTablePluginId(tableMetadata),
      getSplitGenSchemaColumns(),
      tableMetadata.getReadDefinition().getPartitionColumnsList(), null,
      tableMetadata.getReadDefinition().getExtendedProperty(), false);
  }

  public static TableFunctionConfig getDataFileScanTableFunctionConfig(
    final TableMetadata tableMetadata,
    ScanFilter scanFilter,
    List<SchemaPath> columns,
    boolean arrowCachingEnabled) {
    TableFunctionContext tableFunctionContext = getDataFileScanTableFunctionContext(tableMetadata, scanFilter, columns, arrowCachingEnabled);
    return new TableFunctionConfig(TableFunctionConfig.FunctionType.DATA_FILE_SCAN, false, tableFunctionContext);
  }

  public static TableFunctionConfig getManifestScanTableFunctionConfig(
      final TableMetadata tableMetadata,
      List<SchemaPath> columns,
      BatchSchema schema,
      ScanFilter scanFilter) {
    TableFunctionContext tableFunctionContext = getManifestScanTableFunctionContext(tableMetadata, columns, schema, scanFilter);
    return new TableFunctionConfig(TableFunctionConfig.FunctionType.SPLIT_GEN_MANIFEST_SCAN, true, tableFunctionContext);
  }

  public static TableFunctionConfig getSplitGenFunctionConfig(
          final TableMetadata tableMetadata,
          ScanFilter scanFilter) {
    TableFunctionContext tableFunctionContext = getSplitProducerTableFunctionContext(tableMetadata, scanFilter);
    return new TableFunctionConfig(TableFunctionConfig.FunctionType.SPLIT_GENERATION, true, tableFunctionContext);
  }

  public static TableFunctionConfig getFooterReadFunctionConfig(
    final TableMetadata tableMetadata,
    final BatchSchema tableSchema,
    ScanFilter scanFilter) {
    TableFunctionContext tableFunctionContext = getFooterReadTableFunctionContext(tableMetadata, tableSchema, scanFilter);
    return new TableFunctionConfig(TableFunctionConfig.FunctionType.FOOTER_READER, true, tableFunctionContext);
  }

  private static TableFunctionContext getFooterReadTableFunctionContext(TableMetadata tableMetadata, BatchSchema tableSchema, ScanFilter scanFilter) {
    return new TableFunctionContext(
      tableMetadata.getFormatSettings(), MetadataRefreshExecConstants.FooterRead.OUTPUT_SCHEMA.BATCH_SCHEMA,
      tableSchema,
      ImmutableList.of(tableMetadata.getName().getPathComponents()), scanFilter,
      tableMetadata.getStoragePluginId(), getInternalTablePluginId(tableMetadata),
      getFooterReadOutputSchemaColumns(),
      Lists.newArrayList(), Lists.newArrayList(),
      Optional.ofNullable(tableMetadata.getReadDefinition()).map(ReadDefinition::getExtendedProperty).orElse(null),
      false);
  }

  private static List<SchemaPath> getFooterReadOutputSchemaColumns() {
    return MetadataRefreshExecConstants.FooterRead.OUTPUT_SCHEMA.BATCH_SCHEMA.getFields()
      .stream()
      .map(field -> SchemaPath.getSimplePath(field.getName()))
      .collect(Collectors.toList());
  }

  public static TableFunctionConfig getInternalMetadataManifestScanTableFunctionConfig(
    final TableMetadata tableMetadata,
    List<SchemaPath> columns,
    BatchSchema schema,
    ScanFilter scanFilter) {
    TableFunctionContext tableFunctionContext = getManifestScanTableFunctionContext(tableMetadata, columns, schema, scanFilter);
    return new TableFunctionConfig(TableFunctionConfig.FunctionType.METADATA_REFRESH_MANIFEST_SCAN, true, tableFunctionContext);
  }
}
