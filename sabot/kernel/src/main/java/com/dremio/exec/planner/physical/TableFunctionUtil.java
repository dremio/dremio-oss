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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.commons.compress.utils.Lists;
import org.apache.iceberg.expressions.Expression;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.config.FooterReaderTableFunctionContext;
import com.dremio.exec.physical.config.SplitGenManifestScanTableFunctionContext;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.physical.config.TableFunctionContext;
import com.dremio.exec.planner.sql.CalciteArrowHelper;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.ScanFilter;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.iceberg.IcebergSerDe;
import com.dremio.exec.store.iceberg.InternalIcebergScanTableMetadata;
import com.dremio.exec.store.metadatarefresh.MetadataRefreshExecConstants;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.dremio.service.namespace.file.proto.FileType;
import com.google.common.collect.ImmutableList;

import io.protostuff.ByteString;

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
    ScanFilter scanFilter,
    Expression icebergAnyColExpression) {
    try {
    byte[] icebergAnyColExpressionbytes = IcebergSerDe.serializeToByteArray(icebergAnyColExpression);
    ByteString partitionSpecMap = null;
    if(tableMetadata.getDatasetConfig().getPhysicalDataset().getIcebergMetadata() != null){
      partitionSpecMap = tableMetadata.getDatasetConfig().getPhysicalDataset().getIcebergMetadata().getPartitionSpecs();
    }
      return new SplitGenManifestScanTableFunctionContext(partitionSpecMap,
              icebergAnyColExpressionbytes,
              tableMetadata.getFormatSettings(), schema,
              tableMetadata.getSchema(),
              ImmutableList.of(tableMetadata.getName().getPathComponents()), scanFilter,
              tableMetadata.getStoragePluginId(),
              getInternalTablePluginId(tableMetadata),
              columns,
              tableMetadata.getReadDefinition().getPartitionColumnsList(), null,
              tableMetadata.getReadDefinition().getExtendedProperty(), false, false, true,
              tableMetadata.getDatasetConfig().getPhysicalDataset().getInternalSchemaSettings());
    } catch (IOException e) {
      throw UserException.validationError().message("Not able serialized iceberg prune condititions" + icebergAnyColExpression.toString()).buildSilently();
    }
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
    boolean arrowCachingEnabled,
    boolean isConvertedIcebergDataset) {
    final BatchSchema schema = tableMetadata.getSchema().maskAndReorder(columns);
    return new TableFunctionContext(
      tableMetadata.getFormatSettings(),
      tableMetadata.getSchema(),
      schema,
      ImmutableList.of(tableMetadata.getName().getPathComponents()), scanFilter,
      tableMetadata.getStoragePluginId(), getInternalTablePluginId(tableMetadata), columns,
      tableMetadata.getReadDefinition().getPartitionColumnsList(), null,
      tableMetadata.getReadDefinition().getExtendedProperty(),
      arrowCachingEnabled, isConvertedIcebergDataset, false,
      tableMetadata.getDatasetConfig().getPhysicalDataset().getInternalSchemaSettings()
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
      tableMetadata.getReadDefinition().getExtendedProperty(), false, false, false,
      tableMetadata.getDatasetConfig().getPhysicalDataset().getInternalSchemaSettings());
  }

  public static TableFunctionConfig getDataFileScanTableFunctionConfig(
    final TableMetadata tableMetadata,
    ScanFilter scanFilter,
    List<SchemaPath> columns,
    boolean arrowCachingEnabled,
    boolean isConvertedIcebergDataset) {
    TableFunctionContext tableFunctionContext = getDataFileScanTableFunctionContext(tableMetadata, scanFilter, columns, arrowCachingEnabled, isConvertedIcebergDataset);
    return new TableFunctionConfig(TableFunctionConfig.FunctionType.DATA_FILE_SCAN, false, tableFunctionContext);
  }

  public static TableFunctionConfig getManifestScanTableFunctionConfig(
      final TableMetadata tableMetadata,
      List<SchemaPath> columns,
      BatchSchema schema,
      ScanFilter scanFilter,
      Expression icebergNonPartitionExpression) {
    TableFunctionContext tableFunctionContext = getManifestScanTableFunctionContext(tableMetadata, columns, schema, scanFilter, icebergNonPartitionExpression);
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
    ScanFilter scanFilter, FileType fileType) {
    TableFunctionContext tableFunctionContext = getFooterReadTableFunctionContext(tableMetadata, tableSchema, scanFilter, fileType);
    return new TableFunctionConfig(TableFunctionConfig.FunctionType.FOOTER_READER, true, tableFunctionContext);
  }

  private static TableFunctionContext getFooterReadTableFunctionContext(TableMetadata tableMetadata, BatchSchema tableSchema, ScanFilter scanFilter, FileType fileType) {
    return new FooterReaderTableFunctionContext(fileType,
      tableMetadata.getFormatSettings(), MetadataRefreshExecConstants.FooterRead.OUTPUT_SCHEMA.BATCH_SCHEMA,
      tableSchema,
      ImmutableList.of(tableMetadata.getName().getPathComponents()), scanFilter,
      tableMetadata.getStoragePluginId(), getInternalTablePluginId(tableMetadata),
      getFooterReadOutputSchemaColumns(),
      Lists.newArrayList(), Lists.newArrayList(),
      Optional.ofNullable(tableMetadata.getReadDefinition()).map(ReadDefinition::getExtendedProperty).orElse(null),
      false, false, false, tableMetadata.getDatasetConfig().getPhysicalDataset().getInternalSchemaSettings());
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
    TableFunctionContext tableFunctionContext = getManifestScanTableFunctionContext(tableMetadata, columns, schema, scanFilter, null);
    return new TableFunctionConfig(TableFunctionConfig.FunctionType.METADATA_REFRESH_MANIFEST_SCAN, true, tableFunctionContext);
  }

  public static Function<Prel, TableFunctionPrel> getHashExchangeTableFunctionCreator(final TableMetadata tableMetadata, boolean isIcebergMetadata) {
    return input -> getSplitAssignTableFunction(input, tableMetadata, isIcebergMetadata);
  }

  private static TableFunctionPrel getSplitAssignTableFunction(Prel input, TableMetadata tableMetadata, boolean isIcebergMetadata) {
    RelDataTypeFactory.FieldInfoBuilder fieldInfoBuilder = new RelDataTypeFactory.FieldInfoBuilder(input.getCluster().getTypeFactory());
    input.getRowType().getFieldList().forEach(f -> fieldInfoBuilder.add(f));
    RelDataType intType = CalciteArrowHelper.wrap(CompleteType.INT).toCalciteType(input.getCluster().getTypeFactory(), false);
    fieldInfoBuilder.add(HashPrelUtil.HASH_EXPR_NAME, intType);
    RelDataType output = fieldInfoBuilder.build();

    BatchSchema outputSchema = CalciteArrowHelper.fromCalciteRowType(output);
    ImmutableList.Builder<SchemaPath> builder = ImmutableList.builder();
    for (Field field : outputSchema) {
      builder.add(SchemaPath.getSimplePath(field.getName()));
    }
    ImmutableList<SchemaPath> outputColumns = builder.build();
    TableFunctionContext tableFunctionContext = new TableFunctionContext(tableMetadata.getFormatSettings(), outputSchema, tableMetadata.getSchema(),
      ImmutableList.of(tableMetadata.getName().getPathComponents()), null, tableMetadata.getStoragePluginId(), getInternalTablePluginId(tableMetadata), outputColumns, null, null, null, false, false, isIcebergMetadata,
      tableMetadata.getDatasetConfig().getPhysicalDataset().getInternalSchemaSettings());
    TableFunctionConfig tableFunctionConfig = new TableFunctionConfig(TableFunctionConfig.FunctionType.SPLIT_ASSIGNMENT, true, tableFunctionContext);
    return new TableFunctionPrel(input.getCluster(), input.getTraitSet(), null, input, tableMetadata, outputColumns, tableFunctionConfig, output);
  }
}
