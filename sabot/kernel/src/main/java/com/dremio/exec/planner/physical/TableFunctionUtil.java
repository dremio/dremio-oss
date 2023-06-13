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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.iceberg.ManifestContent;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.config.DeletedFilesMetadataTableFunctionContext;
import com.dremio.exec.physical.config.EasyScanTableFunctionContext;
import com.dremio.exec.physical.config.ExtendedFormatOptions;
import com.dremio.exec.physical.config.FooterReaderTableFunctionContext;
import com.dremio.exec.physical.config.ManifestListScanTableFunctionContext;
import com.dremio.exec.physical.config.ManifestScanFilters;
import com.dremio.exec.physical.config.ManifestScanTableFunctionContext;
import com.dremio.exec.physical.config.PartitionTransformTableFunctionContext;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.physical.config.TableFunctionContext;
import com.dremio.exec.planner.sql.CalciteArrowHelper;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.OperationType;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.ScanFilter;
import com.dremio.exec.store.SplitIdentity;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.exec.store.iceberg.InternalIcebergScanTableMetadata;
import com.dremio.exec.store.metadatarefresh.MetadataRefreshExecConstants;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.namespace.file.proto.FileType;
import com.google.common.base.Preconditions;
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

  public static TableFunctionContext getTableFunctionContext(TableMetadata tableMetadata, BatchSchema schema,  List<SchemaPath> columns) {
    return new TableFunctionContext(tableMetadata.getFormatSettings(),
      schema,
      tableMetadata.getSchema(),
      ImmutableList.of(tableMetadata.getName().getPathComponents()),
      null,
      tableMetadata.getStoragePluginId(),
      getInternalTablePluginId(tableMetadata),
      columns,
      tableMetadata.getReadDefinition().getPartitionColumnsList(), null,
      tableMetadata.getReadDefinition().getExtendedProperty(), false, false, true,
      tableMetadata.getDatasetConfig().getPhysicalDataset().getInternalSchemaSettings());
  }

  public static <E, T> TableFunctionContext getManifestScanTableFunctionContext(
      final TableMetadata tableMetadata,
      List<SchemaPath> columns,
      BatchSchema schema,
      ScanFilter scanFilter,
      ManifestContent manifestContent,
      ManifestScanFilters manifestScanFilters,
      boolean isCarryForwardEnabled) {
      ByteString partitionSpecMap = null;
      ByteString jsonPartitionSpecMap = null;
      String icebergSchema = null;
    if (tableMetadata.getDatasetConfig().getPhysicalDataset().getIcebergMetadata() != null) {
      partitionSpecMap = tableMetadata.getDatasetConfig().getPhysicalDataset().getIcebergMetadata().getPartitionSpecs();
      jsonPartitionSpecMap = tableMetadata.getDatasetConfig().getPhysicalDataset().getIcebergMetadata().getPartitionSpecsJsonMap();
      icebergSchema = tableMetadata.getDatasetConfig().getPhysicalDataset().getIcebergMetadata().getJsonSchema();
    }
    return new ManifestScanTableFunctionContext(partitionSpecMap,
      jsonPartitionSpecMap,
      icebergSchema,
      tableMetadata.getFormatSettings(), schema,
      tableMetadata.getSchema(),
      ImmutableList.of(tableMetadata.getName().getPathComponents()), scanFilter,
      tableMetadata.getStoragePluginId(),
      getInternalTablePluginId(tableMetadata),
      columns,
      tableMetadata.getReadDefinition().getPartitionColumnsList(), null,
      tableMetadata.getReadDefinition().getExtendedProperty(), false, false, true,
      tableMetadata.getDatasetConfig().getPhysicalDataset().getInternalSchemaSettings(),
      manifestContent,
      manifestScanFilters,
      isCarryForwardEnabled);
  }

  public static <E, T> TableFunctionContext getManifestListScanTableFunctionContext(TableMetadata tableMetadata,
                                                                                    ScanFilter scanFilter,
                                                                                    BatchSchema schema,
                                                                                    List<SchemaPath> projectedCols,
                                                                                    boolean isCarryForwardEnabled) {
    return new ManifestListScanTableFunctionContext(
      tableMetadata.getFormatSettings(),
      schema,
      schema,
      ImmutableList.of(tableMetadata.getName().getPathComponents()),
      scanFilter,
      tableMetadata.getStoragePluginId(),
      getInternalTablePluginId(tableMetadata),
      projectedCols,
      tableMetadata.getReadDefinition().getPartitionColumnsList(), null,
      tableMetadata.getReadDefinition().getExtendedProperty(), false, false, true,
      tableMetadata.getDatasetConfig().getPhysicalDataset().getInternalSchemaSettings(),
      isCarryForwardEnabled
    );
  }

  public static <E, T> TableFunctionContext getEasyScanTableFunctionContext(
          final TableMetadata tableMetadata,
          List<SchemaPath> columns,
          FileConfig fileConfig,
          BatchSchema schema,
          ScanFilter scanFilter,
          ExtendedFormatOptions extendedFormatOptions, StoragePluginId sourcePluginId) {
    return new EasyScanTableFunctionContext( fileConfig, schema,
      tableMetadata.getSchema(),
      ImmutableList.of(tableMetadata.getName().getPathComponents()), scanFilter,
      sourcePluginId,
      getInternalTablePluginId(tableMetadata),
      columns,
      tableMetadata.getReadDefinition().getPartitionColumnsList(), null,
      tableMetadata.getReadDefinition().getExtendedProperty(), false, false, true,
      tableMetadata.getDatasetConfig().getPhysicalDataset().getInternalSchemaSettings(),
      extendedFormatOptions);
  }

  public static List<SchemaPath> getSplitGenSchemaColumns() {
    List<SchemaPath> schemaPathList = new ArrayList<>();
    schemaPathList.add(new SchemaPath(RecordReader.SPLIT_IDENTITY));
    schemaPathList.add(new SchemaPath(RecordReader.SPLIT_INFORMATION));
    schemaPathList.add(new SchemaPath(RecordReader.COL_IDS));
    return schemaPathList;
  }

  public static RelDataType getSplitRowType(RelOptCluster cluster) {
    final RelDataTypeFactory.Builder builder = cluster.getTypeFactory().builder();
    builder.add(new RelDataTypeFieldImpl(RecordReader.SPLIT_IDENTITY, 0, cluster.getTypeFactory().createStructType(
      ImmutableList.of(
        cluster.getTypeFactory().createSqlType(SqlTypeName.VARCHAR),
        cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT),
        cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT),
        cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT)),
      ImmutableList.of(
        SplitIdentity.PATH,
        SplitIdentity.OFFSET,
        SplitIdentity.LENGTH,
        SplitIdentity.FILE_LENGTH
      ))));
    builder.add(new RelDataTypeFieldImpl(RecordReader.SPLIT_INFORMATION, 0, cluster.getTypeFactory().createSqlType(SqlTypeName.VARBINARY)));
    builder.add(new RelDataTypeFieldImpl(RecordReader.COL_IDS, 0, cluster.getTypeFactory().createSqlType(SqlTypeName.VARBINARY)));
    return builder.build();
  }

  public static TableFunctionContext getDataFileScanTableFunctionContext(
    final TableMetadata tableMetadata,
    ScanFilter scanFilter,
    List<SchemaPath> columns,
    boolean arrowCachingEnabled,
    boolean isConvertedIcebergDataset,
    List<String> implicitPartitionCols) {
    final BatchSchema schema = tableMetadata.getSchema().maskAndReorder(columns);

    return new TableFunctionContext(
      tableMetadata.getFormatSettings(),
      tableMetadata.getSchema(),
      schema,
      ImmutableList.of(tableMetadata.getName().getPathComponents()), scanFilter,
      tableMetadata.getStoragePluginId(), getInternalTablePluginId(tableMetadata), columns,
      mergeSafely(tableMetadata.getReadDefinition().getPartitionColumnsList(), implicitPartitionCols), null,
      tableMetadata.getReadDefinition().getExtendedProperty(),
      arrowCachingEnabled, isConvertedIcebergDataset, false,
      tableMetadata.getDatasetConfig().getPhysicalDataset().getInternalSchemaSettings()
    );
  }

  private static<T> List<T> mergeSafely(List<T>... additions) {
    List<T> source = new ArrayList<>();
    for (List<T> addition : additions) {
      if (addition != null) {
        source.addAll(addition);
      }
    }
    return source;
  }

  public static <E, T> TableFunctionContext getSplitProducerTableFunctionContext(
    final TableMetadata tableMetadata,
    ScanFilter scanFilter,
    boolean hasPartitionColumns) {
    return new TableFunctionContext(
      tableMetadata.getFormatSettings(), RecordReader.SPLIT_GEN_AND_COL_IDS_SCAN_SCHEMA,
      tableMetadata.getSchema(),
      ImmutableList.of(tableMetadata.getName().getPathComponents()), scanFilter,
      tableMetadata.getStoragePluginId(), getInternalTablePluginId(tableMetadata),
      getSplitGenSchemaColumns(),
      hasPartitionColumns ? tableMetadata.getReadDefinition().getPartitionColumnsList() :Collections.emptyList(), null,
      tableMetadata.getReadDefinition().getExtendedProperty(), false, false, false,
      tableMetadata.getDatasetConfig().getPhysicalDataset().getInternalSchemaSettings());
  }

  public static TableFunctionConfig getDataFileScanTableFunctionConfig(
    final TableMetadata tableMetadata,
    ScanFilter scanFilter,
    List<SchemaPath> columns,
    boolean arrowCachingEnabled,
    boolean isConvertedIcebergDataset,
    boolean limitDataScanParallelism,
    long survivingFileCount,
    List<String> implicitPartitionCols) {
    TableFunctionContext tableFunctionContext = getDataFileScanTableFunctionContext(tableMetadata, scanFilter, columns, arrowCachingEnabled, isConvertedIcebergDataset, implicitPartitionCols);
    TableFunctionConfig config = new TableFunctionConfig(TableFunctionConfig.FunctionType.DATA_FILE_SCAN, false, tableFunctionContext);
    if(limitDataScanParallelism) {
      config.setMinWidth(1);
      config.setMaxWidth(survivingFileCount);
    }
    return config;
  }

  public static TableFunctionConfig getEasyScanTableFunctionConfig(
          final TableMetadata tableMetadata,
          ScanFilter scanFilter,
          BatchSchema schema,
          List<SchemaPath> columns,
          FileConfig fileConfig,
          ExtendedFormatOptions extendedFormatOptions, StoragePluginId sourcePluginId) {
    TableFunctionContext tableFunctionContext = getEasyScanTableFunctionContext(tableMetadata, columns, fileConfig, schema, scanFilter, extendedFormatOptions, sourcePluginId);
    TableFunctionConfig config = new TableFunctionConfig(TableFunctionConfig.FunctionType.EASY_DATA_FILE_SCAN, false, tableFunctionContext);
    return config;
  }

  public static TableFunctionConfig getSplitGenManifestScanTableFunctionConfig(
      final TableMetadata tableMetadata,
      List<SchemaPath> columns,
      BatchSchema schema,
      ScanFilter scanFilter,
      ManifestScanFilters manifestScanFilters) {
    TableFunctionContext tableFunctionContext = getManifestScanTableFunctionContext(tableMetadata, columns, schema,
        scanFilter, ManifestContent.DATA, manifestScanFilters, false);
    return new TableFunctionConfig(TableFunctionConfig.FunctionType.SPLIT_GEN_MANIFEST_SCAN, true, tableFunctionContext);
  }

  public static TableFunctionConfig getManifestScanTableFunctionConfig(
      TableMetadata tableMetadata,
      List<SchemaPath> columns,
      BatchSchema schema,
      ScanFilter scanFilter,
      ManifestContent manifestContent,
      ManifestScanFilters manifestScanFilters,
      boolean isCarryForwardEnabled) {
    TableFunctionContext tableFunctionContext = getManifestScanTableFunctionContext(tableMetadata, columns, schema,
        scanFilter, manifestContent, manifestScanFilters, isCarryForwardEnabled);
    return new TableFunctionConfig(TableFunctionConfig.FunctionType.ICEBERG_MANIFEST_SCAN, true,
        tableFunctionContext);
  }

  public static TableFunctionConfig getManifestListScanTableFunctionConfig(
    TableMetadata tableMetadata,
    ScanFilter scanFilter,
    BatchSchema schema,
    List<SchemaPath> projectedCols) {
    TableFunctionContext tableFunctionContext = getManifestListScanTableFunctionContext(tableMetadata, scanFilter, schema, projectedCols, true);
    return new TableFunctionConfig(TableFunctionConfig.FunctionType.ICEBERG_MANIFEST_LIST_SCAN, true,
      tableFunctionContext);
  }

  public static TableFunctionConfig getSplitGenFunctionConfig(
          final TableMetadata tableMetadata,
          ScanFilter scanFilter) {
    TableFunctionContext tableFunctionContext = getSplitProducerTableFunctionContext(tableMetadata, scanFilter, true);
    return new TableFunctionConfig(TableFunctionConfig.FunctionType.SPLIT_GENERATION, true, tableFunctionContext);
  }

  public static TableFunctionConfig getEasySplitGenFunctionConfig(
    final TableMetadata tableMetadata,
    ScanFilter scanFilter) {
    TableFunctionContext tableFunctionContext = getSplitProducerTableFunctionContext(tableMetadata, scanFilter, false);
    return new TableFunctionConfig(TableFunctionConfig.FunctionType.EASY_SPLIT_GENERATION, true, tableFunctionContext);
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
      new ArrayList<>(), new ArrayList<>(),
      Optional.ofNullable(tableMetadata.getReadDefinition()).map(ReadDefinition::getExtendedProperty).orElse(null),
      false, false, false, tableMetadata.getDatasetConfig().getPhysicalDataset().getInternalSchemaSettings());
  }

  private static List<SchemaPath> getFooterReadOutputSchemaColumns() {
    return MetadataRefreshExecConstants.FooterRead.OUTPUT_SCHEMA.BATCH_SCHEMA.getFields()
      .stream()
      .map(field -> SchemaPath.getSimplePath(field.getName()))
      .collect(Collectors.toList());
  }

  public static TableFunctionConfig getMetadataManifestScanTableFunctionConfig(
    final TableMetadata tableMetadata,
    List<SchemaPath> columns,
    BatchSchema schema,
    ScanFilter scanFilter) {
    TableFunctionContext tableFunctionContext = getManifestScanTableFunctionContext(tableMetadata, columns, schema,
      scanFilter, ManifestContent.DATA, ManifestScanFilters.empty(), false);
    return new TableFunctionConfig(TableFunctionConfig.FunctionType.METADATA_MANIFEST_FILE_SCAN, true, tableFunctionContext);
  }

  public static TableFunctionConfig getIcebergPartitionTransformTableFunctionConfig(IcebergTableProps icebergTableProps, BatchSchema schema, List<SchemaPath> schemaPathList) {
    //Does this validation requires ? If so should it return PartitionTransformTableFunctionContext with null spec and schema?
    Preconditions.checkNotNull(icebergTableProps);
    TableFunctionContext tableFunctionContext = new PartitionTransformTableFunctionContext(icebergTableProps.getPartitionSpec(), icebergTableProps.getIcebergSchema(), schema, schemaPathList);
    return new TableFunctionConfig(TableFunctionConfig.FunctionType.ICEBERG_PARTITION_TRANSFORM, true, tableFunctionContext);
  }

  public static TableFunctionConfig getIcebergSplitGenTableFunctionConfig(TableMetadata tableMetadata,
      BatchSchema outputSchema, boolean isConvertedIcebergDataset) {
    TableFunctionContext context = new TableFunctionContext(
        null,
        outputSchema,
        null,
        null,
        null,
        tableMetadata.getStoragePluginId(),
        null,
        outputSchema.getFields().stream().map(f -> SchemaPath.getSimplePath(f.getName())).collect(Collectors.toList()),
        null,
        null,
        tableMetadata.getReadDefinition().getExtendedProperty(),
        false,
        isConvertedIcebergDataset,
        false,
        null);
    return new TableFunctionConfig(TableFunctionConfig.FunctionType.ICEBERG_SPLIT_GEN, true, context);
  }

  public static TableFunctionConfig getIcebergDeleteFileAggTableFunctionConfig(BatchSchema outputSchema) {
    TableFunctionContext context = new TableFunctionContext(
        null,
        outputSchema,
        null,
        null,
        null,
        null,
        null,
        outputSchema.getFields().stream().map(f -> SchemaPath.getSimplePath(f.getName())).collect(Collectors.toList()),
        null,
        null,
        null,
        false,
        false,
        false,
        null);
    return new TableFunctionConfig(TableFunctionConfig.FunctionType.ICEBERG_DELETE_FILE_AGG, true, context);
  }

  public static TableFunctionConfig getIcebergOrphanFileDeleteFunctionConfig(BatchSchema outputSchema, TableMetadata tableMetadata) {
    TableFunctionContext context = new TableFunctionContext(
      tableMetadata.getFormatSettings(),
      outputSchema,
      null,
      null,
      null,
      tableMetadata.getStoragePluginId(),
      null,
      outputSchema.getFields().stream().map(f -> SchemaPath.getSimplePath(f.getName())).collect(Collectors.toList()),
      null,
      null,
      null,
      false,
      false,
      false,
      null);
    return new TableFunctionConfig(TableFunctionConfig.FunctionType.ICEBERG_ORPHAN_FILE_DELETE, true, context);
  }

  public static TableFunctionConfig getIcebergPartitionStatsFunctionConfig(BatchSchema outputSchema, TableMetadata tableMetadata) {
    TableFunctionContext context = new TableFunctionContext(
      tableMetadata.getFormatSettings(),
      outputSchema,
      null,
      null,
      null,
      tableMetadata.getStoragePluginId(),
      null,
      outputSchema.getFields().stream().map(f -> SchemaPath.getSimplePath(f.getName())).collect(Collectors.toList()),
      null,
      null,
      null,
      false,
      false,
      false,
      null);
    return new TableFunctionConfig(TableFunctionConfig.FunctionType.ICEBERG_PARTITION_STATS_SCAN, true, context);
  }

  public static TableFunctionContext getDeletedFilesMetadataTableFunctionContext(OperationType operationType, BatchSchema schema, List<SchemaPath> columns, boolean isIcebergMetadata) {
    return new DeletedFilesMetadataTableFunctionContext(operationType,
      null,
      schema,
      null,
      null,
      null,
      null,
      null,
      columns,
      null,
      null,
      null,
      false,
      false,
      isIcebergMetadata,
      null);
  }

  public static Function<Prel, TableFunctionPrel> getHashExchangeTableFunctionCreator(final TableMetadata tableMetadata, boolean isIcebergMetadata) {
    return input -> getSplitAssignTableFunction(input, tableMetadata, isIcebergMetadata, null);
  }

  public static Function<Prel, TableFunctionPrel> getHashExchangeTableFunctionCreator(final TableMetadata tableMetadata, boolean isIcebergMetadata, StoragePluginId storagePluginId) {
    return input -> getSplitAssignTableFunction(input, tableMetadata, isIcebergMetadata, storagePluginId);
  }

  private static TableFunctionPrel getSplitAssignTableFunction(Prel input, TableMetadata tableMetadata, boolean isIcebergMetadata, StoragePluginId storagePluginId) {
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
      ImmutableList.of(tableMetadata.getName().getPathComponents()), null, storagePluginId == null ? tableMetadata.getStoragePluginId() : storagePluginId, getInternalTablePluginId(tableMetadata), outputColumns, null, null, null, false, false, isIcebergMetadata,
      tableMetadata.getDatasetConfig().getPhysicalDataset().getInternalSchemaSettings());
    TableFunctionConfig tableFunctionConfig = new TableFunctionConfig(TableFunctionConfig.FunctionType.SPLIT_ASSIGNMENT, true, tableFunctionContext);
    return new TableFunctionPrel(input.getCluster(), input.getTraitSet(), null, input, tableMetadata, tableFunctionConfig, output);
  }

  public static List<String> getDataset(TableFunctionConfig functionConfig) {
    TableFunctionContext functionContext = functionConfig.getFunctionContext();
    Collection<List<String>> referencedTables = functionContext.getReferencedTables();
    return referencedTables != null ? referencedTables.iterator().next() : null;
  }
}
