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

import static com.dremio.exec.store.SystemSchemas.FILE_PATH;
import static com.dremio.exec.store.SystemSchemas.FILE_TYPE;
import static com.dremio.exec.store.SystemSchemas.METADATA_FILE_PATH;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.config.CarryForwardAwareTableFunctionContext;
import com.dremio.exec.physical.config.DeletedFilesMetadataTableFunctionContext;
import com.dremio.exec.physical.config.DirListingTableFunctionContext;
import com.dremio.exec.physical.config.EasyScanTableFunctionContext;
import com.dremio.exec.physical.config.ExtendedFormatOptions;
import com.dremio.exec.physical.config.FooterReaderTableFunctionContext;
import com.dremio.exec.physical.config.IcebergLocationFinderFunctionContext;
import com.dremio.exec.physical.config.IcebergSnapshotsScanTableFunctionContext;
import com.dremio.exec.physical.config.ImmutableManifestScanFilters;
import com.dremio.exec.physical.config.IncrementalRefreshJoinKeyTableFunctionContext;
import com.dremio.exec.physical.config.ManifestScanFilters;
import com.dremio.exec.physical.config.ManifestScanTableFunctionContext;
import com.dremio.exec.physical.config.OrphanFileDeleteTableFunctionContext;
import com.dremio.exec.physical.config.PartitionTransformTableFunctionContext;
import com.dremio.exec.physical.config.SplitProducerTableFunctionContext;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.physical.config.TableFunctionContext;
import com.dremio.exec.planner.sql.CalciteArrowHelper;
import com.dremio.exec.planner.sql.SchemaUtilities;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.OperationType;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.ScanFilter;
import com.dremio.exec.store.SplitIdentity;
import com.dremio.exec.store.SystemSchemas;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.exec.store.iceberg.IcebergFileType;
import com.dremio.exec.store.iceberg.InternalIcebergScanTableMetadata;
import com.dremio.exec.store.iceberg.ManifestContentType;
import com.dremio.exec.store.iceberg.SnapshotsScanOptions;
import com.dremio.exec.store.metadatarefresh.MetadataRefreshExecConstants;
import com.dremio.exec.store.parquet.ParquetScanRowGroupFilter;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.IcebergMetadata;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.dremio.service.namespace.dataset.proto.UserDefinedSchemaSettings;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.namespace.file.proto.FileType;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.protostuff.ByteString;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.type.SqlTypeName;

/** Utility functions related to table functions */
public class TableFunctionUtil {

  public static StoragePluginId getInternalTablePluginId(TableMetadata tableMetadata) {
    if (tableMetadata instanceof InternalIcebergScanTableMetadata) {
      return ((InternalIcebergScanTableMetadata) tableMetadata).getIcebergTableStoragePlugin();
    }
    return null;
  }

  public static <E, T> TableFunctionContext getManifestScanTableFunctionContext(
      final TableMetadata tableMetadata,
      List<SchemaPath> columns,
      BatchSchema schema,
      ScanFilter scanFilter,
      ManifestContentType manifestContentType,
      ManifestScanFilters manifestScanFilters,
      boolean isCarryForwardEnabled,
      boolean includeIcebergPartitionInfo) {

    ByteString partitionSpecMap = null;
    ByteString jsonPartitionSpecMap = null;
    String icebergSchema = null;
    if (tableMetadata.getDatasetConfig().getPhysicalDataset().getIcebergMetadata() != null) {
      partitionSpecMap =
          tableMetadata
              .getDatasetConfig()
              .getPhysicalDataset()
              .getIcebergMetadata()
              .getPartitionSpecs();
      jsonPartitionSpecMap =
          tableMetadata
              .getDatasetConfig()
              .getPhysicalDataset()
              .getIcebergMetadata()
              .getPartitionSpecsJsonMap();
      icebergSchema =
          tableMetadata
              .getDatasetConfig()
              .getPhysicalDataset()
              .getIcebergMetadata()
              .getJsonSchema();
    }

    return new ManifestScanTableFunctionContext(
        partitionSpecMap,
        jsonPartitionSpecMap,
        icebergSchema,
        tableMetadata.getFormatSettings(),
        schema,
        tableMetadata.getSchema(),
        ImmutableList.of(tableMetadata.getName().getPathComponents()),
        scanFilter,
        tableMetadata.getStoragePluginId(),
        getInternalTablePluginId(tableMetadata),
        columns,
        tableMetadata.getReadDefinition().getPartitionColumnsList(),
        tableMetadata.getReadDefinition().getExtendedProperty(),
        false,
        false,
        true,
        tableMetadata.getDatasetConfig().getPhysicalDataset().getInternalSchemaSettings(),
        manifestContentType,
        manifestScanFilters,
        isCarryForwardEnabled,
        ImmutableMap.of(
            SchemaPath.getCompoundPath(SystemSchemas.SPLIT_IDENTITY, SplitIdentity.PATH),
            SchemaPath.getSimplePath(SystemSchemas.FILE_PATH)),
        SystemSchemas.FILE_TYPE,
        IcebergFileType.MANIFEST.name(),
        true,
        includeIcebergPartitionInfo);
  }

  public static <E, T> TableFunctionContext getTableAgnosticManifestScanTableFunctionContext(
      StoragePluginId storagePluginId,
      StoragePluginId internalStoragePlugin,
      List<SchemaPath> manifestFileReaderColumns,
      BatchSchema manifestFileReaderSchema) {
    return new ManifestScanTableFunctionContext(
        null,
        null,
        null,
        null,
        manifestFileReaderSchema,
        manifestFileReaderSchema,
        Collections.EMPTY_LIST,
        null,
        storagePluginId,
        internalStoragePlugin,
        manifestFileReaderColumns,
        Collections.emptyList(),
        null,
        false,
        false,
        true,
        null,
        ManifestContentType.DATA,
        ImmutableManifestScanFilters.empty(),
        true,
        ImmutableMap.of(
            SchemaPath.getCompoundPath(SystemSchemas.SPLIT_IDENTITY, SplitIdentity.PATH),
            SchemaPath.getSimplePath(SystemSchemas.FILE_PATH)),
        SystemSchemas.FILE_TYPE,
        IcebergFileType.MANIFEST.name(),
        false,
        false);
  }

  public static <E, T> TableFunctionContext getDirListingTableFunctionContext(
      StoragePluginId storagePluginId,
      BatchSchema schema,
      List<SchemaPath> projectedCols,
      boolean allowRecursiveListing,
      boolean hasVersion) {
    return new DirListingTableFunctionContext(
        schema, schema, storagePluginId, projectedCols, allowRecursiveListing, hasVersion);
  }

  public static <E, T> TableFunctionContext getSnapshotsScanTableFunctionContext(
      StoragePluginId storagePluginId,
      SnapshotsScanOptions snapshotsScanOptions,
      BatchSchema schema,
      boolean isCarryForwardEnabled) {
    return new IcebergSnapshotsScanTableFunctionContext(
        storagePluginId,
        snapshotsScanOptions,
        schema,
        SchemaUtilities.allColPaths(schema),
        isCarryForwardEnabled,
        ImmutableMap.of(
            SchemaPath.getSimplePath(METADATA_FILE_PATH), SchemaPath.getSimplePath(FILE_PATH)),
        FILE_TYPE,
        IcebergFileType.METADATA_JSON.name());
  }

  private static TableFunctionContext getEasyScanTableFunctionContext(
      final TableMetadata tableMetadata,
      List<SchemaPath> columns,
      FileConfig fileConfig,
      BatchSchema schema,
      ScanFilter scanFilter,
      ExtendedFormatOptions extendedFormatOptions,
      StoragePluginId sourcePluginId,
      ByteString extendedProperty) {
    return new EasyScanTableFunctionContext(
        fileConfig,
        schema,
        tableMetadata.getSchema(),
        ImmutableList.of(tableMetadata.getName().getPathComponents()),
        scanFilter,
        sourcePluginId,
        getInternalTablePluginId(tableMetadata),
        columns,
        tableMetadata.getReadDefinition().getPartitionColumnsList(),
        extendedProperty,
        false,
        false,
        true,
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
    builder.add(
        new RelDataTypeFieldImpl(
            RecordReader.SPLIT_IDENTITY,
            0,
            cluster
                .getTypeFactory()
                .createStructType(
                    ImmutableList.of(
                        cluster.getTypeFactory().createSqlType(SqlTypeName.VARCHAR),
                        cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT),
                        cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT),
                        cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT)),
                    ImmutableList.of(
                        SplitIdentity.PATH,
                        SplitIdentity.OFFSET,
                        SplitIdentity.LENGTH,
                        SplitIdentity.FILE_LENGTH))));
    builder.add(
        new RelDataTypeFieldImpl(
            RecordReader.SPLIT_INFORMATION,
            0,
            cluster.getTypeFactory().createSqlType(SqlTypeName.VARBINARY)));
    builder.add(
        new RelDataTypeFieldImpl(
            RecordReader.COL_IDS,
            0,
            cluster.getTypeFactory().createSqlType(SqlTypeName.VARBINARY)));
    return builder.build();
  }

  public static TableFunctionContext getDataFileScanTableFunctionContext(
      final TableMetadata tableMetadata,
      ScanFilter scanFilter,
      ParquetScanRowGroupFilter rowGroupFilter,
      List<SchemaPath> columns,
      boolean arrowCachingEnabled,
      boolean isConvertedIcebergDataset,
      List<String> implicitPartitionCols) {
    final BatchSchema schema = tableMetadata.getSchema().maskAndReorder(columns);

    return new TableFunctionContext(
        tableMetadata.getFormatSettings(),
        tableMetadata.getSchema(),
        schema,
        ImmutableList.of(tableMetadata.getName().getPathComponents()),
        scanFilter,
        rowGroupFilter,
        tableMetadata.getStoragePluginId(),
        getInternalTablePluginId(tableMetadata),
        columns,
        mergeSafely(
            tableMetadata.getReadDefinition().getPartitionColumnsList(), implicitPartitionCols),
        tableMetadata.getReadDefinition().getExtendedProperty(),
        arrowCachingEnabled,
        isConvertedIcebergDataset,
        false,
        tableMetadata.getDatasetConfig().getPhysicalDataset().getInternalSchemaSettings());
  }

  /**
   * Creates a {@link com.dremio.exec.store.parquet.ScanTableFunction} set up for COPY INTO from a
   * parquet source. Disables schema learning because for COPY INTO operations, the required schema
   * is derived from the schema of the target table.
   *
   * @param sourceFormatSettings source configuration of the location and format
   * @param targetSchema output schema
   * @param namespaceKey namespace key for the source location
   * @param storagePluginId storage plugin id for source
   * @param columns names of the columns of the schema
   * @param extendedProperty additional copy-into query related information
   * @return table function context for a copy-into specific parquet scan table function
   */
  public static TableFunctionContext getDataFileScanTableFunctionContextForCopyInto(
      FileConfig sourceFormatSettings,
      BatchSchema targetSchema,
      NamespaceKey namespaceKey,
      StoragePluginId storagePluginId,
      List<SchemaPath> columns,
      ByteString extendedProperty) {
    UserDefinedSchemaSettings schemaSettings =
        new UserDefinedSchemaSettings()
            .setSchemaLearningEnabled(false)
            .setSchemaImposedOutput(true);
    return new TableFunctionContext(
        sourceFormatSettings,
        targetSchema,
        targetSchema,
        ImmutableList.of(namespaceKey.getPathComponents()),
        null,
        null,
        storagePluginId,
        null,
        columns,
        ImmutableList.of(),
        extendedProperty,
        false,
        false,
        false,
        schemaSettings);
  }

  private static <T> List<T> mergeSafely(List<T>... additions) {
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
      boolean hasPartitionColumns,
      boolean isOneSplitPerFile) {
    return new SplitProducerTableFunctionContext(
        tableMetadata.getFormatSettings(),
        RecordReader.SPLIT_GEN_AND_COL_IDS_SCAN_SCHEMA,
        tableMetadata.getSchema(),
        ImmutableList.of(tableMetadata.getName().getPathComponents()),
        scanFilter,
        null,
        tableMetadata.getStoragePluginId(),
        getInternalTablePluginId(tableMetadata),
        getSplitGenSchemaColumns(),
        hasPartitionColumns
            ? tableMetadata.getReadDefinition().getPartitionColumnsList()
            : Collections.emptyList(),
        tableMetadata.getReadDefinition().getExtendedProperty(),
        false,
        false,
        false,
        tableMetadata.getDatasetConfig().getPhysicalDataset().getInternalSchemaSettings(),
        isOneSplitPerFile);
  }

  public static TableFunctionConfig getDataFileScanTableFunctionConfig(
      TableFunctionContext tableFunctionContext,
      boolean limitDataScanParallelism,
      long survivingFileCount) {
    TableFunctionConfig config =
        new TableFunctionConfig(
            TableFunctionConfig.FunctionType.DATA_FILE_SCAN, false, tableFunctionContext);
    if (limitDataScanParallelism) {
      config.setMinWidth(1);
      config.setMaxWidth(survivingFileCount);
    }
    return config;
  }

  public static TableFunctionConfig getDataFileScanTableFunctionConfig(
      final TableMetadata tableMetadata,
      ScanFilter scanFilter,
      ParquetScanRowGroupFilter rowGroupFilter,
      List<SchemaPath> columns,
      boolean arrowCachingEnabled,
      boolean isConvertedIcebergDataset,
      boolean limitDataScanParallelism,
      long survivingFileCount,
      List<String> implicitPartitionCols) {
    TableFunctionContext tableFunctionContext =
        getDataFileScanTableFunctionContext(
            tableMetadata,
            scanFilter,
            rowGroupFilter,
            columns,
            arrowCachingEnabled,
            isConvertedIcebergDataset,
            implicitPartitionCols);
    return getDataFileScanTableFunctionConfig(
        tableFunctionContext, limitDataScanParallelism, survivingFileCount);
  }

  public static TableFunctionConfig getEasyScanTableFunctionConfig(
      final TableMetadata tableMetadata,
      ScanFilter scanFilter,
      BatchSchema schema,
      List<SchemaPath> columns,
      FileConfig fileConfig,
      ExtendedFormatOptions extendedFormatOptions,
      StoragePluginId sourcePluginId,
      ByteString extendedProperty) {
    TableFunctionContext context =
        getEasyScanTableFunctionContext(
            tableMetadata,
            columns,
            fileConfig,
            schema,
            scanFilter,
            extendedFormatOptions,
            sourcePluginId,
            extendedProperty);
    return new TableFunctionConfig(
        TableFunctionConfig.FunctionType.EASY_DATA_FILE_SCAN, false, context);
  }

  public static TableFunctionConfig getSplitGenManifestScanTableFunctionConfig(
      final TableMetadata tableMetadata,
      List<SchemaPath> columns,
      BatchSchema schema,
      ScanFilter scanFilter,
      ManifestScanFilters manifestScanFilters) {
    TableFunctionContext tableFunctionContext =
        getManifestScanTableFunctionContext(
            tableMetadata,
            columns,
            schema,
            scanFilter,
            ManifestContentType.DATA,
            manifestScanFilters,
            false,
            false);
    return new TableFunctionConfig(
        TableFunctionConfig.FunctionType.SPLIT_GEN_MANIFEST_SCAN, true, tableFunctionContext);
  }

  public static TableFunctionConfig getManifestScanTableFunctionConfig(
      TableMetadata tableMetadata,
      List<SchemaPath> columns,
      BatchSchema schema,
      ScanFilter scanFilter,
      ManifestContentType manifestContentType,
      ManifestScanFilters manifestScanFilters,
      boolean isCarryForwardEnabled,
      boolean includeIcebergPartitionInfo) {
    TableFunctionContext tableFunctionContext =
        getManifestScanTableFunctionContext(
            tableMetadata,
            columns,
            schema,
            scanFilter,
            manifestContentType,
            manifestScanFilters,
            isCarryForwardEnabled,
            includeIcebergPartitionInfo);
    return new TableFunctionConfig(
        TableFunctionConfig.FunctionType.ICEBERG_MANIFEST_SCAN, true, tableFunctionContext);
  }

  public static TableFunctionConfig getTableAgnosticManifestScanFunctionConfig(
      StoragePluginId storagePluginId,
      StoragePluginId internalStoragePlugin,
      List<SchemaPath> manifestFileReaderColumns,
      BatchSchema manifestFileReaderSchema) {
    TableFunctionContext manifestScanTableFunctionContext =
        TableFunctionUtil.getTableAgnosticManifestScanTableFunctionContext(
            storagePluginId,
            internalStoragePlugin,
            manifestFileReaderColumns,
            manifestFileReaderSchema);
    return new TableFunctionConfig(
        TableFunctionConfig.FunctionType.METADATA_MANIFEST_FILE_SCAN,
        true,
        manifestScanTableFunctionContext);
  }

  public static TableFunctionConfig getManifestListScanTableFunctionConfig(
      BatchSchema outputSchema, StoragePluginId storagePluginId) {
    TableFunctionContext context =
        new CarryForwardAwareTableFunctionContext(
            null,
            outputSchema,
            null,
            null,
            null,
            storagePluginId,
            null,
            outputSchema.getFields().stream()
                .map(f -> SchemaPath.getSimplePath(f.getName()))
                .collect(Collectors.toList()),
            null,
            null,
            false,
            false,
            false,
            null,
            true,
            ImmutableMap.of(
                SchemaPath.getSimplePath(SystemSchemas.MANIFEST_LIST_PATH),
                SchemaPath.getSimplePath(SystemSchemas.FILE_PATH)),
            SystemSchemas.FILE_TYPE,
            IcebergFileType.MANIFEST_LIST.name());
    return new TableFunctionConfig(
        TableFunctionConfig.FunctionType.ICEBERG_MANIFEST_LIST_SCAN, true, context);
  }

  public static TableFunctionConfig getDirListingTableFunctionConfig(
      StoragePluginId storagePluginId, BatchSchema schema) {
    TableFunctionContext tableFunctionContext =
        getDirListingTableFunctionContext(
            storagePluginId, schema, SchemaUtilities.allColPaths(schema), true, false);
    return new TableFunctionConfig(
        TableFunctionConfig.FunctionType.DIR_LISTING, true, tableFunctionContext);
  }

  public static TableFunctionConfig getSnapshotsScanTableFunctionConfig(
      StoragePluginId storagePluginId,
      SnapshotsScanOptions snapshotsScanOptions,
      BatchSchema schema,
      boolean isCarryForwardEnabled) {
    TableFunctionContext tableFunctionContext =
        getSnapshotsScanTableFunctionContext(
            storagePluginId, snapshotsScanOptions, schema, isCarryForwardEnabled);
    return new TableFunctionConfig(
        TableFunctionConfig.FunctionType.ICEBERG_SNAPSHOTS_SCAN, true, tableFunctionContext);
  }

  public static TableFunctionConfig getSplitGenFunctionConfig(
      final TableMetadata tableMetadata, ScanFilter scanFilter) {
    TableFunctionContext tableFunctionContext =
        getSplitProducerTableFunctionContext(tableMetadata, scanFilter, true, false);
    return new TableFunctionConfig(
        TableFunctionConfig.FunctionType.SPLIT_GENERATION, true, tableFunctionContext);
  }

  public static TableFunctionConfig getFooterReadFunctionConfig(
      final TableMetadata tableMetadata,
      final BatchSchema tableSchema,
      ScanFilter scanFilter,
      FileType fileType) {
    TableFunctionContext tableFunctionContext =
        getFooterReadTableFunctionContext(tableMetadata, tableSchema, scanFilter, fileType);
    return new TableFunctionConfig(
        TableFunctionConfig.FunctionType.FOOTER_READER, true, tableFunctionContext);
  }

  private static TableFunctionContext getFooterReadTableFunctionContext(
      TableMetadata tableMetadata,
      BatchSchema tableSchema,
      ScanFilter scanFilter,
      FileType fileType) {
    return new FooterReaderTableFunctionContext(
        fileType,
        tableMetadata.getFormatSettings(),
        MetadataRefreshExecConstants.FooterRead.OUTPUT_SCHEMA.BATCH_SCHEMA,
        tableSchema,
        ImmutableList.of(tableMetadata.getName().getPathComponents()),
        scanFilter,
        tableMetadata.getStoragePluginId(),
        getInternalTablePluginId(tableMetadata),
        getFooterReadOutputSchemaColumns(),
        new ArrayList<>(),
        Optional.ofNullable(tableMetadata.getReadDefinition())
            .map(ReadDefinition::getExtendedProperty)
            .orElse(null),
        false,
        false,
        false,
        tableMetadata.getDatasetConfig().getPhysicalDataset().getInternalSchemaSettings());
  }

  private static List<SchemaPath> getFooterReadOutputSchemaColumns() {
    return MetadataRefreshExecConstants.FooterRead.OUTPUT_SCHEMA.BATCH_SCHEMA.getFields().stream()
        .map(field -> SchemaPath.getSimplePath(field.getName()))
        .collect(Collectors.toList());
  }

  public static TableFunctionConfig getMetadataManifestScanTableFunctionConfig(
      final TableMetadata tableMetadata,
      List<SchemaPath> columns,
      BatchSchema schema,
      ScanFilter scanFilter) {
    TableFunctionContext tableFunctionContext =
        getManifestScanTableFunctionContext(
            tableMetadata,
            columns,
            schema,
            scanFilter,
            ManifestContentType.DATA,
            ManifestScanFilters.empty(),
            false,
            false);
    return new TableFunctionConfig(
        TableFunctionConfig.FunctionType.METADATA_MANIFEST_FILE_SCAN, true, tableFunctionContext);
  }

  public static TableFunctionConfig getIcebergIncrementalRefreshJoinKeyTableFunctionConfig(
      final ByteString partitionSpec,
      final String icebergSchema,
      final BatchSchema schema,
      final List<SchemaPath> schemaPathList,
      final TableMetadata tableMetadata) {
    // The ICEBERG_INCREMENTAL_REFRESH_JOIN_KEY table function will need the metadata
    // location and plug in ID to load the partition spec info
    // We special handle InternalIcebergScanTableMetadata, as the metadata location and pluginId
    // are different than the dataset location
    StoragePluginId storagePluginId = tableMetadata.getStoragePluginId();
    if (tableMetadata instanceof InternalIcebergScanTableMetadata) {
      storagePluginId =
          ((InternalIcebergScanTableMetadata) tableMetadata).getIcebergTableStoragePlugin();
    }
    final Optional<String> metadataLocation =
        Optional.ofNullable(tableMetadata.getDatasetConfig())
            .map(DatasetConfig::getPhysicalDataset)
            .map(PhysicalDataset::getIcebergMetadata)
            .map(IcebergMetadata::getMetadataFileLocation);
    if (!metadataLocation.isPresent()) {
      throw new RuntimeException("Could not find metadata location for Iceberg Dataset.");
    }
    final TableFunctionContext tableFunctionContext =
        new IncrementalRefreshJoinKeyTableFunctionContext(
            partitionSpec,
            icebergSchema,
            schema,
            schemaPathList,
            metadataLocation.get(),
            storagePluginId);
    return new TableFunctionConfig(
        TableFunctionConfig.FunctionType.ICEBERG_INCREMENTAL_REFRESH_JOIN_KEY,
        true,
        tableFunctionContext);
  }

  public static TableFunctionConfig getIcebergPartitionTransformTableFunctionConfig(
      IcebergTableProps icebergTableProps, BatchSchema schema, List<SchemaPath> schemaPathList) {
    // Does this validation requires ? If so should it return PartitionTransformTableFunctionContext
    // with null spec and schema?
    Preconditions.checkNotNull(icebergTableProps);
    TableFunctionContext tableFunctionContext =
        new PartitionTransformTableFunctionContext(
            icebergTableProps.getPartitionSpec(),
            icebergTableProps.getIcebergSchema(),
            schema,
            schemaPathList);
    return new TableFunctionConfig(
        TableFunctionConfig.FunctionType.ICEBERG_PARTITION_TRANSFORM, true, tableFunctionContext);
  }

  public static TableFunctionConfig getIcebergSplitGenTableFunctionConfig(
      TableMetadata tableMetadata, BatchSchema outputSchema, boolean isConvertedIcebergDataset) {

    final FileConfig fileConfig =
        new FileConfig()
            .setType(
                tableMetadata
                    .getDatasetConfig()
                    .getPhysicalDataset()
                    .getIcebergMetadata()
                    .getFileType());
    TableFunctionContext context =
        new TableFunctionContext(
            fileConfig,
            outputSchema,
            null,
            null,
            null,
            null,
            tableMetadata.getStoragePluginId(),
            null,
            outputSchema.getFields().stream()
                .map(f -> SchemaPath.getSimplePath(f.getName()))
                .collect(Collectors.toList()),
            null,
            tableMetadata.getReadDefinition().getExtendedProperty(),
            false,
            isConvertedIcebergDataset,
            false,
            null);
    return new TableFunctionConfig(
        TableFunctionConfig.FunctionType.ICEBERG_SPLIT_GEN, true, context);
  }

  public static TableFunctionConfig getIcebergSplitGenTableFunctionConfig(
      BatchSchema outputSchema,
      List<List<String>> tablePath,
      StoragePluginId pluginId,
      ByteString extendedProperty) {
    TableFunctionContext context =
        new TableFunctionContext(
            null,
            outputSchema,
            null,
            tablePath,
            null,
            null,
            pluginId,
            null,
            outputSchema.getFields().stream()
                .map(f -> SchemaPath.getSimplePath(f.getName()))
                .collect(Collectors.toList()),
            null,
            extendedProperty,
            false,
            false,
            false,
            null);
    return new TableFunctionConfig(
        TableFunctionConfig.FunctionType.ICEBERG_SPLIT_GEN, true, context);
  }

  public static TableFunctionConfig getIcebergDeleteFileAggTableFunctionConfig(
      BatchSchema outputSchema) {
    TableFunctionContext context =
        new TableFunctionContext(
            null,
            outputSchema,
            null,
            null,
            null,
            null,
            null,
            null,
            outputSchema.getFields().stream()
                .map(f -> SchemaPath.getSimplePath(f.getName()))
                .collect(Collectors.toList()),
            null,
            null,
            false,
            false,
            false,
            null);
    return new TableFunctionConfig(
        TableFunctionConfig.FunctionType.ICEBERG_DELETE_FILE_AGG, true, context);
  }

  public static TableFunctionConfig getTableFunctionConfig(
      TableFunctionConfig.FunctionType functionType,
      BatchSchema outputSchema,
      StoragePluginId storagePluginId) {
    TableFunctionContext context =
        new TableFunctionContext(
            null,
            outputSchema,
            null,
            null,
            null,
            null,
            storagePluginId,
            null,
            outputSchema.getFields().stream()
                .map(f -> SchemaPath.getSimplePath(f.getName()))
                .collect(Collectors.toList()),
            null,
            null,
            false,
            false,
            false,
            null);
    return new TableFunctionConfig(functionType, true, context);
  }

  public static TableFunctionConfig getOrphanFileDeleteTableFunctionConfig(
      BatchSchema outputSchema, StoragePluginId storagePluginId, String tableLocation) {
    OrphanFileDeleteTableFunctionContext context =
        new OrphanFileDeleteTableFunctionContext(
            outputSchema,
            storagePluginId,
            outputSchema.getFields().stream()
                .map(f -> SchemaPath.getSimplePath(f.getName()))
                .collect(Collectors.toList()),
            tableLocation);
    return new TableFunctionConfig(
        TableFunctionConfig.FunctionType.ICEBERG_ORPHAN_FILE_DELETE, true, context);
  }

  public static TableFunctionConfig getLocationFinderTableFunctionConfig(
      TableFunctionConfig.FunctionType functionType,
      BatchSchema outputSchema,
      StoragePluginId storagePluginId,
      Map<String, String> tablePropertiesSkipCriteria,
      boolean continueOnError) {
    TableFunctionContext context =
        new IcebergLocationFinderFunctionContext(
            storagePluginId,
            outputSchema,
            SchemaUtilities.allColPaths(outputSchema),
            tablePropertiesSkipCriteria,
            continueOnError);
    return new TableFunctionConfig(functionType, true, context);
  }

  public static TableFunctionContext getDeletedFilesMetadataTableFunctionContext(
      OperationType operationType,
      BatchSchema schema,
      List<SchemaPath> columns,
      boolean isIcebergMetadata) {
    return new DeletedFilesMetadataTableFunctionContext(
        operationType,
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
        false,
        false,
        isIcebergMetadata,
        null);
  }

  public static Function<Prel, TableFunctionPrel> getHashExchangeTableFunctionCreator(
      final TableMetadata tableMetadata, boolean isIcebergMetadata) {
    return input ->
        getSplitAssignTableFunction(
            input, tableMetadata, tableMetadata.getSchema(), isIcebergMetadata, null);
  }

  public static Function<Prel, TableFunctionPrel> getHashExchangeTableFunctionCreator(
      final TableMetadata tableMetadata,
      BatchSchema tableSchema,
      boolean isIcebergMetadata,
      StoragePluginId storagePluginId) {
    return input ->
        getSplitAssignTableFunction(
            input, tableMetadata, tableSchema, isIcebergMetadata, storagePluginId);
  }

  public static Function<Prel, TableFunctionPrel> getTableAgnosticHashExchangeTableFunctionCreator(
      StoragePluginId storagePluginId, String user) {
    return input -> {
      RelDataTypeFactory.FieldInfoBuilder fieldInfoBuilder =
          new RelDataTypeFactory.FieldInfoBuilder(input.getCluster().getTypeFactory());
      input.getRowType().getFieldList().forEach(f -> fieldInfoBuilder.add(f));
      RelDataType intType =
          CalciteArrowHelper.wrap(CompleteType.INT)
              .toCalciteType(input.getCluster().getTypeFactory(), false);
      fieldInfoBuilder.add(HashPrelUtil.HASH_EXPR_NAME, intType);
      RelDataType output = fieldInfoBuilder.build();

      BatchSchema outputSchema = CalciteArrowHelper.fromCalciteRowType(output);
      TableFunctionConfig tableFunctionConfig =
          getTableFunctionConfig(
              TableFunctionConfig.FunctionType.SPLIT_ASSIGNMENT, outputSchema, storagePluginId);
      return new TableFunctionPrel(
          input.getCluster(),
          input.getTraitSet(),
          input,
          tableFunctionConfig,
          output,
          null,
          null,
          user);
    };
  }

  private static TableFunctionPrel getSplitAssignTableFunction(
      Prel input,
      TableMetadata tableMetadata,
      BatchSchema tableSchema,
      boolean isIcebergMetadata,
      StoragePluginId storagePluginId) {
    RelDataTypeFactory.FieldInfoBuilder fieldInfoBuilder =
        new RelDataTypeFactory.FieldInfoBuilder(input.getCluster().getTypeFactory());
    input.getRowType().getFieldList().forEach(f -> fieldInfoBuilder.add(f));
    RelDataType intType =
        CalciteArrowHelper.wrap(CompleteType.INT)
            .toCalciteType(input.getCluster().getTypeFactory(), false);
    fieldInfoBuilder.add(HashPrelUtil.HASH_EXPR_NAME, intType);
    RelDataType output = fieldInfoBuilder.build();

    BatchSchema outputSchema = CalciteArrowHelper.fromCalciteRowType(output);
    ImmutableList.Builder<SchemaPath> builder = ImmutableList.builder();
    for (Field field : outputSchema) {
      builder.add(SchemaPath.getSimplePath(field.getName()));
    }
    ImmutableList<SchemaPath> outputColumns = builder.build();
    TableFunctionContext tableFunctionContext =
        new TableFunctionContext(
            tableMetadata.getFormatSettings(),
            outputSchema,
            tableSchema,
            ImmutableList.of(tableMetadata.getName().getPathComponents()),
            null,
            null,
            storagePluginId == null ? tableMetadata.getStoragePluginId() : storagePluginId,
            getInternalTablePluginId(tableMetadata),
            outputColumns,
            null,
            null,
            false,
            false,
            isIcebergMetadata,
            tableMetadata.getDatasetConfig().getPhysicalDataset().getInternalSchemaSettings());
    TableFunctionConfig tableFunctionConfig =
        new TableFunctionConfig(
            TableFunctionConfig.FunctionType.SPLIT_ASSIGNMENT, true, tableFunctionContext);
    return new TableFunctionPrel(
        input.getCluster(),
        input.getTraitSet(),
        null,
        input,
        tableMetadata,
        tableFunctionConfig,
        output);
  }

  public static List<String> getDataset(TableFunctionConfig functionConfig) {
    TableFunctionContext functionContext = functionConfig.getFunctionContext();
    Collection<List<String>> referencedTables = functionContext.getReferencedTables();
    return referencedTables != null ? referencedTables.iterator().next() : null;
  }
}
