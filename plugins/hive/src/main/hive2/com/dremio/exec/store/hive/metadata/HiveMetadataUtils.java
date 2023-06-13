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
package com.dremio.exec.store.hive.metadata;

import static com.dremio.exec.store.hive.metadata.HivePartitionChunkListing.SplitType.DIR_LIST_INPUT_SPLIT;
import static com.dremio.exec.store.hive.metadata.HivePartitionChunkListing.SplitType.INPUT_SPLIT;
import static com.dremio.exec.store.iceberg.IcebergSerDe.serializedSchemaAsJson;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.annotation.Annotation;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSplit;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.HiveDecimalUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.io.FileIO;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.util.Closeable;
import com.dremio.common.util.DateTimes;
import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.DatasetSplit;
import com.dremio.connector.metadata.DatasetSplitAffinity;
import com.dremio.connector.metadata.DatasetStats;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.MetadataOption;
import com.dremio.connector.metadata.PartitionValue;
import com.dremio.connector.metadata.options.DirListInputSplitType;
import com.dremio.connector.metadata.options.IgnoreAuthzErrors;
import com.dremio.connector.metadata.options.MaxLeafFieldCount;
import com.dremio.connector.metadata.options.MaxNestedFieldLevels;
import com.dremio.connector.metadata.options.RefreshTableFilterOption;
import com.dremio.connector.metadata.options.TimeTravelOption;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.ColumnCountTooLargeException;
import com.dremio.exec.planner.cost.ScanCostFactor;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.TimedRunnable;
import com.dremio.exec.store.deltalake.DeltaLakeSchemaConverter;
import com.dremio.exec.store.deltalake.DeltaLakeTable;
import com.dremio.exec.store.deltalake.DeltaLogSnapshot;
import com.dremio.exec.store.dfs.implicit.DecimalTools;
import com.dremio.exec.store.hive.HiveClient;
import com.dremio.exec.store.hive.HivePf4jPlugin;
import com.dremio.exec.store.hive.HiveSchemaConverter;
import com.dremio.exec.store.hive.HiveSchemaTypeOptions;
import com.dremio.exec.store.hive.HiveStoragePlugin;
import com.dremio.exec.store.hive.HiveUtilities;
import com.dremio.exec.store.hive.deltalake.DeltaHiveInputFormat;
import com.dremio.exec.store.hive.exec.apache.HadoopFileSystemWrapper;
import com.dremio.exec.store.hive.exec.apache.PathUtils;
import com.dremio.exec.store.hive.exec.metadata.SchemaConverter;
import com.dremio.exec.store.hive.iceberg.IcebergHiveTableOperations;
import com.dremio.exec.store.hive.iceberg.IcebergInputFormat;
import com.dremio.exec.store.iceberg.DremioFileIO;
import com.dremio.exec.store.iceberg.IcebergSerDe;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.exec.store.iceberg.TableSchemaProvider;
import com.dremio.exec.store.iceberg.TableSnapshotProvider;
import com.dremio.exec.store.iceberg.TimeTravelProcessors;
import com.dremio.hive.proto.HiveReaderProto;
import com.dremio.hive.proto.HiveReaderProto.ColumnInfo;
import com.dremio.hive.proto.HiveReaderProto.HivePrimitiveType;
import com.dremio.hive.proto.HiveReaderProto.HiveSplitXattr;
import com.dremio.hive.proto.HiveReaderProto.HiveTableXattr;
import com.dremio.hive.proto.HiveReaderProto.PartitionXattr;
import com.dremio.hive.proto.HiveReaderProto.Prop;
import com.dremio.hive.thrift.TException;
import com.dremio.options.OptionManager;
import com.dremio.service.namespace.dataset.proto.IcebergMetadata;
import com.dremio.service.namespace.dataset.proto.ScanStats;
import com.dremio.service.namespace.dataset.proto.ScanStatsType;
import com.dremio.service.namespace.dirlist.proto.DirListInputSplitProto;
import com.dremio.service.namespace.file.proto.FileType;
import com.dremio.service.users.SystemUser;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.google.common.math.LongMath;

import io.protostuff.ByteString;

public class HiveMetadataUtils {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HiveMetadataUtils.class);

  private static final String EMPTY_STRING = "";
  private static final Long ONE = 1L;
  private static final int INPUT_SPLIT_LENGTH_RUNNABLE_PARALLELISM = 16;
  private static final long MAX_NAMENODE_FS_CALL_TIMEOUT = 2000L;
  private static final Joiner PARTITION_FIELD_SPLIT_KEY_JOINER = Joiner.on("__");
  public static final String TABLE_TYPE = "table_type";
  public static final String ICEBERG = "iceberg";
  public static final String METADATA_LOCATION = "metadata_location";

  public static class SchemaComponents {
    private final String tableName;
    private final String dbName;

    public SchemaComponents(String dbName, String tableName) {
      this.dbName = dbName;
      this.tableName = tableName;
    }

    public String getTableName() {
      return tableName;
    }

    public String getDbName() {
      return dbName;
    }
  }

  /**
   * Applies Hive configuration if Orc fileIds are not supported by the table's underlying filesystem.
   *
   * @param storageCapabilities        The storageCapabilities.
   * @param tableOrPartitionProperties Properties of the table or partition which may be altered.
   */
  public static void injectOrcIncludeFileIdInSplitsConf(final HiveStorageCapabilities storageCapabilities,
                                                        final Properties tableOrPartitionProperties) {
    if (!storageCapabilities.supportsOrcSplitFileIds()) {
      tableOrPartitionProperties.put(HiveConf.ConfVars.HIVE_ORC_INCLUDE_FILE_ID_IN_SPLITS.varname, "false");
    }
  }

  public static boolean isValidPathSchema(final List<String> pathComponents) {
    return pathComponents != null && (pathComponents.size() == 2 || pathComponents.size() == 3);
  }

  public static SchemaComponents resolveSchemaComponents(final List<String> pathComponents) {
    // extract database and table names from dataset path
    switch (pathComponents.size()) {
      case 2:
        return new SchemaComponents("default", pathComponents.get(1));
      case 3:
        return new SchemaComponents(pathComponents.get(1), pathComponents.get(2));
      default:
        // invalid. Guarded against at both entry points.
        throw UserException.connectionError()
          .message("Dataset path '%s' is invalid.", pathComponents)
          .build(logger);
    }
  }

  public static String resolveCreateTableLocation(HiveConf conf, SchemaComponents schemaComponents, String queryLocation) {
    try (final Closeable ccls = HivePf4jPlugin.swapClassLoader()) {
      String tableLocation = null;
      if (StringUtils.isNotEmpty(queryLocation)) {
        tableLocation = queryLocation;
      } else {
        String warehouseLocation = HiveConf.getVar(conf, HiveConf.ConfVars.METASTOREWAREHOUSE);
        if(StringUtils.isEmpty(warehouseLocation) || HiveConf.ConfVars.METASTOREWAREHOUSE.getDefaultValue().equals(warehouseLocation)) {
          logger.error("Advanced Property {} not set. Please set it to have a valid location to create table.", HiveConf.ConfVars.METASTOREWAREHOUSE.varname);
          throw UserException.unsupportedError().message("Unable to create table. Please set the default warehouse location").buildSilently();
        }

        warehouseLocation = com.dremio.common.utils.PathUtils.removeTrailingSlash(warehouseLocation);
        tableLocation = String.format("%s/%s/%s", warehouseLocation, schemaComponents.getDbName(), schemaComponents.getTableName());
      }

      try {
        return Utilities.getQualifiedPath(conf, new Path(tableLocation));
      } catch (HiveException e) {
        throw UserException.ioExceptionError()
          .message("Location given to create table %s is invalid %s.", schemaComponents.getTableName(), tableLocation)
          .buildSilently();
      }
    }
  }

  public static  String getIcebergTableLocation(HiveClient client, HiveMetadataUtils.SchemaComponents schemaComponents) throws TException {
    Table table = client.getTable(schemaComponents.getDbName(), schemaComponents.getTableName(), true);
    Preconditions.checkArgument(HiveMetadataUtils.isIcebergTable(table), String.format("Not iceberg table DatabaseName: %s  TableName: %s",  schemaComponents.getDbName(), schemaComponents.getTableName()));
    String tableMetadataLocation = table.getSd().getLocation();
    return tableMetadataLocation;
  }

  public static InputFormat<?, ?> getInputFormat(Table table, final HiveConf hiveConf, OptionManager options) {
    try (final Closeable ccls = HivePf4jPlugin.swapClassLoader()) {
      final JobConf job = new JobConf(hiveConf);
      return getInputFormat(table, job, null, options);
    }
  }

  public static InputFormat<?, ?> getInputFormat(Table table, final JobConf job, Partition partition, OptionManager options) {
    if (isIcebergTable(table)) {
      return new IcebergInputFormat();
    }
    if (isDeltaTable(table, options)) {
      return new DeltaHiveInputFormat();
    }
    final Class<? extends InputFormat> inputFormatClazz = getInputFormatClass(job, table, partition);
    job.setInputFormat(inputFormatClazz);
    return job.getInputFormat();
  }

  public static boolean isTransactionalTable(Table table) {
    return (table != null && table.getParameters() != null && AcidUtils.isTablePropertyTransactional(table.getParameters()));
  }

  public static boolean isValidInputFormatForIcebergExecution(Table table, HiveConf conf, HiveStoragePlugin plugin) {
    final InputFormat<?, ?> format = HiveMetadataUtils.getInputFormat(table, conf, plugin.getSabotContext().getOptionManager());
    return ((isParquetFormat(format) && !shouldUseFileSplitsFromInputFormat(format))
      || isAvroFormat(format)
      || (isOrcFormat(format) && !isTransactionalTable(table)))
      && !isIcebergInputFormat(format);
  }

  public static boolean isIcebergTable(Table table) {
    String tableTypeValue = table.getParameters().get(TABLE_TYPE);
    return tableTypeValue != null && tableTypeValue.equalsIgnoreCase(ICEBERG);
  }

  private static boolean isDeltaTable(Table table, OptionManager options) {
    return DeltaHiveInputFormat.isDeltaTable(table, options);
  }

  public static BatchSchema getBatchSchema(Table table, final HiveConf hiveConf, HiveSchemaTypeOptions typeOptions, HiveStoragePlugin plugin) {
    InputFormat<?, ?> format = getInputFormat(table, hiveConf, plugin.getSabotContext().getOptionManager());
    final List<Field> fields = new ArrayList<>();
    final List<String> partitionColumns = new ArrayList<>();
    HiveMetadataUtils.populateFieldsAndPartitionColumns(table, fields, partitionColumns, format, typeOptions);
    return BatchSchema.newBuilder().addFields(fields).build();
  }

  public static boolean isVarcharTruncateSupported(InputFormat<?, ?> format) {
    return isParquetFormat(format);
  }

  public static boolean hasVarcharColumnInTableSchema(
    final Table table, final HiveConf hiveConf, final HiveStoragePlugin plugin
  ) {
    InputFormat<?, ?> format = getInputFormat(table, hiveConf, plugin.getSabotContext().getOptionManager());
    if (!isVarcharTruncateSupported(format)) {
      return false;
    }

    for (FieldSchema hiveField : table.getSd().getCols()) {
      if (isFieldTypeVarchar(hiveField)) {
        return true;
      }
    }

    for (FieldSchema hiveField : table.getPartitionKeys()) {
      if (isFieldTypeVarchar(hiveField)) {
        return true;
      }
    }

    return false;
  }

  private static boolean isFieldTypeVarchar(FieldSchema hiveField) {
    final TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(hiveField.getType());
    if (typeInfo.getCategory() == Category.PRIMITIVE) {
      PrimitiveTypeInfo pTypeInfo = (PrimitiveTypeInfo) typeInfo;
      if (pTypeInfo.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.VARCHAR ||
        pTypeInfo.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.CHAR) {
        return true;
      }
    }
    return false;
  }

  private static void populateFieldsAndPartitionColumns(
    final Table table,
    final List<Field> fields,
    final List<String> partitionColumns,
    InputFormat<?, ?> format,
    final HiveSchemaTypeOptions typeOptions) {
    for (FieldSchema hiveField : table.getSd().getCols()) {
      final TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(hiveField.getType());
      Field f = HiveSchemaConverter.getArrowFieldFromHiveType(hiveField.getName(), typeInfo, format, typeOptions);
      if (f != null) {
        fields.add(f);
      }
    }
    for (FieldSchema field : table.getPartitionKeys()) {
      Field f = HiveSchemaConverter.getArrowFieldFromHiveType(field.getName(),
        TypeInfoUtils.getTypeInfoFromTypeString(field.getType()), format, typeOptions);
      if (f != null) {
        fields.add(f);
        partitionColumns.add(field.getName());
      }
    }
  }

  private static List<ColumnInfo> buildColumnInfo(final Table table, final InputFormat<?, ?> format, final HiveSchemaTypeOptions typeOptions) {
    final List<ColumnInfo> columnInfos = new ArrayList<>();
    for (FieldSchema hiveField : table.getSd().getCols()) {
      final TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(hiveField.getType());
      Field f = HiveSchemaConverter.getArrowFieldFromHiveType(hiveField.getName(), typeInfo, format, typeOptions);
      if (f != null) {
        columnInfos.add(getColumnInfo(typeInfo));
      }
    }
    return columnInfos;
  }

  private static ColumnInfo getColumnInfo(final TypeInfo typeInfo) {
    if (typeInfo.getCategory() == Category.PRIMITIVE) {
      final PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) typeInfo;
      switch (primitiveTypeInfo.getPrimitiveCategory()) {
        case BOOLEAN:
          return ColumnInfo.newBuilder()
            .setPrimitiveType(HivePrimitiveType.BOOLEAN)
            .setPrecision(0)
            .setScale(0)
            .setIsPrimitive(true)
            .build();

        case BYTE:
          return ColumnInfo.newBuilder()
            .setPrimitiveType(HivePrimitiveType.BYTE)
            .setPrecision(0)
            .setScale(0)
            .setIsPrimitive(true)
            .build();

        case SHORT:
          return ColumnInfo.newBuilder()
            .setPrimitiveType(HivePrimitiveType.SHORT)
            .setPrecision(0)
            .setScale(0)
            .setIsPrimitive(true)
            .build();

        case INT:
          return ColumnInfo.newBuilder()
            .setPrimitiveType(HivePrimitiveType.INT)
            .setPrecision(0)
            .setScale(0)
            .setIsPrimitive(true)
            .build();

        case LONG:
          return ColumnInfo.newBuilder()
            .setPrimitiveType(HivePrimitiveType.LONG)
            .setPrecision(0)
            .setScale(0)
            .setIsPrimitive(true)
            .build();

        case FLOAT:
          return ColumnInfo.newBuilder()
            .setPrimitiveType(HivePrimitiveType.FLOAT)
            .setPrecision(0)
            .setScale(0)
            .setIsPrimitive(true)
            .build();

        case DOUBLE:
          return ColumnInfo.newBuilder()
            .setPrimitiveType(HivePrimitiveType.DOUBLE)
            .setPrecision(0)
            .setScale(0)
            .setIsPrimitive(true)
            .build();

        case DATE:
          return ColumnInfo.newBuilder()
            .setPrimitiveType(HivePrimitiveType.DATE)
            .setPrecision(0)
            .setScale(0)
            .setIsPrimitive(true)
            .build();

        case TIMESTAMP:
          return ColumnInfo.newBuilder()
            .setPrimitiveType(HivePrimitiveType.TIMESTAMP)
            .setPrecision(0)
            .setScale(0)
            .setIsPrimitive(true)
            .build();

        case BINARY:
          return ColumnInfo.newBuilder()
            .setPrimitiveType(HivePrimitiveType.BINARY)
            .setPrecision(0)
            .setScale(0)
            .setIsPrimitive(true)
            .build();

        case DECIMAL:
          final DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) primitiveTypeInfo;
          return ColumnInfo.newBuilder()
            .setPrimitiveType(HivePrimitiveType.DECIMAL)
            .setPrecision(decimalTypeInfo.getPrecision())
            .setScale(decimalTypeInfo.getScale())
            .setIsPrimitive(true)
            .build();

        case STRING:
          return ColumnInfo.newBuilder()
            .setPrimitiveType(HivePrimitiveType.STRING)
            .setPrecision(0)
            .setScale(0)
            .setIsPrimitive(true)
            .build();

        case VARCHAR:
          return ColumnInfo.newBuilder()
            .setPrimitiveType(HivePrimitiveType.VARCHAR)
            .setPrecision(0)
            .setScale(0)
            .setIsPrimitive(true)
            .build();

        case CHAR:
          return ColumnInfo.newBuilder()
            .setPrimitiveType(HivePrimitiveType.CHAR)
            .setPrecision(((CharTypeInfo) typeInfo).getLength())
            .setScale(0)
            .setIsPrimitive(true)
            .build();

        default:
          break;
      }
    }

    return ColumnInfo.newBuilder()
      .setPrecision(0)
      .setScale(0)
      .setIsPrimitive(false)
      .build();
  }

  public static TableMetadata getTableMetadata(final HiveClient client,
                                               final EntityPath datasetPath,
                                               final boolean ignoreAuthzErrors,
                                               final int maxMetadataLeafColumns,
                                               final int maxNestedLevels,
                                               final TimeTravelOption timeTravelOption,
                                               final HiveSchemaTypeOptions typeOptions,
                                               final HiveConf hiveConf,
                                               final HiveStoragePlugin plugin) throws ConnectorException {

    try {
      final SchemaComponents schemaComponents = resolveSchemaComponents(datasetPath.getComponents());

      // if the dataset path is not canonized we need to get it from the source
      final Table table = client.getTable(schemaComponents.getDbName(), schemaComponents.getTableName(), ignoreAuthzErrors);
      if (table == null) {
        // invalid. Guarded against at both entry points.
        throw new ConnectorException(String.format("Dataset path '%s', table not found.", datasetPath));
      }
      final Properties tableProperties = MetaStoreUtils.getSchema(table.getSd(), table.getSd(), table.getParameters(), table.getDbName(), table.getTableName(), table.getPartitionKeys());
      TableMetadata tableMetadata;
      if (isIcebergTable(table)) {
        tableMetadata = getTableMetadataFromIceberg(hiveConf, datasetPath, table, tableProperties, timeTravelOption, typeOptions, plugin);
      } else if (isDeltaTable(table, plugin.getSabotContext().getOptionManager())) {
        tableMetadata = getTableMetadataFromDelta(table, tableProperties, maxMetadataLeafColumns, typeOptions, plugin);
      } else {
        tableMetadata = getTableMetadataFromHMS(table, tableProperties, datasetPath,
          maxMetadataLeafColumns, maxNestedLevels, typeOptions, hiveConf, plugin);
      }
      HiveMetadataUtils.injectOrcIncludeFileIdInSplitsConf(tableMetadata.getTableStorageCapabilities(), tableProperties);
      return tableMetadata;
    } catch (ConnectorException e) {
      throw e;
    } catch (Exception e) {
      throw new ConnectorException(e);
    }
  }

  private static TableMetadata getTableMetadataFromIceberg(final HiveConf hiveConf,
                                                           final EntityPath datasetPath,
                                                           final Table table,
                                                           final Properties tableProperties,
                                                           final TimeTravelOption timeTravelOption,
                                                           final HiveSchemaTypeOptions typeOptions,
                                                           final HiveStoragePlugin plugin) throws IOException {
    JobConf jobConf = new JobConf(hiveConf);

    String metadataLocation = tableProperties.getProperty(METADATA_LOCATION, "");
    com.dremio.io.file.FileSystem fs = plugin.createFS(metadataLocation, SystemUser.SYSTEM_USERNAME, null);
    FileIO fileIO = plugin.createIcebergFileIO(fs, null, null, null, null);
    IcebergHiveTableOperations hiveTableOperations = new IcebergHiveTableOperations(fileIO, metadataLocation);
    BaseTable icebergTable = new BaseTable(hiveTableOperations, new Path(metadataLocation).getName());
    icebergTable.refresh();

    final Snapshot snapshot;
    org.apache.iceberg.Schema schema;
    if (timeTravelOption != null) {
      TimeTravelOption.TimeTravelRequest travelRequest = timeTravelOption.getTimeTravelRequest();
      final TableSnapshotProvider tableSnapshotProvider =
        TimeTravelProcessors.getTableSnapshotProvider(datasetPath.getComponents(), travelRequest);
      final TableSchemaProvider tableSchemaProvider =
              TimeTravelProcessors.getTableSchemaProvider(travelRequest);
      snapshot = tableSnapshotProvider.apply(icebergTable);
      schema = tableSchemaProvider.apply(icebergTable, snapshot);
    } else {
      snapshot = icebergTable.currentSnapshot();
      schema = icebergTable.schema();
    }

    long numRecords = snapshot != null ? Long.parseLong(snapshot.summary().getOrDefault("total-records", "0")) : 0L;
    long numDataFiles = snapshot != null ? Long.parseLong(snapshot.summary().getOrDefault("total-data-files", "0")) : 0L;
    long numPositionDeletes = snapshot != null ?
        Long.parseLong(snapshot.summary().getOrDefault("total-position-deletes", "0")) : 0L;
    long numEqualityDeletes = snapshot != null ?
        Long.parseLong(snapshot.summary().getOrDefault("total-equality-deletes", "0")) : 0L;
    long numDeleteFiles = snapshot != null ?
        Long.parseLong(snapshot.summary().getOrDefault("total-delete-files", "0")) : 0L;

    if (numDeleteFiles > 0 &&
      !plugin.getSabotContext().getOptionManager().getOption(ExecConstants.ENABLE_ICEBERG_MERGE_ON_READ_SCAN)) {
      throw UserException.unsupportedError()
        .message("Iceberg V2 tables with delete files are not supported")
        .buildSilently();
    }

    if (numEqualityDeletes > 0 &&
      !plugin.getSabotContext().getOptionManager().getOption(ExecConstants.ENABLE_ICEBERG_MERGE_ON_READ_SCAN_WITH_EQUALITY_DELETE)) {
      throw UserException.unsupportedError()
        .message("Iceberg V2 tables with equality deletes are not supported.")
        .buildSilently();
    }

    SchemaConverter schemaConverter = SchemaConverter.getBuilder().setTableName(table.getTableName())
        .setMapTypeEnabled(typeOptions.isMapTypeEnabled()).build();
    BatchSchema batchSchema = schemaConverter.fromIceberg(schema);
    Map<Integer, PartitionSpec> specsMap = icebergTable.specs();
    specsMap = IcebergUtils.getPartitionSpecMapBySchema(specsMap, schema);
    byte[] specs = IcebergSerDe.serializePartitionSpecAsJsonMap(specsMap);
    final long snapshotId = snapshot != null ? snapshot.snapshotId() : -1;

    IcebergMetadata icebergMetadata = new IcebergMetadata()
            .setFileType(FileType.ICEBERG)
            .setPartitionSpecsJsonMap(ByteString.copyFrom(specs))
            .setJsonSchema(serializedSchemaAsJson(schema))
            .setMetadataFileLocation(metadataLocation)
            .setSnapshotId(snapshotId)
            .setDeleteManifestStats(new ScanStats()
                .setScanFactor(ScanCostFactor.EASY.getFactor())
                .setType(ScanStatsType.EXACT_ROW_COUNT)
                .setRecordCount(numDeleteFiles))
            .setDeleteStats(new ScanStats()
                .setScanFactor(ScanCostFactor.PARQUET.getFactor())
                .setType(ScanStatsType.EXACT_ROW_COUNT)
                .setRecordCount(numPositionDeletes))
            .setEqualityDeleteStats(new ScanStats()
              .setScanFactor(ScanCostFactor.PARQUET.getFactor())
              .setType(ScanStatsType.EXACT_ROW_COUNT)
              .setRecordCount(numEqualityDeletes));

    return TableMetadata.newBuilder()
      .table(table)
      .tableProperties(tableProperties)
      .batchSchema(batchSchema)
      .fields(batchSchema.getFields())
      .partitionColumns(schemaConverter.getPartitionColumns(icebergTable))
      .columnInfos(new ArrayList<>())
      .icebergMetadata(icebergMetadata)
      .manifestStats(DatasetStats.of(numDataFiles, ScanCostFactor.EASY.getFactor()))
      .recordCount(numRecords)
      .build();
  }

  private static TableMetadata getTableMetadataFromDelta(final Table table,
                                                         final Properties tableProperties,
                                                         final int maxMetadataLeafColumns,
                                                         final HiveSchemaTypeOptions typeOptions,
                                                         final HiveStoragePlugin plugin) throws IOException {
    final String tableLocation = DeltaHiveInputFormat.getLocation(table, plugin.getSabotContext().getOptionManager());
    final com.dremio.io.file.FileSystem fs = plugin.createFS(tableLocation, SystemUser.SYSTEM_USERNAME, null);
    final DeltaLakeTable deltaTable = new DeltaLakeTable(plugin.getSabotContext(), fs, tableLocation);
    final DeltaLogSnapshot snapshot = deltaTable.getConsolidatedSnapshot();

    final BatchSchema batchSchema = DeltaLakeSchemaConverter.withMapEnabled(typeOptions.isMapTypeEnabled()).fromSchemaString(snapshot.getSchema());
    HiveMetadataUtils.checkLeafFieldCounter(batchSchema.getFields().size(), maxMetadataLeafColumns, "");

    return TableMetadata.newBuilder()
      .table(table)
      .tableProperties(tableProperties)
      .batchSchema(batchSchema)
      .fields(batchSchema.getFields())
      .partitionColumns(snapshot.getPartitionColumns())
      .columnInfos(new ArrayList<>())
      .manifestStats(DatasetStats.of(snapshot.getDataFileEntryCount(), ScanCostFactor.EASY.getFactor()))
      .recordCount(snapshot.getNetOutputRows())
      .build();
  }

  private static TableMetadata getTableMetadataFromHMS(final Table table,
                                                       final Properties tableProperties,
                                                       final EntityPath datasetPath,
                                                       final int maxMetadataLeafColumns,
                                                       final int maxNestedLevels,
                                                       final HiveSchemaTypeOptions typeOptions,
                                                       final HiveConf hiveConf,
                                                       final HiveStoragePlugin plugin) throws ConnectorException {


    final SchemaComponents schemaComponents = resolveSchemaComponents(datasetPath.getComponents());

    final InputFormat<?, ?> format = getInputFormat(table, hiveConf, plugin.getSabotContext().getOptionManager());

    final List<Field> fields = new ArrayList<>();
    final List<String> partitionColumns = new ArrayList<>();

    HiveMetadataUtils.populateFieldsAndPartitionColumns(table, fields, partitionColumns, format, typeOptions);
    HiveMetadataUtils.checkLeafFieldCounter(fields.size(), maxMetadataLeafColumns, schemaComponents.getTableName());
    HiveSchemaConverter.checkFieldNestedLevels(table, maxNestedLevels, typeOptions.isMapTypeEnabled());
    final BatchSchema batchSchema = BatchSchema.newBuilder().addFields(fields).build();

    final List<ColumnInfo> columnInfos = buildColumnInfo(table, format, typeOptions);

    return TableMetadata.newBuilder()
      .table(table)
      .tableProperties(tableProperties)
      .batchSchema(batchSchema)
      .fields(fields)
      .partitionColumns(partitionColumns)
      .columnInfos(columnInfos)
      .build();
  }

  /**
   * Get the stats from table properties. If not found -1 is returned for each stats field.
   * CAUTION: stats may not be up-to-date with the underlying data. It is always good to run the ANALYZE command on
   * Hive table to have up-to-date stats.
   *
   * @param properties
   * @return
   */
  public static HiveDatasetStats getStatsFromProps(final Properties properties) {
    long numRows = -1;
    long sizeInBytes = -1;
    try {
      final String numRowsProp = properties.getProperty(StatsSetupConst.ROW_COUNT);
      if (numRowsProp != null) {
        numRows = Long.valueOf(numRowsProp);
      }

      final String sizeInBytesProp = properties.getProperty(StatsSetupConst.TOTAL_SIZE);
      if (sizeInBytesProp != null) {
        sizeInBytes = Long.valueOf(sizeInBytesProp);
      }
    } catch (final NumberFormatException e) {
      logger.error("Failed to parse Hive stats in metastore.", e);
      // continue with the defaults.
    }

    return new HiveDatasetStats(numRows, sizeInBytes);
  }

  public static HiveReaderProto.SerializedInputSplit serialize(InputSplit split) {
    final ByteArrayDataOutput output = ByteStreams.newDataOutput();
    try {
      split.write(output);
    } catch (IOException e) {
      throw UserException.dataReadError(e).message(e.getMessage()).build(logger);
    }
    return HiveReaderProto.SerializedInputSplit.newBuilder()
      .setInputSplitClass(split.getClass().getName())
      .setInputSplit(com.google.protobuf.ByteString.copyFrom(output.toByteArray())).build();
  }

  public static boolean allowParquetNative(boolean currentStatus, Class<? extends InputFormat> clazz) {
    return currentStatus && MapredParquetInputFormat.class.isAssignableFrom(clazz);
  }

  public static boolean isInputFormatEqual(boolean currentStatus,
                                           Class<? extends InputFormat> clazz1,
                                           Class<? extends InputFormat> clazz2) {
    return currentStatus && (clazz1.isAssignableFrom(clazz2) || clazz2.isAssignableFrom(clazz1));
  }

  public static boolean isRecursive(Properties properties) {
    return "true".equalsIgnoreCase(properties.getProperty("mapred.input.dir.recursive", "false")) &&
      "true".equalsIgnoreCase(properties.getProperty("hive.mapred.supports.subdirectories", "false"));
  }

  public static void configureJob(final JobConf job, final Table table, final Properties tableProperties,
                                  Properties partitionProperties, StorageDescriptor storageDescriptor) {

    addConfToJob(job, tableProperties);
    if (partitionProperties != null) {
      addConfToJob(job, partitionProperties);
    }

    HiveUtilities.addACIDPropertiesIfNeeded(job);
    addInputPath(storageDescriptor, job);
  }

  public static List<Long> getInputSplitSizes(final JobConf job, String tableName, List<InputSplit> inputSplits) {
    List<TimedRunnable<Long>> splitSizeJobs = new ArrayList<>();
    long maxDeltas = populateSplitJobAndGetMaxDeltas(job, tableName, inputSplits, splitSizeJobs);
    long maxTimeoutPerCore = getMaxTimeoutPerCore(inputSplits, maxDeltas);
    return runInputSplitSizeRunnable(splitSizeJobs, tableName, maxTimeoutPerCore);
  }

  public static HiveSplitXattr buildHiveSplitXAttr(int partitionId, InputSplit inputSplit) {
    final HiveSplitXattr.Builder splitAttr = HiveSplitXattr.newBuilder();
    splitAttr.setPartitionId(partitionId);
    splitAttr.setInputSplit(serialize(inputSplit));
    setFileStats(inputSplit, splitAttr);

    return splitAttr.build();
  }

  private static void setFileStats(InputSplit inputSplit, HiveSplitXattr.Builder splitAttr) {
    if (inputSplit instanceof ParquetInputFormat.ParquetSplit) {
      splitAttr.setFileLength(((ParquetInputFormat.ParquetSplit) inputSplit).getFileSize());
      splitAttr.setLastModificationTime(((ParquetInputFormat.ParquetSplit) inputSplit).getModificationTime());
    }
  }

  public static List<DatasetSplit> getDatasetSplitsFromDirListSplits(TableMetadata tableMetadata,
                                                                     PartitionMetadata partitionMetadata) {

    if (partitionMetadata.getDirListInputSplit() == null) {
      throw UserException
        .dataReadError()
        .message("Splits expected but not available for table: '%s', partition: '%s'",
          tableMetadata.getTable().getTableName(),
          getPartitionValueLogString(partitionMetadata.getPartition()))
        .build(logger);
    }

    final DirListInputSplitProto.DirListInputSplit dirListInputSplit = partitionMetadata.getDirListInputSplit();
    return Collections.singletonList(DatasetSplit.of(Collections.emptyList(), 1, 1, dirListInputSplit::writeTo));
  }

  public static List<DatasetSplit> getDatasetSplitsFromInputSplits(TableMetadata tableMetadata,
                                                                   MetadataAccumulator metadataAccumulator,
                                                                   PartitionMetadata partitionMetadata,
                                                                   StatsEstimationParameters statsParams) {

    // This should be checked prior to entry.
    if (!partitionMetadata.getInputSplitBatchIterator().hasNext()) {
      throw UserException
        .dataReadError()
        .message("Splits expected but not available for table: '%s', partition: '%s'",
          tableMetadata.getTable().getTableName(),
          getPartitionValueLogString(partitionMetadata.getPartition()))
        .build(logger);
    }

    final List<InputSplit> inputSplits = partitionMetadata.getInputSplitBatchIterator().next();

    if (logger.isTraceEnabled()) {
      if (partitionMetadata.getPartitionValues().isEmpty()) {
        logger.trace("Getting {} datasetSplits for default partition",
          inputSplits.size());
      } else {
        logger.trace("Getting {} datasetSplits for hive partition '{}'",
          inputSplits.size(),
          getPartitionValueLogString(partitionMetadata.getPartition()));
      }
    }

    if (inputSplits.isEmpty()) {
      /**
       * Not possible.
       * currentHivePartitionMetadata.getInputSplitBatchIterator().hasNext() means inputSplits
       * exist.
       */
      throw new RuntimeException(
        String.format("Table '%s', partition '%s', Splits expected but not available for table.",
          tableMetadata.getTable().getTableName(),
          getPartitionValueLogString(partitionMetadata.getPartition())));
    }

    final List<DatasetSplit> datasetSplits = new ArrayList<>(inputSplits.size());

    final List<Long> inputSplitSizes = getInputSplitSizes(
      partitionMetadata.getDatasetSplitBuildConf().getJob(),
      tableMetadata.getTable().getTableName(),
      inputSplits);

    final long totalSizeOfInputSplits = inputSplitSizes.stream().mapToLong(Long::longValue).sum();
    final int estimatedRecordSize = tableMetadata.getBatchSchema().estimateRecordSize(statsParams.getListSizeEstimate(), statsParams.getVarFieldSizeEstimate());

    for (int i = 0; i < inputSplits.size(); i++) {
      final InputSplit inputSplit = inputSplits.get(i);
      final long inputSplitLength = inputSplitSizes.get(i);

      final long splitEstimatedRecords = findRowCountInSplit(
        statsParams,
        partitionMetadata.getDatasetSplitBuildConf().getMetastoreStats(),
        inputSplitLength / (double) totalSizeOfInputSplits,
        inputSplitLength,
        partitionMetadata.getDatasetSplitBuildConf().getFormat(),
        estimatedRecordSize);

      metadataAccumulator.accumulateTotalEstimatedRecords(splitEstimatedRecords);

      try {
        datasetSplits.add(
          DatasetSplit.of(
            Arrays.stream(inputSplit.getLocations())
              .map((input) -> DatasetSplitAffinity.of(input, inputSplitLength))
              .collect(ImmutableList.toImmutableList()),
            inputSplitSizes.get(i),
            splitEstimatedRecords,
            os -> os.write(buildHiveSplitXAttr(partitionMetadata.getPartitionId(), inputSplit).toByteArray())));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    return datasetSplits;
  }

  public static List<DatasetSplit> getDatasetSplitsForIcebergTables(TableMetadata tableMetadata) {
    return Arrays.asList(DatasetSplit.of(Collections.emptyList(), 0, 0));
  }

  /**
   * Helper class that returns the size of the {@link InputSplit}. For non-transactional tables, the size is straight
   * forward. For transactional tables (currently only supported in ORC format), length need to be derived by
   * fetching the file status of the delta files.
   * <p>
   * Logic for this class is derived from {@link OrcInputFormat#getRecordReader(InputSplit, JobConf, Reporter)}
   */
  private static class InputSplitSizeRunnable extends TimedRunnable<Long> {

    private final InputSplit split;
    private final Configuration conf;
    private final String tableName;

    public InputSplitSizeRunnable(final Configuration conf, final String tableName, final InputSplit split) {
      this.conf = conf;
      this.tableName = tableName;
      this.split = split;
    }

    @Override
    protected Long runInner() throws Exception {
      try {
        if (!(split instanceof OrcSplit)) {
          return split.getLength();
        }
      } catch (IOException e) {
        throw UserException.dataReadError(e).message(e.getMessage()).build(logger);
      }

      final OrcSplit orcSplit = (OrcSplit) split;

      try {
        if (!orcSplit.isAcid()) {
          return split.getLength();
        }
      } catch (IOException e) {
        throw UserException.dataReadError(e).message(e.getMessage()).build(logger);
      }

      try {
        long size = 0;

        final org.apache.hadoop.fs.Path path = orcSplit.getPath();
        final org.apache.hadoop.fs.Path root;
        final int bucket;

        // If the split has a base, extract the base file size, bucket and root path info.
        if (orcSplit.hasBase()) {
          if (orcSplit.isOriginal()) {
            root = path.getParent();
          } else {
            root = path.getParent().getParent();
          }
          size += orcSplit.getLength();
          bucket = AcidUtils.parseBaseOrDeltaBucketFilename(orcSplit.getPath(), conf).getBucket();
        } else {
          root = path;
          bucket = (int) orcSplit.getStart();
        }

        final org.apache.hadoop.fs.Path[] deltas = AcidUtils.deserializeDeltas(root, orcSplit.getDeltas());
        // go through each delta directory and add the size of the delta file belonging to the bucket to total split size
        for (org.apache.hadoop.fs.Path delta : deltas) {
          size += getSize(bucket, delta);
        }

        return (size > 0) ? size : ONE;
      } catch (Exception e) {
        logger.debug("Failed to derive the input split size of transactional Hive tables", e);
        // return a non-zero number - we don't want the metadata fetch operation to fail. We could ask the customer to
        // update the stats so that they can be used as part of the planning
        return ONE;
      }
    }

    private long getSize(int bucket, org.apache.hadoop.fs.Path delta) {
      long size = 0;
      try {
        final org.apache.hadoop.fs.Path deltaFile = AcidUtils.createBucketFile(delta, bucket);
        final FileSystem fs = deltaFile.getFileSystem(conf);
        final FileStatus fileStatus = fs.getFileStatus(deltaFile);
        size = fileStatus.getLen();
      } catch (IOException e) {
        // ignore exception since deltaFile may not exist
        logger.debug("Exception thrown while checking delta file sizes. Deltas not existing are expected exceptions.", e);
      }
      return size;
    }

    @Override
    protected IOException convertToIOException(Exception e) {
      return new IOException("Failure while trying to get split length for table " + tableName, e);
    }
  }

  public static HiveStorageCapabilities getHiveStorageCapabilities(final StorageDescriptor storageDescriptor) {
    return getHiveStorageCapabilities(storageDescriptor.getLocation());
  }

  public static HiveStorageCapabilities getHiveStorageCapabilities(final String location) {
    if (null != location) {
      final URI uri;
      try {
        uri = URI.create(location);
      } catch (IllegalArgumentException e) {
        // unknown table source, default to HDFS.
        return HiveStorageCapabilities.DEFAULT_HDFS;
      }

      final String scheme = uri.getScheme();
      if (!Strings.isNullOrEmpty(scheme)) {
        if (scheme.regionMatches(true, 0, "s3", 0, 2) ||
          scheme.regionMatches(true, 0, "wasb", 0, 4) ||
          scheme.regionMatches(true, 0, "abfs", 0, 4) ||
          scheme.regionMatches(true, 0, "wasbs", 0, 5) ||
          scheme.regionMatches(true, 0, "abfss", 0, 5) ||
          scheme.regionMatches(true, 0, "gs", 0, 2)) {
          /* Cloud FS do not support impersonation, last modified times or orc split file ids. */
          return HiveStorageCapabilities.newBuilder()
            .supportsImpersonation(false)
            .supportsLastModifiedTime(false)
            .supportsOrcSplitFileIds(false)
            .build();
        } else if (!scheme.regionMatches(true, 0, "hdfs", 0, 4)) {
          /* Most hive supported non-HDFS file systems allow for impersonation and last modified times, but
             not orc split file ids.  */
          return HiveStorageCapabilities.newBuilder()
            .supportsImpersonation(true)
            .supportsLastModifiedTime(true)
            .supportsOrcSplitFileIds(false)
            .build();
        }
      }
    }
    // Default to HDFS.
    return HiveStorageCapabilities.DEFAULT_HDFS;
  }

  /**
   * When impersonation is not possible and when last modified times are not available,
   * {@link HiveReaderProto.FileSystemPartitionUpdateKey} should not be generated.
   *
   * @param hiveStorageCapabilities The capabilities of the storage mechanism.
   * @param format                  The file input format.
   * @return true if FSUpdateKeys should be generated. False if not.
   */
  public static boolean shouldGenerateFileSystemUpdateKeys(final HiveStorageCapabilities hiveStorageCapabilities,
                                                           final InputFormat<?, ?> format) {

    if (!hiveStorageCapabilities.supportsImpersonation() && !hiveStorageCapabilities.supportsLastModifiedTime()) {
      return false;
    }

    // Files in a filesystem have last modified times and filesystem permissions. Generate
    // FileSystemPartitionUpdateKeys for formats representing files. Subclasses of FilInputFormat
    // as well as OrcInputFormat represent files.
    if ((format instanceof FileInputFormat) || (format instanceof OrcInputFormat)) {
      return true;
    }

    return false;
  }

  /**
   * {@link HiveReaderProto.FileSystemPartitionUpdateKey} stores the last modified time for each
   * entity so that changes can be detected. When impersonation is not enabled, checking each file
   * for access permissions is not required.
   * <p>
   * When the storage layer supports last modified times then entities should be recorded for each
   * folder which would signify if there was a change in any file in the directory.
   *
   * @param hiveStorageCapabilities     The capabilities of the storage mechanism.
   * @param storageImpersonationEnabled true if storage impersonation is enabled for the connection.
   * @return true if FSUpdateKeys should be generated. False if not.
   */
  public static boolean shouldGenerateFSUKeysForDirectoriesOnly(final HiveStorageCapabilities hiveStorageCapabilities,
                                                                final boolean storageImpersonationEnabled) {

    return !storageImpersonationEnabled && hiveStorageCapabilities.supportsLastModifiedTime();
  }

  /**
   * When splitType is {@link HivePartitionChunkListing.SplitType#DIR_LIST_INPUT_SPLIT},
   * some of the stats are not required (eg. which are expensive to generate).
   *
   * @return true if stats should be trimmed. False if not.
   */
  public static boolean trimStats(HivePartitionChunkListing.SplitType splitType) {
    if (splitType == DIR_LIST_INPUT_SPLIT) {
      return true;
    }
    return false;
  }

  public static PartitionMetadata getPartitionMetadata(final boolean storageImpersonationEnabled,
                                                       final boolean enforceVarcharWidth,
                                                       TableMetadata tableMetadata,
                                                       MetadataAccumulator metadataAccumulator,
                                                       Partition partition,
                                                       HiveConf hiveConf,
                                                       int partitionId,
                                                       int maxInputSplitsPerPartition,
                                                       HivePartitionChunkListing.SplitType splitType,
                                                       OptionManager optionManager) {
    try (final Closeable ccls = HivePf4jPlugin.swapClassLoader()) {
      final Table table = tableMetadata.getTable();
      final Properties tableProperties = tableMetadata.getTableProperties();
      final JobConf job = new JobConf(hiveConf);
      final PartitionXattr partitionXattr;

      List<InputSplit> inputSplits = Collections.emptyList();
      DirListInputSplitProto.DirListInputSplit dirListInputSplit = null;
      boolean trimStats = trimStats(splitType);
      HiveDatasetStats metastoreStats = null;
      InputFormat<?, ?> format = getInputFormat(table, job, partition, optionManager);
      Class<? extends InputFormat> inputFormatClazz = getInputFormatClass(job, table, partition);
      metadataAccumulator.setTableLocation(table.getSd().getLocation());

      if (null == partition) {
        partitionXattr = getPartitionXattr(table, fromProperties(tableMetadata.getTableProperties()));

        final HiveStorageCapabilities tableStorageCapabilities = tableMetadata.getTableStorageCapabilities();

        final StorageDescriptor storageDescriptor = table.getSd();
        if (splitType == DIR_LIST_INPUT_SPLIT) {
          dirListInputSplit = DirListInputSplitProto.DirListInputSplit.newBuilder()
                  .setRootPath(storageDescriptor.getLocation())
                  .setOperatingPath(storageDescriptor.getLocation())
                  .setReadSignature(Long.MAX_VALUE)
                  .build();
        } else if (splitType == INPUT_SPLIT) {
          if (inputPathExists(storageDescriptor, job)) {
            configureJob(job, table, tableProperties, null, storageDescriptor);
            inputSplits = getInputSplits(format, job);
          }
        }

        if (!trimStats && shouldGenerateFileSystemUpdateKeys(tableStorageCapabilities, format)) {
          final boolean generateFSUKeysForDirectoriesOnly =
            shouldGenerateFSUKeysForDirectoriesOnly(tableStorageCapabilities, storageImpersonationEnabled);
          final HiveReaderProto.FileSystemPartitionUpdateKey updateKey =
            getFSBasedUpdateKey(table.getSd().getLocation(), job, isRecursive(tableProperties), generateFSUKeysForDirectoriesOnly, 0);
          if (updateKey != null) {
            metadataAccumulator.accumulateFileSystemPartitionUpdateKey(updateKey);
          } else {
            metadataAccumulator.setNotAllFSBasedPartitions();
          }
        }

        metadataAccumulator.accumulateReaderType(inputFormatClazz);
        metastoreStats = getStatsFromProps(tableProperties);
      } else {
        final Properties partitionProperties = buildPartitionProperties(partition, table);
        final HiveStorageCapabilities partitionStorageCapabilities = getHiveStorageCapabilities(partition.getSd());

        final StorageDescriptor storageDescriptor = partition.getSd();
        if (splitType == DIR_LIST_INPUT_SPLIT) {
          dirListInputSplit = DirListInputSplitProto.DirListInputSplit.newBuilder()
                  .setRootPath(storageDescriptor.getLocation())
                  .setOperatingPath(storageDescriptor.getLocation())
                  .setReadSignature(Long.MAX_VALUE)
                  .build();
        } else if (splitType == INPUT_SPLIT) {
          if (inputPathExists(storageDescriptor, job)) {
            configureJob(job, table, tableProperties, partitionProperties, storageDescriptor);
            inputSplits = getInputSplits(format, job);
          }
        }

        if (!trimStats && shouldGenerateFileSystemUpdateKeys(partitionStorageCapabilities, format)) {
          final boolean generateFSUKeysForDirectoriesOnly =
            shouldGenerateFSUKeysForDirectoriesOnly(partitionStorageCapabilities, storageImpersonationEnabled);
          final HiveReaderProto.FileSystemPartitionUpdateKey updateKey =
            getFSBasedUpdateKey(partition.getSd().getLocation(), job, isRecursive(partitionProperties), generateFSUKeysForDirectoriesOnly, partitionId);
          if (updateKey != null) {
            metadataAccumulator.accumulateFileSystemPartitionUpdateKey(updateKey);
          }
        } else {
          metadataAccumulator.setNotAllFSBasedPartitions();
        }

        metadataAccumulator.accumulateReaderType(inputFormatClazz);
        metadataAccumulator.accumulatePartitionHash(partition);
        partitionXattr = metadataAccumulator.buildPartitionXattrDictionaries(partition, fromProperties(partitionProperties));

        metastoreStats = getStatsFromProps(partitionProperties);
      }

      List<PartitionValue> partitionValues = getPartitionValues(table, partition, enforceVarcharWidth);

      return PartitionMetadata.newBuilder()
        .partitionId(partitionId)
        .partition(partition)
        .partitionValues(partitionValues)
        .inputSplitBatchIterator(
          InputSplitBatchIterator.newBuilder()
            .tableMetadata(tableMetadata)
            .partition(partition)
            .inputSplits(inputSplits)
            .maxInputSplitsPerPartition(maxInputSplitsPerPartition)
            .build())
        .dirListInputSplit(dirListInputSplit)
        .datasetSplitBuildConf(
          DatasetSplitBuildConf.newBuilder()
            .job(job)
            .metastoreStats(metastoreStats)
            .format(format)
            .build())
        .partitionXattr(partitionXattr)
        .build();
    }
  }

  private static List<InputSplit> getInputSplits(final InputFormat<?, ?> format, final JobConf job) {
    InputSplit[] inputSplits;
    try {
      if (isParquetFormat(format) && !shouldUseFileSplitsFromInputFormat(format)) {
        inputSplits = new ParquetInputFormat().getSplits(job, 1);
      } else {
        inputSplits = format.getSplits(job, 1);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    if (null == inputSplits) {
      return Collections.emptyList();
    } else {
      return Arrays.asList(inputSplits);
    }
  }

  private static boolean shouldUseFileSplitsFromInputFormat(InputFormat<?, ?> inputFormat)
  {
    return Arrays.stream(inputFormat.getClass().getAnnotations())
            .map(Annotation::annotationType)
            .map(Class::getSimpleName)
            .anyMatch(name -> name.equals("UseFileSplitsFromInputFormat"));
  }

  private static boolean isParquetFormat(InputFormat<?, ?> format) {
    return MapredParquetInputFormat.class.isAssignableFrom(format.getClass());
  }

  private static boolean isOrcFormat(InputFormat<?, ?> format) {
    return OrcInputFormat.class.isAssignableFrom(format.getClass());
  }

  private static boolean isAvroFormat(InputFormat<?, ?> format) {
    return AvroContainerInputFormat.class.isAssignableFrom(format.getClass());
  }

  private static boolean isIcebergInputFormat(InputFormat<?, ?> format) {
    return (format instanceof IcebergInputFormat);
  }

  public static FileType getFileTypeFromInputFormat(Class<? extends InputFormat> inputFormat) {
    if (MapredParquetInputFormat.class.isAssignableFrom(inputFormat)) {
      return FileType.PARQUET;
    } else if (OrcInputFormat.class.isAssignableFrom(inputFormat)) {
      return FileType.ORC;
    } else if (AvroContainerInputFormat.class.isAssignableFrom(inputFormat)) {
      return FileType.AVRO;
    } else {
      throw UserException
        .unsupportedError()
        .message("File Format Type not support. Input File Format is %s", inputFormat.toString())
        .buildSilently();
    }
  }
  /**
   * Find the rowcount based on stats in Hive metastore or estimate using filesize/filetype/recordSize/split size
   *
   * @param statsParams         parameters controling the stats calculations
   * @param statsFromMetastore
   * @param sizeRatio           Ration of this split contributing to all stats in given <i>statsFromMetastore</i>
   * @param splitSizeInBytes
   * @param format
   * @param estimatedRecordSize
   * @return
   */
  public static long findRowCountInSplit(StatsEstimationParameters statsParams, HiveDatasetStats statsFromMetastore,
                                         final double sizeRatio, final long splitSizeInBytes, InputFormat<?, ?> format,
                                         final int estimatedRecordSize) {

    final Class<? extends InputFormat> inputFormat =
      format == null ? null : ((Class<? extends InputFormat>) format.getClass());

    double compressionFactor = 1.0;
    if (MapredParquetInputFormat.class.equals(inputFormat)) {
      compressionFactor = statsParams.getHiveSettings().getParquetCompressionFactor();
    } else if (OrcInputFormat.class.equals(inputFormat)) {
      compressionFactor = 30f;
    } else if (AvroContainerInputFormat.class.equals(inputFormat)) {
      compressionFactor = 10f;
    } else if (RCFileInputFormat.class.equals(inputFormat)) {
      compressionFactor = 10f;
    }

    final long estimatedRowCount = (long) Math.ceil(splitSizeInBytes * compressionFactor / estimatedRecordSize);

    // Metastore stats are for complete partition. Multiply it by the size ratio of this split
    final long metastoreRowCount = (long) Math.ceil(sizeRatio * statsFromMetastore.getRecordCount());

    logger.trace("Hive stats estimation: compression factor '{}', recordSize '{}', estimated '{}', from metastore '{}'",
      compressionFactor, estimatedRecordSize, estimatedRowCount, metastoreRowCount);

    if (statsParams.useMetastoreStats() && statsFromMetastore.hasContent()) {
      return metastoreRowCount;
    }

    // return the maximum of estimate and metastore count
    return Math.max(estimatedRowCount, metastoreRowCount);
  }

  public static PartitionXattr getTablePartitionProperty(HiveTableXattr.Builder tableExtended) {
    // set a single partition for a table
    final PartitionXattr.Builder partitionXattrBuilder = PartitionXattr.newBuilder();
    if (tableExtended.hasTableInputFormatSubscript()) {
      partitionXattrBuilder.setInputFormatSubscript(tableExtended.getTableInputFormatSubscript());
    }
    if (tableExtended.hasTableStorageHandlerSubscript()) {
      partitionXattrBuilder.setStorageHandlerSubscript(tableExtended.getTableStorageHandlerSubscript());
    }
    if (tableExtended.hasTableSerializationLibSubscript()) {
      partitionXattrBuilder.setSerializationLibSubscript(tableExtended.getTableSerializationLibSubscript());
    }
    partitionXattrBuilder.addAllPropertySubscript(tableExtended.getTablePropertySubscriptList());
    return partitionXattrBuilder.build();
  }

  public static PartitionXattr getPartitionXattr(Table table, List<Prop> props) {
    final PartitionXattr.Builder partitionXattrBuilder = PartitionXattr.newBuilder();
    if (table.getSd().getInputFormat() != null) {
      partitionXattrBuilder.setInputFormat(table.getSd().getInputFormat());
    }

    final String storageHandler = table.getParameters().get(META_TABLE_STORAGE);
    if (storageHandler != null) {
      partitionXattrBuilder.setStorageHandler(storageHandler);
    }

    if (table.getSd().getSerdeInfo().getSerializationLib() != null) {
      partitionXattrBuilder.setSerializationLib(table.getSd().getSerdeInfo().getSerializationLib());
    }
    partitionXattrBuilder.addAllPartitionProperty(props);
    return partitionXattrBuilder.build();
  }

  public static HiveReaderProto.FileSystemPartitionUpdateKey getFSBasedUpdateKey(String partitionDir, JobConf job,
                                                                                 boolean isRecursive, boolean directoriesOnly,
                                                                                 int partitionId) {
    final List<HiveReaderProto.FileSystemCachedEntity> cachedEntities = new ArrayList<>();
    final Path rootLocation = new Path(partitionDir);
    try {
      // TODO: DX-16001 - make async configurable for Hive.
      final HadoopFileSystemWrapper fs = new HadoopFileSystemWrapper(rootLocation, job);

      if (fs.exists(rootLocation)) {
        final FileStatus rootStatus = fs.getFileStatus(rootLocation);
        if (rootStatus.isDirectory()) {
          cachedEntities.add(HiveReaderProto.FileSystemCachedEntity.newBuilder()
            .setPath(EMPTY_STRING)
            .setLastModificationTime(rootStatus.getModificationTime())
            .setIsDir(true)
            .build());

          final RemoteIterator<LocatedFileStatus> statuses = isRecursive ? fs.listFiles(rootLocation, true) : fs.listFiles(rootLocation, false);
          while (statuses.hasNext()) {
            LocatedFileStatus fileStatus = statuses.next();
            final Path filePath = fileStatus.getPath();
            if (fileStatus.isDirectory()) {
              cachedEntities.add(HiveReaderProto.FileSystemCachedEntity.newBuilder()
                .setPath(PathUtils.relativePath(filePath, rootLocation))
                .setLastModificationTime(fileStatus.getModificationTime())
                .setIsDir(true)
                .build());
            } else if (fileStatus.isFile() && !directoriesOnly) {
              cachedEntities.add(HiveReaderProto.FileSystemCachedEntity.newBuilder()
                .setPath(PathUtils.relativePath(filePath, rootLocation))
                .setLastModificationTime(fileStatus.getModificationTime())
                .setIsDir(false)
                .build());
            }
          }
        } else {
          cachedEntities.add(HiveReaderProto.FileSystemCachedEntity.newBuilder()
            .setPath(EMPTY_STRING)
            .setLastModificationTime(rootStatus.getModificationTime())
            .setIsDir(false)
            .build());
        }
        return HiveReaderProto.FileSystemPartitionUpdateKey.newBuilder()
          .setPartitionId(partitionId)
          .setPartitionRootDir(fs.makeQualified(rootLocation).toString())
          .addAllCachedEntities(cachedEntities)
          .build();
      }
      return null;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static boolean inputPathExists(StorageDescriptor sd, JobConf job) {

    final Path path = new Path(sd.getLocation());
    try {
      // TODO: DX-16001 - make async configurable for Hive.
      final HadoopFileSystemWrapper fs = new HadoopFileSystemWrapper(path, job);
      return fs.exists(path);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static void addInputPath(StorageDescriptor sd, JobConf job) {
    final org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(sd.getLocation());
    FileInputFormat.addInputPath(job, path);
  }

  @SuppressWarnings("unchecked")
  public static List<Prop> fromProperties(Properties props) {
    final List<Prop> output = new ArrayList<>();
    for (Map.Entry<Object, Object> eo : props.entrySet()) {
      Map.Entry<String, String> e = (Map.Entry<String, String>) (Object) eo;
      output.add(Prop.newBuilder().setKey(e.getKey()).setValue(e.getValue()).build());
    }
    return output;
  }

  public static List<PartitionValue> getPartitionValues(Table table, Partition partition, boolean enforceVarcharWidth) {
    if (partition == null) {
      return Collections.emptyList();
    }

    final List<String> partitionValues = partition.getValues();
    final List<PartitionValue> output = new ArrayList<>();
    final List<FieldSchema> partitionKeys = table.getPartitionKeys();
    for (int i = 0; i < partitionKeys.size(); i++) {
      final PartitionValue value = getPartitionValue(partitionKeys.get(i), partitionValues.get(i), enforceVarcharWidth);
      if (value != null) {
        output.add(value);
      }
    }
    return output;
  }

  private static PartitionValue getPartitionValue(FieldSchema partitionCol, String value, boolean enforceVarcharWidth) {
    final TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(partitionCol.getType());
    final String name = partitionCol.getName();

    if ("__HIVE_DEFAULT_PARTITION__".equals(value)) {
      return PartitionValue.of(name);
    }

    switch (typeInfo.getCategory()) {
      case PRIMITIVE:
        final PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) typeInfo;
        switch (primitiveTypeInfo.getPrimitiveCategory()) {
          case BINARY:
            try {
              byte[] bytes = value.getBytes("UTF-8");
              return PartitionValue.of(name, ByteBuffer.wrap(bytes));
            } catch (UnsupportedEncodingException e) {
              throw new RuntimeException("UTF-8 not supported?", e);
            }
          case BOOLEAN:
            return PartitionValue.of(name, Boolean.parseBoolean(value));
          case DOUBLE:
            try {
              return PartitionValue.of(name, Double.parseDouble(value));
            } catch (NumberFormatException ex) {
              return PartitionValue.of(name);
            }
          case FLOAT:
            try {
              return PartitionValue.of(name, Float.parseFloat(value));
            } catch (NumberFormatException ex) {
              return PartitionValue.of(name);
            }
          case BYTE:
          case SHORT:
          case INT:
            try {
              return PartitionValue.of(name, Integer.parseInt(value));
            } catch (NumberFormatException ex) {
              return PartitionValue.of(name);
            }
          case LONG:
            try {
              return PartitionValue.of(name, Long.parseLong(value));
            } catch (NumberFormatException ex) {
              return PartitionValue.of(name);
            }
          case STRING:
            return PartitionValue.of(name, value);
          case VARCHAR:
            String truncatedVarchar = value;
            if (enforceVarcharWidth && (value.length() > ((VarcharTypeInfo) typeInfo).getLength())) {
              truncatedVarchar = value.substring(0, ((VarcharTypeInfo) typeInfo).getLength());
            }
            return PartitionValue.of(name, truncatedVarchar);
          case CHAR:
            String truncatedChar = value.trim();
            if (enforceVarcharWidth && (truncatedChar.length() > ((CharTypeInfo) typeInfo).getLength())) {
              truncatedChar = value.substring(0, ((CharTypeInfo) typeInfo).getLength());
            }
            return PartitionValue.of(name, truncatedChar);
          case TIMESTAMP:
            return PartitionValue.of(name, DateTimes.toMillisFromJdbcTimestamp(value));
          case DATE:
            return PartitionValue.of(name, DateTimes.toMillisFromJdbcDate(value));
          case DECIMAL:
            final DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) typeInfo;
            if (decimalTypeInfo.getPrecision() > 38) {
              throw UserException.unsupportedError()
                .message("Dremio only supports decimals up to 38 digits in precision. This Hive table has a partition value with scale of %d digits.", decimalTypeInfo.getPrecision())
                .build(logger);
            }
            final HiveDecimal decimal = HiveDecimalUtils.enforcePrecisionScale(HiveDecimal.create(value), decimalTypeInfo);
            if (decimal == null) {
              return PartitionValue.of(name);
            }
            final BigDecimal original = decimal.bigDecimalValue();
            // we can't just use unscaledValue() since BigDecimal doesn't store trailing zeroes and we need to ensure decoding includes the correct scale.
            final BigInteger unscaled = original.movePointRight(decimalTypeInfo.scale()).unscaledValue();
            return PartitionValue.of(name, ByteBuffer.wrap(DecimalTools.signExtend16(unscaled.toByteArray())));
          default:
            break;
        }
        HiveUtilities.throwUnsupportedHiveDataTypeError(primitiveTypeInfo.getPrimitiveCategory().toString());
        break;
      default:
        HiveUtilities.throwUnsupportedHiveDataTypeError(typeInfo.getCategory().toString());
        break;
    }

    return null; // unreachable
  }

  /**
   * Wrapper around {@link MetaStoreUtils#getPartitionMetadata(Partition, Table)} which also adds parameters from table
   * to properties returned by {@link MetaStoreUtils#getPartitionMetadata(Partition, Table)}.
   *
   * @param partition the source of partition level parameters
   * @param table     the source of table level parameters
   * @return properties
   */
  public static Properties buildPartitionProperties(final Partition partition, final Table table) {
    final Properties properties = MetaStoreUtils.getPartitionMetadata(partition, table);

    // SerDe expects properties from Table, but above call doesn't add Table properties.
    // Include Table properties in final list in order to not to break SerDes that depend on
    // Table properties. For example AvroSerDe gets the schema from properties (passed as second argument)
    for (Map.Entry<String, String> entry : table.getParameters().entrySet()) {
      if (entry.getKey() != null && entry.getValue() != null) {
        properties.put(entry.getKey(), entry.getValue());
      }
    }

    return properties;
  }

  /**
   * Utility method which adds give configs to {@link JobConf} object.
   *
   * @param job        {@link JobConf} instance.
   * @param properties New config properties
   */
  public static void addConfToJob(final JobConf job, final Properties properties) {
    for (Map.Entry entry : properties.entrySet()) {
      job.set((String) entry.getKey(), (String) entry.getValue());
    }
  }

  public static Class<? extends InputFormat> getInputFormatClass(final JobConf job, final Table table, final Partition partition) {
    try (Closeable ccls = HivePf4jPlugin.swapClassLoader()) {
      if (partition != null) {
        if (partition.getSd().getInputFormat() != null) {
          return (Class<? extends InputFormat>) Class.forName(partition.getSd().getInputFormat());
        }

        if (partition.getParameters().get(META_TABLE_STORAGE) != null) {
          final HiveStorageHandler storageHandler = HiveUtils.getStorageHandler(job, partition.getParameters().get(META_TABLE_STORAGE));
          return storageHandler.getInputFormatClass();
        }
      }

      if (table.getSd().getInputFormat() != null) {
        return (Class<? extends InputFormat>) Class.forName(table.getSd().getInputFormat());
      }

      if (table.getParameters().get(META_TABLE_STORAGE) != null) {
        final HiveStorageHandler storageHandler = HiveUtils.getStorageHandler(job, table.getParameters().get(META_TABLE_STORAGE));
        return storageHandler.getInputFormatClass();
      }
    } catch (HiveException | ClassNotFoundException e) {
      throw UserException.dataReadError(e).message(e.getMessage()).build(logger);
    }

    throw UserException.dataReadError().message("Unable to get Hive table InputFormat class. There is neither " +
      "InputFormat class explicitly specified nor a StorageHandler class provided.").build(logger);
  }

  public static int getHash(Partition partition) {
    return Objects.hashCode(
      partition.getSd(),
      partition.getParameters(),
      partition.getValues());
  }

  public static int getHash(Table table, boolean enforceVarcharWidth, final HiveConf hiveConf, final HiveStoragePlugin plugin) {
    List<Object> hashParts = Lists.newArrayList(table.getTableType(),
      table.getParameters(),
      table.getPartitionKeys(),
      table.getSd(),
      table.getViewExpandedText(),
      table.getViewOriginalText());
    if (enforceVarcharWidth && hasVarcharColumnInTableSchema(table, hiveConf, plugin)) {
      hashParts.add(Boolean.TRUE);
    }
    return Objects.hashCode(hashParts.toArray());
  }

  public static void checkLeafFieldCounter(int leafCounter, int maxMetadataLeafColumns, String tableName) {
    if (leafCounter > maxMetadataLeafColumns) {
      throw new ColumnCountTooLargeException(maxMetadataLeafColumns);
    }
  }

  public static int getMaxLeafFieldCount(MetadataOption... options) {
    if (null != options) {
      for (MetadataOption option : options) {
        if (option instanceof MaxLeafFieldCount) {
          return ((MaxLeafFieldCount) option).getValue();
        }
      }
    }
    return 0;
  }

  public static int getMaxNestedFieldLevels(MetadataOption... options) {
    if (null != options) {
      for (MetadataOption option : options) {
        if (option instanceof MaxNestedFieldLevels) {
          return ((MaxNestedFieldLevels) option).getValue();
        }
      }
    }
    return 0;
  }

  public static boolean isIgnoreAuthzErrors(MetadataOption... options) {
    if (null != options) {
      for (MetadataOption option : options) {
        if (option instanceof IgnoreAuthzErrors) {
          return true;
        }
      }
    }
    return false;
  }

  public static boolean isDirListInputSplitType(MetadataOption... options) {
    if (null != options) {
      for (MetadataOption option : options) {
        if (option instanceof DirListInputSplitType) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Helper method which returns a list of partition names from a map of partition values
   * if the MetadataOption is RefreshTableFilterOption, else null
   *
   * Ex. ["year"=>"2020", "month"=>"Feb"] -> "year=2020/month=Feb"
   */
  public static List<String> getFilteredPartitionNames(List<String> partitionCols, List<FieldSchema> partitionFields, MetadataOption... options) {
    if (null != options) {
      for (MetadataOption option : options) {
        if (option instanceof RefreshTableFilterOption) {

          List<String> partitionStringList = new ArrayList<>();
          Map<String, String> filteredPartitionsMap = ((RefreshTableFilterOption) option).getPartition();
          for (String partitionColName : partitionCols) {
            String partitionColValue = filteredPartitionsMap.get(partitionColName);
            FieldSchema fieldSchema = partitionFields.stream().filter(field -> partitionColName.equals(field.getName())).findFirst().orElse(null);
            if (fieldSchema != null) {
              TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(fieldSchema.getType());
              if (typeInfo.getCategory() == Category.PRIMITIVE &&
                ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.CHAR) {
                int extra = ((CharTypeInfo) typeInfo).getLength();
                if (extra > 0) {
                  partitionColValue = String.format("%" + (-extra) + "s", partitionColValue);
                }
              }
            }
            partitionStringList.add(partitionColName + "=" + partitionColValue);
          }
          return Collections.singletonList(partitionStringList.stream().collect(Collectors.joining("/")));
        }
      }
    }
    return null;
  }

  public static String getPartitionValueLogString(Partition partition) {
    return ((null == partition) || (null == partition.getValues()) ? "default" :
      HiveMetadataUtils.PARTITION_FIELD_SPLIT_KEY_JOINER.join(partition.getValues()));
  }

  @VisibleForTesting
  static long getMaxTimeoutPerCore(List<InputSplit> inputSplits, long maxDeltas) {
    int effectiveParallelism = Math.min(INPUT_SPLIT_LENGTH_RUNNABLE_PARALLELISM, inputSplits.size());
    long splitsPerCore = (long) Math.ceil((double) inputSplits.size() / effectiveParallelism);
    long deltasPerCore = quietCheckedMultiply(splitsPerCore, maxDeltas);
    return quietCheckedMultiply(deltasPerCore, MAX_NAMENODE_FS_CALL_TIMEOUT);
  }

  @VisibleForTesting
  static long populateSplitJobAndGetMaxDeltas(JobConf job, String tableName, List<InputSplit> inputSplits, List<TimedRunnable<Long>> splitSizeJobs) {
    long maxDeltas = 0;
    for (InputSplit inputSplit : inputSplits) {
      splitSizeJobs.add(new InputSplitSizeRunnable(job, tableName, inputSplit));
      if (inputSplit instanceof OrcSplit) {
        maxDeltas = Math.max(((OrcSplit) inputSplit).getDeltas().size(), maxDeltas);
      }
    }
    /*
     +1 is for : in case of Orc it is for base directory
               : in case of NonOrc, if is for minimum Non zero time out of 2000 ms which will be overwrite by TimedRunnable.run method, if required.
     */
    return maxDeltas + 1;
  }

  @VisibleForTesting
  static List<Long> runInputSplitSizeRunnable(List<TimedRunnable<Long>> splitSizeRunnables, String tableName, long timeoutMillis) {
    if (!splitSizeRunnables.isEmpty()) {
      try {
        return TimedRunnable.run(
          String.format("Table '%s', Get split sizes", tableName),
          logger,
          splitSizeRunnables,
          INPUT_SPLIT_LENGTH_RUNNABLE_PARALLELISM,
          timeoutMillis);
      } catch (IOException e) {
        throw UserException.dataReadError(e).message(e.getMessage()).build(logger);
      }
    } else {
      return Collections.emptyList();
    }
  }

  private static long quietCheckedMultiply(long a, long b) {
    try {
      return LongMath.checkedMultiply(a, b);
    } catch (ArithmeticException e) {
      return Long.MAX_VALUE;
    }
  }
}
