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
package com.dremio.exec.store.parquet;

import static com.dremio.exec.ExecConstants.PARQUET_READER_INT96_AS_TIMESTAMP;
import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.arrow.schema.SchemaConverter;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import com.carrotsearch.hppc.cursors.ObjectLongCursor;
import com.dremio.common.arrow.DremioArrowSchema;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.BytesOutput;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.DatasetSplit;
import com.dremio.connector.metadata.DatasetSplitAffinity;
import com.dremio.connector.metadata.DatasetStats;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.GetMetadataOption;
import com.dremio.connector.metadata.ListPartitionChunkOption;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.connector.metadata.PartitionValue;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.ColumnCountTooLargeException;
import com.dremio.exec.catalog.FileConfigMetadata;
import com.dremio.exec.catalog.MetadataObjectsUtils;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.planner.cost.ScanCostFactor;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.PartitionChunkListingImpl;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.SampleMutator;
import com.dremio.exec.store.dfs.FileDatasetHandle;
import com.dremio.exec.store.dfs.FileSelection;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.FormatPlugin;
import com.dremio.exec.store.dfs.MetadataUtils;
import com.dremio.exec.store.dfs.PhysicalDatasetUtils;
import com.dremio.exec.store.dfs.PreviousDatasetInfo;
import com.dremio.exec.store.dfs.implicit.AdditionalColumnsRecordReader;
import com.dremio.exec.store.dfs.implicit.ConstantColumnPopulators;
import com.dremio.exec.store.dfs.implicit.ImplicitFilesystemColumnFinder;
import com.dremio.exec.store.dfs.implicit.NameValuePair;
import com.dremio.exec.store.file.proto.FileProtobuf.FileSystemCachedEntity;
import com.dremio.exec.store.file.proto.FileProtobuf.FileUpdateKey;
import com.dremio.exec.store.parquet.ParquetGroupScanUtils.RowGroupInfo;
import com.dremio.exec.store.parquet2.ParquetRowiseReader;
import com.dremio.io.file.FileAttributes;
import com.dremio.io.file.FileSystem;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators.BooleanValidator;
import com.dremio.parquet.reader.ParquetDirectByteBufferAllocator;
import com.dremio.sabot.exec.context.OperatorContextImpl;
import com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf.ColumnValueCount;
import com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf.DictionaryEncodedColumns;
import com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf.ParquetDatasetSplitXAttr;
import com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf.ParquetDatasetXAttr;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.net.HostAndPort;

/**
 * Parquet dataset accessor.
 * ReadDefinition and splits are computed as same time as dataset.
 */
@Options
public class ParquetFormatDatasetAccessor implements FileDatasetHandle {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetFormatDatasetAccessor.class);

  public static final BooleanValidator PARQUET_TEST_SCHEMA_FALLBACK_ONLY_VALIDATOR = new BooleanValidator("dremio.test.parquet.schema.fallback.only", false);
  public static final String PARQUET_SCHEMA_FALLBACK_DISABLED = "dremio.test.parquet.schema.fallback.disabled";

  // The same as com.dremio.service.reflection.ReflectionServiceImpl.ACCELERATOR_STORAGEPLUGIN_NAME
  public static final String ACCELERATOR_STORAGEPLUGIN_NAME = "__accelerator";

  private final DatasetType type;
  private final FileSystem fs;
  private final FileSelection fileSelection;
  private final FileSystemPlugin<?> fsPlugin;
  private final NamespaceKey tableSchemaPath;
  private final FileUpdateKey updateKey;
  private final FormatPlugin formatPlugin;
  private final PreviousDatasetInfo oldConfig;
  private final int maxLeafColumns;

  private PartitionChunkListingImpl partitionChunkListing;
  private long recordCount;
  private ParquetDatasetXAttr extended;
  private List<String> partitionColumns;
  private BatchSchema schema;

  public ParquetFormatDatasetAccessor(
      DatasetType type,
      FileSystem fs,
      FileSelection fileSelection,
      FileSystemPlugin<?> fsPlugin,
      NamespaceKey tableSchemaPath,
      FileUpdateKey updateKey,
      FormatPlugin formatPlugin,
      PreviousDatasetInfo oldConfig,
      int maxLeafColumns) {
    this.type = type;
    this.fs = fs;
    this.fileSelection = fileSelection;
    this.fsPlugin = fsPlugin;
    this.tableSchemaPath = tableSchemaPath;
    this.updateKey = updateKey;
    this.formatPlugin = formatPlugin;
    this.oldConfig = oldConfig;
    this.maxLeafColumns = maxLeafColumns;
    this.partitionChunkListing = new PartitionChunkListingImpl();
  }


  private BatchSchema getBatchSchema(final BatchSchema oldSchema, final FileSelection selection, final FileSystem fs) throws Exception {
    final SabotContext context = ((ParquetFormatPlugin) formatPlugin).getContext();
    final Optional<FileAttributes> firstFileO = selection.getFirstFile();
    if (!firstFileO.isPresent()) {
      throw UserException.dataReadError().message("Unable to find any files for datasets.").build(logger);
    }

    if (context.getOptionManager().getOption(PARQUET_TEST_SCHEMA_FALLBACK_ONLY_VALIDATOR)) {
      // Only run tests for reading the records in the first parquet to generate schema
      return getBatchSchemaFromReader(selection, fs);
    }

    final FileAttributes firstFile = firstFileO.get();
    final ParquetMetadata footer = SingletonParquetFooterCache.readFooter(fsPlugin.getSystemUserFS(), firstFile, ParquetMetadataConverter.NO_FILTER,
      context.getOptionManager().getOption(ExecConstants.PARQUET_MAX_FOOTER_LEN_VALIDATOR));

    Schema arrowSchema;
    try {
      arrowSchema = DremioArrowSchema.fromMetaData(footer.getFileMetaData().getKeyValueMetaData());
    } catch (Exception e) {
      arrowSchema = null;
      logger.warn("Invalid Arrow Schema", e);
    }

    final List<Field> fields;
    if (arrowSchema == null) {
      final SchemaConverter converter = new SchemaConverter(context.getOptionManager().getOption(PARQUET_READER_INT96_AS_TIMESTAMP).getBoolVal());
      try {
        arrowSchema = converter.fromParquet(footer.getFileMetaData().getSchema()).getArrowSchema();
        // Convert all the arrow fields to dremio fields
        fields = CompleteType.convertToDremioFields(arrowSchema.getFields());
      } catch (Exception e) {
        if (context.getConfig().getBoolean(PARQUET_SCHEMA_FALLBACK_DISABLED)) {
          // Let it fail if test is running.
          throw e;
        } else {
          logger.warn("Cannot convert parquet schema to dremio schema using parquet-arrow schema converter, fall back to generate schema from first parquet file");
          logger.debug("Cannot convert parquet schema to dremio schema using parquet-arrow schema converter", e);
          // Fall back to read the records in the first parquet file to generate schema
          return getBatchSchemaFromReader(selection, fs);
        }
      }
    } else {
      fields = arrowSchema.getFields().stream().collect(Collectors.toList());
    }
    if (fields.size() > maxLeafColumns) {
      throw new ColumnCountTooLargeException(maxLeafColumns);
    }

    final boolean isAccelerator = fsPlugin.getId().getName().equals(ACCELERATOR_STORAGEPLUGIN_NAME);

    final ImplicitFilesystemColumnFinder finder = new ImplicitFilesystemColumnFinder(context.getOptionManager(), fs, GroupScan.ALL_COLUMNS, isAccelerator);

    final List<NameValuePair<?>> pairs = finder.getImplicitFieldsForSample(selection);
    final HashMap<String, Field> fieldHashMap = new HashMap<>();
    for (Field field : fields) {
      fieldHashMap.put(field.getName().toLowerCase(), field);
    }

    for (NameValuePair<?> pair : pairs) {
      if (!fieldHashMap.containsKey(pair.getName().toLowerCase())) {
        if (pair instanceof ConstantColumnPopulators.VarCharNameValuePair) {
          fields.add(CompleteType.VARCHAR.toField(pair.getName()));
        } else if (pair instanceof ConstantColumnPopulators.BigIntNameValuePair) {
          fields.add(CompleteType.BIGINT.toField(pair.getName()));
        } else {
          throw new RuntimeException("Unexpected NameValuePair type from getImplicitFieldsForSample.");
        }
      }
    }

    BatchSchema newSchema = BatchSchema.newBuilder().addFields(fields).build();
    boolean mixedTypesDisabled = context.getOptionManager().getOption(ExecConstants.MIXED_TYPES_DISABLED);
    return oldSchema != null ? oldSchema.merge(newSchema, mixedTypesDisabled) : newSchema;
  }

  /**
   * Read the records in the first parquet file to generate schema for selected parquet files
   *
   * @param selection parquet file selection
   * @param fs        file system wrapper
   * @return schema of selected parquet files
   */
  private BatchSchema getBatchSchemaFromReader(final FileSelection selection, final FileSystem fs) throws Exception {
    final SabotContext context = ((ParquetFormatPlugin) formatPlugin).getContext();

    try (
        BufferAllocator sampleAllocator = context.getAllocator().newChildAllocator("sample-alloc", 0, Long.MAX_VALUE);
        OperatorContextImpl operatorContext = new OperatorContextImpl(context.getConfig(), sampleAllocator, context.getOptionManager(), 1000);
        SampleMutator mutator = new SampleMutator(sampleAllocator)
    ) {
      final CompressionCodecFactory codec = CodecFactory.createDirectCodecFactory(new Configuration(),
          new ParquetDirectByteBufferAllocator(operatorContext.getAllocator()), 0);
      for (FileAttributes firstFile : selection.getFileAttributesList()) {
        ParquetMetadata footer = SingletonParquetFooterCache.readFooter(fsPlugin.getSystemUserFS(), firstFile, ParquetMetadataConverter.NO_FILTER,
          context.getOptionManager().getOption(ExecConstants.PARQUET_MAX_FOOTER_LEN_VALIDATOR));

        if (footer.getBlocks().size() == 0) {
          continue;
        }

        final boolean autoCorrectCorruptDates = context.getOptionManager().getOption(ExecConstants.PARQUET_AUTO_CORRECT_DATES_VALIDATOR) &&
          ((ParquetFormatPlugin) formatPlugin).getConfig().autoCorrectCorruptDates;
        final MutableParquetMetadata mutableParquetMetadata = new MutableParquetMetadata(footer);
        final ParquetReaderUtility.DateCorruptionStatus dateStatus = ParquetReaderUtility.detectCorruptDates(mutableParquetMetadata, GroupScan.ALL_COLUMNS,
            autoCorrectCorruptDates);
        final SchemaDerivationHelper schemaHelper = SchemaDerivationHelper.builder()
            .readInt96AsTimeStamp(operatorContext.getOptions().getOption(PARQUET_READER_INT96_AS_TIMESTAMP).getBoolVal())
            .dateCorruptionStatus(dateStatus)
            .build();

        boolean isAccelerator = fsPlugin.getId().getName().equals(ACCELERATOR_STORAGEPLUGIN_NAME);

        final ImplicitFilesystemColumnFinder finder = new ImplicitFilesystemColumnFinder(context.getOptionManager(), fs, GroupScan.ALL_COLUMNS, isAccelerator);

        final long maxFooterLen = context.getOptionManager().getOption(ExecConstants.PARQUET_MAX_FOOTER_LEN_VALIDATOR);
        try (InputStreamProvider streamProvider = new SingleStreamProvider(fs, firstFile.getPath(), firstFile.size(), maxFooterLen, false, null, null, false);
            RecordReader reader = new AdditionalColumnsRecordReader(operatorContext, new ParquetRowiseReader(operatorContext, mutableParquetMetadata, 0,
                 firstFile.getPath().toString(), ParquetScanProjectedColumns.fromSchemaPaths(GroupScan.ALL_COLUMNS),
                 fs, schemaHelper, streamProvider, codec, true), finder.getImplicitFieldsForSample(selection), sampleAllocator)) {

          reader.setup(mutator);

          mutator.allocate(100);
          // Read the parquet file to populate inner list types
          reader.next();

          mutator.getContainer().buildSchema(BatchSchema.SelectionVectorMode.NONE);
          return mutator.getContainer().getSchema();
        }
      }
    } catch (Exception e) {
      throw e;
    }

    throw UserException.dataReadError().message("Only empty parquet files found.").build(logger);
  }

  private void buildIfNecessary() throws Exception {
    if (partitionChunkListing.computed()) {
      return;
    }
    schema = getBatchSchema(oldConfig.getSchema(), fileSelection, fs);

    final ParquetGroupScanUtils parquetGroupScanUtils = ((ParquetFormatPlugin) formatPlugin).getGroupScan(SYSTEM_USERNAME, fsPlugin, fileSelection, tableSchemaPath.getPathComponents(), GroupScan.ALL_COLUMNS, schema, null);

    // TODO: copy sort columns

    this.recordCount = parquetGroupScanUtils.getScanStats().getRecordCount();

    final ParquetDatasetXAttr.Builder datasetXAttr = ParquetDatasetXAttr.newBuilder().setSelectionRoot(fileSelection.getSelectionRoot());

    final ImplicitFilesystemColumnFinder finder = new ImplicitFilesystemColumnFinder(fsPlugin.getContext().getOptionManager(), fs, GroupScan.ALL_COLUMNS);
    List<RowGroupInfo> rowGroups = parquetGroupScanUtils.getRowGroupInfos();

    final List<List<NameValuePair<?>>> pairs = finder.getImplicitFields(parquetGroupScanUtils.getSelectionRoot(), rowGroups);
    final Set<String> allImplicitColumns = Sets.newLinkedHashSet();

    for (int i = 0; i < parquetGroupScanUtils.getRowGroupInfos().size(); i++) {
      final ParquetGroupScanUtils.RowGroupInfo rowGroupInfo = parquetGroupScanUtils.getRowGroupInfos().get(i);

      final String pathString = rowGroupInfo.getFileAttributes().getPath().toString();
      final long splitRecordCount = rowGroupInfo.getRowCount();
      final long size = rowGroupInfo.getTotalBytes();

      final List<DatasetSplitAffinity> affinities = new ArrayList<>();
      for (ObjectLongCursor<HostAndPort> item : rowGroupInfo.getByteMap()) {
        affinities.add(DatasetSplitAffinity.of(item.key.toString(), item.value));
      }

      // Create a list of (partition name, partition value) pairs. Order of these pairs should be same a table
      // partition column list. Also if a partition value doesn't exist for a file, use null as the partition value
      final LinkedHashMap<String, PartitionValue> partitionValues = new LinkedHashMap<>();
      final Map<SchemaPath, MajorType> typeMap = checkNotNull(parquetGroupScanUtils.getColumnTypeMap());
      final Map<SchemaPath, Object> pValues = parquetGroupScanUtils.getPartitionValueMap().get(rowGroupInfo.getFileAttributes());
      for (SchemaPath pCol : parquetGroupScanUtils.getPartitionColumns()) {
        final MajorType pColType = typeMap.get(pCol);
        final MinorType minorType = MinorType.valueOf(pColType.getMinorType().getNumber());
        final Object pVal;
        if (pValues != null && pValues.containsKey(pCol)) {
          pVal = pValues.get(pCol);
        } else {
          pVal = null;
        }
        partitionValues.put(pCol.getAsUnescapedPath(), MetadataUtils.toPartitionValue(pCol, pVal, minorType, PartitionValue.PartitionValueType.VISIBLE));
      }

      if (!ACCELERATOR_STORAGEPLUGIN_NAME.equals(fsPlugin.getName())) {
        // add implicit fields
        for (NameValuePair<?> p : pairs.get(i)) {
          if (!partitionValues.containsKey(p.getName())) {
            final Object obj = p.getValue();
            PartitionValue v;
            if (obj == null) {
              v = PartitionValue.of(p.getName(), PartitionValue.PartitionValueType.IMPLICIT);
            } else if (obj instanceof String) {
              v = PartitionValue.of(p.getName(), (String) p.getValue(), PartitionValue.PartitionValueType.IMPLICIT);
            } else if (obj instanceof Long) {
              v = PartitionValue.of(p.getName(), (Long) p.getValue(), PartitionValue.PartitionValueType.IMPLICIT);
            } else {
              throw new UnsupportedOperationException(String.format("Unable to handle value %s of type %s.", obj, obj.getClass().getName()));
            }
            partitionValues.put(p.getName(), v);
            allImplicitColumns.add(p.getName());
          }
        }
      }

      List<ColumnValueCount> columnValueCounts = Lists.newArrayList();
      for (Map.Entry<SchemaPath, Long> entry : rowGroupInfo.getColumnValueCounts().entrySet()) {
        columnValueCounts.add(ColumnValueCount.newBuilder()
            .setColumn(entry.getKey().getAsUnescapedPath())
            .setCount(entry.getValue())
            .build());
      }

      Long length = null;
      if (fsPlugin.getContext().getOptionManager().getOption(ExecConstants.PARQUET_CACHED_ENTITY_SET_FILE_SIZE)) {
        length = rowGroupInfo.getFileAttributes().size();
      }
      // set xattr
      ParquetDatasetSplitXAttr splitExtended = ParquetDatasetSplitXAttr.newBuilder()
          .setPath(pathString)
          .setStart(rowGroupInfo.getStart())
          .setRowGroupIndex(rowGroupInfo.getRowGroupIndex())
          .setUpdateKey(FileSystemCachedEntity.newBuilder()
              .setPath(pathString)
              .setLastModificationTime(rowGroupInfo.getFileAttributes().lastModifiedTime().toMillis())
              .setLength(length))
          .addAllColumnValueCounts(columnValueCounts)
          .setLength(rowGroupInfo.getLength())
          .build();


      List<PartitionValue> partitionValueList = ImmutableList.copyOf(partitionValues.values());
      DatasetSplit split = DatasetSplit.of(affinities, size, splitRecordCount, splitExtended::writeTo);
      partitionChunkListing.put(partitionValueList, split);
    }

    final List<String> filePartitionColumns = MetadataUtils.getStringColumnNames(parquetGroupScanUtils.getPartitionColumns());
    if (filePartitionColumns != null) {
      allImplicitColumns.addAll(filePartitionColumns);
    }


    // scan for global dictionaries
    final DictionaryEncodedColumns dictionaryEncodedColumns = ParquetFormatPlugin.scanForDictionaryEncodedColumns(fs, fileSelection.getSelectionRoot(), schema);
    if (dictionaryEncodedColumns != null) {
      logger.debug("Found global dictionaries for table {} for columns {}", tableSchemaPath, dictionaryEncodedColumns);
      datasetXAttr.setDictionaryEncodedColumns(dictionaryEncodedColumns);
    }

    for (Map.Entry<SchemaPath, Long> entry : parquetGroupScanUtils.getColumnValueCounts().entrySet()) {
      datasetXAttr.addColumnValueCountsBuilder()
          .setColumn(entry.getKey().getAsUnescapedPath())
          .setCount(entry.getValue())
          .build();
    }

    extended = datasetXAttr.build();
    partitionColumns = Lists.newArrayList(allImplicitColumns);
    partitionChunkListing.computePartitionChunks();
  }


  @Override
  public DatasetType getDatasetType() {
    return type;
  }

  @Override
  public EntityPath getDatasetPath() {
    return MetadataObjectsUtils.toEntityPath(tableSchemaPath);
  }

  @Override
  public FileConfigMetadata getDatasetMetadata(GetMetadataOption... options) throws ConnectorException {
    final BatchSchema schema;

    try {
      schema = getBatchSchema(oldConfig != null ? oldConfig.getSchema() : null, fileSelection, fs);
    } catch (Exception ex) {
      Throwables.propagateIfPossible(ex, ConnectorException.class);
      throw new ConnectorException(ex);
    }

    return new FileConfigMetadata() {

      @Override
      public DatasetStats getDatasetStats() {
        return DatasetStats.of(recordCount, true, ScanCostFactor.PARQUET.getFactor());
      }

      @Override
      public List<String> getSortColumns() {
        if (oldConfig == null) {
          return Collections.emptyList();
        }

        return oldConfig.getSortColumns() == null ? Collections.emptyList() : oldConfig.getSortColumns();
      }

      @Override
      public List<String> getPartitionColumns() {
        return partitionColumns;
      }

      @Override
      public BytesOutput getExtraInfo() {
        return os -> extended.writeTo(os);
      }

      @Override
      public Schema getRecordSchema() {
        return schema;
      }

      @Override
      public FileConfig getFileConfig() {
        return PhysicalDatasetUtils.toFileFormat(formatPlugin).asFileConfig().setLocation(fileSelection.getSelectionRoot());
      }
    };
  }

  @Override
  public PartitionChunkListing listPartitionChunks(ListPartitionChunkOption... options) throws ConnectorException {
    try {
      buildIfNecessary();
    } catch (Exception e) {
      Throwables.propagateIfPossible(e, ConnectorException.class);
      throw new ConnectorException(e);
    }

    return partitionChunkListing;
  }

  @Override
  public BytesOutput provideSignature(DatasetMetadata metadata) {
    return updateKey::writeTo;
  }

}
