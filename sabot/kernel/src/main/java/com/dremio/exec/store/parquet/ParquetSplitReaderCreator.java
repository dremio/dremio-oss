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

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.InvalidMetadataErrorContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.map.CaseInsensitiveMap;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.FileTypeCoercion;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.dfs.SplitReaderCreator;
import com.dremio.exec.store.dfs.implicit.CompositeReaderConfig;
import com.dremio.exec.store.iceberg.deletes.EqualityDeleteFilter;
import com.dremio.io.file.FileAttributes;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.store.iceberg.proto.IcebergProtobuf;
import com.dremio.sabot.exec.store.iceberg.proto.IcebergProtobuf.DefaultNameMapping;
import com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.dataset.proto.UserDefinedSchemaSettings;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.namespace.file.proto.FileType;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import io.protostuff.ByteString;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.parquet.hadoop.metadata.BlockMetaData;

/**
 * A lightweight object used to manage the creation of a reader. Allows pre-initialization of data
 * before reader construction.
 */
public class ParquetSplitReaderCreator extends SplitReaderCreator implements AutoCloseable {
  protected SplitAndPartitionInfo datasetSplit;
  // set to true while creating input stream provider. When true, the footer is trimmed and unneeded
  // row groups are removed from the footer
  private boolean trimFooter = false;
  private final boolean autoCorrectCorruptDates;
  protected final OperatorContext context;
  private final boolean enableDetailedTracing;
  private FileSystem fs;
  private int numSplitsToPrefetch;
  private boolean prefetchReader;
  private final boolean readInt96AsTimeStamp;
  protected final CompositeReaderConfig readerConfig;
  private final ParquetReaderFactory readerFactory;
  private final List<SchemaPath> realFields;
  private final boolean supportsColocatedReads;
  private final boolean trimRowGroups;
  private final boolean vectorize;
  protected final ParquetFilters filters;
  private final List<SchemaPath> columns;
  protected final BatchSchema fullSchema;
  private final FileConfig formatSettings;
  private List<IcebergProtobuf.IcebergSchemaField> icebergSchemaFields;
  private List<DefaultNameMapping> icebergDefaultNameMapping;
  private final Map<String, Set<Integer>> pathToRowGroupsMap;
  protected final ParquetSplitReaderCreatorIterator parquetSplitReaderCreatorIterator;
  private final boolean ignoreSchemaLearning;
  private final boolean isConvertedIcebergDataset;
  private final UserDefinedSchemaSettings userDefinedSchemaSettings;
  protected final ByteString extendedProperties;

  private final BiConsumer<InputStreamProvider, MutableParquetMetadata> depletionListener =
      (inputStreamProvider, footer) -> {
        if (!prefetchReader || !fs.supportsAsync()) {
          return;
        }

        SplitReaderCreator nextCreator = next;
        int numPrefetched = 0;
        while (nextCreator != null) {
          nextCreator.createInputStreamProvider(inputStreamProvider, footer);
          nextCreator = nextCreator.getNext();
          numPrefetched++;
          if (numPrefetched == numSplitsToPrefetch) {
            break;
          }
        }
      };

  public ParquetSplitReaderCreator(
      boolean autoCorrectCorruptDates,
      OperatorContext context,
      boolean enableDetailedTracing,
      FileSystem fs,
      int numSplitsToPrefetch,
      boolean prefetchReader,
      boolean readInt96AsTimeStamp,
      CompositeReaderConfig readerConfig,
      ParquetReaderFactory readerFactory,
      List<SchemaPath> realFields,
      boolean supportsColocatedReads,
      boolean trimRowGroups,
      boolean vectorize,
      SplitAndPartitionInfo splitInfo,
      List<List<String>> tablePath,
      ParquetFilters filters,
      List<SchemaPath> columns,
      BatchSchema fullSchema,
      FileConfig formatSettings,
      List<IcebergProtobuf.IcebergSchemaField> icebergSchemaFields,
      List<DefaultNameMapping> icebergDefaultNameMapping,
      Map<String, Set<Integer>> pathToRowGroupsMap,
      ParquetSplitReaderCreatorIterator parquetSplitReaderCreatorIterator,
      ParquetProtobuf.ParquetDatasetSplitScanXAttr splitXAttr,
      boolean ignoreSchemaLearning,
      boolean isConvertedIcebergDataset,
      UserDefinedSchemaSettings userDefinedSchemaSettings,
      ByteString extendedProperties) {
    this.pathToRowGroupsMap = pathToRowGroupsMap;
    this.parquetSplitReaderCreatorIterator = parquetSplitReaderCreatorIterator;
    this.datasetSplit = splitInfo;
    this.splitXAttr = splitXAttr;
    this.path = Path.of(splitXAttr.getPath());
    this.tablePath = tablePath;
    if (!fs.supportsPath(path)) {
      throw UserException.invalidMetadataError()
          .addContext("%s: Invalid FS for file '%s'", fs.getScheme(), path)
          .setAdditionalExceptionContext(
              new InvalidMetadataErrorContext(ImmutableList.copyOf(tablePath)))
          .buildSilently();
    }

    this.autoCorrectCorruptDates = autoCorrectCorruptDates;
    this.context = context;
    this.enableDetailedTracing = enableDetailedTracing;
    this.fs = fs;
    this.numSplitsToPrefetch = numSplitsToPrefetch;
    this.prefetchReader = prefetchReader;
    this.readInt96AsTimeStamp = readInt96AsTimeStamp;
    this.readerConfig = readerConfig;
    this.readerFactory = readerFactory;
    this.realFields = realFields;
    this.supportsColocatedReads = supportsColocatedReads;
    this.trimRowGroups = trimRowGroups;
    this.vectorize = vectorize;
    this.filters = filters;
    this.columns = columns;
    this.fullSchema = fullSchema;
    this.formatSettings = formatSettings;
    this.icebergSchemaFields = icebergSchemaFields;
    this.icebergDefaultNameMapping = icebergDefaultNameMapping;
    this.ignoreSchemaLearning = ignoreSchemaLearning;
    this.isConvertedIcebergDataset = isConvertedIcebergDataset;
    this.userDefinedSchemaSettings = userDefinedSchemaSettings;
    this.extendedProperties = extendedProperties;
  }

  @Override
  public void addRowGroupsToRead(Set<Integer> rowGroupsToRead) {
    rowGroupsToRead.add(splitXAttr.getRowGroupIndex());
  }

  @Override
  public SplitAndPartitionInfo getSplit() {
    return this.datasetSplit;
  }

  @Override
  public void createInputStreamProvider(
      InputStreamProvider lastInputStreamProvider, MutableParquetMetadata lastFooter) {
    if (inputStreamProvider != null) {
      parquetSplitReaderCreatorIterator.setLastInputStreamProvider(inputStreamProvider);
      return;
    }

    boolean fromRowGroupBasedSplit = false;
    trimFooter =
        path.equals(
                lastInputStreamProvider != null ? lastInputStreamProvider.getStreamPath() : null)
            && fromRowGroupBasedSplit
            && trimRowGroups;

    handleEx(
        () -> {
          long length, mTime;
          MutableParquetMetadata newFooter;
          if (splitXAttr.hasFileLength()
              && splitXAttr.hasLastModificationTime()
              && context
                  .getOptions()
                  .getOption(ExecConstants.PARQUET_CACHED_ENTITY_SET_FILE_SIZE)) {
            length = splitXAttr.getFileLength();
            mTime = splitXAttr.getLastModificationTime();
          } else {
            FileAttributes fileAttributes = fs.getFileAttributes(path);
            length = fileAttributes.size();
            mTime = fileAttributes.lastModifiedTime().toMillis();
          }
          int currRowGroupIndex = splitXAttr.getRowGroupIndex();
          Function<MutableParquetMetadata, Integer> rowGroupIndexProvider =
              (f) -> splitXAttr.getRowGroupIndex();
          BlockMetaData currentBlockMetadata = null;
          if (lastFooter != null && currRowGroupIndex < lastFooter.getBlocks().size()) {
            currentBlockMetadata = lastFooter.getBlocks().get(currRowGroupIndex);
          }
          newFooter = lastFooter;
          Function<MutableParquetMetadata, Integer> newRowGroupIndexProvider =
              rowGroupIndexProvider;
          if (lastInputStreamProvider != null
              && path.equals(lastInputStreamProvider.getStreamPath())
              && (currentBlockMetadata == null)) {
            newFooter = null;
            newRowGroupIndexProvider =
                (f) -> {
                  int rowGroupIndex = rowGroupIndexProvider.apply(f);
                  parquetSplitReaderCreatorIterator.trimRowGroupsFromFooter(
                      f, path.toString(), rowGroupIndex);
                  return rowGroupIndex;
                };
            context.getStats().addLongStat(ScanOperator.Metric.NUM_EXTRA_FOOTER_READS, 1);
          }
          inputStreamProvider =
              parquetSplitReaderCreatorIterator.createInputStreamProvider(
                  lastInputStreamProvider,
                  newFooter,
                  Path.of(splitXAttr.getPath()),
                  datasetSplit,
                  newRowGroupIndexProvider,
                  length,
                  mTime,
                  splitXAttr.getOriginalPath().isEmpty()
                      ? splitXAttr.getPath()
                      : splitXAttr.getOriginalPath());
          return null;
        });
    parquetSplitReaderCreatorIterator.setLastInputStreamProvider(inputStreamProvider);
  }

  @Override
  public RecordReader createRecordReader(MutableParquetMetadata footer) {
    Preconditions.checkNotNull(inputStreamProvider);
    depletionListener.accept(inputStreamProvider, footer);
    return handleEx(
        () -> {
          try {
            if (trimFooter) {
              // footer needs to be trimmed
              Set<Integer> rowGroupsToRetain = pathToRowGroupsMap.get(splitXAttr.getPath());
              Preconditions.checkArgument(
                  rowGroupsToRetain.size() != 0,
                  "Parquet reader should read at least one row group");
              long numRowGroupsTrimmed = footer.removeUnusedRowGroups(rowGroupsToRetain);
              context
                  .getStats()
                  .addLongStat(ScanOperator.Metric.NUM_ROW_GROUPS_TRIMMED, numRowGroupsTrimmed);
            }

            SchemaDerivationHelper.Builder schemaHelperBuilder =
                SchemaDerivationHelper.builder()
                    .readInt96AsTimeStamp(readInt96AsTimeStamp)
                    .dateCorruptionStatus(
                        ParquetReaderUtility.detectCorruptDates(
                            footer, columns, autoCorrectCorruptDates))
                    .mapDataTypeEnabled(
                        context.getOptions().getOption(ExecConstants.ENABLE_MAP_DATA_TYPE));

            if (formatSettings.getType() == FileType.ICEBERG
                || formatSettings.getType() == FileType.DELTA) {
              schemaHelperBuilder.noSchemaLearning(fullSchema);
            }

            final SchemaDerivationHelper schemaHelper = schemaHelperBuilder.build();
            Preconditions.checkArgument(
                formatSettings.getType() != FileType.ICEBERG || icebergSchemaFields != null);
            ParquetScanProjectedColumns projectedColumns =
                ParquetScanProjectedColumns.fromSchemaPathAndIcebergSchema(
                    realFields,
                    icebergSchemaFields,
                    icebergDefaultNameMapping,
                    isConvertedIcebergDataset,
                    context,
                    fullSchema);
            RecordReader inner;
            if (!isConvertedIcebergDataset && DatasetHelper.isIcebergFile(formatSettings)) {
              inner = createIcebergRecordReader(footer, projectedColumns, schemaHelper);
            } else if (DatasetHelper.isDeltaLake(formatSettings)) {
              inner = createDeltaLakeRecordReader(footer, projectedColumns, schemaHelper);
            } else {
              inner =
                  createParquetRecordReader(
                      footer,
                      projectedColumns,
                      schemaHelperBuilder.noSchemaLearning(fullSchema).build());
            }
            return inner;
          } finally {
            this.inputStreamProvider = null;
          }
        });
  }

  @Override
  public void setIcebergSchemaFields(List<IcebergProtobuf.IcebergSchemaField> icebergSchemaFields) {
    this.icebergSchemaFields = icebergSchemaFields;
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(inputStreamProvider);
    inputStreamProvider = null;
  }

  private RecordReader createIcebergRecordReader(
      MutableParquetMetadata footer,
      ParquetScanProjectedColumns projectedColumns,
      SchemaDerivationHelper schemaHelper) {
    // If there is an equality delete filter, ensure the set of projected columns passed to the
    // inner
    // IcebergParquetReader contains all columns used in equality delete conditions.  This is only
    // required for the
    // inner reader - the outer ParquetCoercionReader maintains the original set of projected
    // columns.
    ParquetScanProjectedColumns innerProjectedColumns =
        filters.hasEqualityDeleteFilter()
            ? addEqualityDeleteFilterFieldsToProjectedColumns(
                projectedColumns, filters.getEqualityDeleteFilter())
            : projectedColumns;

    IcebergParquetReader innerIcebergParquetReader =
        new IcebergParquetReader(
            context,
            readerFactory,
            fullSchema,
            innerProjectedColumns,
            new IcebergParquetFilters(filters),
            splitXAttr,
            fs,
            footer,
            schemaHelper,
            vectorize,
            enableDetailedTracing,
            supportsColocatedReads,
            inputStreamProvider,
            isConvertedIcebergDataset);
    return createRecordReader(projectedColumns, innerIcebergParquetReader);
  }

  private RecordReader createDeltaLakeRecordReader(
      MutableParquetMetadata footer,
      ParquetScanProjectedColumns projectedColumns,
      SchemaDerivationHelper schemaHelper) {
    DeltaLakeParquetReader innerDeltaParquetReader =
        new DeltaLakeParquetReader(
            context,
            readerFactory,
            fullSchema,
            projectedColumns,
            new DeltaLakeParquetFilters(filters),
            splitXAttr,
            fs,
            footer,
            schemaHelper,
            vectorize,
            enableDetailedTracing,
            supportsColocatedReads,
            inputStreamProvider);
    return createRecordReader(projectedColumns, innerDeltaParquetReader);
  }

  private RecordReader createParquetRecordReader(
      MutableParquetMetadata footer,
      ParquetScanProjectedColumns projectedColumns,
      SchemaDerivationHelper schemaHelper) {
    final UpPromotingParquetReader innerParquetReader =
        new UpPromotingParquetReader(
            context,
            readerFactory,
            fullSchema,
            projectedColumns,
            filters,
            splitXAttr,
            fs,
            footer,
            path.toString(),
            Iterables.getFirst(tablePath, null),
            schemaHelper,
            vectorize,
            enableDetailedTracing,
            supportsColocatedReads,
            inputStreamProvider,
            userDefinedSchemaSettings);
    return createRecordReader(projectedColumns, innerParquetReader);
  }

  protected RecordReader createRecordReader(
      ParquetScanProjectedColumns projectedColumns, RecordReader innerReader) {
    Map<String, Field> fieldsByName = CaseInsensitiveMap.newHashMap();
    fullSchema.getFields().forEach(field -> fieldsByName.put(field.getName(), field));
    RecordReader wrappedRecordReader =
        ParquetCoercionReader.newInstance(
            context,
            projectedColumns.getBatchSchemaProjectedColumns(),
            innerReader,
            fullSchema,
            new FileTypeCoercion(fieldsByName),
            filters);
    return readerConfig.wrapIfNecessary(context.getAllocator(), wrappedRecordReader, datasetSplit);
  }

  private ParquetScanProjectedColumns addEqualityDeleteFilterFieldsToProjectedColumns(
      ParquetScanProjectedColumns projectedColumns, EqualityDeleteFilter equalityDeleteFilter) {
    // create a new list of projected columns which includes all equality fields that weren't
    // already present
    Set<SchemaPath> projectedSet = new HashSet<>(projectedColumns.getBatchSchemaProjectedColumns());
    Set<SchemaPath> equalityFieldsSet = new HashSet<>(equalityDeleteFilter.getAllEqualityFields());
    Set<SchemaPath> missingEqualityFields = Sets.difference(equalityFieldsSet, projectedSet);
    List<SchemaPath> projectedColumnsWithEqualityFields =
        new ArrayList<>(projectedColumns.getBatchSchemaProjectedColumns());
    projectedColumnsWithEqualityFields.addAll(missingEqualityFields);
    return projectedColumns.cloneForSchemaPaths(projectedColumnsWithEqualityFields, false);
  }
}
