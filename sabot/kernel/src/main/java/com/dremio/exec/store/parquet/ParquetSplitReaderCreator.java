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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.common.exceptions.InvalidMetadataErrorContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.map.CaseInsensitiveMap;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.physical.visitor.GlobalDictionaryFieldInfo;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.CoercionReader;
import com.dremio.exec.store.HiveParquetCoercionReader;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.dfs.SplitReaderCreator;
import com.dremio.exec.store.dfs.implicit.CompositeReaderConfig;
import com.dremio.io.file.FileAttributes;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.store.iceberg.proto.IcebergProtobuf;
import com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.namespace.file.proto.FileType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

/**
 * A lightweight object used to manage the creation of a reader. Allows pre-initialization of data before reader
 * construction.
 */
public class ParquetSplitReaderCreator extends SplitReaderCreator implements AutoCloseable {
  private SplitAndPartitionInfo datasetSplit;
  // set to true while creating input stream provider. When true, the footer is trimmed and unneeded row groups are removed from the footer
  private boolean trimFooter = false;
  private final boolean autoCorrectCorruptDates;
  private final OperatorContext context;
  private final boolean enableDetailedTracing;
  private FileSystem fs;
  private final GlobalDictionaries globalDictionaries;
  private final Map<String, GlobalDictionaryFieldInfo> globalDictionaryEncodedColumns;
  private int numSplitsToPrefetch;
  private boolean prefetchReader;
  private final boolean readInt96AsTimeStamp;
  private final CompositeReaderConfig readerConfig;
  private final ParquetReaderFactory readerFactory;
  private final List<SchemaPath> realFields;
  private final boolean supportsColocatedReads;
  private final boolean trimRowGroups;
  private final boolean vectorize;
  private final List<ParquetFilterCondition> conditions;
  private final List<SchemaPath> columns;
  private final BatchSchema fullSchema;
  private final FileConfig formatSettings;
  private List<IcebergProtobuf.IcebergSchemaField> icebergSchemaFields;
  private final Map<String, Set<Integer>> pathToRowGroupsMap;
  private final ParquetSplitReaderCreatorIterator parquetSplitReaderCreatorIterator;
  private final boolean ignoreSchemaLearning;
  private final boolean isConvertedIcebergDataset;

  private final BiConsumer<InputStreamProvider, MutableParquetMetadata> depletionListener = (inputStreamProvider, footer) -> {
    if (!prefetchReader || !fs.supportsAsync()) {
      return;
    }

    SplitReaderCreator nextCreator = next;
    int numPrefetched = 0;
    while (nextCreator != null) {
      nextCreator.createInputStreamProvider(inputStreamProvider, footer);
      nextCreator = ((ParquetSplitReaderCreator)nextCreator).next;
      numPrefetched++;
      if (numPrefetched == numSplitsToPrefetch) {
        break;
      }
    }
  };

  public ParquetSplitReaderCreator(boolean autoCorrectCorruptDates,
                                   OperatorContext context,
                                   boolean enableDetailedTracing,
                                   InputStreamProviderFactory factory,
                                   FileSystem fs,
                                   GlobalDictionaries globalDictionaries,
                                   Map<String, GlobalDictionaryFieldInfo> globalDictionaryEncodedColumns,
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
                                   List<ParquetFilterCondition> conditions,
                                   List<SchemaPath> columns,
                                   BatchSchema fullSchema,
                                   boolean arrowCachingEnabled,
                                   FileConfig formatSettings,
                                   List<IcebergProtobuf.IcebergSchemaField> icebergSchemaFields,
                                   Map<String, Set<Integer>> pathToRowGroupsMap,
                                   ParquetSplitReaderCreatorIterator parquetSplitReaderCreatorIterator,
                                   ParquetProtobuf.ParquetDatasetSplitScanXAttr splitXAttr,
                                   boolean ignoreSchemaLearning,
                                   boolean isConvertedIcebergDataset) {
    this.pathToRowGroupsMap = pathToRowGroupsMap;
    this.parquetSplitReaderCreatorIterator = parquetSplitReaderCreatorIterator;
    this.datasetSplit = splitInfo;
    this.splitXAttr = splitXAttr;
    this.path = Path.of(splitXAttr.getPath());
    this.tablePath = tablePath;
    if (!fs.supportsPath(path)) {
      throw UserException.invalidMetadataError()
        .addContext(String.format("%s: Invalid FS for file '%s'", fs.getScheme(), path))
        .addContext("File", path)
        .setAdditionalExceptionContext(
          new InvalidMetadataErrorContext(
            ImmutableList.copyOf(tablePath)))
        .buildSilently();
    }

    this.autoCorrectCorruptDates = autoCorrectCorruptDates;
    this.context = context;
    this.enableDetailedTracing = enableDetailedTracing;
    this.fs = fs;
    this.globalDictionaries = globalDictionaries;
    this.globalDictionaryEncodedColumns = globalDictionaryEncodedColumns;
    this.numSplitsToPrefetch = numSplitsToPrefetch;
    this.prefetchReader = prefetchReader;
    this.readInt96AsTimeStamp = readInt96AsTimeStamp;
    this.readerConfig = readerConfig;
    this.readerFactory = readerFactory;
    this.realFields = realFields;
    this.supportsColocatedReads = supportsColocatedReads;
    this.trimRowGroups = trimRowGroups;
    this.vectorize = vectorize;
    this.conditions = conditions;
    this.columns = columns;
    this.fullSchema = fullSchema;
    this.formatSettings = formatSettings;
    this.icebergSchemaFields = icebergSchemaFields;
    this.ignoreSchemaLearning = ignoreSchemaLearning;
    this.isConvertedIcebergDataset = isConvertedIcebergDataset;
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
  public void createInputStreamProvider(InputStreamProvider lastInputStreamProvider, MutableParquetMetadata lastFooter) {
    if(inputStreamProvider != null) {
      parquetSplitReaderCreatorIterator.setLastInputStreamProvider(inputStreamProvider);
      return;
    }

    boolean fromRowGroupBasedSplit = false;
    trimFooter = path.equals(lastInputStreamProvider != null ? lastInputStreamProvider.getStreamPath() : null) && fromRowGroupBasedSplit && trimRowGroups;

    handleEx(() -> {
      long length, mTime;
      if (splitXAttr.hasFileLength() && splitXAttr.hasLastModificationTime() && context.getOptions().getOption(ExecConstants.PARQUET_CACHED_ENTITY_SET_FILE_SIZE)) {
        length = splitXAttr.getFileLength();
        mTime = splitXAttr.getLastModificationTime();
      } else {
        FileAttributes fileAttributes = fs.getFileAttributes(path);
        length = fileAttributes.size();
        mTime = fileAttributes.lastModifiedTime().toMillis();
      }
      inputStreamProvider = parquetSplitReaderCreatorIterator.createInputStreamProvider(lastInputStreamProvider, lastFooter, Path.of(splitXAttr.getPath()), datasetSplit, (f) -> splitXAttr.getRowGroupIndex(), length, mTime);
      return null;
    });
    parquetSplitReaderCreatorIterator.setLastInputStreamProvider(inputStreamProvider);
  }

  @Override
  public RecordReader createRecordReader(MutableParquetMetadata footer) {
    Preconditions.checkNotNull(inputStreamProvider);
    depletionListener.accept(inputStreamProvider, footer);
    return handleEx(() -> {
      try {
        if (trimFooter) {
          // footer needs to be trimmed
          Set<Integer> rowGroupsToRetain = pathToRowGroupsMap.get(splitXAttr.getPath());
          Preconditions.checkArgument(rowGroupsToRetain.size() != 0, "Parquet reader should read at least one row group");
          long numRowGroupsTrimmed = footer.removeUnusedRowGroups(rowGroupsToRetain);
          context.getStats().addLongStat(ScanOperator.Metric.NUM_ROW_GROUPS_TRIMMED, numRowGroupsTrimmed);
        }

        SchemaDerivationHelper.Builder schemaHelperBuilder = SchemaDerivationHelper.builder()
                .readInt96AsTimeStamp(readInt96AsTimeStamp)
                .dateCorruptionStatus(ParquetReaderUtility.detectCorruptDates(footer, columns, autoCorrectCorruptDates));

        if (formatSettings.getType() == FileType.ICEBERG || formatSettings.getType() == FileType.DELTA) {
          schemaHelperBuilder.noSchemaLearning(fullSchema);
        }

        final SchemaDerivationHelper schemaHelper = schemaHelperBuilder.build();
        Preconditions.checkArgument(formatSettings.getType() != FileType.ICEBERG || icebergSchemaFields != null);
        ParquetScanProjectedColumns projectedColumns = ParquetScanProjectedColumns.fromSchemaPathAndIcebergSchema(realFields, icebergSchemaFields);
        RecordReader inner;
        if (!isConvertedIcebergDataset && DatasetHelper.isIcebergFile(formatSettings)) {
          IcebergParquetReader innerIcebergParquetReader = new IcebergParquetReader(
                  context,
                  readerFactory,
                  fullSchema,
                  projectedColumns,
                  globalDictionaryEncodedColumns,
                  conditions,
                  splitXAttr,
                  fs,
                  footer,
                  globalDictionaries,
                  schemaHelper,
                  vectorize,
                  enableDetailedTracing,
                  supportsColocatedReads,
                  inputStreamProvider
          );
          RecordReader wrappedRecordReader = new CoercionReader(context, projectedColumns.getBatchSchemaProjectedColumns(), innerIcebergParquetReader, fullSchema);
          inner = readerConfig.wrapIfNecessary(context.getAllocator(), wrappedRecordReader, datasetSplit);
        } else if (DatasetHelper.isDeltaLake(formatSettings)) {
          DeltaLakeParquetReader innerDeltaParquetReader = new DeltaLakeParquetReader(
                  context,
                  readerFactory,
                  fullSchema,
                  projectedColumns,
                  globalDictionaryEncodedColumns,
                  conditions,
                  splitXAttr,
                  fs,
                  footer,
                  globalDictionaries,
                  schemaHelper,
                  vectorize,
                  enableDetailedTracing,
                  supportsColocatedReads,
                  inputStreamProvider
          );
          RecordReader wrappedRecordReader = new CoercionReader(context, projectedColumns.getBatchSchemaProjectedColumns(), innerDeltaParquetReader, fullSchema);
          inner = readerConfig.wrapIfNecessary(context.getAllocator(), wrappedRecordReader, datasetSplit);
        } else {
          boolean mixedTypesDisabled = context.getOptions().getOption(ExecConstants.MIXED_TYPES_DISABLED);
          if (mixedTypesDisabled) {
            SchemaDerivationHelper schemaDerivationHelper = schemaHelperBuilder.noSchemaLearning(fullSchema).build();
            final UpPromotingParquetReader innerParquetReader = new UpPromotingParquetReader(
                    context,
                    readerFactory,
                    fullSchema,
                    projectedColumns,
                    globalDictionaryEncodedColumns,
                    conditions,
                    splitXAttr,
                    fs,
                    footer,
                    path.toString(),
                    Iterables.getFirst(tablePath, null),
                    globalDictionaries,
                    schemaDerivationHelper,
                    vectorize,
                    enableDetailedTracing,
                    supportsColocatedReads,
                    inputStreamProvider);

            Map<String, Field> fieldsByName = CaseInsensitiveMap.newHashMap();
            fullSchema.getFields().forEach(field -> fieldsByName.put(field.getName(), field));
            RecordReader wrappedRecordReader = HiveParquetCoercionReader.newInstance(context,
                    projectedColumns.getBatchSchemaProjectedColumns(), innerParquetReader, fullSchema,
                    new ParquetTypeCoercion(fieldsByName), conditions);
            return readerConfig.wrapIfNecessary(context.getAllocator(), wrappedRecordReader, datasetSplit);
          }

          final UnifiedParquetReader innerParquetReader = new UnifiedParquetReader(
                  context,
                  readerFactory,
                  fullSchema,
                  projectedColumns,
                  globalDictionaryEncodedColumns,
                  conditions,
                  readerFactory.newFilterCreator(context, null, null, context.getAllocator()),
                  ParquetDictionaryConvertor.DEFAULT,
                  splitXAttr,
                  fs,
                  footer,
                  globalDictionaries,
                  schemaHelper,
                  vectorize,
                  enableDetailedTracing,
                  supportsColocatedReads,
                  inputStreamProvider,
                  new ArrayList<>());
          innerParquetReader.setIgnoreSchemaLearning(ignoreSchemaLearning);
          inner = readerConfig.wrapIfNecessary(context.getAllocator(), innerParquetReader, datasetSplit);
        }
        return inner;
      }finally {
        this.datasetSplit = null;
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

}
