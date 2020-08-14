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

import static com.dremio.exec.store.parquet.ParquetFormatDatasetAccessor.ACCELERATOR_STORAGEPLUGIN_NAME;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.util.Preconditions;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.InvalidMetadataErrorContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.datastore.LegacyProtobufSerializer;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.planner.physical.visitor.GlobalDictionaryFieldInfo;
import com.dremio.exec.store.FilteringCoercionReader;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.PrefetchingIterator;
import com.dremio.exec.store.dfs.SplitReaderCreator;
import com.dremio.exec.store.dfs.implicit.CompositeReaderConfig;
import com.dremio.exec.store.dfs.implicit.ImplicitFilesystemColumnFinder;
import com.dremio.exec.util.ColumnUtils;
import com.dremio.io.file.FileAttributes;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators.BooleanValidator;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.exec.store.iceberg.proto.IcebergProtobuf.IcebergDatasetXAttr;
import com.dremio.sabot.exec.store.iceberg.proto.IcebergProtobuf.IcebergSchemaField;
import com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf.ParquetDatasetSplitScanXAttr;
import com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf.ParquetDatasetXAttr;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.sabot.op.spi.ProducerOperator;
import com.dremio.sabot.op.spi.ProducerOperator.Creator;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.file.FileFormat;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.namespace.file.proto.FileType;
import com.dremio.service.namespace.file.proto.IcebergFileConfig;
import com.dremio.service.namespace.file.proto.ParquetFileConfig;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Parquet scan batch creator from dataset config
 */
@Options
public class ParquetOperatorCreator implements Creator<ParquetSubScan> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetOperatorCreator.class);

  public static BooleanValidator PREFETCH_READER = new BooleanValidator("store.parquet.prefetch_reader", true);

  @Override
  public ProducerOperator create(FragmentExecutionContext fragmentExecContext, final OperatorContext context, final ParquetSubScan config) throws ExecutionSetupException {
    final Stopwatch watch = Stopwatch.createStarted();

    Creator creator = new Creator(fragmentExecContext, context, config);
    try {
      final ScanOperator scan = creator.createScan();
      logger.debug("Took {} ms to create Parquet Scan.", watch.elapsed(TimeUnit.MILLISECONDS));
      return scan;
    } catch (Exception ex) {
      throw new ExecutionSetupException("Failed to create scan operator.", ex);
    }
  }

  public Iterator<RecordReader> getReaders(FragmentExecutionContext fragmentExecContext, final OperatorContext context, final ParquetSubScan config) throws ExecutionSetupException {
    return new Creator(fragmentExecContext, context, config).getReaders();
  }

  /**
   * An object that holds the relevant creation fields so we don't have to have an really long lambda.
   */
  private static final class Creator {
    private final FileSystemPlugin<?> plugin;
    private final FileSystem fs;
    private final boolean isAccelerator;
    private final ParquetReaderFactory readerFactory;
    private final List<SchemaPath> realFields;
    private final GlobalDictionaries globalDictionaries;
    private final boolean vectorize;
    private final boolean autoCorrectCorruptDates;
    private final boolean readInt96AsTimeStamp;
    private final boolean enableDetailedTracing;
    private final boolean prefetchReader;
    private final boolean trimRowGroups;
    private final boolean supportsColocatedReads;
    private final Map<String, GlobalDictionaryFieldInfo> globalDictionaryEncodedColumns;
    private final CompositeReaderConfig readerConfig;
    private final ParquetSubScan config;
    private final OperatorContext context;
    private final InputStreamProviderFactory factory;
    private final FragmentExecutionContext fragmentExecutionContext;

    public Creator(FragmentExecutionContext fragmentExecContext, final OperatorContext context, final ParquetSubScan config) throws ExecutionSetupException {
      this.context = context;
      this.config = config;
      this.factory = context.getConfig().getInstance(InputStreamProviderFactory.KEY, InputStreamProviderFactory.class, InputStreamProviderFactory.DEFAULT);
      this.prefetchReader = context.getOptions().getOption(PREFETCH_READER);
      this.trimRowGroups = context.getOptions().getOption(ExecConstants.TRIM_ROWGROUPS_FROM_FOOTER);
      this.plugin = fragmentExecContext.getStoragePlugin(config.getPluginId());
      try {
        this.fs = plugin.createFS(config.getProps().getUserName(), context);
      } catch (IOException e) {
        throw new ExecutionSetupException("Cannot access plugin filesystem", e);
      }
      this.isAccelerator = config.getPluginId().getName().equals(ACCELERATOR_STORAGEPLUGIN_NAME);
      this.readerFactory = UnifiedParquetReader.getReaderFactory(context.getConfig());

      if (DatasetHelper.isIcebergFile(config.getFormatSettings())) {
        this.realFields = getRealIcebergFields();
      } else {
        // TODO (AH )Fix implicit columns with mod time and global dictionaries
        this.realFields = new ImplicitFilesystemColumnFinder(
                context.getOptions(), fs, config.getColumns(), isAccelerator).getRealFields();
      }

      // load global dictionaries, globalDictionaries must be closed by the last reader
      this.globalDictionaries = GlobalDictionaries.create(context, fs,  config.getGlobalDictionaryEncodedColumns());
      this.vectorize = context.getOptions().getOption(ExecConstants.PARQUET_READER_VECTORIZE);

      this.autoCorrectCorruptDates =
          context.getOptions().getOption(ExecConstants.PARQUET_AUTO_CORRECT_DATES_VALIDATOR)
          && autoCorrectCorruptDatesFromFileFormat(config.getFormatSettings());
      this.readInt96AsTimeStamp = context.getOptions().getOption(ExecConstants.PARQUET_READER_INT96_AS_TIMESTAMP_VALIDATOR);
      this.enableDetailedTracing = context.getOptions().getOption(ExecConstants.ENABLED_PARQUET_TRACING);
      this.supportsColocatedReads = plugin.supportsColocatedReads();
      this.readerConfig = CompositeReaderConfig.getCompound(context, config.getFullSchema(), config.getColumns(), config.getPartitionColumns());

      this.globalDictionaryEncodedColumns = Maps.newHashMap();

      if (globalDictionaries != null) {
        for (GlobalDictionaryFieldInfo fieldInfo : config.getGlobalDictionaryEncodedColumns()) {
          globalDictionaryEncodedColumns.put(fieldInfo.getFieldName(), fieldInfo);
        }
      }
      this.fragmentExecutionContext = fragmentExecContext;
    }

    private boolean autoCorrectCorruptDatesFromFileFormat(FileConfig fileConfig) {
      boolean autoCorrect;
      if (DatasetHelper.isIcebergFile(fileConfig)) {
        autoCorrect = ((IcebergFileConfig) FileFormat.getForFile(fileConfig)).getParquetDataFormat().getAutoCorrectCorruptDates();
      } else {
        autoCorrect = ((ParquetFileConfig) FileFormat.getForFile(fileConfig)).getAutoCorrectCorruptDates();
      }
      return autoCorrect;
    }

    // iceberg has no implicit columns.
    private List<SchemaPath> getRealIcebergFields() {
      Set<SchemaPath> selectedPaths = new LinkedHashSet<>();
      if (config.getColumns() == null || ColumnUtils.isStarQuery(config.getColumns())) {
        selectedPaths.addAll(GroupScan.ALL_COLUMNS);
      } else {
        selectedPaths.addAll(config.getColumns());
      }
      return ImmutableList.copyOf(selectedPaths);
    }

    public ScanOperator createScan() throws Exception {
      List<ParquetSplitReaderCreator> splits = config.getSplits().stream().map(ParquetSplitReaderCreator::new).sorted().collect(Collectors.toList());
      ParquetSplitReaderCreator next = null;

      // set forward links
      for(int i = splits.size() - 1; i > -1; i--) {
        ParquetSplitReaderCreator cur = splits.get(i);
        cur.setNext(next);
        next = cur;
      }

      PrefetchingIterator<ParquetSplitReaderCreator> iterator = new PrefetchingIterator<>(splits);
      try {
        return new ScanOperator(config, context, iterator, globalDictionaries, fragmentExecutionContext.getForemanEndpoint(), fragmentExecutionContext.getQueryContextInformation());
      } catch (Exception ex) {
        AutoCloseables.close(iterator);
        throw ex;
      }
    }

    public Iterator<RecordReader> getReaders() {
      List<ParquetSplitReaderCreator> splits = config.getSplits().stream().map(ParquetSplitReaderCreator::new).collect(Collectors.toList());
      ParquetSplitReaderCreator next = null;

      // set forward links
      for(int i = splits.size() - 1; i > -1; i--) {
        ParquetSplitReaderCreator cur = splits.get(i);
        cur.setNext(next);
        next = cur;
      }

      return new PrefetchingIterator<>(splits);
    }

    /**
     * A lightweight object used to manage the creation of a reader. Allows pre-initialization of data before reader
     * construction.
     */
    private class ParquetSplitReaderCreator extends SplitReaderCreator implements Comparable<ParquetSplitReaderCreator>, AutoCloseable {
      private SplitAndPartitionInfo datasetSplit;
      // set to true while creating input stream provider. When true, the footer is trimmed and unneeded row groups are removed from the footer
      private boolean trimFooter = false;

      public ParquetSplitReaderCreator(SplitAndPartitionInfo splitInfo) {
        this.datasetSplit = splitInfo;
        try {
          this.splitXAttr = LegacyProtobufSerializer.parseFrom(ParquetDatasetSplitScanXAttr.PARSER, datasetSplit.getDatasetSplitInfo().getExtendedProperty());
        } catch (InvalidProtocolBufferException e) {
          throw new RuntimeException("Could not deserialize parquet dataset split scan attributes", e);
        }
        this.path = Path.of(splitXAttr.getPath());
        this.tablePath = config.getTablePath();
        if (!fs.supportsPath(path)) {
          throw UserException.invalidMetadataError()
            .addContext(String.format("%s: Invalid FS for file '%s'", fs.getScheme(), path))
            .addContext("File", path)
            .setAdditionalExceptionContext(
              new InvalidMetadataErrorContext(
                ImmutableList.copyOf(config.getReferencedTables())))
            .buildSilently();
        }

      }

      private void clearAllLocalFields() {
        datasetSplit = null;
        splitXAttr = null;
        inputStreamProvider = null;
      }

      @Override
      public void addRowGroupsToRead(Set<Integer> rowGroupsToRead) {
        rowGroupsToRead.add(splitXAttr.getRowGroupIndex());
      }

      @Override
      public void createInputStreamProvider(Path lastPath, MutableParquetMetadata lastFooter) {
        if(inputStreamProvider != null) {
          return;
        }

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

          final MutableParquetMetadata validLastFooter = path.equals(lastPath) ? lastFooter : null;
          // if we are not using the last footer, mark the footer for trimming
          trimFooter = (validLastFooter == null) && trimRowGroups;

          BiConsumer<Path, MutableParquetMetadata> depletionListener = (path, footer) -> {
            if(!prefetchReader || next == null) {
              return;
            }

            next.createInputStreamProvider(path, footer);
          };

          final boolean readFullFile = length < context.getOptions().getOption(ExecConstants.PARQUET_FULL_FILE_READ_THRESHOLD) &&
              ((float)config.getColumns().size()) / config.getFullSchema().getFieldCount() > context.getOptions().getOption(ExecConstants.PARQUET_FULL_FILE_READ_COLUMN_RATIO);

          final Collection<List<String>> referencedTables = config.getReferencedTables();
          final List<String> dataset = referencedTables == null || referencedTables.isEmpty() ? null : referencedTables.iterator().next();
          ParquetScanProjectedColumns projectedColumns = ParquetScanProjectedColumns.fromSchemaPathAndIcebergSchema(realFields, getIcebergColumnIDList());

          inputStreamProvider = factory.create(
              fs,
              context,
              path,
              length,
              datasetSplit.getPartitionInfo().getSize(),
              projectedColumns,
              validLastFooter,
              (footer) -> splitXAttr.getRowGroupIndex(),
              depletionListener,
              readFullFile,
              dataset,
              mTime,
              config.isArrowCachingEnabled());
          return null;
        });
      }

      @Override
      public RecordReader createRecordReader() {
        Preconditions.checkNotNull(inputStreamProvider);
        return handleEx(() -> {
          try {
            final MutableParquetMetadata footer = inputStreamProvider.getFooter();
            if (trimFooter) {
              // footer needs to be trimmed
              Set<Integer> rowGroupsToRetain = Sets.newHashSet();
              this.getRowGroupsFromSameFile(this.path, rowGroupsToRetain);
              Preconditions.checkArgument(rowGroupsToRetain.size() != 0, "Parquet reader should read at least one row group");
              long numRowGroupsTrimmed = footer.removeUnusedRowGroups(rowGroupsToRetain);
              context.getStats().addLongStat(ScanOperator.Metric.NUM_ROW_GROUPS_TRIMMED, numRowGroupsTrimmed);
            }

            SchemaDerivationHelper.Builder schemaHelperBuilder = SchemaDerivationHelper.builder()
                .readInt96AsTimeStamp(readInt96AsTimeStamp)
                .dateCorruptionStatus(ParquetReaderUtility.detectCorruptDates(footer, config.getColumns(), autoCorrectCorruptDates));

            List<IcebergSchemaField> icebergColumnIDList = null;

            if (config.getFormatSettings().getType() == FileType.ICEBERG) {
              icebergColumnIDList = getIcebergColumnIDList();
              schemaHelperBuilder.noSchemaLearning(config.getFullSchema());
            }

            final SchemaDerivationHelper schemaHelper = schemaHelperBuilder.build();

            ParquetScanProjectedColumns projectedColumns = ParquetScanProjectedColumns.fromSchemaPathAndIcebergSchema(realFields, icebergColumnIDList);
            RecordReader inner;
            if (DatasetHelper.isIcebergFile(config.getFormatSettings())) {
              IcebergParquetReader innerIcebergParquetReader = new IcebergParquetReader(
                context,
                readerFactory,
                config.getFullSchema(),
                projectedColumns,
                globalDictionaryEncodedColumns,
                config.getConditions(),
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
              RecordReader wrappedRecordReader = new FilteringCoercionReader(context, projectedColumns.getBatchSchemaProjectedColumns(), innerIcebergParquetReader, config.getFullSchema(), config.getConditions());
              inner = readerConfig.wrapIfNecessary(context.getAllocator(), wrappedRecordReader, datasetSplit);
            } else {
              final UnifiedParquetReader innerParquetReader = new UnifiedParquetReader(
                context,
                readerFactory,
                config.getFullSchema(),
                projectedColumns,
                globalDictionaryEncodedColumns,
                config.getConditions(),
                ParquetFilterCreator.DEFAULT,
                ParquetDictionaryConvertor.DEFAULT,
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
              inner = readerConfig.wrapIfNecessary(context.getAllocator(), innerParquetReader, datasetSplit);
            }
            return inner;
          }finally {
            // minimize heap memory of this object.
            clearAllLocalFields();
          }
        });
      }

      @Override
      public int compareTo(ParquetSplitReaderCreator other) {
        int retVal = path.compareTo(other.path);
        if (retVal != 0) {
          return retVal;
        }

        if (splitXAttr.hasStart() && other.splitXAttr.hasStart()) {
          return Long.compare(splitXAttr.getStart(), other.splitXAttr.getStart());
        }

        return Integer.compare(splitXAttr.getRowGroupIndex(), other.splitXAttr.getRowGroupIndex());
      }

      @Override
      public void close() throws Exception {
        AutoCloseables.close(inputStreamProvider);
      }

    }

    private List<IcebergSchemaField> getIcebergColumnIDList() {
      if (config.getFormatSettings().getType() != FileType.ICEBERG) {
        return null;
      }

      try {
        IcebergDatasetXAttr icebergDatasetXAttr = LegacyProtobufSerializer.parseFrom(IcebergDatasetXAttr.PARSER,
          config.getExtendedProperty().asReadOnlyByteBuffer());
        return icebergDatasetXAttr.getColumnIdsList();
      } catch (InvalidProtocolBufferException ie) {
        try {
          ParquetDatasetXAttr parquetDatasetXAttr = LegacyProtobufSerializer.parseFrom(ParquetDatasetXAttr.PARSER,
            config.getExtendedProperty().asReadOnlyByteBuffer());
          // found XAttr from 5.0.1 release. return null
          return null;
        } catch (InvalidProtocolBufferException pe) {
          throw new RuntimeException("Could not deserialize Parquet dataset info", pe);
        }
      }
    }
  }
}
