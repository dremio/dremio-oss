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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.util.Preconditions;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.InvalidMetadataErrorContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.datastore.LegacyProtobufSerializer;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.physical.visitor.GlobalDictionaryFieldInfo;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.implicit.CompositeReaderConfig;
import com.dremio.exec.store.dfs.implicit.ImplicitFilesystemColumnFinder;
import com.dremio.io.file.FileAttributes;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators.BooleanValidator;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf.ParquetDatasetSplitScanXAttr;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.sabot.op.spi.ProducerOperator;
import com.dremio.sabot.op.spi.ProducerOperator.Creator;
import com.dremio.service.namespace.file.FileFormat;
import com.dremio.service.namespace.file.proto.ParquetFileConfig;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
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

  /**
   * An object that holds the relevant creation fields so we don't have to have an really long lambda.
   */
  private static final class Creator {
    private final FileSystemPlugin<?> plugin;
    private final FileSystem fs;
    private final boolean isAccelerator;
    private final ParquetReaderFactory readerFactory;
    private final ImplicitFilesystemColumnFinder finder;
    private final GlobalDictionaries globalDictionaries;
    private final boolean vectorize;
    private final boolean autoCorrectCorruptDates;
    private final boolean readInt96AsTimeStamp;
    private final boolean enableDetailedTracing;
    private final boolean prefetchReader;
    private final boolean supportsColocatedReads;
    private final Map<String, GlobalDictionaryFieldInfo> globalDictionaryEncodedColumns;
    private final CompositeReaderConfig readerConfig;
    private final ParquetSubScan config;
    private final OperatorContext context;
    private final InputStreamProviderFactory factory;

    public Creator(FragmentExecutionContext fragmentExecContext, final OperatorContext context, final ParquetSubScan config) throws ExecutionSetupException {
      this.context = context;
      this.config = config;
      this.factory = context.getConfig().getInstance(InputStreamProviderFactory.KEY, InputStreamProviderFactory.class, InputStreamProviderFactory.DEFAULT);
      this.prefetchReader = context.getOptions().getOption(PREFETCH_READER);
      this.plugin = fragmentExecContext.getStoragePlugin(config.getPluginId());
      try {
        this.fs = plugin.createFS(config.getProps().getUserName(), context);
      } catch (IOException e) {
        throw new ExecutionSetupException("Cannot access plugin filesystem", e);
      }
      this.isAccelerator = config.getPluginId().getName().equals(ACCELERATOR_STORAGEPLUGIN_NAME);
      this.readerFactory = UnifiedParquetReader.getReaderFactory(context.getConfig());

      // TODO (AH )Fix implicit columns with mod time and global dictionaries
      this.finder = new ImplicitFilesystemColumnFinder(context.getOptions(), fs, config.getColumns(), isAccelerator);

      // load global dictionaries, globalDictionaries must be closed by the last reader
      this.globalDictionaries = GlobalDictionaries.create(context, fs,  config.getGlobalDictionaryEncodedColumns());
      this.vectorize = context.getOptions().getOption(ExecConstants.PARQUET_READER_VECTORIZE);
      this.autoCorrectCorruptDates =
          context.getOptions().getOption(ExecConstants.PARQUET_AUTO_CORRECT_DATES_VALIDATOR)
          && ((ParquetFileConfig) FileFormat.getForFile(config.getFormatSettings())).getAutoCorrectCorruptDates();
      this.readInt96AsTimeStamp = context.getOptions().getOption(ExecConstants.PARQUET_READER_INT96_AS_TIMESTAMP_VALIDATOR);
      this.enableDetailedTracing = context.getOptions().getOption(ExecConstants.ENABLED_PARQUET_TRACING);
      this.supportsColocatedReads = plugin.supportsColocatedReads();
      this.readerConfig = CompositeReaderConfig.getCompound(config.getFullSchema(), config.getColumns(), config.getPartitionColumns());

      this.globalDictionaryEncodedColumns = Maps.newHashMap();

      if (globalDictionaries != null) {
        for (GlobalDictionaryFieldInfo fieldInfo : config.getGlobalDictionaryEncodedColumns()) {
          globalDictionaryEncodedColumns.put(fieldInfo.getFieldName(), fieldInfo);
        }
      }
    }

    public ScanOperator createScan() throws Exception {
      List<SplitReaderCreator> splits = config.getSplits().stream().map(SplitReaderCreator::new).sorted().collect(Collectors.toList());
      SplitReaderCreator next = null;

      // set forward links
      for(int i = splits.size() - 1; i > -1; i--) {
        SplitReaderCreator cur = splits.get(i);
        cur.setNext(next);
        next = cur;
      }

      PrefetchingIterator iterator = new PrefetchingIterator(splits);
      try {
        return new ScanOperator(config, context, iterator, globalDictionaries);
      } catch (Exception ex) {
        AutoCloseables.close(iterator);
        throw ex;
      }
    }

    /**
     * Interface to allow a runnable that throws IOException.
     */
    private interface RunnableIO<T> {
      T run() throws IOException;
    }

    /**
     * A split, separates initialization of the input reader from actually constructing the reader to allow prefetching.
     */
    private class PrefetchingIterator implements Iterator<RecordReader>, AutoCloseable {

      private int location = -1;
      private final List<Creator.SplitReaderCreator> creators;
      private Path path;
      private ParquetMetadata footer;

      public PrefetchingIterator(List<Creator.SplitReaderCreator> creators) {
        super();
        this.creators = creators;
      }

      @Override
      public boolean hasNext() {
        return location < creators.size() - 1;
      }

      @Override
      public RecordReader next() {
        Preconditions.checkArgument(hasNext());
        location++;
        final Creator.SplitReaderCreator current = creators.get(location);
        current.createInputStreamProvider(path, footer);
        this.path = current.getPath();
        this.footer = current.getFooter();
        return current.createRecordReader();
      }

      @Override
      public void close() throws Exception {
        // this is for cleanup if we prematurely exit.
        AutoCloseables.close(creators);
      }

    }

    /**
     * A lightweight object used to manage the creation of a reader. Allows pre-initialization of data before reader
     * construction.
     */
    private class SplitReaderCreator implements Comparable<SplitReaderCreator>, AutoCloseable {
      private SplitAndPartitionInfo datasetSplit;
      private ParquetDatasetSplitScanXAttr splitXAttr;
      private InputStreamProvider inputStreamProvider;
      private Path path;
      private SplitReaderCreator next;

      public SplitReaderCreator(SplitAndPartitionInfo splitInfo) {
        this.datasetSplit = splitInfo;
        try {
          this.splitXAttr = LegacyProtobufSerializer.parseFrom(ParquetDatasetSplitScanXAttr.PARSER, datasetSplit.getDatasetSplitInfo().getExtendedProperty());
        } catch (InvalidProtocolBufferException e) {
          throw new RuntimeException("Could not deserialize parquet dataset split scan attributes", e);
        }
        this.path = Path.of(splitXAttr.getPath());

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

      public void setNext(SplitReaderCreator next) {
        this.next = next;
      }

      public Path getPath() {
        return path;
      }

      public ParquetMetadata getFooter() {
        return handleEx(() -> inputStreamProvider.getFooter());
      }

      private <T> T handleEx(RunnableIO<T> r) {
        try {
          return r.run();
        } catch (FileNotFoundException e) {
          throw UserException.invalidMetadataError(e)
            .addContext("Parquet file not found")
            .addContext("File", splitXAttr.getPath())
            .setAdditionalExceptionContext(new InvalidMetadataErrorContext(
              config.getTablePath()))
            .build(logger);
        } catch (IOException e) {
          throw UserException.dataReadError(e)
            .addContext("Failure opening parquet file")
            .addContext("File", splitXAttr.getPath())
            .build(logger);
        }
      }

      private void createInputStreamProvider(Path lastPath, ParquetMetadata lastFooter) {
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

          final ParquetMetadata validLastFooter = path.equals(lastPath) ? lastFooter : null;

          BiConsumer<Path, ParquetMetadata> depletionListener = (path, footer) -> {
            if(!prefetchReader || next == null) {
              return;
            }

            next.createInputStreamProvider(path, footer);
          };

          final boolean readFullFile = length < context.getOptions().getOption(ExecConstants.PARQUET_FULL_FILE_READ_THRESHOLD) &&
              ((float)config.getColumns().size()) / config.getFullSchema().getFieldCount() > context.getOptions().getOption(ExecConstants.PARQUET_FULL_FILE_READ_COLUMN_RATIO);

          final Collection<List<String>> referencedTables = config.getReferencedTables();
          final List<String> dataset = referencedTables == null || referencedTables.isEmpty() ? null : referencedTables.iterator().next();

          inputStreamProvider = factory.create(
              plugin,
              fs,
              context,
              path,
              length,
              datasetSplit.getPartitionInfo().getSize(),
              finder.getRealFields(),
              validLastFooter,
              splitXAttr.getRowGroupIndex(),
              depletionListener,
              readFullFile,
              dataset,
              mTime);
          return null;
        });
      }

      public RecordReader createRecordReader() {
        Preconditions.checkNotNull(inputStreamProvider);
        return handleEx(() -> {
          try {
            final ParquetMetadata footer = inputStreamProvider.getFooter();

            final SchemaDerivationHelper schemaHelper = SchemaDerivationHelper.builder()
                .readInt96AsTimeStamp(readInt96AsTimeStamp)
                .dateCorruptionStatus(ParquetReaderUtility.detectCorruptDates(footer, config.getColumns(), autoCorrectCorruptDates))
                .build();

            final UnifiedParquetReader inner = new UnifiedParquetReader(
              context,
              readerFactory,
              config.getFullSchema(),
              finder.getRealFields(),
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
            return readerConfig.wrapIfNecessary(context.getAllocator(), inner, datasetSplit);
          }finally {
            // minimize heap memory of this object.
            clearAllLocalFields();
          }
        });
      }

      @Override
      public int compareTo(SplitReaderCreator other) {
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
  }

}
