/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.InvalidMetadataErrorContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.datastore.LegacyProtobufSerializer;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.physical.visitor.GlobalDictionaryFieldInfo;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.FileSystemWrapper;
import com.dremio.exec.store.dfs.implicit.CompositeReaderConfig;
import com.dremio.exec.store.dfs.implicit.ImplicitFilesystemColumnFinder;
import com.dremio.parquet.reader.ParquetDirectByteBufferAllocator;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf.ParquetDatasetSplitScanXAttr;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.sabot.op.spi.ProducerOperator;
import com.dremio.sabot.op.spi.ProducerOperator.Creator;
import com.dremio.service.Pointer;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.SplitInfo;
import com.dremio.service.namespace.file.FileFormat;
import com.dremio.service.namespace.file.proto.ParquetFileConfig;
import com.google.common.base.Function;
import com.google.common.base.Stopwatch;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Parquet scan batch creator from dataset config
 */
public class ParquetOperatorCreator implements Creator<ParquetSubScan> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetOperatorCreator.class);

  @Override
  public ProducerOperator create(FragmentExecutionContext fragmentExecContext, final OperatorContext context, final ParquetSubScan config) throws ExecutionSetupException {
    final FileSystemPlugin plugin = fragmentExecContext.getStoragePlugin(config.getPluginId());
    final FileSystemWrapper fs = plugin.createFs(config.getProps().getUserName(), context);

    final Stopwatch watch = Stopwatch.createStarted();

    boolean isAccelerator = config.getPluginId().getName().equals(ACCELERATOR_STORAGEPLUGIN_NAME);

    final ParquetReaderFactory readerFactory = UnifiedParquetReader.getReaderFactory(context.getConfig());

    // TODO (AH )Fix implicit columns with mod time and global dictionaries
    final ImplicitFilesystemColumnFinder finder = new ImplicitFilesystemColumnFinder(context.getOptions(), fs, config.getColumns(), isAccelerator);
    // load global dictionaries, globalDictionaries must be closed by the last reader
    final GlobalDictionaries globalDictionaries = GlobalDictionaries.create(context, fs,  config.getGlobalDictionaryEncodedColumns());
    final boolean vectorize = context.getOptions().getOption(ExecConstants.PARQUET_READER_VECTORIZE);
    final boolean autoCorrectCorruptDates = context.getOptions().getOption(ExecConstants.PARQUET_AUTO_CORRECT_DATES_VALIDATOR) &&
      ((ParquetFileConfig) FileFormat.getForFile(config.getFormatSettings())).getAutoCorrectCorruptDates();
    final boolean readInt96AsTimeStamp = context.getOptions().getOption(ExecConstants
      .PARQUET_READER_INT96_AS_TIMESTAMP).getBoolVal();
    final boolean enableDetailedTracing = context.getOptions().getOption(ExecConstants.ENABLED_PARQUET_TRACING);
    final CodecFactory codec = CodecFactory.createDirectCodecFactory(fs.getConf(), new ParquetDirectByteBufferAllocator(context.getAllocator()), 0);

    final boolean supportsColocatedReads = plugin.supportsColocatedReads();
    final Map<String, GlobalDictionaryFieldInfo> globalDictionaryEncodedColumns = Maps.newHashMap();

    if (globalDictionaries != null) {
      for (GlobalDictionaryFieldInfo fieldInfo : config.getGlobalDictionaryEncodedColumns()) {
        globalDictionaryEncodedColumns.put(fieldInfo.getFieldName(), fieldInfo);
      }
    }

    final CompositeReaderConfig readerConfig = CompositeReaderConfig.getCompound(config.getFullSchema(), config.getColumns(), config.getPartitionColumns());
    final List<ParquetDatasetSplit> sortedSplits = Lists.newArrayList();

    // cache the previous footer so we don't retrieve multiple times.
    Pointer<Path> lastPath = new Pointer<>();
    Pointer<ParquetMetadata> lastFooter = new Pointer<>();

    for (SplitInfo split : config.getSplits()) {
      ParquetDatasetSplit parquetSplit = new ParquetDatasetSplit(split);
      if (!fs.isValidFS(parquetSplit.getSplitXAttr().getPath())) {
        throw UserException.invalidMetadataError()
          .addContext(String.format("%s: Invalid FS for file '%s'", fs.getScheme(), parquetSplit.getSplitXAttr().getPath()))
          .addContext("File", parquetSplit.getSplitXAttr().getPath())
          .setAdditionalExceptionContext(
            new InvalidMetadataErrorContext(
              ImmutableList.copyOf(config.getReferencedTables())))
          .build(logger);
      }

      sortedSplits.add(parquetSplit);
    }
    Collections.sort(sortedSplits);

    final InputStreamProviderFactory factory = context.getConfig()
      .getInstance("dremio.plugins.parquet.input_stream_factory", InputStreamProviderFactory.class, InputStreamProviderFactory.DEFAULT);

    FluentIterable < RecordReader > readers = FluentIterable.from(sortedSplits).transform(new Function<ParquetDatasetSplit, RecordReader>() {
      @Override
      public RecordReader apply(ParquetDatasetSplit split) {

        boolean useSingleStream =
          // option is set for single stream
          context.getOptions().getOption(ExecConstants.PARQUET_SINGLE_STREAM) ||
            // number of columns is above threshold
          finder.getRealFields().size() >= context.getOptions().getOption(ExecConstants.PARQUET_SINGLE_STREAM_COLUMN_THRESHOLD) ||
            // split size is below multi stream size limit and the limit is enabled
          (context.getOptions().getOption(ExecConstants.PARQUET_MULTI_STREAM_SIZE_LIMIT_ENABLE) &&
            split.getDatasetSplit().getSize() < context.getOptions().getOption(ExecConstants.PARQUET_MULTI_STREAM_SIZE_LIMIT));

        try {
          Path p = new Path(split.getSplitXAttr().getPath());
          long length;
          if (split.getSplitXAttr().hasFileLength() && context.getOptions().getOption(ExecConstants.PARQUET_CACHED_ENTITY_SET_FILE_SIZE)) {
            length = split.getSplitXAttr().getFileLength();
          } else {
            length = fs.getFileStatus(p).getLen();
          }

          boolean readFullFile = length < context.getOptions().getOption(ExecConstants.PARQUET_FULL_FILE_READ_THRESHOLD) &&
            ((float)config.getColumns().size()) / config.getFullSchema().getFieldCount() > context.getOptions().getOption(ExecConstants.PARQUET_FULL_FILE_READ_COLUMN_RATIO);

          final InputStreamProvider inputStreamProvider = factory
            .create(plugin, fs, context, p, length, split.getDatasetSplit().getSize(), readFullFile, finder.getRealFields(),
              p.equals(lastPath.value) ? lastFooter.value : null, split.getSplitXAttr().getRowGroupIndex());
          final ParquetMetadata footer = inputStreamProvider.getFooter();
          lastFooter.value = footer;
          lastPath.value = p;

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
            split.getSplitXAttr(),
            fs,
            footer,
            globalDictionaries,
            codec,
            schemaHelper,
            vectorize,
            enableDetailedTracing,
            supportsColocatedReads,
            inputStreamProvider
          );
          return readerConfig.wrapIfNecessary(context.getAllocator(), inner, split.getDatasetSplit());
        } catch (FileNotFoundException e) {
          throw UserException.invalidMetadataError(e)
            .addContext("Parquet file not found")
            .addContext("File", split.getSplitXAttr().getPath())
            .setAdditionalExceptionContext(new InvalidMetadataErrorContext(
              config.getTablePath()))
            .build(logger);
        } catch (IOException e) {
          throw UserException.dataReadError(e)
            .addContext("Failure opening parquet file")
            .addContext("File", split.getSplitXAttr().getPath())
            .build(logger);
        }
      }
    });

    final UserGroupInformation ugi = plugin.getUGIForUser(config.getProps().getUserName());

    final ScanOperator scan = new ScanOperator(config, context, readers.iterator(), globalDictionaries, ugi);
    logger.debug("Took {} ms to create Parquet Scan SqlOperatorImpl.", watch.elapsed(TimeUnit.MILLISECONDS));
    return scan;
  }

  private static class ParquetDatasetSplit implements Comparable<ParquetDatasetSplit> {
    private final SplitInfo datasetSplit;
    private final ParquetDatasetSplitScanXAttr splitXAttr;

    ParquetDatasetSplit(SplitInfo datasetSplit) {
      this.datasetSplit = datasetSplit;
      try {
        this.splitXAttr = LegacyProtobufSerializer.parseFrom(ParquetDatasetSplitScanXAttr.PARSER, datasetSplit.getSplitExtendedProperty());
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException("Could not deserialize parquet dataset split scan attributes", e);
      };
    }

    SplitInfo getDatasetSplit() {
      return datasetSplit;
    }

    ParquetDatasetSplitScanXAttr getSplitXAttr() {
      return splitXAttr;
    }

    @Override
    public int compareTo(ParquetDatasetSplit other) {
      return splitXAttr.getPath().compareTo(other.getSplitXAttr().getPath());
    }
  }
}
