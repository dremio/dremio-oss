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

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
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
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.sabot.op.spi.ProducerOperator;
import com.dremio.sabot.op.spi.ProducerOperator.Creator;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;
import com.dremio.service.namespace.file.FileFormat;
import com.dremio.service.namespace.file.proto.ParquetDatasetSplitXAttr;
import com.dremio.service.namespace.file.proto.ParquetFileConfig;
import com.google.common.base.Function;
import com.google.common.base.Stopwatch;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Parquet scan batch creator from dataset config
 */
public class ParquetOperatorCreator implements Creator<ParquetSubScan> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetOperatorCreator.class);

  @Override
  public ProducerOperator create(FragmentExecutionContext fragmentExecContext, final OperatorContext context, final ParquetSubScan config) throws ExecutionSetupException {
    final FileSystemPlugin plugin = fragmentExecContext.getStoragePlugin(config.getPluginId());
    final FileSystemWrapper fs = plugin.getFs(config.getUserName(), context.getStats());

    final Stopwatch watch = Stopwatch.createStarted();

    boolean isAccelerator = config.getPluginId().getName().equals("__accelerator");

    final ParquetReaderFactory readerFactory = UnifiedParquetReader.getReaderFactory(context.getConfig());

    // TODO (AH )Fix implicit columns with mod time and global dictionaries
    final ImplicitFilesystemColumnFinder finder = new ImplicitFilesystemColumnFinder(context.getOptions(), fs, config.getColumns(), isAccelerator);
    // load global dictionaries, globalDictionaries must be closed by the last reader
    final GlobalDictionaries globalDictionaries = GlobalDictionaries.create(context, fs,  config.getGlobalDictionaryEncodedColumns());
    final boolean vectorize = context.getOptions().getOption(ExecConstants.PARQUET_READER_VECTORIZE);
    final boolean autoCorrectCorruptDates = ((ParquetFileConfig)FileFormat.getForFile(config.getFormatSettings())).getAutoCorrectCorruptDates();
    final boolean readInt96AsTimeStamp = context.getOptions().getOption(ExecConstants
      .PARQUET_READER_INT96_AS_TIMESTAMP).getBoolVal();
    final boolean enableDetailedTracing = context.getOptions().getOption(ExecConstants.ENABLED_PARQUET_TRACING);
    final CodecFactory codec = CodecFactory.createDirectCodecFactory(fs.getConf(), new ParquetDirectByteBufferAllocator(context.getAllocator()), 0);

    final Map<String, GlobalDictionaryFieldInfo> globalDictionaryEncodedColumns = Maps.newHashMap();

    if (globalDictionaries != null) {
      for (GlobalDictionaryFieldInfo fieldInfo : config.getGlobalDictionaryEncodedColumns()) {
        globalDictionaryEncodedColumns.put(fieldInfo.getFieldName(), fieldInfo);
      }
    }

    final CompositeReaderConfig readerConfig = CompositeReaderConfig.getCompound(config.getSchema(), config.getColumns(), config.getPartitionColumns());
    final List<ParquetDatasetSplit> sortedSplits = Lists.newArrayList();
    final SingletonParquetFooterCache footerCache = new SingletonParquetFooterCache();

    for (DatasetSplit split : config.getSplits()) {
      sortedSplits.add(new ParquetDatasetSplit(split));
    }
    Collections.sort(sortedSplits);

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
          Long length = split.getSplitXAttr().getUpdateKey().getLength();
          if (length == null || !context.getOptions().getOption(ExecConstants.PARQUET_CACHED_ENTITY_SET_FILE_SIZE)) {
            length = fs.getFileStatus(p).getLen();
          }
          InputStreamProvider inputStreamProvider = new InputStreamProvider(fs, p, useSingleStream);

          final ParquetMetadata footer = footerCache.getFooter(inputStreamProvider.stream(), split.getSplitXAttr().getPath(), length, fs);

          final SchemaDerivationHelper schemaHelper = SchemaDerivationHelper.builder()
              .readInt96AsTimeStamp(readInt96AsTimeStamp)
              .dateCorruptionStatus(ParquetReaderUtility.detectCorruptDates(footer, config.getColumns(), autoCorrectCorruptDates))
              .build();

          final UnifiedParquetReader inner = new UnifiedParquetReader(
            context,
            readerFactory,
            config.getSchema(),
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
            inputStreamProvider
          );
          return readerConfig.wrapIfNecessary(context.getAllocator(), inner, split.getDatasetSplit());
        } catch (IOException e) {
          throw UserException.dataReadError(e).addContext("Failure opening parquet file").addContext("File", split.getSplitXAttr().getPath()).build(logger);
        }

      }
    });

    final ScanOperator scan = new ScanOperator(fragmentExecContext.getSchemaUpdater(), config, context, readers.iterator(), globalDictionaries);
    logger.debug("Took {} ms to create Parquet Scan SqlOperatorImpl.", watch.elapsed(TimeUnit.MILLISECONDS));
    return scan;
  }

  private static class ParquetDatasetSplit implements Comparable {
    private final DatasetSplit datasetSplit;
    private final ParquetDatasetSplitXAttr splitXAttr;

    ParquetDatasetSplit(DatasetSplit datasetSplit) {
      this.datasetSplit = datasetSplit;
      this.splitXAttr = ParquetDatasetXAttrSerDe.PARQUET_DATASET_SPLIT_XATTR_SERIALIZER.revert(datasetSplit.getExtendedProperty().toByteArray());;
    }

    DatasetSplit getDatasetSplit() {
      return datasetSplit;
    }

    ParquetDatasetSplitXAttr getSplitXAttr() {
      return splitXAttr;
    }

    @Override
    public int compareTo(Object o) {
      final ParquetDatasetSplit other = (ParquetDatasetSplit) o;
      return splitXAttr.getPath().compareTo(other.getSplitXAttr().getPath());
    }
  }
}
