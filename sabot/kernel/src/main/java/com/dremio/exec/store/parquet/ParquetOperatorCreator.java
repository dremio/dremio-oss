/*
 * Copyright (C) 2017 Dremio Corporation
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

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.CodecFactory;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.physical.visitor.GlobalDictionaryFieldInfo;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.FileSystemStoragePlugin2;
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
import com.google.common.collect.Maps;

/**
 * Parquet scan batch creator from dataset config
 */
public class ParquetOperatorCreator implements Creator<ParquetSubScan> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetOperatorCreator.class);

  public static final String ENABLE_BYTES_READ_COUNTER = "parquet.benchmark.bytes.read";
  public static final String ENABLE_BYTES_TOTAL_COUNTER = "parquet.benchmark.bytes.total";
  public static final String ENABLE_TIME_READ_COUNTER = "parquet.benchmark.time.read";

  @Override
  public ProducerOperator create(FragmentExecutionContext fragmentExecContext, final OperatorContext context, final ParquetSubScan config) throws ExecutionSetupException {
    final FileSystemStoragePlugin2 registry = (FileSystemStoragePlugin2) fragmentExecContext.getStoragePlugin(config.getPluginId());
    final FileSystemPlugin fsPlugin = registry.getFsPlugin();

    final FileSystemWrapper fs = registry.getFs();

    final Configuration conf = fsPlugin.getFsConf();
    conf.setBoolean(ENABLE_BYTES_READ_COUNTER, false);
    conf.setBoolean(ENABLE_BYTES_TOTAL_COUNTER, false);
    conf.setBoolean(ENABLE_TIME_READ_COUNTER, false);

    final Stopwatch watch = Stopwatch.createStarted();

    boolean isAccelerator = config.getPluginId().getName().equals("__accelerator");

    final ParquetReaderFactory readerFactory = UnifiedParquetReader.getReaderFactory(context.getConfig());

    // TODO (AH )Fix implicit columns with mod time and global dictionaries
    final ImplicitFilesystemColumnFinder finder = new ImplicitFilesystemColumnFinder(context.getOptions(), fs, config.getColumns(), isAccelerator);
    // load global dictionaries, globalDictionaries must be closed by the last reader
    final GlobalDictionaries globalDictionaries = GlobalDictionaries.create(context, fs,  config.getGlobalDictionaryEncodedColumns());
    final boolean vectorize = context.getOptions().getOption(ExecConstants.PARQUET_READER_VECTORIZE);
    final boolean autoCorrectCorruptDates = ((ParquetFileConfig)FileFormat.getForFile(config.getFormatSettings())).getAutoCorrectCorruptDates();
    final boolean readInt96AsTimeStamp = context.getOptions().getOption(ExecConstants.PARQUET_READER_INT96_AS_TIMESTAMP).bool_val;
    final boolean enableDetailedTracing = context.getOptions().getOption(ExecConstants.ENABLED_PARQUET_TRACING);
    final CodecFactory codec = CodecFactory.createDirectCodecFactory(fs.getConf(), new ParquetDirectByteBufferAllocator(context.getAllocator()), 0);

    final Map<String, GlobalDictionaryFieldInfo> globalDictionaryEncodedColumns = Maps.newHashMap();

    if (globalDictionaries != null) {
      for (GlobalDictionaryFieldInfo fieldInfo : config.getGlobalDictionaryEncodedColumns()) {
        globalDictionaryEncodedColumns.put(fieldInfo.getFieldName(), fieldInfo);
      }
    }

    final CompositeReaderConfig readerConfig = CompositeReaderConfig.getCompound(config.getSchema(), config.getColumns(), config.getPartitionColumns());

    FluentIterable<RecordReader> readers = FluentIterable.from(config.getSplits()).transform(new Function<DatasetSplit, RecordReader>(){

      @Override
      public RecordReader apply(DatasetSplit split) {
        ParquetDatasetSplitXAttr datasetSplitXAttr = ParquetDatasetXAttrSerDe.PARQUET_DATASET_SPLIT_XATTR_SERIALIZER.revert(split.getExtendedProperty().toByteArray());
        UnifiedParquetReader inner = new UnifiedParquetReader(
          context,
          readerFactory,
          finder.getRealFields(),
          config.getColumns(),
          globalDictionaryEncodedColumns,
          config.getConditions(),
          fsPlugin.getFooterCache(),
          datasetSplitXAttr,
          fs,
          globalDictionaries,
          codec,
          autoCorrectCorruptDates,
          readInt96AsTimeStamp,
          vectorize,
          enableDetailedTracing
        );
        return readerConfig.wrapIfNecessary(context.getAllocator(), inner, split);
      }});

    final ScanOperator scan = new ScanOperator(fragmentExecContext.getSchemaUpdater(), config, context, readers.iterator(), globalDictionaries);
    logger.debug("Took {} ms to create Parquet Scan SqlOperatorImpl.", watch.elapsed(TimeUnit.MILLISECONDS));
    return scan;
  }
}
