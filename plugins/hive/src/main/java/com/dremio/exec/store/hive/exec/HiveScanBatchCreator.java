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
package com.dremio.exec.store.hive.exec;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.hive.ql.io.parquet.ProjectionPusher;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.store.EmptyRecordReader;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.dfs.FileSystemWrapper;
import com.dremio.exec.store.dfs.implicit.CompositeReaderConfig;
import com.dremio.exec.store.hive.HiveStoragePlugin2;
import com.dremio.exec.store.parquet.ParquetFooterCache;
import com.dremio.exec.store.parquet.ParquetReaderFactory;
import com.dremio.exec.store.parquet.UnifiedParquetReader;
import com.dremio.exec.util.ColumnUtils;
import com.dremio.hive.proto.HiveReaderProto.HiveSplitXattr;
import com.dremio.hive.proto.HiveReaderProto.HiveTableXattr;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.sabot.op.spi.ProducerOperator;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;

@SuppressWarnings("unused")
public class HiveScanBatchCreator implements ProducerOperator.Creator<HiveSubScan> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HiveScanBatchCreator.class);

  /**
   * Use different classes for different Hive native formats:
   * ORC, AVRO, RCFFile, Text and Parquet.
   * If input format is none of them falls to default reader.
   */
  static Map<String, Class<? extends HiveAbstractReader>> readerMap = new HashMap<>();
  static {
    readerMap.put(OrcInputFormat.class.getCanonicalName(), HiveOrcReader.class);
    readerMap.put(AvroContainerInputFormat.class.getCanonicalName(), HiveAvroReader.class);
    readerMap.put(RCFileInputFormat.class.getCanonicalName(), HiveRCFileReader.class);
    readerMap.put(MapredParquetInputFormat.class.getCanonicalName(), HiveParquetReader.class);
    readerMap.put(TextInputFormat.class.getCanonicalName(), HiveTextReader.class);
  }

  private ProducerOperator createNativeParquet(HiveConf hiveConf, FragmentExecutionContext fragmentExecContext, OperatorContext context, HiveSubScan config, HiveTableXattr tableAttr, CompositeReaderConfig compositeReader) throws ExecutionSetupException{
    final JobConf jobConf = new JobConf(hiveConf);

    final boolean isStarQuery = ColumnUtils.isStarQuery(config.getColumns());
    final boolean useNewReaderIfPossible = context.getOptions().getOption(ExecConstants.PARQUET_NEW_RECORD_READER).bool_val;
    final boolean vectorize = context.getOptions().getOption(ExecConstants.PARQUET_READER_VECTORIZE);
    final boolean enableDetailedTracing = context.getOptions().getOption(ExecConstants.ENABLED_PARQUET_TRACING);
    final ParquetReaderFactory readerFactory = UnifiedParquetReader.getReaderFactory(context.getConfig());

    ParquetFooterCache footerCache = null;

    final List<RecordReader> readers = Lists.newArrayList();
    try {
      // filter out the partitions columns from selected column list
      for (DatasetSplit split : config.getSplits()) {
        final HiveSplitXattr splitAttr = HiveSplitXattr.parseFrom(split.getExtendedProperty().toByteArray());
        final FileSplit fileSplit = (FileSplit) HiveAbstractReader.deserializeInputSplit(splitAttr.getInputSplit());
        if (footerCache == null) {
          footerCache = new ParquetFooterCache(FileSystemWrapper.get(fileSplit.getPath(), jobConf), 10, true);
        }
        final Path finalPath = fileSplit.getPath();



        final RecordReader innerReader = new FileSplitParquetRecordReader(
            context,
            readerFactory,
            compositeReader.getInnerColumns(),
            config.getColumns(),
            config.getConditions(),
            fileSplit,
            footerCache,
            jobConf,
            vectorize,
            enableDetailedTracing
        );

        readers.add(compositeReader.wrapIfNecessary(context.getAllocator(), innerReader, split));
      }
    } catch (final Exception e) {
      AutoCloseables.close(e, readers);
      throw new ExecutionSetupException("Failed to create RecordReaders. " + e.getMessage(), e);
    }

    // If there are no readers created (which is possible when the table is empty or no row groups are matched),
    // create an empty RecordReader to output the schema
    if (readers.size() == 0) {
      readers.add(new EmptyRecordReader());
    }

    return new ScanOperator(fragmentExecContext.getSchemaUpdater(), config, context, readers.iterator());
  }

  private ProducerOperator createBasicReader(HiveConf hiveConf, FragmentExecutionContext fragmentExecContext, OperatorContext context, HiveSubScan config, HiveTableXattr tableAttr, CompositeReaderConfig compositeReader){

    final String formatName = tableAttr.getInputFormat();

    Class<? extends HiveAbstractReader> readerClass = HiveDefaultReader.class;
    if (readerMap.containsKey(formatName)) {
      readerClass = readerMap.get(formatName);
    }

    List<RecordReader> readers = new ArrayList<>();

    Constructor<? extends HiveAbstractReader> readerConstructor = null;
    try {
      readerConstructor = readerClass.getConstructor(ReadDefinition.class, DatasetSplit.class, List.class, OperatorContext.class, HiveConf.class);
      for (DatasetSplit split : config.getSplits()) {
        final RecordReader innerReader = readerConstructor.newInstance(config.getReadDefinition(), split, compositeReader.getInnerColumns(), context, hiveConf);
        readers.add(compositeReader.wrapIfNecessary(context.getAllocator(), innerReader, split));
      }

      // If there are no readers created (which is possible when the table is empty), create an empty RecordReader to
      // output the schema
      if (readers.size() == 0) {
        readers.add(new EmptyRecordReader());
      }
    } catch(ReflectiveOperationException e) {
      throw new IllegalStateException("Failure while creating Hive reader.", e);
    }

    return new ScanOperator(fragmentExecContext.getSchemaUpdater(), config, context, readers.iterator());
  }

  @Override
  public ProducerOperator create(FragmentExecutionContext fragmentExecContext, OperatorContext context, HiveSubScan config) throws ExecutionSetupException {
    try{
      HiveStoragePlugin2 storagePlugin = (HiveStoragePlugin2) fragmentExecContext.getStoragePlugin(config.getPluginId());
      HiveConf conf = storagePlugin.getHiveConf();
      final HiveTableXattr tableAttr = HiveTableXattr.parseFrom(config.getReadDefinition().getExtendedProperty().toByteArray());
      final CompositeReaderConfig compositeConfig = CompositeReaderConfig.getCompound(config.getSchema(), config.getColumns(), config.getReadDefinition().getPartitionColumnsList());

      switch(tableAttr.getReaderType()){
      case NATIVE_PARQUET:
        return createNativeParquet(conf, fragmentExecContext, context, config, tableAttr, compositeConfig);
      case BASIC:
        return createBasicReader(conf, fragmentExecContext, context, config, tableAttr, compositeConfig);
      }
    } catch (InvalidProtocolBufferException e) {
      throw new ExecutionSetupException("Failure parsing table extended properties.");
    }
    throw new UnsupportedOperationException();


  }
}
