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

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.security.UserGroupInformation;

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
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterators;
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

  private ProducerOperator createNativeParquet(
      final HiveConf hiveConf,
      final FragmentExecutionContext fragmentExecContext,
      final OperatorContext context,
      final HiveSubScan config,
      final HiveTableXattr tableAttr,
      final CompositeReaderConfig compositeReader) throws ExecutionSetupException {
    final JobConf jobConf = new JobConf(hiveConf);

    final boolean isStarQuery = ColumnUtils.isStarQuery(config.getColumns());
    final boolean useNewReaderIfPossible = context.getOptions().getOption(ExecConstants.PARQUET_NEW_RECORD_READER).bool_val;
    final boolean vectorize = context.getOptions().getOption(ExecConstants.PARQUET_READER_VECTORIZE);
    final boolean enableDetailedTracing = context.getOptions().getOption(ExecConstants.ENABLED_PARQUET_TRACING);
    final ParquetReaderFactory readerFactory = UnifiedParquetReader.getReaderFactory(context.getConfig());

    if(config.getSplits().isEmpty()) {
      return new ScanOperator(fragmentExecContext.getSchemaUpdater(), config, context, Iterators.<RecordReader>singletonIterator(new EmptyRecordReader()));
    }

    Iterable<RecordReader> readers = null;
    try {
      final UserGroupInformation currentUGI = UserGroupInformation.getCurrentUser();
      final HiveSplitXattr firstAttr = HiveSplitXattr.parseFrom(config.getSplits().get(0).getExtendedProperty().toByteArray());
      final FileSplit firstSplit = (FileSplit) HiveAbstractReader.deserializeInputSplit(firstAttr.getInputSplit());
      final ParquetFooterCache footerCache = new ParquetFooterCache(FileSystemWrapper.get(firstSplit.getPath(), jobConf), 10, true);
      readers = FluentIterable.from(config.getSplits()).transform(new Function<DatasetSplit, RecordReader>(){


        @Override
        public RecordReader apply(final DatasetSplit split) {
          return currentUGI.doAs(new PrivilegedAction<RecordReader>() {
            @Override
            public RecordReader run() {
              try {
                final HiveSplitXattr splitAttr = HiveSplitXattr.parseFrom(split.getExtendedProperty().toByteArray());
                final FileSplit fileSplit = (FileSplit) HiveAbstractReader.deserializeInputSplit(splitAttr.getInputSplit());
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

                return compositeReader.wrapIfNecessary(context.getAllocator(), innerReader, split);
              } catch (IOException | ReflectiveOperationException e) {
                throw new RuntimeException("Failed to create RecordReaders. " + e.getMessage(), e);
              }
            }
          });

        }});

      return new ScanOperator(fragmentExecContext.getSchemaUpdater(), config, context, readers.iterator());

    } catch (final Exception e) {
      if(readers != null) {
        AutoCloseables.close(e, readers);
      }
      throw Throwables.propagate(e);
    }
  }

  private ProducerOperator createBasicReader(
      final HiveConf hiveConf,
      final FragmentExecutionContext fragmentExecContext,
      final OperatorContext context,
      final HiveSubScan config,
      final HiveTableXattr tableAttr,
      final CompositeReaderConfig compositeReader){

    final String formatName = tableAttr.getInputFormat();

    Class<? extends HiveAbstractReader> readerClass = HiveDefaultReader.class;
    if (readerMap.containsKey(formatName)) {
      readerClass = readerMap.get(formatName);
    }

    final Class<? extends HiveAbstractReader> readerClassF = readerClass;

    if(config.getSplits().isEmpty()) {
      return new ScanOperator(fragmentExecContext.getSchemaUpdater(), config, context, Iterators.<RecordReader>singletonIterator(new EmptyRecordReader()));
    }

    Iterable<RecordReader> readers = null;

    try {
      final UserGroupInformation currentUGI = UserGroupInformation.getCurrentUser();
      final Constructor<? extends HiveAbstractReader> readerConstructor = readerClassF.getConstructor(HiveTableXattr.class, DatasetSplit.class, List.class, List.class, OperatorContext.class, HiveConf.class);

       readers = FluentIterable.from(config.getSplits()).transform(new Function<DatasetSplit, RecordReader>(){

        @Override
        public RecordReader apply(final DatasetSplit split) {
          return currentUGI.doAs(new PrivilegedAction<RecordReader>() {
            @Override
            public RecordReader run() {
              try {
                RecordReader innerReader = readerConstructor.newInstance(tableAttr, split, compositeReader.getInnerColumns(), config.getPartitionColumns(), context, hiveConf);
                return compositeReader.wrapIfNecessary(context.getAllocator(), innerReader, split);
              } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                throw new RuntimeException(e);
              }
            }
          });
        }});
      return new ScanOperator(fragmentExecContext.getSchemaUpdater(), config, context, readers.iterator());
    } catch (NoSuchMethodException | SecurityException | IOException e) {
      if(readers != null) {
        AutoCloseables.close(e, readers);
      }
      throw Throwables.propagate(e);
    }
  }

  @Override
  public ProducerOperator create(FragmentExecutionContext fragmentExecContext, OperatorContext context, HiveSubScan config) throws ExecutionSetupException {
    try{
      HiveStoragePlugin2 storagePlugin = (HiveStoragePlugin2) fragmentExecContext.getStoragePlugin(config.getPluginId());
      HiveConf conf = storagePlugin.getHiveConf();
      final HiveTableXattr tableAttr = HiveTableXattr.parseFrom(config.getExtendedProperty().toByteArray());
      final CompositeReaderConfig compositeConfig = CompositeReaderConfig.getCompound(config.getSchema(), config.getColumns(), config.getPartitionColumns());

      switch(tableAttr.getReaderType()){
      case NATIVE_PARQUET:
        return createNativeParquet(conf, fragmentExecContext, context, config, tableAttr, compositeConfig);
      case BASIC:
        return createBasicReader(conf, fragmentExecContext, context, config, tableAttr, compositeConfig);
      default:
        throw new UnsupportedOperationException(tableAttr.getReaderType().name());
      }
    } catch (InvalidProtocolBufferException e) {
      throw new ExecutionSetupException("Failure parsing table extended properties.", e);
    }

  }
}
