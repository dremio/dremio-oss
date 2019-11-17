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
package com.dremio.exec.store.hive.exec;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.store.EmptyRecordReader;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.ScanFilter;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.dfs.implicit.CompositeReaderConfig;
import com.dremio.exec.store.hive.ContextClassLoaderSwapper;
import com.dremio.exec.store.hive.HiveUtilities;
import com.dremio.exec.store.parquet.ParquetFilterCondition;
import com.dremio.exec.store.parquet.ParquetReaderFactory;
import com.dremio.exec.store.parquet.ParquetScanFilter;
import com.dremio.exec.store.parquet.UnifiedParquetReader;
import com.dremio.hive.proto.HiveReaderProto.HiveSplitXattr;
import com.dremio.hive.proto.HiveReaderProto.HiveTableXattr;
import com.dremio.hive.proto.HiveReaderProto.Prop;
import com.dremio.options.OptionManager;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.sabot.op.spi.ProducerOperator;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

/**
 * Helper class for {@link ScanWithDremioReader} to create a {@link ProducerOperator} that uses readers provided by
 * Dremio.
 */
class ScanWithDremioReader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ScanWithDremioReader.class);

  static ProducerOperator createProducer(
      final HiveConf hiveConf,
      final FragmentExecutionContext fragmentExecContext,
      final OperatorContext context,
      final HiveProxyingSubScan config,
      final HiveTableXattr tableXattr,
      final CompositeReaderConfig compositeReader,
      final UserGroupInformation readerUGI) {

    Iterable<RecordReader> readers = null;

    try (ContextClassLoaderSwapper ccls = ContextClassLoaderSwapper.newInstance()) {
      final JobConf jobConf = new JobConf(hiveConf);

      final OptionManager options = context.getOptions();
      final boolean vectorize = options.getOption(ExecConstants.PARQUET_READER_VECTORIZE);
      final boolean enableDetailedTracing = options.getOption(ExecConstants.ENABLED_PARQUET_TRACING);
      final ParquetReaderFactory readerFactory = UnifiedParquetReader.getReaderFactory(context.getConfig());

      if(config.getSplits().isEmpty()) {
        return new ScanOperator(config, context, Iterators.singletonIterator(new EmptyRecordReader()));
      }

      final UserGroupInformation currentUGI = UserGroupInformation.getCurrentUser();
      final List<HiveParquetSplit> sortedSplits = Lists.newArrayList();

      for (SplitAndPartitionInfo split : config.getSplits()) {
        sortedSplits.add(new HiveParquetSplit(split));
      }
      Collections.sort(sortedSplits);

      final ScanFilter scanFilter = config.getFilter();
      final List<ParquetFilterCondition> conditions;
      if (scanFilter == null) {
        conditions = null;
      } else {
        conditions = ((ParquetScanFilter) scanFilter).getConditions();
      }

      readers = FluentIterable.from(sortedSplits).transform(new Function<HiveParquetSplit, RecordReader>(){

        @Override
        public RecordReader apply(final HiveParquetSplit hiveParquetSplit) {
          return currentUGI.doAs(new PrivilegedAction<RecordReader>() {
            @Override
            public RecordReader run() {

              final List<Prop> partitionProperties;
              // If Partition Properties are stored in DatasetMetadata (Pre 3.2.0)
              if (HiveReaderProtoUtil.isPreDremioVersion3dot2dot0LegacyFormat(tableXattr)) {
                logger.debug("Reading partition properties from DatasetMetadata");
                partitionProperties = HiveReaderProtoUtil.getPartitionProperties(tableXattr, hiveParquetSplit.getPartitionId());
              } else {
                logger.debug("Reading partition properties from PartitionChunk");
                partitionProperties = HiveReaderProtoUtil.getPartitionProperties(tableXattr,
                  HiveReaderProtoUtil.getPartitionXattr(hiveParquetSplit.getDatasetSplit()));
              }

              for (Prop prop: partitionProperties) {
                jobConf.set(prop.getKey(), prop.getValue());
              }

              final RecordReader innerReader = new FileSplitParquetRecordReader(
                  context,
                  readerFactory,
                  config.getFullSchema(),
                  compositeReader.getInnerColumns(),
                  conditions,
                  hiveParquetSplit.getFileSplit(),
                  jobConf,
                  config.getReferencedTables(),
                  vectorize,
                  config.getFullSchema(),
                  enableDetailedTracing,
                  readerUGI
              );
              return compositeReader.wrapIfNecessary(context.getAllocator(), innerReader, hiveParquetSplit.getDatasetSplit());
            }
          });

        }});

      return new ScanOperator(config, context, readers.iterator());
    } catch (final Exception e) {
      if(readers != null) {
        AutoCloseables.close(e, readers);
      }
      throw Throwables.propagate(e);
    }
  }

  private static class HiveParquetSplit implements Comparable {
    private final SplitAndPartitionInfo datasetSplit;
    private final FileSplit fileSplit;
    private final int partitionId;

    HiveParquetSplit(SplitAndPartitionInfo splitAndPartitionInfo) {
      this.datasetSplit = splitAndPartitionInfo;
      try {
        final HiveSplitXattr splitAttr = HiveSplitXattr.parseFrom(datasetSplit.getDatasetSplitInfo().getExtendedProperty());
        final FileSplit fullFileSplit = (FileSplit) HiveUtilities.deserializeInputSplit(splitAttr.getInputSplit());
        // make a copy of file split, we only need file path, start and length, throw away hosts
        this.fileSplit = new FileSplit(fullFileSplit.getPath(), fullFileSplit.getStart(), fullFileSplit.getLength(), (String[])null);
        this.partitionId = splitAttr.getPartitionId();
      } catch (IOException | ReflectiveOperationException e) {
        throw new RuntimeException("Failed to parse dataset split for " + datasetSplit.getPartitionInfo().getSplitKey(), e);
      }
    }

    public int getPartitionId() {
      return partitionId;
    }

    SplitAndPartitionInfo getDatasetSplit() {
      return datasetSplit;
    }

    FileSplit getFileSplit() {
      return fileSplit;
    }

    @Override
    public int compareTo(Object o) {
      final HiveParquetSplit other = (HiveParquetSplit) o;
      final int ret = fileSplit.getPath().compareTo(other.fileSplit.getPath());
      if (ret == 0) {
        return Long.compare(fileSplit.getStart(), other.getFileSplit().getStart());
      }
      return ret;
    }
  }
}
