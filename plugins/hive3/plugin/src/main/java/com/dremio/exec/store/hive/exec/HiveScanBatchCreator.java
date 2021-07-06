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

import static com.dremio.hive.proto.HiveReaderProto.ReaderType.NATIVE_PARQUET;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.pf4j.Extension;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.EmptyRecordReader;
import com.dremio.exec.store.ScanFilter;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.dfs.implicit.CompositeReaderConfig;
import com.dremio.exec.store.hive.HiveImpersonationUtil;
import com.dremio.exec.store.hive.Hive3StoragePlugin;
import com.dremio.exec.store.hive.proxy.HiveProxiedScanBatchCreator;
import com.dremio.exec.store.parquet.RecordReaderIterator;
import com.dremio.hive.proto.HiveReaderProto;
import com.dremio.hive.proto.HiveReaderProto.HiveTableXattr;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.sabot.op.spi.ProducerOperator;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.protobuf.InvalidProtocolBufferException;

@SuppressWarnings("unused")
@Extension
public class HiveScanBatchCreator implements HiveProxiedScanBatchCreator {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HiveScanBatchCreator.class);

  private final FragmentExecutionContext fragmentExecContext;
  private final OperatorContext context;
  private final Hive3StoragePlugin storagePlugin;
  private final HiveTableXattr tableXattr;
  private final HiveConf conf;
  private final CompositeReaderConfig compositeConfig;
  private final UserGroupInformation proxyUgi;
  private final BatchSchema fullSchema;
  private final ScanFilter scanFilter;
  private final Collection<List<String>> referencedTables;
  private final boolean isPartitioned;

  private HiveProxyingSubScan subScanConfig;
  private RecordReaderIterator recordReaderIterator;

  private HiveScanBatchCreator(FragmentExecutionContext fragmentExecContext, OperatorContext context, StoragePluginId storagePluginId,
                               byte[] extendedProperty, List<SchemaPath> columns, List<String> partitionColumns, OpProps opProps,
                               BatchSchema fullSchema, Collection<List<String>> referencedTables, ScanFilter scanFilter) throws ExecutionSetupException {
    this.fragmentExecContext = fragmentExecContext;
    this.context = context;


    this.storagePlugin = fragmentExecContext.getStoragePlugin(storagePluginId);
    this.conf = storagePlugin.getHiveConf();

    try {
      this.tableXattr = HiveTableXattr.parseFrom(extendedProperty);
    } catch (InvalidProtocolBufferException e) {
      throw new ExecutionSetupException("Failure parsing table extended properties.", e);
    }

    // handle unexpected reader type
    if (tableXattr.getReaderType() != HiveReaderProto.ReaderType.NATIVE_PARQUET &&
      tableXattr.getReaderType() != HiveReaderProto.ReaderType.BASIC) {
      throw new UnsupportedOperationException(tableXattr.getReaderType().name());
    }

    this.isPartitioned = partitionColumns != null && partitionColumns.size() > 0;

    this.proxyUgi = getUGI(storagePlugin, opProps);
    this.fullSchema = fullSchema;
    this.referencedTables = referencedTables;
    this.scanFilter = scanFilter;
    this.compositeConfig = CompositeReaderConfig.getCompound(context, fullSchema, columns, partitionColumns);
  }

  public HiveScanBatchCreator(FragmentExecutionContext fragmentExecContext, OperatorContext context,
                              HiveProxyingSubScan config) throws ExecutionSetupException {
    this(fragmentExecContext, context, config.getPluginId(), config.getExtendedProperty(), config.getColumns(),
      config.getPartitionColumns(), config.getProps(), config.getFullSchema(), config.getReferencedTables(),
      config.getFilter());
    this.subScanConfig = config;
  }


  public HiveScanBatchCreator(FragmentExecutionContext fragmentExecContext, OperatorContext context, OpProps opProps,
                              TableFunctionConfig tableFunctionConfig) throws ExecutionSetupException {
    this(fragmentExecContext, context, tableFunctionConfig.getFunctionContext().getPluginId(),
      tableFunctionConfig.getFunctionContext().getExtendedProperty().toByteArray(),
      tableFunctionConfig.getFunctionContext().getColumns(),
      tableFunctionConfig.getFunctionContext().getPartitionColumns(), opProps,
      tableFunctionConfig.getFunctionContext().getFullSchema(),
      tableFunctionConfig.getFunctionContext().getReferencedTables(),
      tableFunctionConfig.getFunctionContext().getScanFilter());
  }

  private boolean isParquetSplit(final HiveTableXattr tableXattr, SplitAndPartitionInfo split, boolean isPartitioned) {
    if (tableXattr.getReaderType() == NATIVE_PARQUET) {
      return true;
    }

    Optional<String> tableInputFormat;
    if (isPartitioned) {
      final HiveReaderProto.PartitionXattr partitionXattr = HiveReaderProtoUtil.getPartitionXattr(split);
      tableInputFormat = HiveReaderProtoUtil.getPartitionInputFormat(tableXattr, partitionXattr);
    } else {
      tableInputFormat = HiveReaderProtoUtil.getTableInputFormat(tableXattr);
    }

    if (!tableInputFormat.isPresent()) {
      return false;
    }

    try {
      return MapredParquetInputFormat.class.isAssignableFrom(
        (Class<? extends InputFormat>) Class.forName(tableInputFormat.get()));
    } catch (ClassNotFoundException e) {
      return false;
    }
  }

  private void classifySplitsAsParquetAndNonParquet(List<SplitAndPartitionInfo> allSplits,
                                                    final HiveTableXattr tableXattr,
                                                    boolean isPartitioned,
                                                    List<SplitAndPartitionInfo> parquetSplits,
                                                    List<SplitAndPartitionInfo> nonParquetSplits) {
    // separate splits into parquet splits and non parquet splits
    for (SplitAndPartitionInfo split : allSplits) {
      if (isParquetSplit(tableXattr, split, isPartitioned)) {
        parquetSplits.add(split);
      } else {
        nonParquetSplits.add(split);
      }
    }
  }

  @Override
  public ProducerOperator create() throws ExecutionSetupException {
    addSplits(subScanConfig.getSplits());
    return new ScanOperator(subScanConfig, context, recordReaderIterator);
  }


  @Override
  public RecordReaderIterator createRecordReaderIterator() {
    return RecordReaderIterator.from(new EmptyRecordReader());
  }

  @Override
  public void addSplits(List<SplitAndPartitionInfo> splits) {
    List<SplitAndPartitionInfo> parquetSplits = new ArrayList<>();
    List<SplitAndPartitionInfo> nonParquetSplits = new ArrayList<>();
    classifySplitsAsParquetAndNonParquet(splits, tableXattr, isPartitioned, parquetSplits, nonParquetSplits);

    if (!parquetSplits.isEmpty()) {
      context.getStats().setLongStat(ScanOperator.Metric.HIVE_FILE_FORMATS,
        context.getStats().getLongStat(ScanOperator.Metric.HIVE_FILE_FORMATS) | (1 << ScanOperator.HiveFileFormat.PARQUET.ordinal()));
    }

    // there is a bug here: the runtimefilters are not present in the new readeriterators
    // This will also get fixed after we refactor the code to reuse the logic in parquetsplitreadercreatoriterator
    // The prefetch across batch functionality will also get picked up then
    recordReaderIterator = RecordReaderIterator.join(
        Objects.requireNonNull(ScanWithDremioReader.createReaders(conf, storagePlugin, fragmentExecContext, context,
            tableXattr, compositeConfig, proxyUgi, scanFilter, fullSchema, referencedTables, parquetSplits)),
        Objects.requireNonNull(ScanWithHiveReader.createReaders(conf, fragmentExecContext, context,
            tableXattr, compositeConfig, proxyUgi, scanFilter, isPartitioned, referencedTables, nonParquetSplits)));
  }

  @VisibleForTesting
  public UserGroupInformation getUGI(Hive3StoragePlugin storagePlugin, OpProps props) {
    final String userName = storagePlugin.getUsername(props.getUserName());
    return HiveImpersonationUtil.createProxyUgi(userName);
  }
}
