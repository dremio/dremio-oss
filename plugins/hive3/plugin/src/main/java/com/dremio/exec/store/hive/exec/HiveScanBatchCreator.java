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
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.pf4j.Extension;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.dfs.implicit.CompositeReaderConfig;
import com.dremio.exec.store.hive.HiveImpersonationUtil;
import com.dremio.exec.store.hive.Hive3StoragePlugin;
import com.dremio.exec.store.hive.proxy.HiveProxiedScanBatchCreator;
import com.dremio.exec.store.parquet.RecordReaderIterator;
import com.dremio.exec.util.ConcatenatedCloseableIterator;
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
  public ProducerOperator create(FragmentExecutionContext fragmentExecContext, OperatorContext context, HiveProxyingSubScan config) throws ExecutionSetupException {
    final Hive3StoragePlugin storagePlugin = fragmentExecContext.getStoragePlugin(config.getPluginId());
    final HiveConf conf = storagePlugin.getHiveConf();

    final HiveTableXattr tableXattr;
    try {
      tableXattr = HiveTableXattr.parseFrom(config.getExtendedProperty());
    } catch (InvalidProtocolBufferException e) {
      throw new ExecutionSetupException("Failure parsing table extended properties.", e);
    }

    // handle unexpected reader type
    if (tableXattr.getReaderType() != HiveReaderProto.ReaderType.NATIVE_PARQUET &&
      tableXattr.getReaderType() != HiveReaderProto.ReaderType.BASIC) {
      throw new UnsupportedOperationException(tableXattr.getReaderType().name());
    }

    final UserGroupInformation proxyUgi = getUGI(storagePlugin, config);

    final CompositeReaderConfig compositeConfig = CompositeReaderConfig.getCompound(context, config.getFullSchema(), config.getColumns(), config.getPartitionColumns());

    final boolean isPartitioned = config.getPartitionColumns() != null && config.getPartitionColumns().size() > 0;
    List<SplitAndPartitionInfo> parquetSplits = new ArrayList<>();
    List<SplitAndPartitionInfo> nonParquetSplits = new ArrayList<>();
    classifySplitsAsParquetAndNonParquet(config.getSplits(), tableXattr, isPartitioned, parquetSplits, nonParquetSplits);

    RecordReaderIterator recordReaders = RecordReaderIterator.join(
        Objects.requireNonNull(ScanWithDremioReader.createReaders(conf, storagePlugin, fragmentExecContext, context,
            config, tableXattr, compositeConfig, proxyUgi, parquetSplits)),
        Objects.requireNonNull(ScanWithHiveReader.createReaders(conf, fragmentExecContext, context,
            config, tableXattr, compositeConfig, proxyUgi, nonParquetSplits)));

    return new ScanOperator(config, context, recordReaders);
  }

  @VisibleForTesting
  public UserGroupInformation getUGI(Hive3StoragePlugin storagePlugin, HiveProxyingSubScan config) {
    final String userName = storagePlugin.getUsername(config.getProps().getUserName());
    return HiveImpersonationUtil.createProxyUgi(userName);
  }
}
