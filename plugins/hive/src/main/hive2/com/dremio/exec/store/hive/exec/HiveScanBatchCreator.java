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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.hive.serde2.OpenCSVSerde;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.pf4j.Extension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.SupportsPF4JStoragePlugin;
import com.dremio.exec.store.dfs.implicit.CompositeReaderConfig;
import com.dremio.exec.store.hive.HiveImpersonationUtil;
import com.dremio.exec.store.hive.HiveStoragePlugin;
import com.dremio.exec.store.hive.HiveUtilities;
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
  private static final Logger logger = LoggerFactory.getLogger(HiveScanBatchCreator.class);

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
    SupportsPF4JStoragePlugin pf4JStoragePlugin = fragmentExecContext.getStoragePlugin(config.getPluginId());
    final HiveStoragePlugin storagePlugin = pf4JStoragePlugin.getPF4JStoragePlugin();
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

    final boolean isPartitioned = config.getPartitionColumns() != null && config.getPartitionColumns().size() > 0;
    if (pf4JStoragePlugin.isAWSGlue()) {
      String tablePath = String.join(".", config.getProxiedSubScan().getTableSchemaPath());
      if (!allSplitsAreOnS3(config.getSplits())) {
        throw UserException.unsupportedError().message("AWS Glue table [%s] uses an unsupported storage system", tablePath).buildSilently();
      }

      if (!allSplitsAreSupported(tableXattr, config.getSplits(), isPartitioned)) {
        throw UserException.unsupportedError().message("AWS Glue table [%s] uses an unsupported file format", tablePath).buildSilently();
      }
    }

    final UserGroupInformation proxyUgi = getUGI(storagePlugin, config);

    final CompositeReaderConfig compositeConfig = CompositeReaderConfig.getCompound(context, config.getFullSchema(), config.getColumns(), config.getPartitionColumns());

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

  private boolean allSplitsAreSupported(HiveTableXattr tableXattr, List<SplitAndPartitionInfo> splits, boolean isPartitioned) {
    for (SplitAndPartitionInfo split : splits) {
      String serialiazationLib;
      if (isPartitioned) {
        final HiveReaderProto.PartitionXattr partitionXattr = HiveReaderProtoUtil.getPartitionXattr(split);
        serialiazationLib = HiveReaderProtoUtil.getPartitionSerializationLib(tableXattr, partitionXattr);
      } else {
        Optional<String> tableSerializationLib = HiveReaderProtoUtil.getTableSerializationLib(tableXattr);

        if (tableSerializationLib.isPresent()) {
          serialiazationLib = tableSerializationLib.get();
        } else {
          logger.error("Serialization lib property not found in table metadata.");
          return false;
        }
      }

      try {
        Class<? extends SerDe> aClass = (Class<? extends SerDe>) Class.forName(serialiazationLib);
        if (!OrcSerde.class.isAssignableFrom(aClass) &&
                !ParquetHiveSerDe.class.isAssignableFrom(aClass) &&
                !LazySimpleSerDe.class.isAssignableFrom(aClass) &&
                !OpenCSVSerde.class.isAssignableFrom(aClass)) {
          logger.error("Split [{}] is not of Parquet/Orc/CSV type. SerDe of split: {}", split, serialiazationLib);
          return false;
        }
      } catch (ClassNotFoundException e) {
        logger.error("SerDe class not found for {}", serialiazationLib);
        return false;
      }
    }

    return true;
  }

  boolean allSplitsAreOnS3(List<SplitAndPartitionInfo> splits) {
    for (SplitAndPartitionInfo split : splits) {
      try {
        final HiveReaderProto.HiveSplitXattr splitAttr = HiveReaderProto.HiveSplitXattr.parseFrom(split.getDatasetSplitInfo().getExtendedProperty());
        final FileSplit fullFileSplit = (FileSplit) HiveUtilities.deserializeInputSplit(splitAttr.getInputSplit());
        if (!AsyncReaderUtils.S3_FILE_SYSTEM.contains(fullFileSplit.getPath().toUri().getScheme().toLowerCase())) {
          logger.error("Data file {} is not on S3.", fullFileSplit.getPath());
          return false;
        }
      } catch (IOException | ReflectiveOperationException e) {
        throw new RuntimeException("Failed to parse dataset split for " + split.getPartitionInfo().getSplitKey(), e);
      }
    }
    return true;
  }

  @VisibleForTesting
  public UserGroupInformation getUGI(HiveStoragePlugin storagePlugin, HiveProxyingSubScan config) {
    final String userName = storagePlugin.getUsername(config.getProps().getUserName());
    return HiveImpersonationUtil.createProxyUgi(userName);
  }
}
