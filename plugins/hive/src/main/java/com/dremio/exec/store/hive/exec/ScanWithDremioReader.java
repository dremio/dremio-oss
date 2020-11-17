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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;

import com.dremio.common.util.Closeable;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.dfs.implicit.CompositeReaderConfig;
import com.dremio.exec.store.hive.BaseHiveStoragePlugin;
import com.dremio.exec.store.hive.HivePf4jPlugin;
import com.dremio.exec.store.parquet.RecordReaderIterator;
import com.dremio.hive.proto.HiveReaderProto.HiveTableXattr;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

/**
 * Helper class for {@link ScanWithDremioReader} to create a {@link ProducerOperator} that uses readers provided by
 * Dremio.
 */
class ScanWithDremioReader {

  static RecordReaderIterator createReaders(
      final HiveConf hiveConf,
      final BaseHiveStoragePlugin hiveStoragePlugin,
      final FragmentExecutionContext fragmentExecContext,
      final OperatorContext context,
      final HiveProxyingSubScan config,
      final HiveTableXattr tableXattr,
      final CompositeReaderConfig compositeReader,
      final UserGroupInformation readerUGI,
      List<SplitAndPartitionInfo> splits) {

    try (Closeable ccls = HivePf4jPlugin.swapClassLoader()) {

      if(splits.isEmpty()) {
        return RecordReaderIterator.from(Collections.emptyIterator());
      }

      final JobConf jobConf = new JobConf(hiveConf);

      final List<HiveParquetSplit> sortedSplits = Lists.newArrayList();
      for (SplitAndPartitionInfo split : splits) {
        sortedSplits.add(new HiveParquetSplit(split));
      }
      Collections.sort(sortedSplits);

      return new HiveParquetSplitReaderIterator(
              jobConf,
              context,
              config,
              sortedSplits,
              readerUGI,
              compositeReader,
              hiveStoragePlugin,
              tableXattr);
    } catch (final Exception e) {
      throw Throwables.propagate(e);
    }
  }

}
