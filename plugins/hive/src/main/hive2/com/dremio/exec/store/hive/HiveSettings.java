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
package com.dremio.exec.store.hive;

import com.dremio.options.OptionManager;
import com.dremio.options.TypeValidators;

/**
 * Accessor for Hive2 plugin options.
 */
public final class HiveSettings {

  /**
   * Options for tuning the number of bytes to reserve and limit in Hive Scans.
   */
  public static final TypeValidators.LongValidator RESERVE = HivePluginOptions.RESERVE;
  public static final TypeValidators.LongValidator LIMIT = HivePluginOptions.LIMIT;

  private final OptionManager options;

  public HiveSettings(OptionManager options) {
    this.options = options;
  }
  /**
   * Options to enable vectorized ORC reader and filter pushdown into vectorized ORC reader
   */
  public boolean vectorizeOrcReaders() {
    return options.getOption(HivePluginOptions.HIVE_ORC_READER_VECTORIZE);
  }

  public boolean enableOrcFilterPushdown() {
    return options.getOption(HivePluginOptions.ENABLE_FILTER_PUSHDOWN_HIVE_ORC);
  }

  /**
   * Option tells whether to use the stats in Hive metastore for table (and partitions in table) row count.
   * Default is false and we estimate the row count using the file size, record_size and type of file.
   * If analyze queries are run on tables in Hive, then this option can be enabled.
   */
  public boolean useStatsInMetastore() {
    return options.getOption(HivePluginOptions.HIVE_USE_STATS_IN_METASTORE);
  }

  /**
   * Partition batch size override, used mainly for testing.
   */
  public long getPartitionBatchSize() {
    return options.getOption(HivePluginOptions.HIVE_PARTITION_BATCH_SIZE_VALIDATOR);
  }

  /**
   * Maximum number of input splits per partition override, used mainly for testing.
   */
  public long getMaxInputSplitsPerPartition() {
    return options.getOption(HivePluginOptions.HIVE_MAX_INPUTSPLITS_PER_PARTITION_VALIDATOR);
  }

  /**
   * Option to use bytebuffers using direct memory while reading ORC files;
   */
  public boolean useDirectMemoryForOrcReaders() {
    return options.getOption(HivePluginOptions.HIVE_ORC_READER_USE_DIRECT_MEMORY);
  }
}
