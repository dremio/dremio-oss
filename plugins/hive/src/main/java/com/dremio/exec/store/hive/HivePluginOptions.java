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
package com.dremio.exec.store.hive;

import com.dremio.options.OptionValidator;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators.BooleanValidator;
import com.dremio.options.TypeValidators.RangeLongValidator;

/**
 * Dremio advanced configuration options for Hive storage plugin
 */
@Options
public interface HivePluginOptions {

  /**
   * Options to enable vectorized ORC reader and filter pushdown into vectorized ORC reader
   */
  BooleanValidator HIVE_ORC_READER_VECTORIZE = new BooleanValidator("store.hive.orc.vectorize", true);
  BooleanValidator ENABLE_FILTER_PUSHDOWN_HIVE_ORC =
      new BooleanValidator("store.hive.orc.vectorize.enable_filter_pushdown", true);

  /**
   * Use Dremio native parquet reader to read Hive parquet files.
   */
  String HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS = "store.hive.optimize_scan_with_native_readers";
  OptionValidator HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS_VALIDATOR =
      new BooleanValidator(HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS, true);

  /**
   * Option tells whether to use the stats in Hive metastore for table (and partitions in table) row count.
   * Default is false and we estimate the row count using the file size, record_size and type of file.
   * If analyze queries are run on tables in Hive, then this option can be enabled.
   */
  String HIVE_USE_STATS_IN_METASTORE_KEY = "store.hive.use_stats_in_metastore";
  BooleanValidator HIVE_USE_STATS_IN_METASTORE = new BooleanValidator(HIVE_USE_STATS_IN_METASTORE_KEY, false);

  /**
   * Partition batch size override, used mainly for testing.
   */
  String HIVE_PARTITION_BATCH_SIZE_KEY = "store.hive.partition_batch_size";
  RangeLongValidator HIVE_PARTITION_BATCH_SIZE_VALIDATOR =
    new RangeLongValidator(HIVE_PARTITION_BATCH_SIZE_KEY, 1, Integer.MAX_VALUE, 1000);

  /**
   * Maximum number of input splits per partition override, used mainly for testing.
   */
  String HIVE_MAX_INPUTSPLITS_PER_PARTITION_KEY = "store.hive.max_inputsplits_per_partition";
  RangeLongValidator HIVE_MAX_INPUTSPLITS_PER_PARTITION_VALIDATOR =
    new RangeLongValidator(HIVE_MAX_INPUTSPLITS_PER_PARTITION_KEY, 1, Integer.MAX_VALUE, 500);
}
