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

import com.dremio.exec.planner.physical.Prel;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators;
import com.dremio.options.TypeValidators.BooleanValidator;
import com.dremio.options.TypeValidators.DoubleValidator;
import com.dremio.options.TypeValidators.RangeDoubleValidator;
import com.dremio.options.TypeValidators.RangeLongValidator;
import com.dremio.options.TypeValidators.RegexStringValidator;

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
   * Option tells whether to use the stats in Hive metastore for table (and partitions in table) row count.
   * Default is false and we estimate the row count using the file size, record_size and type of file.
   * If analyze queries are run on tables in Hive, then this option can be enabled.
   */
  String HIVE_USE_STATS_IN_METASTORE_KEY = "store.hive.use_stats_in_metastore";
  BooleanValidator HIVE_USE_STATS_IN_METASTORE = new BooleanValidator(HIVE_USE_STATS_IN_METASTORE_KEY, false);

  /**
   * Compression factor override for estimating the row count for hive parquet tables.
   */
  String HIVE_PARQUET_COMPRESSION_FACTOR_KEY = "store.hive.parquet_compression_factor";
  DoubleValidator HIVE_PARQUET_COMPRESSION_FACTOR_VALIDATOR =
    new RangeDoubleValidator(HIVE_PARQUET_COMPRESSION_FACTOR_KEY, 0.01d, 100.00d, 30.00d);

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

  /**
   * Options for tuning the number of bytes to reserve and limit in Hive Scans.
   */
  TypeValidators.LongValidator RESERVE = new TypeValidators.PositiveLongValidator("planner.op.scan.hive.reserve_bytes", Long.MAX_VALUE, Prel.DEFAULT_RESERVE);
  TypeValidators.LongValidator LIMIT = new TypeValidators.PositiveLongValidator("planner.op.scan.hive.limit_bytes", Long.MAX_VALUE, Prel.DEFAULT_LIMIT);

  /**
   * Option to use bytebuffers using direct memory while reading ORC files
   */
  BooleanValidator HIVE_ORC_READER_USE_DIRECT_MEMORY = new BooleanValidator("store.hive.orc.use_direct_memory", true);

  /**
   * Option to exclude Hive table or partition properties from loading into KV store.
   * If no exclusion is intended set to (?!)
   */
  RegexStringValidator HIVE_PROPERTY_EXCLUSION_REGEX = new RegexStringValidator("store.hive.property_exclusion_regex",
    "impala_intermediate" // impala_intermediate_stats_chunk is a 4k chunk of base64 encoded column stat for each partition
  );
}
