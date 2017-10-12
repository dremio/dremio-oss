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
package com.dremio.exec;

import com.dremio.exec.proto.CoordExecRPC.FragmentCodec;
import com.dremio.exec.server.options.OptionValidator;
import com.dremio.exec.server.options.Options;
import com.dremio.exec.server.options.TypeValidators.BooleanValidator;
import com.dremio.exec.server.options.TypeValidators.DoubleValidator;
import com.dremio.exec.server.options.TypeValidators.EnumValidator;
import com.dremio.exec.server.options.TypeValidators.EnumeratedStringValidator;
import com.dremio.exec.server.options.TypeValidators.LongValidator;
import com.dremio.exec.server.options.TypeValidators.PositiveLongValidator;
import com.dremio.exec.server.options.TypeValidators.PowerOfTwoLongValidator;
import com.dremio.exec.server.options.TypeValidators.RangeDoubleValidator;
import com.dremio.exec.server.options.TypeValidators.RangeLongValidator;
import com.dremio.exec.server.options.TypeValidators.StringValidator;
import com.dremio.exec.testing.ExecutionControls;
import com.dremio.sabot.op.common.hashtable.HashTable;

@Options
public interface ExecConstants {
  String ZK_CONNECTION = "dremio.exec.zk.connect";
  String ZK_TIMEOUT = "dremio.exec.zk.timeout";
  String ZK_SESSION_TIMEOUT = "dremio.exec.zk.session.timeout";
  String ZK_ROOT = "dremio.exec.zk.root";
  String ZK_REFRESH = "dremio.exec.zk.refresh";
  String BIT_RETRY_TIMES = "dremio.exec.rpc.bit.server.retry.count";
  String BIT_RETRY_DELAY = "dremio.exec.rpc.bit.server.retry.delay";
  String BIT_RPC_TIMEOUT = "dremio.exec.rpc.bit.timeout";
  String INITIAL_USER_PORT = "dremio.exec.rpc.user.server.port";
  String USER_RPC_TIMEOUT = "dremio.exec.rpc.user.timeout";
  String CLIENT_RPC_THREADS = "dremio.exec.rpc.user.client.threads";
  String BIT_SERVER_RPC_THREADS = "dremio.exec.rpc.bit.server.threads";
  String USER_SERVER_RPC_THREADS = "dremio.exec.rpc.user.server.threads";
  String REGISTRATION_ADDRESS = "dremio.exec.rpc.publishedhost";

  /** incoming buffer size (number of batches) */
  String INCOMING_BUFFER_SIZE = "dremio.exec.buffer.size";
  String SPOOLING_BUFFER_DELETE = "dremio.exec.buffer.spooling.delete";
  String SPOOLING_BUFFER_SIZE = "dremio.exec.buffer.spooling.size";
  String BATCH_PURGE_THRESHOLD = "dremio.exec.sort.purge.threshold";
  String SPILL_DIRS = "dremio.exec.sort.external.spill.directories";
  String HTTP_ENABLE = "dremio.exec.http.enabled";

  /** Spill disk space configurations */
  PositiveLongValidator SPILL_DISK_SPACE_CHECK_INTERVAL = new PositiveLongValidator("dremio.exec.spill.check.interval", Integer.MAX_VALUE, 60*1000);
  PositiveLongValidator SPILL_DISK_SPACE_CHECK_SPILLS = new PositiveLongValidator("dremio.exec.spill.check.spills", Integer.MAX_VALUE, 1);
  PositiveLongValidator SPILL_DISK_SPACE_LIMIT_BYTES = new PositiveLongValidator("dremio.exec.spill.limit.bytes", Integer.MAX_VALUE, 1024*1024*1024);
  DoubleValidator SPILL_DISK_SPACE_LIMIT_PERCENTAGE = new RangeDoubleValidator("dremio.exec.spill.limit.percentage", 0.0, 100.0, 1.0);

  /** Size of JDBC batch queue (in batches) above which throttling begins. */
  String JDBC_BATCH_QUEUE_THROTTLING_THRESHOLD = "dremio.jdbc.batch_queue_throttling_threshold";

  String JDBC_ROW_COUNT_QUERY_TIMEOUT = "store.jdbc.row_count_query_timeout_seconds";
  LongValidator JDBC_ROW_COUNT_QUERY_TIMEOUT_VALIDATOR = new PositiveLongValidator(JDBC_ROW_COUNT_QUERY_TIMEOUT, Integer.MAX_VALUE, 5);


  /**
   * Currently if a query is cancelled, but one of the fragments reports the status as FAILED instead of CANCELLED or
   * FINISHED we report the query result as CANCELLED by swallowing the failures occurred in fragments. This BOOT
   * setting allows the user to see the query status as failure. Useful for developers/testers.
   */
  String RETURN_ERROR_FOR_FAILURE_IN_CANCELLED_FRAGMENTS =
      "dremio.exec.debug.return_error_for_failure_in_cancelled_fragments";

  PositiveLongValidator TARGET_BATCH_RECORDS_MIN = new PositiveLongValidator("exec.batch.records.min", Character.MAX_VALUE, 127);
  PositiveLongValidator TARGET_BATCH_RECORDS_MAX = new PositiveLongValidator("exec.batch.records.max", Character.MAX_VALUE, 4095);
  PositiveLongValidator TARGET_BATCH_SIZE_BYTES = new PositiveLongValidator("exec.batch.size-bytes", Integer.MAX_VALUE, 1024*1024);
  PositiveLongValidator BATCH_LIST_SIZE_ESTIMATE = new PositiveLongValidator("exec.batch.field.list.size-estimate", Integer.MAX_VALUE, 5);
  PositiveLongValidator BATCH_VARIABLE_FIELD_SIZE_ESTIMATE =
      new PositiveLongValidator("exec.batch.field.variable-width.size-estimate", Integer.MAX_VALUE, 15);

  String OPERATOR_TARGET_BATCH_BYTES = "dremio.exec.operator_batch_bytes";
  OptionValidator OPERATOR_TARGET_BATCH_BYTES_VALIDATOR = new LongValidator(OPERATOR_TARGET_BATCH_BYTES, 10*1024*1024);

  String CLIENT_SUPPORT_COMPLEX_TYPES = "dremio.client.supports-complex-types";

  BooleanValidator ENABLE_VECTORIZED_HASHAGG = new BooleanValidator("exec.operator.aggregate.vectorize", true);
  BooleanValidator ENABLE_VECTORIZED_HASHJOIN = new BooleanValidator("exec.operator.join.vectorize", true);
  BooleanValidator ENABLE_VECTORIZED_HASHJOIN_SPECIFIC = new BooleanValidator("exec.operator.join.vectorize.specific", false);
  BooleanValidator ENABLE_VECTORIZED_COPIER = new BooleanValidator("exec.operator.copier.vectorize", true);
  BooleanValidator ENABLE_VECTORIZED_PARTITIONER = new BooleanValidator("exec.operator.partitioner.vectorize", true);

  String OUTPUT_FORMAT_OPTION = "store.format";
  OptionValidator OUTPUT_FORMAT_VALIDATOR = new StringValidator(OUTPUT_FORMAT_OPTION, "parquet");
  String PARQUET_BLOCK_SIZE = "store.parquet.block-size";
  LongValidator PARQUET_BLOCK_SIZE_VALIDATOR = new LongValidator(PARQUET_BLOCK_SIZE, 256*1024*1024);
  String PARQUET_PAGE_SIZE = "store.parquet.page-size";
  LongValidator PARQUET_PAGE_SIZE_VALIDATOR = new LongValidator(PARQUET_PAGE_SIZE, 100000);
  String PARQUET_DICT_PAGE_SIZE = "store.parquet.dictionary.page-size";
  LongValidator PARQUET_DICT_PAGE_SIZE_VALIDATOR = new LongValidator(PARQUET_DICT_PAGE_SIZE, 1024*1024);
  String PARQUET_WRITER_COMPRESSION_TYPE = "store.parquet.compression";
  EnumeratedStringValidator PARQUET_WRITER_COMPRESSION_TYPE_VALIDATOR = new EnumeratedStringValidator(
      PARQUET_WRITER_COMPRESSION_TYPE, "snappy", "snappy", "gzip", "none");

  String PARQUET_MEMORY_THRESHOLD = "store.parquet.memory_threshold";
  LongValidator PARQUET_MEMORY_THRESHOLD_VALIDATOR = new LongValidator(PARQUET_MEMORY_THRESHOLD, 512*1024*1024);

  String PARQUET_WRITER_ENABLE_DICTIONARY_ENCODING = "store.parquet.enable_dictionary_encoding";
  BooleanValidator PARQUET_WRITER_ENABLE_DICTIONARY_ENCODING_VALIDATOR = new BooleanValidator(
      PARQUET_WRITER_ENABLE_DICTIONARY_ENCODING, true);

  String PARQUET_WRITER_ENABLE_DICTIONARY_ENCODING_BINARY_TYPE = "store.parquet.enable_dictionary_encoding_binary_type";
  BooleanValidator PARQUET_WRITER_ENABLE_DICTIONARY_ENCODING_BINARY_TYPE_VALIDATOR = new BooleanValidator(
    PARQUET_WRITER_ENABLE_DICTIONARY_ENCODING_BINARY_TYPE, false);

  LongValidator PARQUET_MAXIMUM_PARTITIONS_VALIDATOR = new LongValidator("store.max_partitions", 10000);

  LongValidator PARQUET_MIN_RECORDS_FOR_FLUSH_VALIDATOR = new LongValidator("store.parquet.min_records_for_flush", 25000);

  String PARQUET_NEW_RECORD_READER = "store.parquet.use_new_reader";
  BooleanValidator PARQUET_RECORD_READER_IMPLEMENTATION_VALIDATOR = new BooleanValidator(PARQUET_NEW_RECORD_READER, false);

  BooleanValidator PARQUET_READER_VECTORIZE = new BooleanValidator("store.parquet.vectorize", true);
//  BooleanValidator PARQUET_FOOTER_NOSEEK = new BooleanValidator("store.parquet.footer.noseek", true);
  LongValidator PARQUET_FOOTER_CACHESIZE_COORD = new PositiveLongValidator("store.parquet.footer.cache-size.coord", 200000, 10000);
  LongValidator PARQUET_FOOTER_CACHESIZE_EXEC = new PositiveLongValidator("store.parquet.footer.cache-size.exec", 200000, 100);
  BooleanValidator ENABLED_PARQUET_TRACING = new BooleanValidator("store.parquet.vectorize.tracing.enable", false);

  String PARQUET_READER_INT96_AS_TIMESTAMP = "store.parquet.reader.int96_as_timestamp";
  BooleanValidator PARQUET_READER_INT96_AS_TIMESTAMP_VALIDATOR = new BooleanValidator(PARQUET_READER_INT96_AS_TIMESTAMP, true);

  BooleanValidator USE_LEGACY_CATALOG_NAME = new BooleanValidator("client.use_legacy_catalog_name", false);

  String JSON_ALL_TEXT_MODE = "store.json.all_text_mode";
  BooleanValidator JSON_READER_ALL_TEXT_MODE_VALIDATOR = new BooleanValidator(JSON_ALL_TEXT_MODE, false);
  BooleanValidator JSON_EXTENDED_TYPES = new BooleanValidator("store.json.extended_types", false);
  BooleanValidator JSON_WRITER_UGLIFY = new BooleanValidator("store.json.writer.uglify", false);

  DoubleValidator TEXT_ESTIMATED_ROW_SIZE = new RangeDoubleValidator(
      "store.text.estimated_row_size_bytes", 1, Long.MAX_VALUE, 10.0);

  /**
   * The column label (for directory levels) in results when querying files in a directory
   * E.g.  labels: dir0   dir1
   *    structure: foo
   *                |-    bar  -  a.parquet
   *                |-    baz  -  b.parquet
   */
  String FILESYSTEM_PARTITION_COLUMN_LABEL = "dremio.exec.storage.file.partition.column.label";
  StringValidator FILESYSTEM_PARTITION_COLUMN_LABEL_VALIDATOR = new StringValidator(FILESYSTEM_PARTITION_COLUMN_LABEL, "dir");

  String JSON_READ_NUMBERS_AS_DOUBLE = "store.json.read_numbers_as_double";
  BooleanValidator JSON_READ_NUMBERS_AS_DOUBLE_VALIDATOR = new BooleanValidator(JSON_READ_NUMBERS_AS_DOUBLE, false);

  /* Mongo configurations */
  String MONGO_ALL_TEXT_MODE = "store.mongo.all_text_mode";
  OptionValidator MONGO_READER_ALL_TEXT_MODE_VALIDATOR = new BooleanValidator(MONGO_ALL_TEXT_MODE, false);
  String MONGO_READER_READ_NUMBERS_AS_DOUBLE = "store.mongo.read_numbers_as_double";
  OptionValidator MONGO_READER_READ_NUMBERS_AS_DOUBLE_VALIDATOR = new BooleanValidator(MONGO_READER_READ_NUMBERS_AS_DOUBLE, false);
  String MONGO_BSON_RECORD_READER = "store.mongo.bson.record.reader";
  OptionValidator MONGO_BSON_RECORD_READER_VALIDATOR = new BooleanValidator(MONGO_BSON_RECORD_READER, true);

  /* Mongo Rules */
  BooleanValidator MONGO_RULES_AGGREGATE = new BooleanValidator("store.mongo.enable_aggregate_rule", true);
  BooleanValidator MONGO_RULES_FILTER = new BooleanValidator("store.mongo.enable_filter_rule", true);
  BooleanValidator MONGO_RULES_FLATTEN = new BooleanValidator("store.mongo.enable_flatten_rule", true);
  BooleanValidator MONGO_RULES_LIMIT = new BooleanValidator("store.mongo.enable_limit_rule", true);
  BooleanValidator MONGO_RULES_SORT = new BooleanValidator("store.mongo.enable_sort_rule", false);
  BooleanValidator MONGO_RULES_PROJECT = new BooleanValidator("store.mongo.enable_project_rule", true);
  BooleanValidator MONGO_RULES_TOPN = new BooleanValidator("store.mongo.enable_topn_rule", false);
  BooleanValidator MONGO_RULES_SAMPLE = new BooleanValidator("store.mongo.enable_sample_rule", true);

  /* Elastic Rules */
  BooleanValidator ELASTIC_RULES_AGGREGATE = new BooleanValidator("store.elastic.enable_aggregate_rule", true);
  BooleanValidator ELASTIC_RULES_FILTER = new BooleanValidator("store.elastic.enable_filter_rule", true);
  BooleanValidator ELASTIC_RULES_LIMIT = new BooleanValidator("store.elastic.enable_limit_rule", true);
  BooleanValidator ELASTIC_RULES_PROJECT = new BooleanValidator("store.elastic.enable_project_rule", true);
  BooleanValidator ELASTIC_RULES_EDGE_PROJECT = new BooleanValidator("store.elastic.enable_edge_project_rule", false);
  BooleanValidator ELASTIC_RULES_SAMPLE = new BooleanValidator("store.elastic.enable_sample_rule", true);

  BooleanValidator ENABLE_UNION_TYPE = new BooleanValidator("exec.enable_union_type", true);

  BooleanValidator ACCELERATION_VERBOSE_LOGGING = new BooleanValidator("accelerator.system.verbose.logging", true);
  LongValidator ACCELERATION_LIMIT = new LongValidator("accelerator.system.limit", 10);
  BooleanValidator ACCELERATION_AGGREGATION_ENABLED = new BooleanValidator("accelerator.system.aggretation.enabled", true);
  BooleanValidator ACCELERATION_RAW_ENABLED = new BooleanValidator("accelerator.system.raw.enabled", false);
  // DX-6734
  BooleanValidator ACCELERATION_RAW_REMOVE_PROJECT = new BooleanValidator("accelerator.raw.remove_project", true);
  BooleanValidator ACCELERATION_ENABLE_MIN_MAX = new BooleanValidator("accelerator.enable_min_max", true);
  BooleanValidator ACCELERATION_ENABLE_AGG_JOIN = new BooleanValidator("accelerator.enable_agg_join", true);
  LongValidator ACCELERATION_ORPHAN_CLEANUP_MILLISECONDS = new LongValidator("acceleration.orphan.cleanup_in_milliseconds", 14400000); //4 hours

  // TODO: We need to add a feature that enables storage plugins to add their own options. Currently we have to declare
  // in core which is not right. Move this option and above two mongo plugin related options once we have the feature.
  String HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS = "store.hive.optimize_scan_with_native_readers";
  OptionValidator HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS_VALIDATOR =
      new BooleanValidator(HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS, true);

  String SLICE_TARGET = "planner.slice_target";
  long SLICE_TARGET_DEFAULT = 100000L;
  PositiveLongValidator SLICE_TARGET_OPTION = new PositiveLongValidator(SLICE_TARGET, Long.MAX_VALUE,
      SLICE_TARGET_DEFAULT);

  String CAST_TO_NULLABLE_NUMERIC = "dremio.exec.functions.cast_empty_string_to_null";
  OptionValidator CAST_TO_NULLABLE_NUMERIC_OPTION = new BooleanValidator(CAST_TO_NULLABLE_NUMERIC, false);

  /**
   * HashTable runtime settings
   */
  String MIN_HASH_TABLE_SIZE_KEY = "exec.min_hash_table_size";
  PositiveLongValidator MIN_HASH_TABLE_SIZE = new PositiveLongValidator(MIN_HASH_TABLE_SIZE_KEY, HashTable.MAXIMUM_CAPACITY, HashTable.DEFAULT_INITIAL_CAPACITY);
  String MAX_HASH_TABLE_SIZE_KEY = "exec.max_hash_table_size";
  PositiveLongValidator MAX_HASH_TABLE_SIZE = new PositiveLongValidator(MAX_HASH_TABLE_SIZE_KEY, HashTable.MAXIMUM_CAPACITY, HashTable.MAXIMUM_CAPACITY);

  /**
   * Limits the maximum level of parallelization to this factor time the number of Nodes
   */
  String MAX_WIDTH_PER_NODE_KEY = "planner.width.max_per_node";
  PositiveLongValidator MAX_WIDTH_PER_NODE = new PositiveLongValidator(MAX_WIDTH_PER_NODE_KEY, Integer.MAX_VALUE, (long) Math.ceil(Runtime.getRuntime().availableProcessors() * 0.70));

  /**
   * Load reduction will only be triggered if the cluster load exceeds the cutoff value
   */
  DoubleValidator LOAD_CUT_OFF = new RangeDoubleValidator("load.cut_off", 0, Integer.MAX_VALUE, 3);

  /**
   * When applying load reduction, max width per node *= 1 - cluster load x load_reduction
   */
  DoubleValidator LOAD_REDUCTION = new RangeDoubleValidator("load.reduction", 0.01, 1, .1);

  BooleanValidator ENABLE_REATTEMPTS = new BooleanValidator("exec.reattempt.enable", true);

  /**
   * The maximum level or parallelization any stage of the query can do. Note that while this
   * might be the number of active Nodes, realistically, this could be well beyond that
   * number of we want to do things like speed results return.
   */
  String MAX_WIDTH_GLOBAL_KEY = "planner.width.max_per_query";
  LongValidator MAX_WIDTH_GLOBAL = new PositiveLongValidator(MAX_WIDTH_GLOBAL_KEY, Integer.MAX_VALUE, 1000);


  /**
   * Factor by which a node with endpoint affinity will be favored while creating assignment
   */
  String AFFINITY_FACTOR_KEY = "planner.affinity_factor";
  DoubleValidator AFFINITY_FACTOR = new DoubleValidator(AFFINITY_FACTOR_KEY, 1.2d);

  String EARLY_LIMIT0_OPT_KEY = "planner.enable_limit0_optimization";
  BooleanValidator EARLY_LIMIT0_OPT = new BooleanValidator(EARLY_LIMIT0_OPT_KEY, false);

  String ENABLE_MEMORY_ESTIMATION_KEY = "planner.memory.enable_memory_estimation";
  OptionValidator ENABLE_MEMORY_ESTIMATION = new BooleanValidator(ENABLE_MEMORY_ESTIMATION_KEY, false);

  /**
   * Maximum query memory per node (in MB). Re-plan with cheaper operators if memory estimation exceeds this limit.
   * <p/>
   * DEFAULT: 2048 MB
   */
  String MAX_QUERY_MEMORY_PER_NODE_KEY = "planner.memory.max_query_memory_per_node";
  LongValidator MAX_QUERY_MEMORY_PER_NODE = new RangeLongValidator(
      MAX_QUERY_MEMORY_PER_NODE_KEY, 1024 * 1024, Long.MAX_VALUE, 2 * 1024 * 1024 * 1024L);

  /**
   * Extra query memory per node for non-blocking operators.
   * NOTE: This option is currently used only for memory estimation.
   * <p/>
   * DEFAULT: 64 MB
   * MAXIMUM: 2048 MB
   */
  String NON_BLOCKING_OPERATORS_MEMORY_KEY = "planner.memory.non_blocking_operators_memory";
  OptionValidator NON_BLOCKING_OPERATORS_MEMORY = new PowerOfTwoLongValidator(
    NON_BLOCKING_OPERATORS_MEMORY_KEY, 1 << 11, 1 << 6);

  String HASH_JOIN_TABLE_FACTOR_KEY = "planner.memory.hash_join_table_factor";
  OptionValidator HASH_JOIN_TABLE_FACTOR = new DoubleValidator(HASH_JOIN_TABLE_FACTOR_KEY, 1.1d);

  String HASH_AGG_TABLE_FACTOR_KEY = "planner.memory.hash_agg_table_factor";
  OptionValidator HASH_AGG_TABLE_FACTOR = new DoubleValidator(HASH_AGG_TABLE_FACTOR_KEY, 1.1d);

  String AVERAGE_FIELD_WIDTH_KEY = "planner.memory.average_field_width";
  OptionValidator AVERAGE_FIELD_WIDTH = new PositiveLongValidator(AVERAGE_FIELD_WIDTH_KEY, Long.MAX_VALUE, 8);

  /**
   * Compression used to send fragments over RPC
   */
  String FRAGMENT_CODEC_KEY = "planner.fragment.codec";
  EnumValidator<FragmentCodec> FRAGMENT_CODEC = new EnumValidator<>(FRAGMENT_CODEC_KEY, FragmentCodec.class, FragmentCodec.SNAPPY);

  BooleanValidator ENABLE_QUEUE = new BooleanValidator("exec.queue.enable", true);
  BooleanValidator REFLECTION_ENABLE_QUEUE = new BooleanValidator("reflection.queue.enable", true);
  LongValidator LARGE_QUEUE_SIZE = new PositiveLongValidator("exec.queue.large", 1000, 10);
  LongValidator SMALL_QUEUE_SIZE = new PositiveLongValidator("exec.queue.small", 100000, 100);
  LongValidator REFLECTION_LARGE_QUEUE_SIZE = new RangeLongValidator("reflection.queue.large", 0, 100, 1);
  LongValidator REFLECTION_SMALL_QUEUE_SIZE = new RangeLongValidator("reflection.queue.small", 0, 10000, 10);
  LongValidator QUEUE_THRESHOLD_SIZE = new PositiveLongValidator("exec.queue.threshold",
      Long.MAX_VALUE, 30000000);
  LongValidator QUEUE_TIMEOUT = new PositiveLongValidator("exec.queue.timeout_millis",
      Long.MAX_VALUE, 60 * 1000 * 5);
  // 24 hour timeout for reflection jobs to enter queue.
  // This will enable reflections to wait for longer running reflections to finish and enter queue.
  LongValidator REFLECTION_QUEUE_TIMEOUT = new PositiveLongValidator("reflection.queue.timeout_millis",
      Long.MAX_VALUE, 24 * 60 * 60 * 1000 );
  BooleanValidator ENABLE_QUEUE_MEMORY_LIMIT = new BooleanValidator("exec.queue.memory.enable", true);
  LongValidator LARGE_QUEUE_MEMORY_LIMIT = new RangeLongValidator("exec.queue.memory.large", 0, Long.MAX_VALUE, 0);
  LongValidator SMALL_QUEUE_MEMORY_LIMIT = new RangeLongValidator("exec.queue.memory.small", 0, Long.MAX_VALUE, 0);

  String ENABLE_VERBOSE_ERRORS_KEY = "exec.errors.verbose";
  OptionValidator ENABLE_VERBOSE_ERRORS = new BooleanValidator(ENABLE_VERBOSE_ERRORS_KEY, false);

  String ENABLE_NEW_TEXT_READER_KEY = "exec.storage.enable_new_text_reader";
  OptionValidator ENABLE_NEW_TEXT_READER = new BooleanValidator(ENABLE_NEW_TEXT_READER_KEY, true);

  String BOOTSTRAP_STORAGE_PLUGINS_FILE = "bootstrap-storage-plugins.json";
  String MAX_LOADING_CACHE_SIZE_CONFIG = "dremio.exec.compile.cache_max_size";

  String ENABLE_WINDOW_FUNCTIONS = "window.enable";
  OptionValidator ENABLE_WINDOW_FUNCTIONS_VALIDATOR = new BooleanValidator(ENABLE_WINDOW_FUNCTIONS, true);

  String NODE_CONTROL_INJECTIONS = "dremio.exec.testing.controls";
  OptionValidator NODE_CONTROLS_VALIDATOR =
    new ExecutionControls.ControlsOptionValidator(NODE_CONTROL_INJECTIONS, ExecutionControls.DEFAULT_CONTROLS, 1);

  String NEW_VIEW_DEFAULT_PERMS_KEY = "new_view_default_permissions";
  OptionValidator NEW_VIEW_DEFAULT_PERMS_VALIDATOR =
      new StringValidator(NEW_VIEW_DEFAULT_PERMS_KEY, "700");

  /**
   * Applicable only when {@link #ENABLE_VECTORIZED_PARTITIONER} is true. This enables the bucket size calculations to
   * be based on the record size.
   */
  BooleanValidator PARTITION_SENDER_BATCH_ADAPTIVE = new BooleanValidator("exec.partitioner.batch.adaptive", false);

  PositiveLongValidator PARTITION_SENDER_MAX_MEM = new PositiveLongValidator("exec.partitioner.mem.max", Integer.MAX_VALUE, 100*1024*1024);
  PositiveLongValidator PARTITION_SENDER_MAX_BATCH_SIZE = new PositiveLongValidator("exec.partitioner.batch.size.max", Integer.MAX_VALUE, 1024*1024);

  BooleanValidator DEBUG_QUERY_PROFILE = new BooleanValidator("dremio.profile.debug_columns", false);

  BooleanValidator MATERIALIZATION_CACHE_ENABLED = new BooleanValidator("dremio.materialization.cache.enabled", true);
  PositiveLongValidator MATERIALIZATION_CACHE_REFRESH_DURATION = new PositiveLongValidator("dremio.materialization.cache.refresh_seconds", Long.MAX_VALUE, 30);
  PositiveLongValidator LAYOUT_REFRESH_MAX_ATTEMPTS = new PositiveLongValidator("layout.refresh.max.attempts", Integer.MAX_VALUE, 3);

  BooleanValidator OLD_ASSIGNMENT_CREATOR = new BooleanValidator("exec.work.assignment.old", false);

  /**
   * This factor determines how much larger the load for a given slice can be than the expected size in order to maintain locality
   * A smaller value will favor even distribution of load, while a larger value will favor locality, even if that means uneven load
   */
  DoubleValidator ASSIGNMENT_CREATOR_BALANCE_FACTOR = new DoubleValidator("exec.work.assignment.locality_factor", 1.5);

  PositiveLongValidator FRAGMENT_CACHE_EVICTION_DELAY_S = new PositiveLongValidator("fragments.cache.eviction.delay_seconds", Integer.MAX_VALUE, 600);

  PositiveLongValidator SOURCE_STATE_REFRESH_MIN = new PositiveLongValidator("store.metadata.state.refresh_min", Character.MAX_VALUE, 5);
  PositiveLongValidator SOURCE_METADATA_REFRESH_MIN = new PositiveLongValidator("store.metadata.base.refresh_min", Character.MAX_VALUE, 5);
  BooleanValidator PARQUET_SINGLE_STREAM = new BooleanValidator("store.parquet.single_stream", false);
  LongValidator PARQUET_SINGLE_STREAM_COLUMN_THRESHOLD = new LongValidator("store.parquet.single_stream_column_threshold", 40);
  LongValidator RESULTS_MAX_AGE_IN_DAYS = new LongValidator("results.max.age_in_days", 30);
  //Configuration used for testing or debugging
  LongValidator DEBUG_RESULTS_MAX_AGE_IN_MILLISECONDS = new LongValidator("debug.results.max.age_in_milliseconds", 0);

  BooleanValidator SORT_FILE_BLOCKS = new BooleanValidator("store.file.sort_blocks", false);

  // Check for storage plugin status at time of creation of the plugin
  BooleanValidator STORAGE_PLUGIN_CHECK_STATE = new BooleanValidator("store.plugin.check_state", true);

  LongValidator FLATTEN_OPERATOR_OUTPUT_MEMORY_LIMIT = new LongValidator("exec.operator.flatten_output_memory_limit", 512*1024*1024);

  PositiveLongValidator PLANNER_IN_SUBQUERY_THRESHOLD = new PositiveLongValidator("planner.in.subquery.threshold", Character.MAX_VALUE, 20);

  BooleanValidator EXTERNAL_SORT_COMPRESS_SPILL_FILES = new BooleanValidator("exec.operator.sort.external.compress_spill_files", true);
}
