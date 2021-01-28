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
package com.dremio.exec;

import java.util.concurrent.TimeUnit;

import com.dremio.common.expression.SupportedEngines;
import com.dremio.exec.proto.CoordExecRPC.FragmentCodec;
import com.dremio.exec.testing.ExecutionControls;
import com.dremio.options.OptionValidator;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators;
import com.dremio.options.TypeValidators.AdminBooleanValidator;
import com.dremio.options.TypeValidators.BooleanValidator;
import com.dremio.options.TypeValidators.DoubleValidator;
import com.dremio.options.TypeValidators.EnumValidator;
import com.dremio.options.TypeValidators.EnumeratedStringValidator;
import com.dremio.options.TypeValidators.LongValidator;
import com.dremio.options.TypeValidators.PositiveLongValidator;
import com.dremio.options.TypeValidators.PowerOfTwoLongValidator;
import com.dremio.options.TypeValidators.RangeDoubleValidator;
import com.dremio.options.TypeValidators.RangeLongValidator;
import com.dremio.options.TypeValidators.StringValidator;
import com.dremio.sabot.op.common.hashtable.HashTable;
import com.dremio.service.spill.DefaultSpillServiceOptions;

@Options
public interface ExecConstants {
  String ZK_CONNECTION = "dremio.exec.zk.connect";
  String ZK_TIMEOUT = "dremio.exec.zk.timeout";
  String ZK_SESSION_TIMEOUT = "dremio.exec.zk.session.timeout";
  String ZK_ROOT = "dremio.exec.zk.root";
  String ZK_REFRESH = "dremio.exec.zk.refresh";
  String ZK_RETRY_UNLIMITED = "dremio.exec.zk.retry.unlimited";
  String ZK_RETRY_LIMIT = "dremio.exec.zk.retry.limit";
  String ZK_INITIAL_TIMEOUT_MS = "dremio.exec.zk.retry.initial_timeout_ms";

  String BIT_SERVER_RPC_THREADS = "dremio.exec.rpc.bit.server.threads";
  String USER_SERVER_RPC_THREADS = "dremio.exec.rpc.user.server.threads";
  String REGISTRATION_ADDRESS = "dremio.exec.rpc.publishedhost";

  /* incoming buffer size (number of batches) */
  String INCOMING_BUFFER_SIZE = "dremio.exec.buffer.size";
  String SPOOLING_BUFFER_DELETE = "dremio.exec.buffer.spooling.delete";
  String SPOOLING_BUFFER_SIZE = "dremio.exec.buffer.spooling.size";
  String BATCH_PURGE_THRESHOLD = "dremio.exec.sort.purge.threshold";
  String SPILL_DIRS = "dremio.exec.sort.external.spill.directories";
  String HTTP_ENABLE = "dremio.exec.http.enabled";

  /* Spill disk space configurations */
  BooleanValidator SPILL_ENABLE_HEALTH_CHECK = new BooleanValidator("dremio.exec.spill.healthcheck.enable", DefaultSpillServiceOptions.ENABLE_HEALTH_CHECK);
  PositiveLongValidator SPILL_DISK_SPACE_CHECK_INTERVAL = new PositiveLongValidator("dremio.exec.spill.check.interval", Integer.MAX_VALUE, DefaultSpillServiceOptions.HEALTH_CHECK_INTERVAL);
  PositiveLongValidator SPILL_DISK_SPACE_LIMIT_BYTES = new PositiveLongValidator("dremio.exec.spill.limit.bytes", Long.MAX_VALUE, DefaultSpillServiceOptions.MIN_DISK_SPACE_BYTES);
  DoubleValidator SPILL_DISK_SPACE_LIMIT_PERCENTAGE = new RangeDoubleValidator("dremio.exec.spill.limit.percentage", 0.0, 100.0, DefaultSpillServiceOptions.MIN_DISK_SPACE_PCT);
  PositiveLongValidator SPILL_SWEEP_INTERVAL = new PositiveLongValidator("dremio.exec.spill.sweep.interval", Long.MAX_VALUE, DefaultSpillServiceOptions.SPILL_SWEEP_INTERVAL);
  PositiveLongValidator SPILL_SWEEP_THRESHOLD = new PositiveLongValidator("dremio.exec.spill.sweep.threshold", Long.MAX_VALUE, DefaultSpillServiceOptions.SPILL_SWEEP_THRESHOLD);

  // Set this value to set the execution preference
  // Default value to use in the operators (for now, only projector and filter use this default)
  String QUERY_EXEC_OPTION_KEY = "exec.preferred.codegenerator";
  EnumValidator<SupportedEngines.CodeGenOption> QUERY_EXEC_OPTION = new EnumValidator<>(
    QUERY_EXEC_OPTION_KEY, SupportedEngines.CodeGenOption.class, SupportedEngines.CodeGenOption.DEFAULT);

  // Configuration option for enabling expression split
  // Splits are enabled when this is set to true and QUERY_EXEC_OPTION is set to Gandiva
  BooleanValidator SPLIT_ENABLED = new BooleanValidator("exec.expression.split.enabled", true);

  String MAX_SPLITS_PER_EXPR_KEY = "exec.expression.split.max_splits_per_expression";
  PositiveLongValidator MAX_SPLITS_PER_EXPRESSION = new PositiveLongValidator(MAX_SPLITS_PER_EXPR_KEY, Long.MAX_VALUE, 10);

  // Configuration option for deciding how much work should be done in Gandiva when there are excessive splits
  // MAX_SPLITS_PER_EXPRESSION is used to configure excessive splits
  // 1 unit of work approximately corresponds to 1 function evaluation in Gandiva
  String WORK_THRESHOLD_FOR_SPLIT_KEY = "exec.expression.split.work_per_split";
  DoubleValidator WORK_THRESHOLD_FOR_SPLIT = new RangeDoubleValidator(WORK_THRESHOLD_FOR_SPLIT_KEY, 0.0, Long.MAX_VALUE, 3.0);

  PositiveLongValidator MAX_FOREMEN_PER_COORDINATOR = new PositiveLongValidator("coordinator.alive_queries.limit", Long.MAX_VALUE, 1000);

  BooleanValidator REST_API_RUN_QUERY_ASYNC = new BooleanValidator("dremio.coordinator.rest.run_query.async", false);

  // max number of concurrent metadata refresh in progress.
  PositiveLongValidator MAX_CONCURRENT_METADATA_REFRESHES = new PositiveLongValidator("coordinator.metadata.refreshes.concurrency",
                                                                                      Integer.MAX_VALUE,
                                                                                      24);

  // Whether or not to replace a group of ORs with a set operation.
  BooleanValidator FAST_OR_ENABLE = new BooleanValidator("exec.operator.orfast", true);

  // Number above which we replace a group of ORs with a set operation.
  PositiveLongValidator FAST_OR_MIN_THRESHOLD = new PositiveLongValidator("exec.operator.orfast.threshold.min", Integer.MAX_VALUE, 5);

  // Number above which we replace a group of ORs with a set operation in gandiva
  PositiveLongValidator FAST_OR_MIN_THRESHOLD_GANDIVA = new PositiveLongValidator("exec.operator.orfast.gandiva_threshold.min", Integer.MAX_VALUE, 5);

  // Number above which we stop replacing a group of ORs with a set operation.
  PositiveLongValidator FAST_OR_MAX_THRESHOLD = new PositiveLongValidator("exec.operator.orfast.threshold.max", Integer.MAX_VALUE, 1500);

  PositiveLongValidator CODE_GEN_NESTED_METHOD_THRESHOLD = new PositiveLongValidator("exec.operator.codegen.nested_method.threshold", Integer.MAX_VALUE, 100);

  /*
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
  PositiveLongValidator OUTPUT_ALLOCATOR_RESERVATION = new PositiveLongValidator("exec.batch.output.alloc.reservation", Integer.MAX_VALUE, 512 * 1024);

  String OPERATOR_TARGET_BATCH_BYTES = "dremio.exec.operator_batch_bytes";
  OptionValidator OPERATOR_TARGET_BATCH_BYTES_VALIDATOR = new LongValidator(OPERATOR_TARGET_BATCH_BYTES, 10*1024*1024);

  BooleanValidator ENABLE_VECTORIZED_HASHAGG = new BooleanValidator("exec.operator.aggregate.vectorize", true);
  BooleanValidator ENABLE_VECTORIZED_HASHJOIN = new BooleanValidator("exec.operator.join.vectorize", true);
  BooleanValidator ENABLE_VECTORIZED_HASHJOIN_SPECIFIC = new BooleanValidator("exec.operator.join.vectorize.specific", false);
  BooleanValidator ENABLE_VECTORIZED_COPIER = new BooleanValidator("exec.operator.copier.vectorize", true);
  BooleanValidator ENABLE_VECTORIZED_PARTITIONER = new BooleanValidator("exec.operator.partitioner.vectorize", true);
  BooleanValidator DEBUG_HASHJOIN_INSERTION = new BooleanValidator("exec.operator.join.debug-insertion", false);

  String OUTPUT_FORMAT_OPTION = "store.format";
  StringValidator OUTPUT_FORMAT_VALIDATOR = new StringValidator(OUTPUT_FORMAT_OPTION, "parquet");

  String PARQUET_WRITE_TIME_THRESHOLD_MILLI_SECS = "store.parquet.write-time-threshold-milli-secs";
  PositiveLongValidator PARQUET_WRITE_TIME_THRESHOLD_MILLI_SECS_VALIDATOR = new PositiveLongValidator(PARQUET_WRITE_TIME_THRESHOLD_MILLI_SECS, Integer.MAX_VALUE, 120000);

  String PARQUET_WRITE_IO_RATE_THRESHOLD_MBPS = "store.parquet.write-io-rate-mbps";
  DoubleValidator PARQUET_WRITE_IO_RATE_THRESHOLD_MBPS_VALIDATOR = new DoubleValidator(PARQUET_WRITE_IO_RATE_THRESHOLD_MBPS, 5.0);

  String PARQUET_BLOCK_SIZE = "store.parquet.block-size";
  LongValidator PARQUET_BLOCK_SIZE_VALIDATOR = new LongValidator(PARQUET_BLOCK_SIZE, 256*1024*1024);
  String PARQUET_PAGE_SIZE = "store.parquet.page-size";
  LongValidator PARQUET_PAGE_SIZE_VALIDATOR = new LongValidator(PARQUET_PAGE_SIZE, 100000);
  String PARQUET_DICT_PAGE_SIZE = "store.parquet.dictionary.page-size";
  LongValidator PARQUET_DICT_PAGE_SIZE_VALIDATOR = new LongValidator(PARQUET_DICT_PAGE_SIZE, 1024*1024);
  String PARQUET_WRITER_COMPRESSION_TYPE = "store.parquet.compression";
  EnumeratedStringValidator PARQUET_WRITER_COMPRESSION_TYPE_VALIDATOR = new EnumeratedStringValidator(
      PARQUET_WRITER_COMPRESSION_TYPE, "snappy", "snappy", "gzip", "none");

  String PARQUET_MAX_FOOTER_LEN = "store.parquet.max_footer_length";
  LongValidator PARQUET_MAX_FOOTER_LEN_VALIDATOR = new LongValidator(PARQUET_MAX_FOOTER_LEN, 16*1024*1024);

  String PARQUET_MEMORY_THRESHOLD = "store.parquet.memory_threshold";
  LongValidator PARQUET_MEMORY_THRESHOLD_VALIDATOR = new LongValidator(PARQUET_MEMORY_THRESHOLD, 512*1024*1024);

  LongValidator PARQUET_MAX_PARTITION_COLUMNS_VALIDATOR = new RangeLongValidator("store.parquet.partition_column_limit", 0, 500, 25);

  BooleanValidator PARQUET_ELIMINATE_NULL_PARTITIONS = new BooleanValidator("store.parquet.exclude_null_implicit_partitions", true);

  String PARQUET_WRITER_ENABLE_DICTIONARY_ENCODING = "store.parquet.enable_dictionary_encoding";
  BooleanValidator PARQUET_WRITER_ENABLE_DICTIONARY_ENCODING_VALIDATOR = new BooleanValidator(
      PARQUET_WRITER_ENABLE_DICTIONARY_ENCODING, false);

  String EXCEL_MAX_FILE_SIZE = "store.excel.max_file_size";
  LongValidator EXCEL_MAX_FILE_SIZE_VALIDATOR = new LongValidator(EXCEL_MAX_FILE_SIZE, 10*1024*1024);

  String PARQUET_WRITER_ENABLE_DICTIONARY_ENCODING_BINARY_TYPE = "store.parquet.enable_dictionary_encoding_binary_type";
  BooleanValidator PARQUET_WRITER_ENABLE_DICTIONARY_ENCODING_BINARY_TYPE_VALIDATOR = new BooleanValidator(
    PARQUET_WRITER_ENABLE_DICTIONARY_ENCODING_BINARY_TYPE, false);

  LongValidator PARQUET_MAXIMUM_PARTITIONS_VALIDATOR = new LongValidator("store.max_partitions", 10000);

  LongValidator PARQUET_MIN_RECORDS_FOR_FLUSH_VALIDATOR = new LongValidator("store.parquet.min_records_for_flush", 25000);

  String PARQUET_NEW_RECORD_READER = "store.parquet.use_new_reader";
  BooleanValidator PARQUET_RECORD_READER_IMPLEMENTATION_VALIDATOR = new BooleanValidator(PARQUET_NEW_RECORD_READER, false);

  String PARQUET_AUTO_CORRECT_DATES = "store.parquet.auto.correct.dates";
  BooleanValidator PARQUET_AUTO_CORRECT_DATES_VALIDATOR = new BooleanValidator(PARQUET_AUTO_CORRECT_DATES, true);

  BooleanValidator PARQUET_READER_VECTORIZE = new BooleanValidator("store.parquet.vectorize", true);
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
  BooleanValidator MONGO_RULES_FILTER = new BooleanValidator("store.mongo.enable_filter_rule", true);
  BooleanValidator MONGO_RULES_SORT = new BooleanValidator("store.mongo.enable_sort_rule", false);
  BooleanValidator MONGO_RULES_PROJECT = new BooleanValidator("store.mongo.enable_project_rule", true);

  /* Elastic Rules */
  BooleanValidator ELASTIC_RULES_AGGREGATE = new BooleanValidator("store.elastic.enable_aggregate_rule", true);
  BooleanValidator ELASTIC_RULES_FILTER = new BooleanValidator("store.elastic.enable_filter_rule", true);
  BooleanValidator ELASTIC_RULES_LIMIT = new BooleanValidator("store.elastic.enable_limit_rule", true);
  BooleanValidator ELASTIC_RULES_PROJECT = new BooleanValidator("store.elastic.enable_project_rule", true);
  BooleanValidator ELASTIC_RULES_EDGE_PROJECT = new BooleanValidator("store.elastic.enable_edge_project_rule", false);
  BooleanValidator ELASTIC_RULES_SAMPLE = new BooleanValidator("store.elastic.enable_sample_rule", true);

  BooleanValidator ELASTIC_ENABLE_MAPPING_CHECKSUM = new BooleanValidator("store.elastic.enable_mapping_checksum", true);

  String ELASTIC_ACTION_RETRIES = "store.elastic.action_retries";
  LongValidator ELASTIC_ACTION_RETRIES_VALIDATOR = new LongValidator(ELASTIC_ACTION_RETRIES, 0);

  BooleanValidator ENABLE_UNION_TYPE = new BooleanValidator("exec.enable_union_type", true);

  BooleanValidator ACCELERATION_VERBOSE_LOGGING = new BooleanValidator("accelerator.system.verbose.logging", true);
  LongValidator ACCELERATION_LIMIT = new LongValidator("accelerator.system.limit", 10);
  LongValidator ACCELERATION_ORPHAN_CLEANUP_MILLISECONDS = new LongValidator("acceleration.orphan.cleanup_in_milliseconds", 14400000); //4 hours

  String SLICE_TARGET = "planner.slice_target";
  long SLICE_TARGET_DEFAULT = 100000L;
  PositiveLongValidator SLICE_TARGET_OPTION = new PositiveLongValidator(SLICE_TARGET, Long.MAX_VALUE,
      SLICE_TARGET_DEFAULT);

  /**
   * HashTable runtime settings
   */
  String MIN_HASH_TABLE_SIZE_KEY = "exec.min_hash_table_size";
  PositiveLongValidator MIN_HASH_TABLE_SIZE = new PositiveLongValidator(MIN_HASH_TABLE_SIZE_KEY, HashTable.MAXIMUM_CAPACITY, HashTable.DEFAULT_INITIAL_CAPACITY);
  String MAX_HASH_TABLE_SIZE_KEY = "exec.max_hash_table_size";
  PositiveLongValidator MAX_HASH_TABLE_SIZE = new PositiveLongValidator(MAX_HASH_TABLE_SIZE_KEY, HashTable.MAXIMUM_CAPACITY, HashTable.MAXIMUM_CAPACITY);

  /**
   * Limits the maximum level of parallelization to this factor time the number of Nodes.
   * The default value is internally computed based on number of cores per executor. The default value
   * mentioned here is meaningless and is only used to ascertain if user has explicitly set the value
   * or not.
   */
  String MAX_WIDTH_PER_NODE_KEY = "planner.width.max_per_node";
  PositiveLongValidator MAX_WIDTH_PER_NODE = new PositiveLongValidator(MAX_WIDTH_PER_NODE_KEY, Integer.MAX_VALUE, 0);

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

  /**
   * A test memory limt setting
   */
  @Deprecated
  LongValidator TEST_MEMORY_LIMIT =
      new RangeLongValidator("debug.test_memory_limit", 1024 * 1024, Long.MAX_VALUE, 2 * 1024 * 1024 * 1024L);

  BooleanValidator USE_NEW_MEMORY_BOUNDED_BEHAVIOR = new BooleanValidator("planner.memory.new_bounding_algo", true);

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

  BooleanValidator DEBUG_QUERY_PROFILE = new BooleanValidator("dremio.profile.debug_columns", false);

  BooleanValidator SCAN_COMPUTE_LOCALITY = new BooleanValidator("exec.operator.scan.compute_locality", false);

  PositiveLongValidator LAYOUT_REFRESH_MAX_ATTEMPTS = new PositiveLongValidator("layout.refresh.max.attempts", Integer.MAX_VALUE, 3);

  BooleanValidator OLD_ASSIGNMENT_CREATOR = new BooleanValidator("exec.work.assignment.old", false);

  /**
   * If set to true, soft affinity will be ignored for leaf fragments during parallelization.
   */
  BooleanValidator SHOULD_IGNORE_LEAF_AFFINITY = new BooleanValidator("planner.assignment.ignore_leaf_affinity", false);

  /**
   * This factor determines how much larger the load for a given slice can be than the expected size in order to maintain locality
   * A smaller value will favor even distribution of load, while a larger value will favor locality, even if that means uneven load
   */
  DoubleValidator ASSIGNMENT_CREATOR_BALANCE_FACTOR = new DoubleValidator("exec.work.assignment.locality_factor", 1.5);

  PositiveLongValidator FRAGMENT_CACHE_EVICTION_DELAY_S = new PositiveLongValidator("fragments.cache.eviction.delay_seconds", Integer.MAX_VALUE, 600);

  BooleanValidator PARQUET_SINGLE_STREAM = new BooleanValidator("store.parquet.single_stream", false);
  LongValidator PARQUET_SINGLE_STREAM_COLUMN_THRESHOLD = new LongValidator("store.parquet.single_stream_column_threshold", 40);
  LongValidator PARQUET_MULTI_STREAM_SIZE_LIMIT = new LongValidator("store.parquet.multi_stream_limit", 1024*1024);
  BooleanValidator PARQUET_MULTI_STREAM_SIZE_LIMIT_ENABLE = new BooleanValidator("store.parquet.multi_stream_limit.enable", true);
  LongValidator PARQUET_FULL_FILE_READ_THRESHOLD = new RangeLongValidator("store.parquet.full_file_read.threshold", 0, Integer.MAX_VALUE, 0);
  DoubleValidator PARQUET_FULL_FILE_READ_COLUMN_RATIO = new RangeDoubleValidator("store.parquet.full_file_read.column_ratio", 0.0, 1.0, 0.25);
  BooleanValidator PARQUET_CACHED_ENTITY_SET_FILE_SIZE = new BooleanValidator("store.parquet.set_file_length",true);
  BooleanValidator PARQUET_COLUMN_ORDERING = new BooleanValidator("store.parquet.column_ordering", false);

  BooleanValidator HIVE_COMPLEXTYPES_ENABLED = new BooleanValidator("store.hive.parquet.support_complex_types", true);
  String PARQUET_LIST_ITEMS_KEY = "store.parquet.list_items.threshold";
  LongValidator PARQUET_LIST_ITEMS_THRESHOLD = new LongValidator(PARQUET_LIST_ITEMS_KEY, 128);

  LongValidator RESULTS_MAX_AGE_IN_DAYS = new LongValidator("results.max.age_in_days", 1);
  // At what hour of the day to do job results cleanup - 0-23
  RangeLongValidator JOB_RESULTS_CLEANUP_START_HOUR = new RangeLongValidator("job.results.cleanup.start_at_hour", 0, 23, 0);
  LongValidator JOB_MAX_AGE_IN_DAYS = new LongValidator("jobs.max.age_in_days", 30);
  // At what hour of the day to do job cleanup - 0-23
  RangeLongValidator JOB_CLEANUP_START_HOUR = new RangeLongValidator("job.cleanup.start_at_hour", 0, 23, 1);

  //Configuration used for testing or debugging
  LongValidator DEBUG_RESULTS_MAX_AGE_IN_MILLISECONDS = new LongValidator("debug.results.max.age_in_milliseconds", 0);

  BooleanValidator SORT_FILE_BLOCKS = new BooleanValidator("store.file.sort_blocks", false);

  PositiveLongValidator LIMIT_FIELD_SIZE_BYTES = new PositiveLongValidator("limits.single_field_size_bytes", Integer.MAX_VALUE, 32000);

  LongValidator FLATTEN_OPERATOR_OUTPUT_MEMORY_LIMIT = new LongValidator("exec.operator.flatten_output_memory_limit", 512*1024*1024);

  PositiveLongValidator PLANNER_IN_SUBQUERY_THRESHOLD = new PositiveLongValidator("planner.in.subquery.threshold", Character.MAX_VALUE, 20);

  BooleanValidator EXTERNAL_SORT_COMPRESS_SPILL_FILES = new BooleanValidator("exec.operator.sort.external.compress_spill_files", true);
  BooleanValidator EXTERNAL_SORT_ENABLE_SPLAY_SORT = new BooleanValidator("exec.operator.sort.external.enable_splay_sort", false);
  BooleanValidator EXTERNAL_SORT_ENABLE_MICRO_SPILL = new BooleanValidator("exec.operator.sort.external.enable_micro_spill", true);
  PositiveLongValidator SORT_MAX_WRITE_BATCH = new PositiveLongValidator("exec.operator.sort.external.spill_batch_records", Character.MAX_VALUE, Character.MAX_VALUE);
  BooleanValidator EXTERNAL_SORT_ARROW_ENCODING = new BooleanValidator("exec.operator.sort.external.arrow_encoding", true);
  BooleanValidator EXTERNAL_SORT_DIRECT_WRITE = new BooleanValidator("exec.operator.sort.external.direct_write", true);
  BooleanValidator EXTERNAL_SORT_VECTOR_COPIER = new BooleanValidator("exec.operator.sort.external.vector_copier", true);
  DoubleValidator EXTERNAL_SORT_SPILL_ALLOCATION_DENSITY = new RangeDoubleValidator("exec.operator.sort.external.spill.allocation_density", 0.0, Double.MAX_VALUE, 0.01);

  PositiveLongValidator EXTERNAL_SORT_BATCHSIZE_MULTIPLIER = new PositiveLongValidator("exec.operator.sort.external.batchsize_multiplier", Character.MAX_VALUE, 2);

  LongValidator VOTING_SCHEDULE = new PositiveLongValidator("vote.schedule.millis", Long.MAX_VALUE, 0);
  PositiveLongValidator LAST_SEARCH_REINDEX  = new PositiveLongValidator("dac.search.last_reindex",  Long.MAX_VALUE, 0);
  PositiveLongValidator SEARCH_MANAGER_REFRESH_MILLIS  = new PositiveLongValidator("dac.search.refresh",  Long.MAX_VALUE, TimeUnit.MINUTES.toMillis(1));

  // this option sets the frequency of task leader refresh for the executor selectors
  PositiveLongValidator EXEC_SELECTOR_TASK_LEADER_REFRESH_MILLIS  = new PositiveLongValidator("exec.selection.refresh",  Long.MAX_VALUE, TimeUnit.MINUTES.toMillis(1));

  TypeValidators.PositiveLongValidator EXEC_SELECTOR_RELEASE_LEADERSHIP_MS =
    new TypeValidators.PositiveLongValidator("exec.selection.leadership.ms", Long.MAX_VALUE, TimeUnit.HOURS
      .toMillis(12));

  //this option sets the capacity of an ArrowBuf in SlicedBufferManager from which various buffers may be sliced.
  PowerOfTwoLongValidator BUF_MANAGER_CAPACITY = new PowerOfTwoLongValidator("exec.sliced_bufmgr.capacity", 1 << 24, 1 << 16);

  TypeValidators.PositiveLongValidator VOTING_RELEASE_LEADERSHIP_MS =
    new TypeValidators.PositiveLongValidator("vote.release.leadership.ms", Long.MAX_VALUE, TimeUnit.HOURS.toMillis
      (35));

  TypeValidators.PositiveLongValidator JOBS_RELEASE_LEADERSHIP_MS =
    new TypeValidators.PositiveLongValidator("jobs.release.leadership.ms", Long.MAX_VALUE, TimeUnit.HOURS.toMillis
      (30));

  TypeValidators.PositiveLongValidator SEARCH_SERVICE_RELEASE_LEADERSHIP_MS =
    new TypeValidators.PositiveLongValidator("searchservice.release.leadership.ms", Long.MAX_VALUE, TimeUnit.HOURS
      .toMillis(12));

  BooleanValidator ENABLE_VECTORIZED_NOSPILL_VARCHAR_NDV_ACCUMULATOR = new BooleanValidator("exec.operator.vectorized_nospill.varchar_ndv", true);

  BooleanValidator TRIM_ROWGROUPS_FROM_FOOTER = new BooleanValidator("exec.parquet.memory.trim_rowgroups", true);
  BooleanValidator TRIM_COLUMNS_FROM_ROW_GROUP = new BooleanValidator("exec.parquet.memory.trim_columns", true);

  AdminBooleanValidator EXECUTOR_ENABLE_HEAP_MONITORING = new AdminBooleanValidator("exec.heap.monitoring.enable", true);
  RangeLongValidator EXECUTOR_HEAP_MONITORING_CLAWBACK_THRESH_PERCENTAGE = new RangeLongValidator("exec.heap.monitoring.thresh.percentage", 50, 100, 85);

  AdminBooleanValidator COORDINATOR_ENABLE_HEAP_MONITORING = new AdminBooleanValidator("coordinator.heap.monitoring.enable", true);
  RangeLongValidator COORDINATOR_HEAP_MONITORING_CLAWBACK_THRESH_PERCENTAGE = new RangeLongValidator("coordinator.heap.monitoring.thresh.percentage", 50, 100, 85);

  BooleanValidator ENABLE_ICEBERG = new BooleanValidator("dremio.iceberg.enabled", false);

  BooleanValidator ENABLE_DELTALAKE = new BooleanValidator("dremio.deltalake.enabled", false);

  // warning threshold for running time of a task
  PositiveLongValidator SLICING_WARN_MAX_RUNTIME_MS = new PositiveLongValidator("dremio.sliced.warn_max_runtime", Long.MAX_VALUE, 120000);

  // warning threshold for spilling
  PositiveLongValidator SPILL_IO_WARN_MAX_RUNTIME_MS = new PositiveLongValidator("dremio.spill.warn_max_runtime", Long.MAX_VALUE, 3000);

  // warning threshold for long IO time
  LongValidator STORE_IO_TIME_WARN_THRESH_MILLIS = new LongValidator("store.io_time_warn_thresh_millis", 10000);

  // global hive-async option
  BooleanValidator ENABLE_HIVE_ASYNC = new TypeValidators.BooleanValidator("store.hive.async", true);

  BooleanValidator ENABLE_REMOTE_JOB_FETCH = new BooleanValidator("jobs.remote.fetch_enabled", true);

  DoubleValidator EXPR_COMPLEXITY_NO_OPTIMIZE_THRESHOLD = new DoubleValidator("exec.expression.complexity.no_optimize.threshold", 2000.00);

  BooleanValidator ENABLE_BOOSTING = new BooleanValidator("exec.storage.enable_arrow_caching", true);
  BooleanValidator ENABLE_BOOST_FILTERING_READER = new BooleanValidator("exec.storage.enable_arrow_filtering_reader", true);
  BooleanValidator ENABLE_BOOST_DELTA_READER = new BooleanValidator("exec.storage.enable_arrow_delta_reader", true);

  // hive parallelism and timeout options for signature validation process
  LongValidator HIVE_SIGNATURE_VALIDATION_PARALLELISM = new TypeValidators.RangeLongValidator("store.hive.signature_validation.parallelism", 1, 32, 16);
  LongValidator HIVE_SIGNATURE_VALIDATION_TIMEOUT_MS = new TypeValidators.LongValidator("store.hive.signature_validation.timeout.ms", 2_000);

  // prewarm the code cache
  String CODE_CACHE_PREWARM_PROP = "CODE_CACHE_PREWARM";
  String CODE_CACHE_LOCATION_PROP = "CODE_CACHE_LOCATION";
  BooleanValidator EXEC_CODE_CACHE_SAVE_EXPR = new BooleanValidator("exec.code_cache.save_expr", false);

  BooleanValidator ENABLE_RUNTIME_FILTER_ON_NON_PARTITIONED_PARQUET =  new BooleanValidator("exec.non_partitioned_parquet.enable_runtime_filter", false); // in beta right now
  RangeLongValidator RUNTIME_FILTER_VALUE_FILTER_MAX_SIZE =  new RangeLongValidator("exec.non_partitioned_parquet.runtime_filter.max_size", 10, 1_000_000, 100);

  String ENABLE_PARQUET_VECTORIZED_COMPLEX_READERS_KEY = "exec.parquet.enable_vectorized_complex";
  BooleanValidator ENABLE_PARQUET_VECTORIZED_COMPLEX_READERS = new BooleanValidator(ENABLE_PARQUET_VECTORIZED_COMPLEX_READERS_KEY, true);

  // Option to toggle support for mixed data types
  BooleanValidator MIXED_TYPES_DISABLED = new BooleanValidator("store.disable.mixed_types", false);
}
