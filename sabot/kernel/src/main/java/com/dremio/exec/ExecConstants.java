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
import com.dremio.exec.planner.sql.parser.PartitionDistributionStrategy;
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
import com.dremio.sabot.task.Observer;
import com.dremio.service.spill.DefaultSpillServiceOptions;

@Options
public interface ExecConstants {
  String ZK_CONNECTION = "dremio.exec.zk.connect";
  String ZK_TIMEOUT = "dremio.exec.zk.timeout";
  String ZK_SESSION_TIMEOUT = "dremio.exec.zk.session.timeout";
  String ZK_ROOT = "dremio.exec.zk.root";
  String ZK_REFRESH = "dremio.exec.zk.refresh";
  String ZK_RETRY_UNLIMITED = "dremio.exec.zk.retry.unlimited";
  String ZK_CONNECTION_HANDLE_ENABLED = "dremio.exec.zk.connection_handle.enabled";
  String ZK_RETRY_LIMIT = "dremio.exec.zk.retry.limit";
  String ZK_INITIAL_TIMEOUT_MS = "dremio.exec.zk.retry.initial_timeout_ms";
  String ZK_SUPERVISOR_INTERVAL_MS = "dremio.exec.zk.supervisor.interval_ms";
  String ZK_SUPERVISOR_READ_TIMEOUT_MS = "dremio.exec.zk.supervisor.read_timeout_ms";
  String ZK_SUPERVISOR_MAX_FAILURES = "dremio.exec.zk.supervisor.max_failures";

  String BIT_SERVER_RPC_THREADS = "dremio.exec.rpc.bit.server.threads";
  String USER_SERVER_RPC_THREADS = "dremio.exec.rpc.user.server.threads";
  String REGISTRATION_ADDRESS = "dremio.exec.rpc.publishedhost";

  /* incoming buffer size (number of batches) */
  RangeLongValidator INCOMING_BUFFER_SIZE = new RangeLongValidator("exec.buffer.size", 0, Integer.MAX_VALUE, 6);

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

  String EXPRESSION_CODE_CACHE_KEY = "exec.expression.byte_code_cache.enabled";
  BooleanValidator EXPRESSION_CODE_CACHE_ENABLED = new BooleanValidator(EXPRESSION_CODE_CACHE_KEY, true);
  String SPLIT_CACHING_ENABLED_KEY = "exec.expression.splits_cache.enabled";
  BooleanValidator SPLIT_CACHING_ENABLED = new BooleanValidator(SPLIT_CACHING_ENABLED_KEY, true);

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

  // Number above which we replace a group of ORs with a set operation.
  PositiveLongValidator FAST_OR_MIN_VARCHAR_THRESHOLD = new PositiveLongValidator("exec.operator.orfast.varchar_threshold.min", Integer.MAX_VALUE, 5);

  // Number above which we replace a group of ORs with a set operation in gandiva
  PositiveLongValidator FAST_OR_MIN_THRESHOLD_GANDIVA = new PositiveLongValidator("exec.operator.orfast.gandiva_threshold.min", Integer.MAX_VALUE, 20);

  // Number above which we replace a group of ORs with a set operation in gandiva
  PositiveLongValidator FAST_OR_MIN_VARCHAR_THRESHOLD_GANDIVA = new PositiveLongValidator("exec.operator.orfast.gandiva_varchar_threshold.min", Integer.MAX_VALUE, 5);

  // Number above which we stop replacing a group of ORs with a set operation.
  PositiveLongValidator FAST_OR_MAX_THRESHOLD = new PositiveLongValidator("exec.operator.orfast.threshold.max", Integer.MAX_VALUE, 1500);

  // Number above which JAVA codegen starts nesting methods
  PositiveLongValidator CODE_GEN_NESTED_METHOD_THRESHOLD = new PositiveLongValidator("exec.operator.codegen.nested_method.threshold", Integer.MAX_VALUE, 100);

  /**
   * Number of constants in expression above which constants are defined inside JAVA arrays in codegen.
   * When arrays are used, each constant takes up one element in the array. Otherwise, each constant is generated as a
   * separate class variable. The array approach gives more real estate in a generated class, before JAVA hits the
   * constant pool limit, as an array symbol takes up only 1 entry in the JAVA constant pool and an array can contain
   * as many constants of a given type.
   */
  PositiveLongValidator CODE_GEN_CONSTANT_ARRAY_THRESHOLD = new PositiveLongValidator("exec.operator.codegen.constant_array.threshold", Integer.MAX_VALUE, 5000);

  // if this option is set to false then CodeGenOption will be set to Java for any expression containing a complex struct
  BooleanValidator ENABLE_GANDIVA_CODEGEN_FOR_COMPOSITE = new BooleanValidator("exec.enable.composite.codegen.gandiva", true);

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
  PositiveLongValidator VARIABLE_WIDTH_VECTOR_MAX_USAGE_PERCENT =
    new PositiveLongValidator("exec.variable-width-vector.max-usage-percent", 100, 95);
  PositiveLongValidator OUTPUT_ALLOCATOR_RESERVATION = new PositiveLongValidator("exec.batch.output.alloc.reservation", Integer.MAX_VALUE, 512 * 1024);

  String OPERATOR_TARGET_BATCH_BYTES = "dremio.exec.operator_batch_bytes";
  OptionValidator OPERATOR_TARGET_BATCH_BYTES_VALIDATOR = new LongValidator(OPERATOR_TARGET_BATCH_BYTES, 10*1024*1024);

  BooleanValidator ENABLE_VECTORIZED_HASHAGG = new BooleanValidator("exec.operator.aggregate.vectorize", true);
  BooleanValidator ENABLE_VECTORIZED_HASHJOIN = new BooleanValidator("exec.operator.join.vectorize", true);
  BooleanValidator ENABLE_VECTORIZED_HASHJOIN_SPECIFIC = new BooleanValidator("exec.operator.join.vectorize.specific", false);
  BooleanValidator ENABLE_VECTORIZED_COPIER = new BooleanValidator("exec.operator.copier.vectorize", true);
  BooleanValidator ENABLE_VECTORIZED_COMPLEX_COPIER = new BooleanValidator("exec.operator.copier.complex.vectorize", true);
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
  DoubleValidator SMALL_PARQUET_BLOCK_SIZE_RATIO = new DoubleValidator("store.small.parquet.block-size-ratio", 0.5);
  String TARGET_COMBINED_SMALL_PARQUET_BLOCK_SIZE = "store.target.combined.small.parquet.block-size";
  LongValidator TARGET_COMBINED_SMALL_PARQUET_BLOCK_SIZE_VALIDATOR = new LongValidator(TARGET_COMBINED_SMALL_PARQUET_BLOCK_SIZE,
    PARQUET_BLOCK_SIZE_VALIDATOR.getDefault().getNumVal());

  String PARQUET_SPLIT_SIZE = "exec.parquet.split-size";
  LongValidator PARQUET_SPLIT_SIZE_VALIDATOR = new LongValidator(PARQUET_SPLIT_SIZE, 300*1024*1024);
  String ORC_SPLIT_SIZE = "exec.orc.split-size";
  LongValidator ORC_SPLIT_SIZE_VALIDATOR = new LongValidator(ORC_SPLIT_SIZE, 256*1024*1024);
  String AVRO_SPLIT_SIZE = "exec.avro.split-size";
  LongValidator AVRO_SPLIT_SIZE_VALIDATOR = new LongValidator(AVRO_SPLIT_SIZE, 256*1024*1024);
  String PARQUET_PAGE_SIZE = "store.parquet.page-size";
  LongValidator PARQUET_PAGE_SIZE_VALIDATOR = new LongValidator(PARQUET_PAGE_SIZE, 100000);
  String PARQUET_DICT_PAGE_SIZE = "store.parquet.dictionary.page-size";
  LongValidator PARQUET_DICT_PAGE_SIZE_VALIDATOR = new LongValidator(PARQUET_DICT_PAGE_SIZE, 1024*1024);
  String PARQUET_WRITER_COMPRESSION_TYPE = "store.parquet.compression";
  EnumeratedStringValidator PARQUET_WRITER_COMPRESSION_TYPE_VALIDATOR = new EnumeratedStringValidator(
      PARQUET_WRITER_COMPRESSION_TYPE, "snappy", "snappy", "gzip", "zstd", "none");
  String PARQUET_WRITER_COMPRESSION_ZSTD_LEVEL = "store.parquet.compression.zstd.level";
  RangeLongValidator PARQUET_WRITER_COMPRESSION_ZSTD_LEVEL_VALIDATOR = new RangeLongValidator(
    PARQUET_WRITER_COMPRESSION_ZSTD_LEVEL, Integer.MIN_VALUE, Integer.MAX_VALUE, -3);

  String PARQUET_MAX_FOOTER_LEN = "store.parquet.max_footer_length";
  LongValidator PARQUET_MAX_FOOTER_LEN_VALIDATOR = new LongValidator(PARQUET_MAX_FOOTER_LEN, 16*1024*1024);

  String PARQUET_MEMORY_THRESHOLD = "store.parquet.memory_threshold";
  LongValidator PARQUET_MEMORY_THRESHOLD_VALIDATOR = new LongValidator(PARQUET_MEMORY_THRESHOLD, 512*1024*1024);

  LongValidator PARQUET_MAX_PARTITION_COLUMNS_VALIDATOR = new RangeLongValidator("store.parquet.partition_column_limit", 0, 500, 25);

  BooleanValidator PARQUET_ELIMINATE_NULL_PARTITIONS = new BooleanValidator("store.parquet.exclude_null_implicit_partitions", true);

  String PARQUET_WRITER_ENABLE_DICTIONARY_ENCODING = "store.parquet.enable_dictionary_encoding";
  BooleanValidator PARQUET_WRITER_ENABLE_DICTIONARY_ENCODING_VALIDATOR = new BooleanValidator(
    PARQUET_WRITER_ENABLE_DICTIONARY_ENCODING, true);

  EnumeratedStringValidator PARQUET_WRITER_VERSION = new EnumeratedStringValidator("store.parquet.writer.version", "v1",
    "v1", "v2");

  String PARQUET_FILES_ESTIMATE_SCALING_FACTOR = "exec.parquet.parquet_files_estimate_scaling_factor";
  LongValidator PARQUET_FILES_ESTIMATE_SCALING_FACTOR_VALIDATOR = new LongValidator(PARQUET_FILES_ESTIMATE_SCALING_FACTOR, 1);

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
  BooleanValidator PARQUET_AUTO_CORRECT_DATES_VALIDATOR = new BooleanValidator(PARQUET_AUTO_CORRECT_DATES, false);

  BooleanValidator PARQUET_READER_VECTORIZE = new BooleanValidator("store.parquet.vectorize", true);
  BooleanValidator ENABLED_PARQUET_TRACING = new BooleanValidator("store.parquet.vectorize.tracing.enable", false);
  BooleanValidator ENABLED_PARQUET_VECTORIZED_DETAILED_STATS = new BooleanValidator(
      "store.parquet.vectorize.detailed.stats", false);
  BooleanValidator USE_COPIER_IN_PARQUET_READER = new BooleanValidator("store.parquet.use_copier", true);

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
  // option used to enable/disable conversions of complex types or incompatible data types to varchar
  BooleanValidator ENABLE_MONGO_VARCHAR_COERCION = new BooleanValidator("store.mongo.enable_incompatible_to_varchar_coercion", true);

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
   * Load reduction will only be triggered if the cluster load exceeds the cutoff value
   */
  DoubleValidator LOAD_CUT_OFF = new RangeDoubleValidator("load.cut_off", 0, Integer.MAX_VALUE, 3);

  /**
   * When applying load reduction, max width per node *= 1 - cluster load x load_reduction
   */
  DoubleValidator LOAD_REDUCTION = new RangeDoubleValidator("load.reduction", 0.01, 1, .1);

  BooleanValidator ENABLE_REATTEMPTS = new BooleanValidator("exec.reattempt.enable", true);

  LongValidator REATTEMPT_LIMIT = new PositiveLongValidator("exec.reattempt.limit", 20, 10);

  // To re-attempt a query on OOM, this one and ENABLE_REATTEMPTS should both be set to true.
  BooleanValidator ENABLE_REATTEMPTS_ON_OOM = new BooleanValidator("exec.reattempt.on_oom.enable", false);

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
  DoubleValidator HASH_JOIN_TABLE_FACTOR = new DoubleValidator(HASH_JOIN_TABLE_FACTOR_KEY, 1.1d);

  String HASH_AGG_TABLE_FACTOR_KEY = "planner.memory.hash_agg_table_factor";
  DoubleValidator HASH_AGG_TABLE_FACTOR = new DoubleValidator(HASH_AGG_TABLE_FACTOR_KEY, 1.1d);

  String AVERAGE_FIELD_WIDTH_KEY = "planner.memory.average_field_width";
  LongValidator AVERAGE_FIELD_WIDTH = new PositiveLongValidator(AVERAGE_FIELD_WIDTH_KEY, Long.MAX_VALUE, 8);

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
  String MAX_SPLIT_CACHE_SIZE_CONFIG = "dremio.exec.compile.split_cache_max_size";

  // enable EXTEND on SELECT
  BooleanValidator ENABLE_EXTEND_ON_SELECT = new BooleanValidator("debug.extend_on_select.enabled", false);

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

  BooleanValidator DATA_SCAN_PARALLELISM = new BooleanValidator("dremio.limit_data_scan_parallelism.enabled", false);

  /**
   * If set to true, soft affinity will be ignored for leaf fragments during parallelization.
   */
  BooleanValidator SHOULD_IGNORE_LEAF_AFFINITY = new BooleanValidator("planner.assignment.ignore_leaf_affinity", false);

  BooleanValidator SHOULD_ASSIGN_FRAGMENT_PRIORITY = new BooleanValidator("planner.assign_priority", true);

  /**
   * Enabling this support option enables the memory arbiter
   */
  BooleanValidator ENABLE_SPILLABLE_OPERATORS = new BooleanValidator("exec.spillable.operators.enabled", false);
  // max memory that can be granted for a run - default: 40MB, max: 100MB
  LongValidator MAX_MEMORY_GRANT_SIZE = new PositiveLongValidator("exec.spillable.operators.max_memory_grant_bytes", 100 * (1 << 20), 40 * (1 << 20));

  // if true dynamically track and consider prev N number of allocations for next allocation grant, else consider all previous allocations
  BooleanValidator DYNAMICALLY_TRACK_ALLOCATIONS = new BooleanValidator("exec.spillable.operators.dynamically_track_allocations", true);

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

  // Time after which the jobs are expired and deleted from the job collection. 0 by default. Used for testing and debugging TTL based expiry.
  LongValidator DEBUG_TTL_JOB_MAX_AGE_IN_MILLISECONDS = new LongValidator("debug.ttl.jobs.max.age_in_milliseconds", 0);

  LongValidator JOB_MAX_AGE_IN_DAYS = new LongValidator("jobs.max.age_in_days", 30);
  // At what hour of the day to do job cleanup - 0-23
  RangeLongValidator JOB_CLEANUP_START_HOUR = new RangeLongValidator("job.cleanup.start_at_hour", 0, 23, 1);

  //Configuration used for testing or debugging
  LongValidator DEBUG_RESULTS_MAX_AGE_IN_MILLISECONDS = new LongValidator("debug.results.max.age_in_milliseconds", 0);

  BooleanValidator SORT_FILE_BLOCKS = new BooleanValidator("store.file.sort_blocks", false);

  PositiveLongValidator LIMIT_FIELD_SIZE_BYTES = new PositiveLongValidator("limits.single_field_size_bytes", Integer.MAX_VALUE, 32000);

  PositiveLongValidator DELTALAKE_METADATA_FIELD_SIZE_BYTES = new PositiveLongValidator("store.deltalake.metadata_field_size_bytes", Integer.MAX_VALUE, 16_777_216L);

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
  PositiveLongValidator LISTING_SERVICE_REFRESH_MILLIS  = new PositiveLongValidator("dac.listing.refresh.ms",  Long.MAX_VALUE, TimeUnit.MINUTES.toMillis(1));

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
  BooleanValidator ENABLE_NDV_REDUCE_HEAP = new BooleanValidator("exec.operator.ndv_reduce_heap", true);
  BooleanValidator ENABLE_VECTORIZED_SPILL_NDV_ACCUMULATOR = new BooleanValidator("exec.operator.vectorized_spill.ndv", true);

  BooleanValidator ENABLE_VECTORIZED_SPILL_VARCHAR_ACCUMULATOR = new BooleanValidator("exec.operator.vectorized_spill.varchar", true);

  BooleanValidator TRIM_ROWGROUPS_FROM_FOOTER = new BooleanValidator("exec.parquet.memory.trim_rowgroups", true);
  BooleanValidator TRIM_COLUMNS_FROM_ROW_GROUP = new BooleanValidator("exec.parquet.memory.trim_columns", true);

  AdminBooleanValidator EXECUTOR_ENABLE_HEAP_MONITORING = new AdminBooleanValidator("exec.heap.monitoring.enable", true);
  RangeLongValidator EXECUTOR_HEAP_MONITORING_CLAWBACK_THRESH_PERCENTAGE = new RangeLongValidator("exec.heap.monitoring.thresh.percentage", 50, 100, 85);
  RangeLongValidator EXECUTOR_HEAP_MONITORING_AGGRESSIVE_WIDTH_LOWER_BOUND = new RangeLongValidator("exec.heap.monitoring.aggressive.lower.bound", 1, 16 * 256, 256);
  RangeLongValidator EXECUTOR_HEAP_MONITORING_LOW_MEM_THRESH_PERCENTAGE = new RangeLongValidator("exec.heap.monitoring.low_mem.percentage", 40, 100, 75);
  RangeLongValidator EXECUTOR_HEAP_MONITOR_DELAY_MILLIS  = new RangeLongValidator("exec.heap.monitoring.delay.millis",0,  Long.MAX_VALUE, 2_000);

  AdminBooleanValidator COORDINATOR_ENABLE_HEAP_MONITORING = new AdminBooleanValidator("coordinator.heap.monitoring.enable", true);
  RangeLongValidator COORDINATOR_HEAP_MONITORING_CLAWBACK_THRESH_PERCENTAGE = new RangeLongValidator("coordinator.heap.monitoring.thresh.percentage", 50, 100, 85);
  RangeLongValidator COORDINATOR_HEAP_MONITOR_DELAY_MILLIS  = new RangeLongValidator("coordinator.heap.monitoring.delay.millis", 0,  Long.MAX_VALUE, 2_000);

  AdminBooleanValidator ENABLE_RECONCILE_QUERIES = new AdminBooleanValidator("coordinator.reconcile.queries.enable", true);
  RangeLongValidator RECONCILE_QUERIES_FREQUENCY_SECS = new RangeLongValidator("coordinator.reconcile.queries.frequency.secs", 1, 1800, 300);

  BooleanValidator ENABLE_DEPRECATED_JOBS_USER_STATS_API = new BooleanValidator("dremio.deprecated_jobs_user_stats_api.enabled", true);
  BooleanValidator ENABLE_JOBS_USER_STATS_API = new BooleanValidator("dremio.jobs_user_stats_api.enabled", true);
  PositiveLongValidator JOBS_USER_STATS_CACHE_REFRESH_HRS = new PositiveLongValidator("dremio.jobs_user_stats_cache.refresh_hrs", Integer.MAX_VALUE, 12L);
  BooleanValidator ENABLE_ICEBERG = new BooleanValidator("dremio.iceberg.enabled", true);
  BooleanValidator ENABLE_ICEBERG_ADVANCED_DML = new BooleanValidator("dremio.iceberg.advanced_dml.enabled", true);
  BooleanValidator ENABLE_ICEBERG_ADVANCED_DML_JOINED_TABLE = new BooleanValidator("dremio.iceberg.advanced_dml.joined_table.enabled", true);
  BooleanValidator ENABLE_ICEBERG_ADVANCED_DML_MERGE_STAR = new BooleanValidator("dremio.iceberg.advanced_dml.merge_star.enabled", true);
  BooleanValidator ENABLE_ICEBERG_DML = new BooleanValidator("dremio.iceberg.dml.enabled", true);
  BooleanValidator ENABLE_COPY_INTO = new BooleanValidator("dremio.copy.into.enabled", true);
  BooleanValidator ENABLE_DML_DISPLAY_RESULT_ONLY = new BooleanValidator("dremio.dml.display.result.only.enabled", false);
  BooleanValidator ENABLE_ICEBERG_MIN_MAX = new BooleanValidator("dremio.iceberg.min_max.enabled", true);
  BooleanValidator CTAS_CAN_USE_ICEBERG = new BooleanValidator("dremio.iceberg.ctas.enabled", true);
  BooleanValidator ENABLE_ICEBERG_SPEC_EVOL_TRANFORMATION = new BooleanValidator("dremio.iceberg.spec_evol_and_transformation.enabled", true);
  BooleanValidator ENABLE_PARTITION_STATS_USAGE = new BooleanValidator("dremio.use_partition_stats_enabled", true);
  BooleanValidator ENABLE_ICEBERG_TIME_TRAVEL = new BooleanValidator("dremio.iceberg.time_travel.enabled", true);
  BooleanValidator ENABLE_ICEBERG_PARTITION_TRANSFORMS = new BooleanValidator("dremio.iceberg.partition_transforms.enabled", true);
  BooleanValidator ENABLE_ICEBERG_METADATA_FUNCTIONS = new BooleanValidator("dremio.iceberg.metadata_functions.enabled", true);
  BooleanValidator ENABLE_ICEBERG_MERGE_ON_READ_SCAN = new BooleanValidator("dremio.iceberg.merge_on_read_scan.enabled", true);
  BooleanValidator ENABLE_ICEBERG_MERGE_ON_READ_SCAN_WITH_EQUALITY_DELETE =
    new BooleanValidator("dremio.iceberg.merge_on_read_scan_with_equality_delete.enabled", false);
  BooleanValidator ENABLE_ICEBERG_COMBINE_SMALL_FILES_FOR_DML = new BooleanValidator("dremio.iceberg.combine_small_files_for_dml.enabled", false);
  BooleanValidator ENABLE_ICEBERG_COMBINE_SMALL_FILES_FOR_OPTIMIZE = new BooleanValidator("dremio.iceberg.combine_small_files_for_optimize.enabled", true);
  BooleanValidator ENABLE_ICEBERG_COMBINE_SMALL_FILES_FOR_PARTITIONED_TABLE_WRITES = new BooleanValidator("dremio.iceberg.combine_small_files_for_partitioned_table_writes.enabled", false);
  BooleanValidator ENABLE_ICEBERG_DML_WITH_NATIVE_ROW_COLUMN_POLICIES = new BooleanValidator("dremio.iceberg.dml.native_row_column_policies.enabled", false);
  BooleanValidator ENABLE_ICEBERG_TABLE_PROPERTIES = new BooleanValidator("dremio.iceberg.table.properties.enabled", true);

  BooleanValidator ENABLE_ICEBERG_ROLLBACK = new BooleanValidator("dremio.iceberg.rollback.enabled", true);
  BooleanValidator ENABLE_ICEBERG_VACUUM = new BooleanValidator("dremio.iceberg.vacuum.enabled", true);
  BooleanValidator ENABLE_ICEBERG_VACUUM_CATALOG = new BooleanValidator("dremio.iceberg.vacuum.catalog.enabled", true);
  BooleanValidator ENABLE_ICEBERG_VACUUM_CATALOG_ON_AZURE = new BooleanValidator("dremio.iceberg.vacuum.catalog.azure.enabled", false);

  BooleanValidator ENABLE_ICEBERG_SINGLE_MANIFEST_WRITER = new BooleanValidator("dremio.iceberg.single_manifest_writer.enabled", true);
  BooleanValidator ENABLE_ICEBERG_VACUUM_REMOVE_ORPHAN_FILES = new BooleanValidator("dremio.iceberg.vacuum.remove_orphan_files.enabled", false);
  BooleanValidator ENABLE_ICEBERG_SORT_ORDER = new BooleanValidator("dremio.iceberg.sort.order.enabled", true);
  LongValidator ICEBERG_VACUUM_CATALOG_RETENTION_PERIOD_MINUTES = new RangeLongValidator("dremio.iceberg.vacuum.catalog.default_retention.mins", 0, Long.MAX_VALUE, TimeUnit.DAYS.toMinutes(5));
  BooleanValidator ENABLE_UNLIMITED_SPLITS_METADATA_CLEAN = new BooleanValidator("dremio.unlimited_splits.metadata.clean.enabled", true);

  BooleanValidator ENABLE_HIVE_DATABASE_LOCATION = new BooleanValidator("dremio.hive.database.location", true);
  BooleanValidator ENABLE_QUERY_LABEL = new BooleanValidator("dremio.query.label.enabled", true);

  BooleanValidator ENABLE_UNLIMITED_SPLITS_DISTRIBUTED_STORAGE_RELOCATION = new BooleanValidator("dremio.execution.support_unlimited_splits_distributed_storage_relocation", true);

  LongValidator OPTIMIZE_TARGET_FILE_SIZE_MB = new PositiveLongValidator("dremio.iceberg.optimize.target_file_size_mb", Long.MAX_VALUE, 256L);
  DoubleValidator OPTIMIZE_MINIMUM_FILE_SIZE_DEFAULT_RATIO = new DoubleValidator("dremio.iceberg.optimize.min_file_size_ratio", 0.75);
  DoubleValidator OPTIMIZE_MAXIMUM_FILE_SIZE_DEFAULT_RATIO = new DoubleValidator("dremio.iceberg.optimize.max_file_size_ratio", 1.8);
  LongValidator OPTIMIZE_MINIMUM_INPUT_FILES = new LongValidator("dremio.iceberg.optimize.min_input_files", 5);

  BooleanValidator ENABLE_USE_VERSION_SYNTAX = new TypeValidators.BooleanValidator("dremio.sql.use_version.enabled", true);
  BooleanValidator VERSIONED_VIEW_ENABLED = new TypeValidators.BooleanValidator("plugins.dataplane.view", true);

  // Enable info schema query in any versioned sources
  BooleanValidator VERSIONED_INFOSCHEMA_ENABLED = new TypeValidators.BooleanValidator("arctic.infoschema.enabled", true);
  BooleanValidator ENABLE_AZURE_SOURCE = new TypeValidators.BooleanValidator("dremio.enable_azure_source", false);

  // warning threshold for running time of a task
  PositiveLongValidator SLICING_WARN_MAX_RUNTIME_MS = new PositiveLongValidator("dremio.sliced.warn_max_runtime", Long.MAX_VALUE, 20000);
  BooleanValidator SLICING_THREAD_MONITOR = new BooleanValidator("dremio.sliced.enable_monitor", true);
  BooleanValidator SLICING_OFFLOAD_ENQUEUE = new BooleanValidator("dremio.sliced.offload_enqueue", true);
  TypeValidators.EnumValidator<Observer.Type> SLICING_OBSERVER_TYPE =
    new TypeValidators.EnumValidator<>("dremio.sliced.observer_type", Observer.Type.class, Observer.Type.BASIC);

  PositiveLongValidator SLICING_THREAD_MIGRATION_MULTIPLE = new com.dremio.options.TypeValidators.PositiveLongValidator("dremio.sliced.migration_multiple", Long.MAX_VALUE, 50);
  PositiveLongValidator SLICING_THREAD_SPINDOWN_MULTIPLE = new com.dremio.options.TypeValidators.PositiveLongValidator("dremio.sliced.spindown_multiple", Long.MAX_VALUE, 100);

  // warning threshold for spilling
  PositiveLongValidator SPILL_IO_WARN_MAX_RUNTIME_MS = new PositiveLongValidator("dremio.spill.warn_max_runtime", Long.MAX_VALUE, 3000);

  // warning threshold for long IO time
  LongValidator STORE_IO_TIME_WARN_THRESH_MILLIS = new LongValidator("store.io_time_warn_thresh_millis", 10000);

  // global hive-async option
  BooleanValidator ENABLE_HIVE_ASYNC = new TypeValidators.BooleanValidator("store.hive.async", true);

  BooleanValidator ENABLE_REMOTE_JOB_FETCH = new BooleanValidator("jobs.remote.fetch_enabled", true);

  DoubleValidator EXPR_COMPLEXITY_NO_OPTIMIZE_THRESHOLD = new DoubleValidator("exec.expression.complexity.no_optimize.threshold", 2000.00);

  BooleanValidator ENABLE_ASYNC_READER_REUSE = new BooleanValidator("exec.storage.enable_async_reader_reuse", true);
  BooleanValidator ENABLE_BOOSTING = new BooleanValidator("exec.storage.enable_arrow_caching", true);
  BooleanValidator ENABLE_BOOST_FILTERING_READER = new BooleanValidator("exec.storage.enable_arrow_filtering_reader", true);
  BooleanValidator ENABLE_BOOST_DELTA_READER = new BooleanValidator("exec.storage.enable_arrow_delta_reader", true);

  // hive parallelism and timeout options for signature validation process
  LongValidator HIVE_SIGNATURE_VALIDATION_PARALLELISM = new TypeValidators.RangeLongValidator("store.hive.signature_validation.parallelism", 1, 32, 16);
  LongValidator HIVE_SIGNATURE_VALIDATION_TIMEOUT_MS = new TypeValidators.LongValidator("store.hive.signature_validation.timeout.ms", 2_000);

  BooleanValidator HIVE_SIGNATURE_CHANGE_RECURSIVE_LISTING = new BooleanValidator("store.hive.signature_change_recursive_listing", false);

  // prewarm the code cache
  String CODE_CACHE_PREWARM_PROP = "CODE_CACHE_PREWARM";
  String CODE_CACHE_LOCATION_PROP = "CODE_CACHE_LOCATION";
  BooleanValidator EXEC_CODE_CACHE_SAVE_EXPR = new BooleanValidator("exec.code_cache.save_expr", false);

  BooleanValidator ENABLE_RUNTIME_FILTER_ON_NON_PARTITIONED_PARQUET =  new BooleanValidator("exec.non_partitioned_parquet.enable_runtime_filter", true);
  RangeLongValidator RUNTIME_FILTER_VALUE_FILTER_MAX_SIZE = new RangeLongValidator("exec.non_partitioned_parquet.runtime_filter.max_size", 10, 1_000_000, 10000);
  RangeLongValidator RUNTIME_FILTER_KEY_MAX_SIZE = new RangeLongValidator("exec.runtime_filter.max_key_size", 32, 1_024, 128);
  BooleanValidator ENABLE_ROW_LEVEL_RUNTIME_FILTERING = new BooleanValidator("exec.row_level.runtime_filter.enable", true);
  String ENABLE_PARQUET_VECTORIZED_COMPLEX_READERS_KEY = "exec.parquet.enable_vectorized_complex";
  BooleanValidator ENABLE_PARQUET_VECTORIZED_COMPLEX_READERS = new BooleanValidator(ENABLE_PARQUET_VECTORIZED_COMPLEX_READERS_KEY, true);
  BooleanValidator PREFETCH_READER = new BooleanValidator("store.parquet.prefetch_reader", true);
  BooleanValidator READ_COLUMN_INDEXES = new BooleanValidator("store.parquet.read_column_indexes", true);
  // Increasing this will increase the number of splits that are prefetched. Unfortunately, it can also lead to multiple footer reads
  // if the future splits are from the same file
  RangeLongValidator NUM_SPLITS_TO_PREFETCH = new RangeLongValidator("store.parquet.num_splits_to_prefetch", 1, 20L, 1);

  // Use this as a factor to scale the rowcount estimation of number of rows in a data file
  DoubleValidator DELTALAKE_ROWCOUNT_ESTIMATION_FACTOR = new RangeDoubleValidator("store.delta.rowcount_estimation_factor", 0.8d, 2.0d, 1.25d);
  LongValidator DELTALAKE_MAX_ADDED_FILE_ESTIMATION_LIMIT = new PositiveLongValidator("store.delta.max.added_file_estimation_limit", Integer.MAX_VALUE, 100);
  BooleanValidator DELTA_LAKE_ENABLE_FULL_ROWCOUNT = new BooleanValidator("store.deltalake.enable_full_rowcount", true);

  StringValidator DISABLED_GANDIVA_FUNCTIONS = new StringValidator("exec.disabled.gandiva-functions", "");
  BooleanValidator GANDIVA_TARGET_HOST_CPU = new BooleanValidator("exec.gandiva.target_host_cpu", true);
  BooleanValidator GANDIVA_OPTIMIZE = new BooleanValidator("exec.gandiva.optimize_ir", true);
  String ICEBERG_CATALOG_TYPE_KEY = "iceberg.catalog_type";
  String ICEBERG_NAMESPACE_KEY = "iceberg.namespace";
  BooleanValidator HADOOP_BLOCK_CACHE_ENABLED = new BooleanValidator("hadoop_block_affinity_cache.enabled", true);

  BooleanValidator ENABLE_DELTALAKE_HIVE_SUPPORT = new BooleanValidator("store.deltalake.hive_support.enabled", true);

  BooleanValidator ENABLE_DELTALAKE_SPARK_SUPPORT = new BooleanValidator("store.deltalake.spark_support.enabled", true);

  BooleanValidator ENABLE_DELTALAKE_TIME_TRAVEL = new BooleanValidator("dremio.deltalake.time_travel.enabled", true);

  /**
   * Controls the 'compression' factor for the TDigest algorithm.
   */
  LongValidator TDIGEST_COMPRESSION = new PositiveLongValidator("exec.statistics.tdigest_compression", 10000L, 100);

  LongValidator ITEMS_SKETCH_MAX_SIZE = new PositiveLongValidator("exec.statistics.items_sketch_max_size", Integer.MAX_VALUE, 32_768);

  BooleanValidator DELTA_LAKE_ENABLE_STATS_READ = new BooleanValidator("store.deltalake.enable_stats_read", true);

  // Option to log the generated Java code on code generation exceptions
  BooleanValidator JAVA_CODE_DUMP = new BooleanValidator("exec.codegen.dump_java_code", false);

  BooleanValidator METADATA_CLOUD_CACHING_ENABLED = new BooleanValidator("metadata.cloud.cache.enabled", true);

  // default Nessie namespace used for internal iceberg tables created during refresh dataset
  StringValidator NESSIE_METADATA_NAMESPACE = new StringValidator("metadata.nessie_iceberg_namespace", "dremio.internal");

  // option used to determine whether footer reader needs to read footer for accurate row counts or not
  BooleanValidator STORE_ACCURATE_PARTITION_STATS = new BooleanValidator("store.accurate.partition_stats", true);

  // Option to enable SysFlight Storage Plugin
  BooleanValidator ENABLE_SYSFLIGHT_SOURCE = new BooleanValidator("sys.flight.enabled", true);

  // option used to determine we should consider the previous timestamp or not, false value to disable check.
  BooleanValidator ENABLE_STORE_PARQUET_ASYNC_TIMESTAMP_CHECK = new BooleanValidator("store.parquet.async.enable_timestamp_check", true);

  // option used to determine S3AsyncClient should get used or S3SyncWithAsync wrapper, false value to support prev implementation
  BooleanValidator S3_NATIVE_ASYNC_CLIENT = new BooleanValidator("dremio.s3.use_native_async_client", true);

  // option used to enable/disable internal schema
  BooleanValidator ENABLE_INTERNAL_SCHEMA = new BooleanValidator("dremio.enable_user_managed_schema", true);

  // option used to enable/disable arrow caching for parquet dataset
  BooleanValidator ENABLE_PARQUET_ARROW_CACHING = new BooleanValidator("dremio.enable_parquet_arrow_caching", false);

  // option used to fallback on name based mapping during iceberg reads when parquet files do not contain IDs
  BooleanValidator ENABLE_ICEBERG_FALLBACK_NAME_BASED_READ = new BooleanValidator("dremio.iceberg.fallback_to_name_based_reader", false);

  // option used to use batch schema to get the field type while resolving the projected column to parquet column names
  BooleanValidator ENABLE_ICEBERG_USE_BATCH_SCHEMA_FOR_RESOLVING_COLUMN = new BooleanValidator("dremio.iceberg.use_batch_schema_for_resolving_columns", true);

  // option used to enable rle and packed stats using ReaderTimer
  BooleanValidator ENABLE_PARQUET_PERF_MONITORING = new BooleanValidator("dremio.exec.parquet_enable_perf_monitoring", false);

  PositiveLongValidator FRAGMENT_STARTER_TIMEOUT = new PositiveLongValidator("exec.startfragment.min.rpc.timeout.millis", Integer.MAX_VALUE, 30000);

  StringValidator CATALOG_SERVICE_LOCAL_TASK_LEADER_NAME = new StringValidator("catalog.service.local.task.leader.name", "catalogserviceV3");

  // option used to set the dremio parquet page size estimate.
  RangeLongValidator PAGE_SIZE_IN_BYTES = new RangeLongValidator("dremio.parquet.page_size_estimate", 1*1024, 500*1024, 100*1024);

  // option used to set the total java heap object in memory based on parquet footer.
  RangeLongValidator TOTAL_HEAP_OBJ_SIZE = new RangeLongValidator("dremio.parquet.num_footer_heap_objects", 0, 100000, 50000);

  BooleanValidator ENABLE_PARQUET_MIXED_TYPES_COERCION = new BooleanValidator("dremio.parquet.enable_mixed_types_coercion", false);

  BooleanValidator ENABLE_IN_PROCESS_TUNNEL = new BooleanValidator("dremio.exec.inprocess.tunnel.enabled", true);

  PositiveLongValidator ORPHANAGE_ENTRY_CLEAN_PERIOD_MINUTES  = new PositiveLongValidator("dremio.orphanage.entry_cleanup_period_minutes",  Long.MAX_VALUE, 5);
  RangeLongValidator ORPHANAGE_PROCESSING_THREAD_COUNT = new RangeLongValidator("dremio.orphanage.processing_thread_count", 1, 100000, 3);

  BooleanValidator CREATE_HIVECLIENT_ON_EXECUTOR_NODES = new BooleanValidator("store.hive.create_client_on_executor", true);

  // perform join analysis after query completes
  BooleanValidator ENABLE_JOIN_ANALYSIS_POPULATOR = new BooleanValidator("jobs.join.analysis.populator", true);

  // Outstanding RPCs per tunnel from executor to coordinator
  // Though it is of long type, the value should be within int range.
  RangeLongValidator OUTSTANDING_RPCS_PER_TUNNEL = new RangeLongValidator("dremio.exec.outstanding_rpcs_per_tunnel", 3, 64, 3);

  BooleanValidator ENABLE_NATIVE_ROW_COLUMN_POLICIES = new BooleanValidator("dremio.native_row_column_policies.enabled", true);

  // Controls behavior to run the table function phase on entire engine
  BooleanValidator TABLE_FUNCTION_WIDTH_USE_ENGINE_SIZE = new BooleanValidator("dremio.exec.table_func_width_use_engine_size", true);

  PositiveLongValidator TABLE_FUNCTION_WIDTH_EXPAND_UPTO_ENGINE_SIZE = new PositiveLongValidator("dremio.exec.table_func_width_expand_upto_engine_size ", Long.MAX_VALUE, 16);

  //option used to enable pattern based log obfuscation
  BooleanValidator ENABLE_PATTERN_BASED_LOG_OBFUSCATION = new BooleanValidator("dremio.logging.enable_pattern_obfuscation", false);

  //option used to append message when a log message is obfuscated. this will be used in non prod environments for debugging purposes
  BooleanValidator APPEND_MESSAGE_TO_OBFUSCATED_LOG = new BooleanValidator("dremio.logging.append_message", false);

  BooleanValidator ENABLE_GANDIVA_PERSISTENT_CACHE = new BooleanValidator("exec.gandiva.enable_persistent_cache", false);

  DoubleValidator EXPR_COMPLEXITY_NO_CACHE_THRESHOLD = new DoubleValidator("exec.expression.complexity.no_cache.threshold", 100.00);

  BooleanValidator ENABLE_MAP_DATA_TYPE = new BooleanValidator("dremio.data_types.map.enabled", true);
  BooleanValidator ENABLE_COMPLEX_HIVE_DATA_TYPE = new BooleanValidator("dremio.data_types.hive_complex.enabled", true);

  BooleanValidator EARLY_ACK_ENABLED = new BooleanValidator("dremio.jdbc_client.early_ack.enabled", true);

  BooleanValidator RESULTS_CLEANUP_SERVICE_ENABLED = new BooleanValidator("dremio.results_cleanup.enabled", false);

  BooleanValidator CATALOG_JOB_COUNT_ENABLED = new BooleanValidator("catalog.job_count.enabled", true);

  // Option to specify the size to truncate large SQL queries; setting value to 0 disables truncation
  LongValidator SQL_TEXT_TRUNCATE_LENGTH = new LongValidator("jobs.sql.truncate.length", 32000);

  // Maximal DOP per hash for AdaptiveHashExchangePrel. ratio = dop / maximal parallel width
  DoubleValidator ADAPTIVE_HASH_DOP_RATIO = new DoubleValidator("adaptive.hash.dop.ratio", 0);

  // Should do adaptive hash distribution
  BooleanValidator ADAPTIVE_HASH = new TypeValidators.BooleanValidator("adaptive.hash.enabled", true);

  // Minimal row count to start adaptive hashing for AdaptiveHashExchangePrel
  PositiveLongValidator ADAPTIVE_HASH_START_ROWCOUNT = new PositiveLongValidator("adaptive.hash.start.rowcount", Integer.MAX_VALUE,100 * 1024);

  BooleanValidator NESSIE_SOURCE_API = new TypeValidators.BooleanValidator("nessie.source.api", true);

  BooleanValidator PARQUET_READER_VECTORIZE_FOR_V2_ENCODINGS = new BooleanValidator("vectorized.read.parquet.v2.encodings", true);

  BooleanValidator ENABLE_COPY_INTO_CONTINUE = new BooleanValidator("dremio.copy.into.continue.enabled", true);

  PositiveLongValidator COPY_ERRORS_TABLE_FUNCTION_MAX_INPUT_FILES = new PositiveLongValidator("dremio.copy.into.errors_max_input_files", 1000, 100);

  PositiveLongValidator SYSTEM_ICEBERG_TABLES_SCHEMA_VERSION = new PositiveLongValidator("dremio.system_iceberg_tables.schema.version", Long.MAX_VALUE, 1);

  PositiveLongValidator SYSTEM_ICEBERG_TABLES_WRITE_BATCH_SIZE = new PositiveLongValidator("dremio.system_iceberg_tables.write.batch.size", Long.MAX_VALUE, 1000);

  PositiveLongValidator SYSTEM_ICEBERG_TABLES_RECORD_LIFESPAN_IN_MILLIS = new PositiveLongValidator("dremio.system_iceberg_tables.record_lifespan_in_millis", Long.MAX_VALUE, TimeUnit.DAYS.toMillis(7));

  PositiveLongValidator SYSTEM_ICEBERG_TABLES_HOUSEKEEPING_THREAD_FREQUENCY_IN_MILLIS = new PositiveLongValidator("dremio.system_iceberg_tables.housekeeping_thread_frequency_in_millis", Long.MAX_VALUE, TimeUnit.DAYS.toMillis(1));
  /**
   * Controls the WARN logging threshold for {@link com.dremio.exec.store.dfs.LoggedFileSystem} calls.
   */
  RangeLongValidator FS_LOGGER_WARN_THRESHOLD_MS = new RangeLongValidator("filesystem.logger.warn.io_threshold_ms", 0, Long.MAX_VALUE, 5000);

  /**
   * Controls the DEBUG logging threshold for {@link com.dremio.exec.store.dfs.LoggedFileSystem} calls.
   */
  RangeLongValidator FS_LOGGER_DEBUG_THRESHOLD_MS = new RangeLongValidator("filesystem.logger.debug.io_threshold_ms", 0, Long.MAX_VALUE, 50);

  /**
   * Enables the use of SourceCapabilities.USE_NATIVE_PRIVILEGES for Versioned sources.
   */
  BooleanValidator VERSIONED_SOURCE_CAPABILITIES_USE_NATIVE_PRIVILEGES_ENABLED = new TypeValidators.BooleanValidator("versioned.source_capabilities.use_native_privileges.enabled", false);

  TypeValidators.EnumValidator<PartitionDistributionStrategy> WRITER_PARTITION_DISTRIBUTION_MODE =
    new TypeValidators.EnumValidator<>("store.writer.partition_distribution_mode", PartitionDistributionStrategy.class, PartitionDistributionStrategy.HASH);

  /**
   * Enables the system iceberg table's storage plugin.
   */
  BooleanValidator ENABLE_SYSTEM_ICEBERG_TABLES_STORAGE = new BooleanValidator("dremio.system_iceberg_tables.storage.enabled", true);

  BooleanValidator JOBS_COUNT_FAST_ENABLED = new BooleanValidator("jobs.count.fast.enabled", false);

  /**
   * If set to true, DirListingRecordReader will exclude files with modified times greater than the start of the
   * query from the listing.
   */
  BooleanValidator DIR_LISTING_EXCLUDE_FUTURE_MOD_TIMES = new BooleanValidator("store.dirlisting.exclude_future_mod_times", true);

  /**
   * Controls whether optimal or legacy partition chunking behavior is used with Easy format plugins.  With this
   * enabled, Easy format datasets will store multiple splits per partition chunk in the KV store.  The legacy
   * behavior - with this key set to false - will result in Easy format datasets having a single split per partition
   * chunk in cases where file modification times are unique.
   */
  BooleanValidator ENABLE_OPTIMAL_FILE_PARTITION_CHUNKS = new BooleanValidator("store.file.enable_optimal_partition_chunks", true);

  /**
   * Sets the max number of splits stored in each partition chunk for Easy format based datasets on filesystem sources.
   * Splits beyond this limit will create additional partition chunks for the given partition. This is used to limit the
   * size of individual rows in the metadata-multi-splits KV store collection.
   */
  RangeLongValidator FILE_SPLITS_PER_PARTITION_CHUNK = new RangeLongValidator("store.file.splits_per_partition_chunk", 0, Integer.MAX_VALUE, 1000);

  /**
   * Enables SHOW CREATE VIEW/TABLE syntax
   */
  BooleanValidator SHOW_CREATE_ENABLED = new BooleanValidator("dremio.sql.show.create.enabled", true);

  /**
   * Enables pipe mgmt feature for ingestion.
   * The feature is not functionally complete. Exercise caution while enabling it.
   */
  BooleanValidator INGESTION_PIPES_ENABLED = new BooleanValidator("dremio.ingestion.pipes.enabled", false);

  RangeLongValidator INGESTION_DEDUP_DEFAULT_PERIOD = new RangeLongValidator("dremio.ingestion.pipes.dedup.default", 1, 365, 14);

  /**
   * Option to customize the amount of time in ms that old Unlimited Split snapshots are kept before they are expired
   */
  PositiveLongValidator DEFAULT_PERIOD_TO_KEEP_SNAPSHOTS_MS = new PositiveLongValidator("dremio.unlimited_splits.metadata.snapshots.ttl", Long.MAX_VALUE, 8 * 24 * 3600 * 1000L); //8 days

  PositiveLongValidator HASHAGG_MAX_BATCH_SIZE = new PositiveLongValidator("dremio.hashagg_max_batch_size", Integer.MAX_VALUE, 0);

  @Deprecated // The ability to disable path traversal prevention is slated to be fully removed in a future release
  BooleanValidator FS_PATH_TRAVERSAL_PREVENTION_ENABLED = new AdminBooleanValidator("filesystem.path_traversal_prevention.enabled", true);

}
