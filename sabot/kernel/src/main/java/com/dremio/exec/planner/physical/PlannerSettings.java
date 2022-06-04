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
package com.dremio.exec.planner.physical;


import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.plan.Context;
import org.apache.calcite.util.CancelFlag;

import com.dremio.common.config.SabotConfig;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.server.options.CachingOptionResolver;
import com.dremio.exec.store.sys.statistics.StatisticsService;
import com.dremio.exec.testing.ExecutionControls;
import com.dremio.options.OptionResolver;
import com.dremio.options.OptionValidator;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators;
import com.dremio.options.TypeValidators.AdminBooleanValidator;
import com.dremio.options.TypeValidators.BooleanValidator;
import com.dremio.options.TypeValidators.DoubleValidator;
import com.dremio.options.TypeValidators.EnumValidator;
import com.dremio.options.TypeValidators.LongValidator;
import com.dremio.options.TypeValidators.PositiveLongValidator;
import com.dremio.options.TypeValidators.QueryLevelOptionValidation;
import com.dremio.options.TypeValidators.RangeDoubleValidator;
import com.dremio.options.TypeValidators.RangeLongValidator;
import com.dremio.options.TypeValidators.StringValidator;
import com.dremio.resource.GroupResourceInformation;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;

@Options
public class PlannerSettings implements Context{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PlannerSettings.class);
  private boolean useDefaultCosting = false; // True: use default Calcite costing, False: use Dremio costing
  private boolean forceSingleMode;
  private long minimumSampleSize = 0;
  // should distribution traits be pulled off during planning
  private boolean pullDistributionTrait = true;

  public static final int MAX_RECURSION_STACK_DEPTH = 100;
  public static final int MAX_BROADCAST_THRESHOLD = Integer.MAX_VALUE;
  public static final int DEFAULT_IDENTIFIER_MAX_LENGTH = 1024;

  public static final double DEFAULT_PARITTION_FILTER_FACTOR = 0.9d;
  public static final double DEFAULT_FILTER_MIN_SELECTIVITY_ESTIMATE_FACTOR_WITH_STATISTICS = 0.005d;
  public static final double DEFAULT_FILTER_MIN_SELECTIVITY_ESTIMATE_FACTOR = 0.5d;
  public static final double DEFAULT_FILTER_MAX_SELECTIVITY_ESTIMATE_FACTOR = 1.0d;
  // default off heap memory for planning (256M)
  private static final long DEFAULT_MAX_OFF_HEAP_ALLOCATION_IN_BYTES = 256 * 1024 * 1024;
  private static final long DEFAULT_BROADCAST_THRESHOLD = 10000000;
  private static final long DEFAULT_CELL_COUNT_THRESHOLD = 10 * DEFAULT_BROADCAST_THRESHOLD; // 10 times DEFAULT_BROADCAST_THRESHOLD
  public static final long MAX_METADATA_VALIDITY_CHECK_INTERVAL = 60 * 60;
  public static final LongValidator PLANNER_MEMORY_RESERVATION = new RangeLongValidator("planner.reservation_bytes",
    0L, Long.MAX_VALUE, 0L);
  public static final LongValidator PLANNER_MEMORY_LIMIT = new RangeLongValidator("planner.memory_limit",
    0L, Long.MAX_VALUE, DEFAULT_MAX_OFF_HEAP_ALLOCATION_IN_BYTES);
  public static final LongValidator MAX_METADATA_CALL_COUNT =
      new LongValidator("planner.max_metadata_call_count", 10_000_000L);

  public static final DoubleValidator MUX_USE_THRESHOLD = new RangeDoubleValidator("planner.mux.use_threshold", 0, Double.MAX_VALUE, 1200.0d);
  public static final LongValidator MUX_BUFFER_THRESHOLD = new RangeLongValidator("planner.mux.buffer_threshold", 0, Long.MAX_VALUE, 1_000_000);
  public static final LongValidator MUX_FRAGS = new RangeLongValidator("planner.mux.fragments_per_node", 0, Long.MAX_VALUE, 0);
  public static final BooleanValidator FLATTEN_FILTER = new BooleanValidator("planner.enable_filter_flatten_pushdown", false /** disabled until DX-7987 is resolved **/);
  public static final BooleanValidator CONSTANT_FOLDING = new BooleanValidator("planner.enable_constant_folding", true);
  public static final BooleanValidator EXCHANGE = new BooleanValidator("planner.disable_exchanges", false);
  public static final BooleanValidator HASHAGG = new BooleanValidator("planner.enable_hashagg", true);
  public static final BooleanValidator STREAMAGG = new BooleanValidator("planner.enable_streamagg", true);
  public static final BooleanValidator HASHJOIN = new BooleanValidator("planner.enable_hashjoin", true);
  public static final BooleanValidator EXTRA_CONDITIONS_HASHJOIN = new BooleanValidator("planner.enable_extra_conditions_in_hashjoin", true);
  public static final BooleanValidator MERGEJOIN = new BooleanValidator("planner.enable_mergejoin", false);
  public static final BooleanValidator NESTEDLOOPJOIN = new BooleanValidator("planner.enable_nestedloopjoin", true);
  public static final BooleanValidator MULTIPHASE = new BooleanValidator("planner.enable_multiphase_agg", true);
  public static final BooleanValidator BROADCAST = new BooleanValidator("planner.enable_broadcast_join", true);
  public static final LongValidator BROADCAST_MIN_THRESHOLD = new PositiveLongValidator("planner.broadcast_min_threshold", MAX_BROADCAST_THRESHOLD, 500000);
  public static final LongValidator BROADCAST_THRESHOLD = new PositiveLongValidator("planner.broadcast_threshold", MAX_BROADCAST_THRESHOLD, DEFAULT_BROADCAST_THRESHOLD);
  public static final LongValidator BROADCAST_CELL_COUNT_THRESHOLD = new PositiveLongValidator("planner.broadcast_cellcount_threshold", MAX_BROADCAST_THRESHOLD, DEFAULT_CELL_COUNT_THRESHOLD);
  public static final DoubleValidator BROADCAST_FACTOR = new RangeDoubleValidator("planner.broadcast_factor", 0, Double.MAX_VALUE, 2.0d);
  public static final DoubleValidator NESTEDLOOPJOIN_FACTOR = new RangeDoubleValidator("planner.nestedloopjoin_factor", 0, Double.MAX_VALUE, 100.0d);
  public static final LongValidator NESTEDLOOPJOIN_MAX_CONDITION_NODES = new PositiveLongValidator("planner.nestedloopjoin_max_condition_nodes", Long.MAX_VALUE, 120);
  public static final BooleanValidator NLJOIN_FOR_SCALAR = new BooleanValidator("planner.enable_nljoin_for_scalar_only", false);
  public static final DoubleValidator JOIN_ROW_COUNT_ESTIMATE_FACTOR = new RangeDoubleValidator("planner.join.row_count_estimate_factor", 0, Double.MAX_VALUE, 1.0d);
  public static final BooleanValidator MUX_EXCHANGE = new BooleanValidator("planner.enable_mux_exchange", true);
  public static final BooleanValidator DEMUX_EXCHANGE = new BooleanValidator("planner.enable_demux_exchange", false);
  public static final LongValidator PARTITION_SENDER_THREADS_FACTOR = new LongValidator("planner.partitioner_sender_threads_factor", 2);
  public static final LongValidator PARTITION_SENDER_MAX_THREADS = new LongValidator("planner.partitioner_sender_max_threads", 8);
  public static final LongValidator PARTITION_SENDER_SET_THREADS = new LongValidator("planner.partitioner_sender_set_threads", -1);
  public static final BooleanValidator PRODUCER_CONSUMER = new BooleanValidator("planner.add_producer_consumer", false);
  public static final LongValidator PRODUCER_CONSUMER_QUEUE_SIZE = new LongValidator("planner.producer_consumer_queue_size", 10);
  public static final BooleanValidator HASH_SINGLE_KEY = new BooleanValidator("planner.enable_hash_single_key", false);
  public static final BooleanValidator HASH_JOIN_SWAP = new BooleanValidator("planner.enable_hashjoin_swap", false);
  public static final RangeDoubleValidator HASH_JOIN_SWAP_MARGIN_FACTOR = new RangeDoubleValidator("planner.join.hash_join_swap_margin_factor", 0, 100, 10d);
  public static final LongValidator STREAM_AGG_MAX_GROUP = new PositiveLongValidator("planner.streamagg.max_group_key", Long.MAX_VALUE, 64);
  public static final BooleanValidator STREAM_AGG_WITH_GROUPS = new BooleanValidator("planner.streamagg.allow_grouping", false);
  public static final String ENABLE_DECIMAL_DATA_TYPE_KEY = "planner.enable_decimal_data_type";
  public static final LongValidator HEP_PLANNER_MATCH_LIMIT = new PositiveLongValidator("planner.hep_match_limit", Integer.MAX_VALUE, Integer.MAX_VALUE);
  public static final BooleanValidator ENHANCED_FILTER_JOIN_PUSHDOWN = new BooleanValidator("planner.enhanced_filter_join_pushdown", true);
  public static final BooleanValidator TRANSITIVE_FILTER_JOIN_PUSHDOWN = new BooleanValidator("planner.filter.transitive_pushdown", true);
  public static final BooleanValidator TRANSITIVE_FILTER_NOT_NULL_EXPR_PUSHDOWN = new BooleanValidator("planner.filter.transitive_pushdown_not_null_expr", false); // Until DX-26452 is fixes
  public static final BooleanValidator ENABLE_RUNTIME_FILTER = new BooleanValidator("planner.filter.runtime_filter", true);
  public static final BooleanValidator ENABLE_TRANSPOSE_PROJECT_FILTER_LOGICAL = new BooleanValidator("planner.experimental.tpf_logical", false);
  public static final BooleanValidator ENABLE_PROJECT_CLEANUP_LOGICAL = new BooleanValidator("planner.experimental.pclean_logical", false);
  public static final BooleanValidator ENABLE_CROSS_JOIN = new BooleanValidator("planner.enable_cross_join", true);
  public static final BooleanValidator ENABLE_DECIMAL_DATA_TYPE = new BooleanValidator
    (ENABLE_DECIMAL_DATA_TYPE_KEY, true);
  public static final BooleanValidator HEP_OPT = new BooleanValidator("planner.enable_hep_opt", true);
  public static final BooleanValidator ENABLE_PARTITION_PRUNING = new BooleanValidator("planner.enable_partition_pruning", true);
  public static final String UNIONALL_DISTRIBUTE_KEY = "planner.enable_unionall_distribute";
  public static final BooleanValidator UNIONALL_DISTRIBUTE = new BooleanValidator(UNIONALL_DISTRIBUTE_KEY, true);
  public static final LongValidator UNION_ALL_INPUT_ROUND_ROBIN_THRESHOLD_VALUE = new LongValidator("planner.input_round_robin_threshold_value", 1L);
  public static final DoubleValidator UNION_ALL_INPUT_ROUND_ROBIN_THRESHOLD_RATIO = new DoubleValidator("planner.input_round_robin_threshold_ratio", 0.1);
  public static final LongValidator PLANNING_MAX_MILLIS = new LongValidator("planner.timeout_per_phase_ms", 60_000);
  public static final BooleanValidator RELATIONAL_PLANNING = new BooleanValidator("planner.enable_relational_planning", true);
  public static final BooleanValidator NESTED_SCHEMA_PROJECT_PUSHDOWN = new BooleanValidator("planner.enable_nested_schema_project_pushdown", true);
  public static final BooleanValidator SORT_IN_JOIN_REMOVER = new BooleanValidator("planner.enable_sort_in_join_remover", true);
  public static final BooleanValidator FULL_NESTED_SCHEMA_SUPPORT = new BooleanValidator("planner.enable_full_nested_schema", true);
  public static final BooleanValidator COMPLEX_TYPE_FILTER_PUSHDOWN = new BooleanValidator("planner.complex_type_filter_pushdown", true);

  public static final BooleanValidator ENABLE_LEAF_LIMITS = new BooleanValidator("planner.leaf_limit_enable", false);
  public static final RangeLongValidator LEAF_LIMIT_SIZE  = new RangeLongValidator("planner.leaf_limit_size", 1, Long.MAX_VALUE, 10000);
  public static final RangeLongValidator LEAF_LIMIT_MAX_WIDTH  = new RangeLongValidator("planner.leaf_limit_width", 1, Long.MAX_VALUE, 10);

  public static final BooleanValidator ENABLE_OUTPUT_LIMITS = new BooleanValidator("planner.output_limit_enable", false);
  public static final RangeLongValidator OUTPUT_LIMIT_SIZE  = new RangeLongValidator("planner.output_limit_size", 1, Long.MAX_VALUE, 1_000_000);
  public static final DoubleValidator COLUMN_UNIQUENESS_ESTIMATION_FACTOR = new RangeDoubleValidator("planner.column_uniqueness_estimation_factor", 0d, 1d, 0.1d);
  public static final DoubleValidator PARTITION_FILTER_FACTOR = new RangeDoubleValidator("planner.partition_filter_factor", 0d, 1d, DEFAULT_PARITTION_FILTER_FACTOR);
  public static final BooleanValidator IEEE_754_DIVIDE_SEMANTICS = new BooleanValidator("planner.ieee_754_divide_semantics", false);

  // CSE stands for Common Sub-expression Elimination. When enabled, planner identifies identical sub-expressions, eliminates the duplicates and introduces a
  // bridge exchange to share the same output with multiple downstream operators. The sender operator writes its output to local files which can be read by
  // multiple reader/receiver operators.
  public static final BooleanValidator ENABLE_CSE = new BooleanValidator("planner.enable_cse", true);

  public static final BooleanValidator ENABLE_CSE_HEURISTIC_REQUIRE_AGGREGATE = new BooleanValidator("planner.cse_heuristic_require_aggregate", true);
  public static final BooleanValidator ENABLE_CSE_HEURISTIC_REQUIRE_FILTER = new BooleanValidator("planner.cse_heuristic_require_filter", false);
  public static final BooleanValidator ENABLE_CSE_HEURISTIC_REQUIRE_JOIN = new BooleanValidator("planner.cse_heuristic_require_join", false);

  public static final BooleanValidator ENABLE_CSE_HEURISTIC_FILTER = new BooleanValidator("planner.enable_cse_heuristic_filter", true);
  public static final LongValidator MAX_CSE_PERMUTATIONS = new LongValidator("planner.max_cse_permutations", 64_000);
  public static final DoubleValidator CSE_COST_ADJUSTMENT_FACTOR = new TypeValidators.RangeDoubleValidator("planner.cse_cost_adjustment_factor", 0.0001d, Double.MAX_VALUE, 0.5d);

  // number of records (per minor fragment) is truncated to at-least MIN_RECORDS_PER_FRAGMENT
  // if num of records for the fragment is greater than this.
  public static final Long MIN_RECORDS_PER_FRAGMENT  = 500L;

  // -1 is unlimited
  public static final LongValidator MAX_CNF_NODE_COUNT = new PositiveLongValidator("planner.max_cnf_node_count", Integer.MAX_VALUE, 25);

  public static final BooleanValidator VDS_AUTO_FIX = new BooleanValidator("validator.enable_vds_autofix", true);

  public static final BooleanValidator NLJ_PUSHDOWN = new BooleanValidator("planner.nlj.expression_pushdown", true);
  public static final BooleanValidator HASH_JOIN_PUSHDOWN = new BooleanValidator("planner.hash_join.expression_pushdown", true);

  public static final BooleanValidator USE_CARTESIAN_COST_FOR_LOGICAL_NLJ = new BooleanValidator("planner.cost.use_cartesian_cost_for_logical_nlj", false);

  public static final BooleanValidator NEW_SELF_JOIN_COST = new BooleanValidator("planner.cost.new_self_join_cost", true);

  public static final BooleanValidator ENABLE_FILTER_WINDOW_OPTIMIZER = new BooleanValidator("planner.enable_filter_window_optimizer", true);

  public static final BooleanValidator REDUCE_ALGEBRAIC_EXPRESSIONS = new BooleanValidator("planner.reduce_algebraic_expressions", false);
  public static final BooleanValidator FILTER_EXTRACT_CONJUNCTIONS = new BooleanValidator("planner.filter.extract_conjunctions", false);

  public static final BooleanValidator ENABlE_PROJCT_NLJ_MERGE = new BooleanValidator("planner.nlj.enable_project_merge", false);

  public static final String ENABLE_DECIMAL_V2_KEY = "planner" +
    ".enable_decimal_v2";
  public static final String ENABLE_VECTORIZED_PARQUET_DECIMAL_KEY = "planner" +
    ".enable_vectorized_parquet_decimal";
  public static final BooleanValidator ENABLE_DECIMAL_V2 = new AdminBooleanValidator
    (ENABLE_DECIMAL_V2_KEY, true);
  public static final BooleanValidator ENABLE_VECTORIZED_PARQUET_DECIMAL = new BooleanValidator
    (ENABLE_VECTORIZED_PARQUET_DECIMAL_KEY, true);

  public static final BooleanValidator ENABLE_PARQUET_IN_EXPRESSION_PUSH_DOWN =
          new BooleanValidator("planner.parquet.in_expression_push_down", true);
  public static final BooleanValidator ENABLE_PARQUET_MULTI_COLUMN_FILTER_PUSH_DOWN =
          new BooleanValidator("planner.parquet.multi_column_filter_push_down", true);

  public static final LongValidator MAX_NODES_PER_PLAN = new LongValidator("planner.max_nodes_per_plan", 25_000);

  public static final BooleanValidator ENABLE_DELTALAKE = new BooleanValidator("dremio.deltalake.enabled", true);
  public static final LongValidator ICEBERG_MANIFEST_SCAN_RECORDS_PER_THREAD = new LongValidator("planner.iceberg.manifestscan.records_per_thread", 1000);
  public static final BooleanValidator UNLIMITED_SPLITS_SUPPORT = new BooleanValidator("dremio.execution.support_unlimited_splits", true);

  public static final DoubleValidator METADATA_REFRESH_INCREASE_FACTOR = new DoubleValidator("dremio.metadata.increase_factor", 0.1);

  public static final DoubleValidator FOOTER_READING_DIRLIST_RATIO= new DoubleValidator("dremio.metadata.footer_read_dirlist_ratio", 50);

  public static final DoubleValidator MIN_FILES_CHANGED_DURING_REFRESH = new DoubleValidator("dremio.metadata.minimum_files_changed", 100);

  public static final PositiveLongValidator METADATA_EXPIRY_CHECK_INTERVAL_SECS = new PositiveLongValidator("dremio.metadata_expiry_check_interval_in_secs", MAX_METADATA_VALIDITY_CHECK_INTERVAL, 60);

  public static final BooleanValidator ENABLE_AGGRESSIVE_MEMORY_CALCULATION =
    new BooleanValidator("planner.memory.aggressive", false);
  public static final TypeValidators.LongValidator ADJUST_RESERVED_WHEN_AGGRESSIVE =
    new TypeValidators.LongValidator("planner.memory.adjust_aggressive_by_mb", 1024);
  public static final BooleanValidator ENABLE_ACCURATE_MEMORY_ESTIMATION = new BooleanValidator("planner.memory.accurate.estimation", true);

  /**
   * Policy regarding storing query results
   */
  public enum StoreQueryResultsPolicy {
    /**
     * Do not save query result
     */
    NO,

    /**
     * Save query results to the path designated by {@code QUERY_RESULTS_STORE_TABLE} option
     */
    DIRECT_PATH,

    /**
     * Save query results to the path designated by {@code QUERY_RESULTS_STORE_TABLE} option
     * appended with attempt id
     */
    PATH_AND_ATTEMPT_ID
  }

  public static final OptionValidator STORE_QUERY_RESULTS = new QueryLevelOptionValidation(
      new EnumValidator<>("planner.store_query_results", StoreQueryResultsPolicy.class, StoreQueryResultsPolicy.NO));

  public static final OptionValidator QUERY_RESULTS_STORE_TABLE = new QueryLevelOptionValidation(new StringValidator("planner.query_results_store_path", "null"));

  // Enable filter reduce expressions rule for tableau's 1=0 queries.
  public static final BooleanValidator ENABLE_REDUCE_PROJECT = new BooleanValidator("planner.enable_reduce_project", true);
  public static final BooleanValidator ENABLE_REDUCE_FILTER = new BooleanValidator("planner.enable_reduce_filter", true);
  public static final BooleanValidator ENABLE_REDUCE_CALC = new BooleanValidator("planner.enable_reduce_calc", true);
  public static final BooleanValidator ENABLE_REDUCE_JOIN = new BooleanValidator("planner.enable_reduce_join", true);

  // Filter reduce expression rules used in conjunction with transitive filter
  public static final BooleanValidator ENABLE_TRANSITIVE_REDUCE_PROJECT = new BooleanValidator("planner.enable_transitive_reduce_project", false);
  public static final BooleanValidator ENABLE_TRANSITIVE_REDUCE_FILTER = new BooleanValidator("planner.enable_transitive_reduce_filter", false);
  public static final BooleanValidator ENABLE_TRANSITIVE_REDUCE_CALC = new BooleanValidator("planner.enable_transitive_reduce_calc", false);

  public static final BooleanValidator ENABLE_TRIVIAL_SINGULAR = new BooleanValidator("planner.enable_trivial_singular", true);

  public static final BooleanValidator ENABLE_SORT_ROUND_ROBIN = new BooleanValidator("planner.enable_sort_round_robin", true);

  public static final RangeLongValidator IDENTIFIER_MAX_LENGTH =
      new RangeLongValidator("planner.identifier_max_length", 128 /* A minimum length is needed because option names are identifiers themselves */,
                              Integer.MAX_VALUE, DEFAULT_IDENTIFIER_MAX_LENGTH);

  public static final BooleanValidator ENABLE_GLOBAL_DICTIONARY = new BooleanValidator("planner.enable_global_dictionary", true);

  public static final DoubleValidator FLATTEN_EXPANSION_AMOUNT = new TypeValidators.RangeDoubleValidator("planner.flatten.expansion_size", 0, Double.MAX_VALUE, 10.0d);

  public static final LongValidator RING_COUNT = new TypeValidators.PowerOfTwoLongValidator("planner.ring_count", 4096, 64);

  public static final BooleanValidator WRITER_TEMP_FILE = new BooleanValidator("planner.writer_temp_file", false);

  /**
   * Controls whether to use the cached prepared statement handles more than once. Setting it to false will remove the
   * handle when it is used the first time before it expires. Setting it to true will reuse the handle as many times as
   * it can before it expires.
   */
  public static final BooleanValidator REUSE_PREPARE_HANDLES = new BooleanValidator("planner.reuse_prepare_statement_handles", false);

  public static final BooleanValidator VERBOSE_PROFILE = new BooleanValidator("planner.verbose_profile", false);
  public static final BooleanValidator USE_STATISTICS = new BooleanValidator("planner.use_statistics", false);
  public static final BooleanValidator USE_MIN_SELECTIVITY_ESTIMATE_FACTOR_FOR_STAT = new BooleanValidator("planner.use_selectivity_estimate_factor_for_stat", false);
  public static final BooleanValidator USE_ROW_COUNT_STATISTICS = new BooleanValidator("planner.use_rowcount_statistics", false);
  public static final BooleanValidator VERBOSE_RULE_MATCH_LISTENER = new BooleanValidator("planner.verbose_rule_match_listener", false);

  public static final BooleanValidator INCLUDE_DATASET_PROFILE = new BooleanValidator("planner.include_dataset_profile", true);

  public static final BooleanValidator ENABLE_JOIN_OPTIMIZATION = new BooleanValidator("planner.enable_join_optimization", true);

  public static final BooleanValidator ENABLE_EXPERIMENTAL_BUSHY_JOIN_OPTIMIZER = new BooleanValidator("planner.experimental.enable_bushy_join_optimizer", false);

  public static final BooleanValidator ENABLE_RANGE_QUERY_REWRITE = new BooleanValidator("planner.enable_range_query_rewrite", false);

  public static final DoubleValidator FILTER_MIN_SELECTIVITY_ESTIMATE_FACTOR =
          new RangeDoubleValidator("planner.filter.min_selectivity_estimate_factor", 0.0, 1.0, DEFAULT_FILTER_MIN_SELECTIVITY_ESTIMATE_FACTOR);
  public static final DoubleValidator FILTER_MIN_SELECTIVITY_ESTIMATE_FACTOR_WITH_STATISTICS =
    new RangeDoubleValidator("planner.filter.min_selectivity_estimate_factor_with_statistics", 0.0, 1.0, DEFAULT_FILTER_MIN_SELECTIVITY_ESTIMATE_FACTOR_WITH_STATISTICS);

  public static final PositiveLongValidator STATISTICS_MAX_COLUMN_LIMIT = new PositiveLongValidator("planner.statistics_max_column_limit", Integer.MAX_VALUE, 50);

  public static final DoubleValidator FILTER_MAX_SELECTIVITY_ESTIMATE_FACTOR =
          new RangeDoubleValidator("planner.filter.max_selectivity_estimate_factor", 0.0, 1.0, DEFAULT_FILTER_MAX_SELECTIVITY_ESTIMATE_FACTOR);

  public static final DoubleValidator SELF_JOIN_ROW_COUNT_FACTOR =
    new RangeDoubleValidator("planner.cost.self_join_row_count_factor", 0.0, 2.0, 1);

  public static final BooleanValidator REMOVE_ROW_ADJUSTMENT = new BooleanValidator("planner.remove_rowcount_adjustment", true);

  public static final BooleanValidator COMPUTE_NDV_STAT = new BooleanValidator("planner.compute_ndv_stat", true);
  public static final BooleanValidator COMPUTE_ROWCOUNT_STAT = new BooleanValidator("planner.compute_row_count_stat", true);
  public static final BooleanValidator COMPUTE_TDIGEST_STAT = new BooleanValidator("planner.compute_tdigest_stat", true);
  public static final BooleanValidator COMPUTE_ITEMSSKETCH_STAT = new BooleanValidator("planner.compute_items_sketch_stat", false);
  public static final BooleanValidator COMPUTE_COUNT_COL_STAT = new BooleanValidator("planner.compute_col_stat", true);

  public static final PositiveLongValidator CASE_EXPRESSIONS_THRESHOLD = new PositiveLongValidator("planner.case_expressions_threshold", 400, 4);

  public static final BooleanValidator ENABLE_SCAN_MIN_COST = new BooleanValidator("planner.cost.minimum.enable", true);
  public static final DoubleValidator DEFAULT_SCAN_MIN_COST = new DoubleValidator("planner.default.min_cost_per_split", 0);
  public static final DoubleValidator ADLS_SCAN_MIN_COST = new DoubleValidator("planner.adl.min_cost_per_split", 1E6);
  public static final DoubleValidator AZURE_STORAGE_SCAN_MIN_COST = new DoubleValidator("planner.azure_storage.min_cost_per_split", 1E6);
  public static final DoubleValidator S3_SCAN_MIN_COST = new DoubleValidator("planner.s3.min_cost_per_split", 1E6);
  public static final DoubleValidator ACCELERATION_SCAN_MIN_COST = new DoubleValidator("planner.acceleration.min_cost_per_split", 0);
  public static final DoubleValidator HOME_SCAN_MIN_COST = new DoubleValidator("planner.home.min_cost_per_split", 0);
  public static final DoubleValidator INTERNAL_SCAN_MIN_COST = new DoubleValidator("planner.internal.min_cost_per_split", 0);
  public static final DoubleValidator ELASTIC_SCAN_MIN_COST = new DoubleValidator("planner.elastic.min_cost_per_split", 0);
  public static final DoubleValidator MONGO_SCAN_MIN_COST = new DoubleValidator("planner.mongo.min_cost_per_split", 0);
  public static final DoubleValidator HBASE_SCAN_MIN_COST = new DoubleValidator("planner.hbase.min_cost_per_split", 0);
  public static final DoubleValidator HIVE_SCAN_MIN_COST = new DoubleValidator("planner.hive.min_cost_per_split", 0);
  public static final DoubleValidator PDFS_SCAN_MIN_COST = new DoubleValidator("planner.pdfs.min_cost_per_split", 0);
  public static final DoubleValidator HDFS_SCAN_MIN_COST = new DoubleValidator("planner.hdfs.min_cost_per_split", 0);
  public static final DoubleValidator MAPRFS_SCAN_MIN_COST = new DoubleValidator("planner.maprfs.min_cost_per_split", 0);
  public static final DoubleValidator NAS_SCAN_MIN_COST = new DoubleValidator("planner.nas.min_cost_per_split", 0);
  public static final BooleanValidator LEGACY_SERIALIZER_ENABLED = new BooleanValidator("planner.legacy_serializer_enabled", false);
  public static final BooleanValidator PLAN_SERIALIZATION = new BooleanValidator("planner.plan_serialization", true);
  public static final LongValidator PLAN_SERIALIZATION_LENGTH_LIMIT = new PositiveLongValidator("planner.plan_serialization_length_limit", Long.MAX_VALUE, 100000);

  private static final Set<String> SOURCES_WITH_MIN_COST = ImmutableSet.of(
    "adl",
    "s3",
    "acceleration",
    "home",
    "internal",
    "elastic",
    "mongo",
    "hbase",
    "hive",
    "pdfs",
    "hdfs",
    "maprfs",
    "nas",
    "azure_storage"
    );

  /**
   * Option to enable additional push downs (filter, project, etc.) to JDBC sources. Enabling the option may cause
   * the SQL query being push down to be written differently from what is submitted by the user. For example, the join
   * order may change, filter pushed past join, etc.
   */
  public static final BooleanValidator JDBC_PUSH_DOWN_PLUS =
      new BooleanValidator("planner.jdbc.experimental.enable_additional_pushdowns", false);

  /**
   * Options to reject queries which will attempt to process more than this many splits: per dataset, and per query
   */
  public static final PositiveLongValidator QUERY_MAX_SPLIT_LIMIT = new PositiveLongValidator("planner.query_max_split_limit", Integer.MAX_VALUE, 300_000);
  public static final PositiveLongValidator DATASET_MAX_SPLIT_LIMIT = new PositiveLongValidator("planner.dataset_max_split_limit", Integer.MAX_VALUE, 300_000);

  /**
   * Options to enable/disable plan cache and set plan cache policy
   */
  public static final BooleanValidator QUERY_PLAN_CACHE_ENABLED = new BooleanValidator("planner.query_plan_cache_enabled", true);

  private final SabotConfig sabotConfig;
  private final ExecutionControls executionControls;
  private final StatisticsService statisticsService;
  public final OptionResolver options;
  private Supplier<GroupResourceInformation> resourceInformation;

  public StatisticsService getStatisticsService() {
    return statisticsService;
  }


  // This flag is used by AbstractRelOptPlanner to set it's "cancelFlag".
  private final CancelFlag cancelFlag = new CancelFlag(new AtomicBoolean(false));

  // This is used to set reason for cancelling the query in DremioHepPlanner and DremioVolcanoPlanner
  private String cancelReason = "";
  private String cancelContext = null;
  private volatile boolean isCancelledByHeapMonitor = false;

  private NodeEndpoint nodeEndpoint = null;

  public PlannerSettings(SabotConfig config, OptionResolver options,
                         Supplier<GroupResourceInformation> resourceInformation,
                         StatisticsService statisticsService) {
    this(config, options, resourceInformation, null, statisticsService);
  }

  public PlannerSettings(SabotConfig config, OptionResolver options,
                         Supplier<GroupResourceInformation> resourceInformation) {
    this(config, options, resourceInformation, null, null);
  }
  public PlannerSettings(SabotConfig config, OptionResolver options,
                         Supplier<GroupResourceInformation> resourceInformation,
                         ExecutionControls executionControls,
                         StatisticsService statisticsService) {
    this.sabotConfig = config;
    this.options = new CachingOptionResolver(options);
    this.resourceInformation = resourceInformation;
    this.executionControls = executionControls;
    this.statisticsService = statisticsService;
  }

  public SabotConfig getSabotConfig() {
    return sabotConfig;
  }

  public OptionResolver getOptions() {
    return options;
  }

  public boolean isPlannerVerbose() {
    return options.getOption(VERBOSE_PROFILE);
  }

  public boolean isLeafLimitsEnabled(){
    return options.getOption(ENABLE_LEAF_LIMITS);
  }

  public boolean useStatistics() {
    return options.getOption(USE_STATISTICS);
  }

  public boolean isUseCartesianCostForLogicalNljEnabled() {
    return options.getOption(USE_CARTESIAN_COST_FOR_LOGICAL_NLJ);
  }

  public boolean useRowCountStatistics() {
    return options.getOption(USE_ROW_COUNT_STATISTICS);
  }

  public final long getMaxNodesPerPlan() {
    return options.getOption(MAX_NODES_PER_PLAN);
  }

  public double getCseCostAdjustmentFactor() {
    return options.getOption(CSE_COST_ADJUSTMENT_FACTOR);
  }

  public final long getMaxNLJConditionNodesPerPlan() {
    return options.getOption(NESTEDLOOPJOIN_MAX_CONDITION_NODES);
  }

  public long getLeafLimit(){
    return options.getOption(LEAF_LIMIT_SIZE);
  }

  public boolean isFilterFlattenTransposeEnabled(){
    return options.getOption(FLATTEN_FILTER);
  }

  public long getStatisticsMaxColumnLimit() {
    return options.getOption(STATISTICS_MAX_COLUMN_LIMIT);
  }

  public boolean isSingleMode() {
    return forceSingleMode || options.getOption(EXCHANGE);
  }

  public long getMaxPlanningPerPhaseMS() {
    return options.getOption(PLANNING_MAX_MILLIS);
  }

  public void forceSingleMode() {
    forceSingleMode = true;
  }

  public int numEndPoints() {
    return resourceInformation.get().getExecutorNodeCount();
  }

  public double getRowCountEstimateFactor(){
    return options.getOption(JOIN_ROW_COUNT_ESTIMATE_FACTOR);
  }

  public double getPartitionFilterFactor(){
    return options.getOption(PARTITION_FILTER_FACTOR);
  }

  public double getBroadcastFactor(){
    return options.getOption(BROADCAST_FACTOR);
  }

  public boolean isEnhancedFilterJoinPushdownEnabled() {
    return options.getOption(ENHANCED_FILTER_JOIN_PUSHDOWN);
  }

  public double getColumnUniquenessEstimationFactor(){
    return options.getOption(COLUMN_UNIQUENESS_ESTIMATION_FACTOR);
  }

  public boolean isTransitiveFilterPushdownEnabled() {
    return options.getOption(TRANSITIVE_FILTER_JOIN_PUSHDOWN);
  }

  public boolean isTransitiveFilterNotNullExprPushdownEnabled() {
    return options.getOption(TRANSITIVE_FILTER_NOT_NULL_EXPR_PUSHDOWN);
  }

  public boolean isComplexTypeFilterPushdownEnabled() {
    return options.getOption(COMPLEX_TYPE_FILTER_PUSHDOWN) && options.getOption(ExecConstants.ENABLE_PARQUET_VECTORIZED_COMPLEX_READERS);
  }

  public boolean isRuntimeFilterEnabled() {
    return options.getOption(ENABLE_RUNTIME_FILTER);
  }

  public boolean isCSEEnabled() {
    return options.getOption(ENABLE_CSE);
  }

  public boolean isCSEHeuristicFilterEnabled() {
    return options.getOption(ENABLE_CSE_HEURISTIC_FILTER);
  }

  public boolean isTransposeProjectFilterLogicalEnabled() {
    return options.getOption(ENABLE_TRANSPOSE_PROJECT_FILTER_LOGICAL);
  }

  public double getNestedLoopJoinFactor(){
    return options.getOption(NESTEDLOOPJOIN_FACTOR);
  }

  public double getSelfJoinRowCountFactor(){
    return options.getOption(SELF_JOIN_ROW_COUNT_FACTOR);
  }

  public boolean removeRowCountAdjustment() {
    return options.getOption(REMOVE_ROW_ADJUSTMENT);
  }

  public boolean isNlJoinForScalarOnly() {
    return options.getOption(NLJOIN_FOR_SCALAR);
  }

  public double getFlattenExpansionAmount(){
    return options.getOption(FLATTEN_EXPANSION_AMOUNT);
  }

  public boolean useDefaultCosting() {
    return useDefaultCosting;
  }

  public void setUseDefaultCosting(boolean defcost) {
    this.useDefaultCosting = defcost;
  }

  public boolean isRelPlanningEnabled() {
    return options.getOption(RELATIONAL_PLANNING);
  }

  public boolean isNestedSchemaProjectPushdownEnabled() {
    return options.getOption(NESTED_SCHEMA_PROJECT_PUSHDOWN);
  }

  public boolean isSortInJoinRemoverEnabled() {
    return options.getOption(SORT_IN_JOIN_REMOVER);
  }

  public boolean isPlanCacheEnabled() {
    return options.getOption(QUERY_PLAN_CACHE_ENABLED);
  }

  public long getCaseExpressionsThreshold() {
    return options.getOption(CASE_EXPRESSIONS_THRESHOLD);
  }

  /**
   * Get the configured value of max parallelization width per
   * executor node. This is internally computed using average number
   * of cores across all executor nodes
   * @return max width per node
   */
  public long getMaxWidthPerNode() {
    Preconditions.checkNotNull(resourceInformation, "Need a valid reference for " +
      "Resource Information");
    Preconditions.checkNotNull(resourceInformation.get(), "Need a valid reference for " +
      "Resource Information");
    return resourceInformation.get().getAverageExecutorCores(options);
  }

  public boolean isHashAggEnabled() {
    return options.getOption(HASHAGG);
  }

  public boolean isConstantFoldingEnabled() {
    return options.getOption(CONSTANT_FOLDING);
  }

  public boolean isReduceProjectExpressionsEnabled() {
    return options.getOption(ENABLE_REDUCE_PROJECT);
  }

  public boolean isProjectLogicalCleanupEnabled() {
    return options.getOption(ENABLE_PROJECT_CLEANUP_LOGICAL);
  }

  public boolean isCrossJoinEnabled() {
    return options.getOption(ENABLE_CROSS_JOIN);
  }

  public boolean isReduceFilterExpressionsEnabled() {
    return options.getOption(ENABLE_REDUCE_FILTER);
  }

  public boolean isReduceCalcExpressionsEnabled() {
    return options.getOption(ENABLE_REDUCE_CALC);
  }

  public boolean isReduceJoinExpressionsEnabled() {
    return options.getOption(ENABLE_REDUCE_JOIN);
  }

  public boolean isTransitiveReduceProjectExpressionsEnabled() {
    return options.getOption(ENABLE_TRANSITIVE_REDUCE_PROJECT);
  }

  public boolean isTransitiveReduceFilterExpressionsEnabled() {
    return options.getOption(ENABLE_TRANSITIVE_REDUCE_FILTER);
  }

  public boolean isTransitiveReduceCalcExpressionsEnabled() {
    return options.getOption(ENABLE_TRANSITIVE_REDUCE_CALC);
  }

  public boolean isGlobalDictionariesEnabled() {
    return options.getOption(ENABLE_GLOBAL_DICTIONARY);
  }

  public boolean isStreamAggEnabled() {
    return options.getOption(STREAMAGG);
  }

  public long streamAggMaxGroupKey() {
    return options.getOption(STREAM_AGG_MAX_GROUP);
  }

  public boolean isHashJoinEnabled() {
    return options.getOption(HASHJOIN);
  }

  public boolean isMergeJoinEnabled() {
    return options.getOption(MERGEJOIN);
  }

  public boolean isNestedLoopJoinEnabled() {
    return options.getOption(NESTEDLOOPJOIN);
  }

  public boolean isMultiPhaseAggEnabled() {
    return options.getOption(MULTIPHASE);
  }

  public boolean isBroadcastJoinEnabled() {
    return options.getOption(BROADCAST);
  }

  public boolean isHashSingleKey() {
    return options.getOption(HASH_SINGLE_KEY);
  }

  public boolean isHashJoinSwapEnabled() {
    return options.getOption(HASH_JOIN_SWAP);
  }

  public boolean isHepOptEnabled() { return options.getOption(HEP_OPT);}

  public double getHashJoinSwapMarginFactor() {
    return options.getOption(HASH_JOIN_SWAP_MARGIN_FACTOR) / 100d;
  }

  public long getBroadcastThreshold() {
    return options.getOption(BROADCAST_THRESHOLD);
  }

  public long getMaxCnfNodeCount() {
    return options.getOption(MAX_CNF_NODE_COUNT);
  }

  public boolean isNewSelfJoinCostEnabled(){ return options.getOption(NEW_SELF_JOIN_COST); }

  public void setMinimumSampleSize(long sampleSize) {
    if (minimumSampleSize == 0 || minimumSampleSize > sampleSize) {
      minimumSampleSize = sampleSize;
    }
  }

  public long getSliceTarget(){
    long sliceTarget = options.getOption(ExecConstants.SLICE_TARGET_OPTION);
    if (isLeafLimitsEnabled() && minimumSampleSize > 0) {
      return Math.min(sliceTarget, minimumSampleSize);
    }
    return sliceTarget;
  }

  public String getFsPartitionColumnLabel() {
    return options.getOption(ExecConstants.FILESYSTEM_PARTITION_COLUMN_LABEL_VALIDATOR);
  }

  public double getFilterMinSelectivityEstimateFactor() {
    if (options.getOption(USE_STATISTICS) && options.getOption(USE_MIN_SELECTIVITY_ESTIMATE_FACTOR_FOR_STAT)) {
      return options.getOption(FILTER_MIN_SELECTIVITY_ESTIMATE_FACTOR_WITH_STATISTICS);
    }
    return options.getOption(FILTER_MIN_SELECTIVITY_ESTIMATE_FACTOR);
  }

  public double getFilterMaxSelectivityEstimateFactor(){
    return options.getOption(FILTER_MAX_SELECTIVITY_ESTIMATE_FACTOR);
  }

  public long getIdentifierMaxLength(){
    return options.getOption(IDENTIFIER_MAX_LENGTH);
  }

  public long getPlanningMemoryLimit() {
    return options.getOption(PLANNER_MEMORY_LIMIT);
  }

  public long maxMetadataCallCount() {
    return options.getOption(MAX_METADATA_CALL_COUNT);
  }

  public long getInitialPlanningMemorySize() {
    return options.getOption(PLANNER_MEMORY_RESERVATION);
  }

  public boolean isUnionAllDistributeEnabled() {
    return options.getOption(UNIONALL_DISTRIBUTE);
  }

  public boolean isPartitionPruningEnabled() {
    return options.getOption(ENABLE_PARTITION_PRUNING);
  }

  public boolean isTrivialSingularOptimized() {
    return options.getOption(ENABLE_TRIVIAL_SINGULAR);
  }

  public long getSerializationLengthLimit() {
    return options.getOption(PLAN_SERIALIZATION_LENGTH_LIMIT);
  }

  public boolean isPlanSerializationEnabled(){
    return options.getOption(PLAN_SERIALIZATION);
  }

  public double getMinimumCostPerSplit(SourceType sourceType) {
    if (SOURCES_WITH_MIN_COST.contains(sourceType.value().toLowerCase())) {
      if (logger.isDebugEnabled()) {
        logger.debug("planner.cost.minimum.enable is enabled and SourceType {} supports minimum cost per split", sourceType.label());
      }
      DoubleValidator validator =
          new DoubleValidator(
              String.format("planner.%s.min_cost_per_split", sourceType.value().toLowerCase()),
              options.getOption(DEFAULT_SCAN_MIN_COST));
      return options.getOption(validator);
    }
    return options.getOption(DEFAULT_SCAN_MIN_COST);
  }

  public boolean useMinimumCostPerSplit() {
    return options.getOption(ENABLE_SCAN_MIN_COST);
  }

  /**
   * Get the number of executor nodes
   * @return number of executor nodes
   */
  public int getExecutorCount() {
    return resourceInformation.get().getExecutorNodeCount();
  }

  public boolean isJoinOptimizationEnabled() {
    return options.getOption(ENABLE_JOIN_OPTIMIZATION);
  }

  public boolean isExperimentalBushyJoinOptimizerEnabled() {
    return options.getOption(ENABLE_EXPERIMENTAL_BUSHY_JOIN_OPTIMIZER);
  }

  boolean shouldPullDistributionTrait() {
    return pullDistributionTrait;
  }

  public int getQueryMaxSplitLimit() {
    return (int) options.getOption(QUERY_MAX_SPLIT_LIMIT);
  }

  public int getDatasetMaxSplitLimit() {
    return (int) options.getOption(DATASET_MAX_SPLIT_LIMIT);
  }

  public boolean isFullNestedSchemaSupport() {
    return options.getOption(FULL_NESTED_SCHEMA_SUPPORT);
  }

  public void pullDistributionTrait(boolean pullDistributionTrait) {
    this.pullDistributionTrait = pullDistributionTrait;
  }

  @Override
  public <T> T unwrap(Class<T> clazz) {
    if(clazz == PlannerSettings.class){
      return (T) this;
    } else if(clazz == CalciteConnectionConfig.class){
      return (T) CONFIG;
    } else if (clazz == SabotConfig.class) {
      return (T) sabotConfig;
    } else if (CancelFlag.class.isAssignableFrom(clazz)) {
      return clazz.cast(cancelFlag);
    } else if (clazz == ExecutionControls.class) {
      return (T) executionControls;
    }
    return null;
  }

  public static final CalciteConnectionConfigImpl CONFIG = new CalciteConnectionConfigImpl(new Properties()) {
    @Override
    public boolean materializationsEnabled() {
      return true;
    }
  };

  public void cancelPlanning(String cancelReason, NodeEndpoint nodeEndpoint, String cancelContext, boolean isCancelledByHeapMonitor) {
    this.cancelReason = cancelReason;
    this.nodeEndpoint = nodeEndpoint;
    this.cancelContext = cancelContext;
    this.isCancelledByHeapMonitor = isCancelledByHeapMonitor;
    cancelFlag.requestCancel();
  }

  public String getCancelReason() {
    return cancelReason;
  }

  public NodeEndpoint getNodeEndpoint() {
    return nodeEndpoint;
  }

  public String getCancelContext() {
    return cancelContext;
  }

  public boolean isCancelledByHeapMonitor() {
    return isCancelledByHeapMonitor;
  }
}
