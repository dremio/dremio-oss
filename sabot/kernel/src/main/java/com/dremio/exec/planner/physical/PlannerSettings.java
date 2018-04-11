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
package com.dremio.exec.planner.physical;


import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.plan.Context;
import org.apache.calcite.util.CancelFlag;

import com.dremio.common.config.SabotConfig;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.server.ClusterResourceInformation;
import com.dremio.exec.server.options.CachingOptionManager;
import com.dremio.exec.server.options.OptionManager;
import com.dremio.exec.server.options.OptionValidator;
import com.dremio.exec.server.options.Options;
import com.dremio.exec.server.options.TypeValidators;
import com.dremio.exec.server.options.TypeValidators.BooleanValidator;
import com.dremio.exec.server.options.TypeValidators.DoubleValidator;
import com.dremio.exec.server.options.TypeValidators.LongValidator;
import com.dremio.exec.server.options.TypeValidators.PositiveLongValidator;
import com.dremio.exec.server.options.TypeValidators.QueryLevelOptionValidation;
import com.dremio.exec.server.options.TypeValidators.RangeDoubleValidator;
import com.dremio.exec.server.options.TypeValidators.RangeLongValidator;
import com.dremio.exec.server.options.TypeValidators.StringValidator;

@Options
public class PlannerSettings implements Context{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PlannerSettings.class);
  private int numEndPoints = 0;
  private boolean useDefaultCosting = false; // True: use default Calcite costing, False: use Dremio costing
  private boolean forceSingleMode;
  private long minimumSampleSize = 0;

  public static final int MAX_BROADCAST_THRESHOLD = Integer.MAX_VALUE;
  public static final int DEFAULT_IDENTIFIER_MAX_LENGTH = 1024;

  // initial off heap memory allocation (1M)
  private static final long INITIAL_OFF_HEAP_ALLOCATION_IN_BYTES = 1024 * 1024;
  // default off heap memory for planning (256M)
  private static final long DEFAULT_MAX_OFF_HEAP_ALLOCATION_IN_BYTES = 256 * 1024 * 1024;
  // max off heap memory for planning (16G)
  private static final long MAX_OFF_HEAP_ALLOCATION_IN_BYTES = 16l * 1024 * 1024 * 1024;

  public static final BooleanValidator FLATTEN_FILTER = new BooleanValidator("planner.enable_filter_flatten_pushdown", false /** disabled until DX-7987 is resolved **/);
  public static final BooleanValidator CONSTANT_FOLDING = new BooleanValidator("planner.enable_constant_folding", true);
  public static final BooleanValidator EXCHANGE = new BooleanValidator("planner.disable_exchanges", false);
  public static final BooleanValidator HASHAGG = new BooleanValidator("planner.enable_hashagg", true);
  public static final BooleanValidator STREAMAGG = new BooleanValidator("planner.enable_streamagg", true);
  public static final BooleanValidator HASHJOIN = new BooleanValidator("planner.enable_hashjoin", true);
  public static final BooleanValidator MERGEJOIN = new BooleanValidator("planner.enable_mergejoin", false);
  public static final BooleanValidator NESTEDLOOPJOIN = new BooleanValidator("planner.enable_nestedloopjoin", true);
  public static final BooleanValidator MULTIPHASE = new BooleanValidator("planner.enable_multiphase_agg", true);
  public static final OptionValidator BROADCAST = new BooleanValidator("planner.enable_broadcast_join", true);
  public static final LongValidator BROADCAST_MIN_THRESHOLD = new PositiveLongValidator("planner.broadcast_min_threshold", MAX_BROADCAST_THRESHOLD, 500000);
  public static final LongValidator BROADCAST_THRESHOLD = new PositiveLongValidator("planner.broadcast_threshold", MAX_BROADCAST_THRESHOLD, 10000000);
  public static final DoubleValidator BROADCAST_FACTOR = new RangeDoubleValidator("planner.broadcast_factor", 0, Double.MAX_VALUE, 2.0d);
  public static final DoubleValidator NESTEDLOOPJOIN_FACTOR = new RangeDoubleValidator("planner.nestedloopjoin_factor", 0, Double.MAX_VALUE, 100.0d);
  public static final BooleanValidator NLJOIN_FOR_SCALAR = new BooleanValidator("planner.enable_nljoin_for_scalar_only", true);
  public static final DoubleValidator JOIN_ROW_COUNT_ESTIMATE_FACTOR = new RangeDoubleValidator("planner.join.row_count_estimate_factor", 0, Double.MAX_VALUE, 1.0d);
  public static final BooleanValidator MUX_EXCHANGE = new BooleanValidator("planner.enable_mux_exchange", true);
  public static final BooleanValidator DEMUX_EXCHANGE = new BooleanValidator("planner.enable_demux_exchange", false);
  public static final LongValidator PARTITION_SENDER_THREADS_FACTOR = new LongValidator("planner.partitioner_sender_threads_factor", 2);
  public static final LongValidator PARTITION_SENDER_MAX_THREADS = new LongValidator("planner.partitioner_sender_max_threads", 8);
  public static final LongValidator PARTITION_SENDER_SET_THREADS = new LongValidator("planner.partitioner_sender_set_threads", -1);
  public static final BooleanValidator PRODUCER_CONSUMER = new BooleanValidator("planner.add_producer_consumer", false);
  public static final LongValidator PRODUCER_CONSUMER_QUEUE_SIZE = new LongValidator("planner.producer_consumer_queue_size", 10);
  public static final BooleanValidator HASH_SINGLE_KEY = new BooleanValidator("planner.enable_hash_single_key", true);
  public static final BooleanValidator HASH_JOIN_SWAP = new BooleanValidator("planner.enable_hashjoin_swap", true);
  public static final OptionValidator HASH_JOIN_SWAP_MARGIN_FACTOR = new RangeDoubleValidator("planner.join.hash_join_swap_margin_factor", 0, 100, 10d);
  public static final String ENABLE_DECIMAL_DATA_TYPE_KEY = "planner.enable_decimal_data_type";
  public static final BooleanValidator ENABLE_DECIMAL_DATA_TYPE = new BooleanValidator(ENABLE_DECIMAL_DATA_TYPE_KEY, false);
  public static final BooleanValidator HEP_OPT = new BooleanValidator("planner.enable_hep_opt", true);
  public static final BooleanValidator ENABLE_PARTITION_PRUNING = new BooleanValidator("planner.enable_partition_pruning", true);
  public static final LongValidator PLANNER_MEMORY_LIMIT = new RangeLongValidator("planner.memory_limit",
      INITIAL_OFF_HEAP_ALLOCATION_IN_BYTES, MAX_OFF_HEAP_ALLOCATION_IN_BYTES, DEFAULT_MAX_OFF_HEAP_ALLOCATION_IN_BYTES);
  public static final String UNIONALL_DISTRIBUTE_KEY = "planner.enable_unionall_distribute";
  public static final BooleanValidator UNIONALL_DISTRIBUTE = new BooleanValidator(UNIONALL_DISTRIBUTE_KEY, true);

  public static final BooleanValidator ENABLE_LEAF_LIMITS = new BooleanValidator("planner.leaf_limit_enable", false);
  public static final RangeLongValidator LEAF_LIMIT_SIZE  = new RangeLongValidator("planner.leaf_limit_size", 1, Long.MAX_VALUE, 10000);
  public static final RangeLongValidator LEAF_LIMIT_MAX_WIDTH  = new RangeLongValidator("planner.leaf_limit_width", 1, Long.MAX_VALUE, 10);

  public static final BooleanValidator ENABLE_OUTPUT_LIMITS = new BooleanValidator("planner.output_limit_enable", false);
  public static final RangeLongValidator OUTPUT_LIMIT_SIZE  = new RangeLongValidator("planner.output_limit_size", 1, Long.MAX_VALUE, 1_000_000);

  public static final OptionValidator STORE_QUERY_RESULTS = new QueryLevelOptionValidation(new BooleanValidator("planner.store_query_results", false));
  public static final OptionValidator QUERY_RESULTS_STORE_TABLE = new QueryLevelOptionValidation(new StringValidator("planner.query_results_store_path", "null"));

  // Enable filter reduce expressions rule for tableau's 1=0 queries.
  public static final BooleanValidator ENABLE_REDUCE_PROJECT = new BooleanValidator("planner.enable_reduce_project", true);
  public static final BooleanValidator ENABLE_REDUCE_FILTER = new BooleanValidator("planner.enable_reduce_filter", true);
  public static final BooleanValidator ENABLE_REDUCE_CALC = new BooleanValidator("planner.enable_reduce_calc", true);
  public static final BooleanValidator ENABLE_TRIVIAL_SINGULAR = new BooleanValidator("planner.enable_trivial_singular", true);

  public static final BooleanValidator ENABLE_SORT_ROUND_ROBIN = new BooleanValidator("planner.enable_sort_round_robin", true);
  public static final BooleanValidator ENABLE_UNIONALL_ROUND_ROBIN = new BooleanValidator("planner.enable_union_all_round_robin", true);

  public static final OptionValidator IDENTIFIER_MAX_LENGTH =
      new RangeLongValidator("planner.identifier_max_length", 128 /* A minimum length is needed because option names are identifiers themselves */,
                              Integer.MAX_VALUE, DEFAULT_IDENTIFIER_MAX_LENGTH);

  public static final BooleanValidator ENABLE_GLOBAL_DICTIONARY = new BooleanValidator("planner.enable_global_dictionary", true);

  public static final DoubleValidator FLATTEN_EXPANSION_AMOUNT = new TypeValidators.RangeDoubleValidator("planner.flatten.expansion_size", 0, Double.MAX_VALUE, 10.0d);

  public static final LongValidator RING_COUNT = new TypeValidators.PowerOfTwoLongValidator("planner.ring_count", 4096, 64);

  public static final BooleanValidator WRITER_TEMP_FILE = new BooleanValidator("planner.writer_temp_file", true);

  /**
   * Controls whether to use the cached prepared statement handles more than once. Setting it to false will remove the
   * handle when it is used the first time before it expires. Setting it to true will reuse the handle as many times as
   * it can before it expires.
   */
  public static final BooleanValidator REUSE_PREPARE_HANDLES = new BooleanValidator("planner.reuse_prepare_statement_handles", false);

  public static final BooleanValidator VERBOSE_PROFILE = new BooleanValidator("planner.verbose_profile", false);

  public static final DoubleValidator FILTER_MIN_SELECTIVITY_ESTIMATE_FACTOR =
      new RangeDoubleValidator("planner.filter.min_selectivity_estimate_factor", 0.0, 1.0, 0.0d);
  public static final DoubleValidator FILTER_MAX_SELECTIVITY_ESTIMATE_FACTOR =
      new RangeDoubleValidator("planner.filter.max_selectivity_estimate_factor", 0.0, 1.0, 1.0d);

  private final SabotConfig sabotConfig;
  public final OptionManager options;
  private final ClusterResourceInformation clusterInfo;

  // This flag is used by AbstractRelOptPlanner to set it's "cancelFlag".
  private final CancelFlag cancelFlag = new CancelFlag(new AtomicBoolean());

  public PlannerSettings(SabotConfig config, OptionManager options, ClusterResourceInformation clusterInfo){
    this.sabotConfig = config;
    this.options = new CachingOptionManager(options);
    this.clusterInfo = clusterInfo;
  }

  public SabotConfig getSabotConfig() {
    return sabotConfig;
  }

  public OptionManager getOptions() {
    return options;
  }

  public boolean isPlannerVerbose() {
    return options.getOption(VERBOSE_PROFILE);
  }

  public boolean isLeafLimitsEnabled(){
    return options.getOption(ENABLE_LEAF_LIMITS);
  }

  public long getLeafLimit(){
    return options.getOption(LEAF_LIMIT_SIZE);
  }

  public boolean isFilterFlattenTransposeEnabled(){
    return options.getOption(FLATTEN_FILTER);
  }

  public boolean isSingleMode() {
    return forceSingleMode || options.getOption(EXCHANGE.getOptionName()).bool_val;
  }

  public void forceSingleMode() {
    forceSingleMode = true;
  }

  public int numEndPoints() {
    return numEndPoints;
  }

  public double getRowCountEstimateFactor(){
    return options.getOption(JOIN_ROW_COUNT_ESTIMATE_FACTOR.getOptionName()).float_val;
  }

  public double getBroadcastFactor(){
    return options.getOption(BROADCAST_FACTOR);
  }

  public double getNestedLoopJoinFactor(){
    return options.getOption(NESTEDLOOPJOIN_FACTOR.getOptionName()).float_val;
  }

  public boolean isNlJoinForScalarOnly() {
    return options.getOption(NLJOIN_FOR_SCALAR.getOptionName()).bool_val;
  }

  public double getFlattenExpansionAmount(){
    return options.getOption(FLATTEN_EXPANSION_AMOUNT);
  }

  public boolean useDefaultCosting() {
    return useDefaultCosting;
  }

  public void setNumEndPoints(int numEndPoints) {
    this.numEndPoints = numEndPoints;
  }

  public void setUseDefaultCosting(boolean defcost) {
    this.useDefaultCosting = defcost;
  }

  public long getNumCoresPerExecutor() {
    if (clusterInfo != null) {
      return clusterInfo.getAverageExecutorCores(options);
    } else {
      throw new UnsupportedOperationException("Cluster Resource Information is needed to get average number of cores in executor");
    }
  }

  public boolean isHashAggEnabled() {
    return options.getOption(HASHAGG.getOptionName()).bool_val;
  }

  public boolean isConstantFoldingEnabled() {
    return options.getOption(CONSTANT_FOLDING.getOptionName()).bool_val;
  }

  public boolean isReduceProjectExpressionsEnabled() {
    return options.getOption(ENABLE_REDUCE_PROJECT.getOptionName()).bool_val;
  }

  public boolean isReduceFilterExpressionsEnabled() {
    return options.getOption(ENABLE_REDUCE_FILTER.getOptionName()).bool_val;
  }

  public boolean isReduceCalcExpressionsEnabled() {
    return options.getOption(ENABLE_REDUCE_CALC.getOptionName()).bool_val;
  }

  public boolean isGlobalDictionariesEnabled() {
    return options.getOption(ENABLE_GLOBAL_DICTIONARY.getOptionName()).bool_val;
  }

  public boolean isStreamAggEnabled() {
    return options.getOption(STREAMAGG.getOptionName()).bool_val;
  }

  public boolean isHashJoinEnabled() {
    return options.getOption(HASHJOIN.getOptionName()).bool_val;
  }

  public boolean isMergeJoinEnabled() {
    return options.getOption(MERGEJOIN);
  }

  public boolean isNestedLoopJoinEnabled() {
    return options.getOption(NESTEDLOOPJOIN.getOptionName()).bool_val;
  }

  public boolean isMultiPhaseAggEnabled() {
    return options.getOption(MULTIPHASE.getOptionName()).bool_val;
  }

  public boolean isBroadcastJoinEnabled() {
    return options.getOption(BROADCAST.getOptionName()).bool_val;
  }

  public boolean isHashSingleKey() {
    return options.getOption(HASH_SINGLE_KEY.getOptionName()).bool_val;
  }

  public boolean isHashJoinSwapEnabled() {
    return options.getOption(HASH_JOIN_SWAP.getOptionName()).bool_val;
  }

  public boolean isHepOptEnabled() { return options.getOption(HEP_OPT.getOptionName()).bool_val;}

  public double getHashJoinSwapMarginFactor() {
    return options.getOption(HASH_JOIN_SWAP_MARGIN_FACTOR.getOptionName()).float_val / 100d;
  }

  public long getBroadcastThreshold() {
    return options.getOption(BROADCAST_THRESHOLD);
  }

  public void setMinimumSampleSize(long sampleSize) {
    if (minimumSampleSize == 0 || minimumSampleSize > sampleSize) {
      minimumSampleSize = sampleSize;
    }
  }

  public long getSliceTarget(){
    long sliceTarget = options.getOption(ExecConstants.SLICE_TARGET).num_val;
    if (isLeafLimitsEnabled() && minimumSampleSize > 0) {
      return Math.min(sliceTarget, minimumSampleSize);
    }
    return sliceTarget;
  }

  public boolean isMemoryEstimationEnabled() {
    return options.getOption(ExecConstants.ENABLE_MEMORY_ESTIMATION_KEY).bool_val;
  }

  public String getFsPartitionColumnLabel() {
    return options.getOption(ExecConstants.FILESYSTEM_PARTITION_COLUMN_LABEL).string_val;
  }

  public double getFilterMinSelectivityEstimateFactor() {
    return options.getOption(FILTER_MIN_SELECTIVITY_ESTIMATE_FACTOR);
  }

  public double getFilterMaxSelectivityEstimateFactor(){
    return options.getOption(FILTER_MAX_SELECTIVITY_ESTIMATE_FACTOR);
  }

  public long getIdentifierMaxLength(){
    return options.getOption(IDENTIFIER_MAX_LENGTH.getOptionName()).num_val;
  }

  public long getPlanningMemoryLimit() {
    return options.getOption(PLANNER_MEMORY_LIMIT.getOptionName()).num_val;
  }

  public static long getInitialPlanningMemorySize() {
    return INITIAL_OFF_HEAP_ALLOCATION_IN_BYTES;
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

  public int getExecutorCount() {
    return clusterInfo.getExecutorNodeCount();
  }

  @Override
  public <T> T unwrap(Class<T> clazz) {
    if(clazz == PlannerSettings.class){
      return (T) this;
    } else if(clazz == CalciteConnectionConfig.class){
      return (T) config;
    } if (clazz == SabotConfig.class) {
      return (T) sabotConfig;
    } else if (CancelFlag.class.isAssignableFrom(clazz)) {
      return clazz.cast(cancelFlag);
    }
    return null;
  }

  private static final Config config = new Config();

  private static class Config extends CalciteConnectionConfigImpl {

    public Config() {
      super(new Properties());
    }

    @Override
    public boolean materializationsEnabled() {
      return true;
    }
  }

}
