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
package com.dremio.exec.ops;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.commons.lang3.ArrayUtils;

import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.proto.UserBitShared.CoreOperatorTypeMetricsMap;
import com.dremio.exec.proto.UserBitShared.MetricsDef;
import com.dremio.exec.store.parquet.ParquetRecordWriter;
import com.dremio.sabot.exec.context.MetricDef;
import com.dremio.sabot.op.aggregate.vectorized.HashAggStats;
import com.dremio.sabot.op.filter.FilterStats;
import com.dremio.sabot.op.join.nlje.NLJEOperator;
import com.dremio.sabot.op.join.vhash.HashJoinStats;
import com.dremio.sabot.op.metrics.MongoStats;
import com.dremio.sabot.op.project.ProjectorStats;
import com.dremio.sabot.op.receiver.merging.MergingReceiverOperator;
import com.dremio.sabot.op.receiver.unordered.UnorderedReceiverOperator;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.sabot.op.screen.ScreenOperator;
import com.dremio.sabot.op.sender.broadcast.BroadcastOperator;
import com.dremio.sabot.op.sender.partition.PartitionSenderOperator;
import com.dremio.sabot.op.sender.roundrobin.RoundRobinOperator;
import com.dremio.sabot.op.sender.single.SingleSenderOperator;
import com.dremio.sabot.op.sort.external.ExternalSortOperator;
import com.dremio.sabot.op.tablefunction.TableFunctionOperator;
import com.dremio.sabot.op.writer.WriterCommitterOperator;
import com.dremio.sabot.op.writer.WriterOperator;

/**
 * Registry of operator metrics.
 */
public class OperatorMetricRegistry {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(OperatorMetricRegistry.class);

  private static final CoreOperatorTypeMetricsMap CORE_OPERATOR_TYPE_METRICS_MAP;
  private static final String[][] OPERATOR_METRICS_NAMES = new String[CoreOperatorType.values().length][];
  public static final int DEFAULT_CORE_OPERATOR_SUBTYPE = 0;

  static {
    final CoreOperatorTypeMetricsMap.Builder builder = getMapBuilder();
    register(builder, CoreOperatorType.SCREEN_VALUE, ScreenOperator.Metric.class);
    register(builder, CoreOperatorType.SINGLE_SENDER_VALUE, SingleSenderOperator.Metric.class);
    register(builder, CoreOperatorType.BROADCAST_SENDER_VALUE, BroadcastOperator.Metric.class);
    register(builder, CoreOperatorType.ROUND_ROBIN_SENDER_VALUE, RoundRobinOperator.Metric.class);
    register(builder, CoreOperatorType.HASH_PARTITION_SENDER_VALUE, PartitionSenderOperator.Metric.class);
    register(builder, CoreOperatorType.MERGING_RECEIVER_VALUE, MergingReceiverOperator.Metric.class);
    register(builder, CoreOperatorType.UNORDERED_RECEIVER_VALUE, UnorderedReceiverOperator.Metric.class);
    register(builder, CoreOperatorType.HASH_AGGREGATE_VALUE, HashAggStats.Metric.class);
    register(builder, CoreOperatorType.HASH_JOIN_VALUE, HashJoinStats.Metric.class);
    register(builder, CoreOperatorType.EXTERNAL_SORT_VALUE, ExternalSortOperator.Metric.class);
    register(builder, CoreOperatorType.HIVE_SUB_SCAN_VALUE, ScanOperator.Metric.class);
    register(builder, CoreOperatorType.MONGO_SUB_SCAN_VALUE, MongoStats.Metric.class);
    register(builder, CoreOperatorType.PARQUET_ROW_GROUP_SCAN_VALUE, ScanOperator.Metric.class);
    register(builder, CoreOperatorType.PARQUET_WRITER_VALUE, ParquetRecordWriter.Metric.class);
    register(builder, CoreOperatorType.ARROW_WRITER_VALUE, WriterOperator.Metric.class);
    register(builder, CoreOperatorType.PROJECT_VALUE, ProjectorStats.Metric.class);
    register(builder, CoreOperatorType.FILTER_VALUE, FilterStats.Metric.class);
    register(builder, CoreOperatorType.NESTED_LOOP_JOIN_VALUE, NLJEOperator.Metric.class);
    register(builder, CoreOperatorType.TABLE_FUNCTION_VALUE, Arrays.asList(ScanOperator.Metric.class, TableFunctionOperator.Metric.class));
    register(builder, CoreOperatorType.DELTALAKE_SUB_SCAN_VALUE, ScanOperator.Metric.class);
    register(builder, CoreOperatorType.ICEBERG_SUB_SCAN_VALUE, ScanOperator.Metric.class);
    register(builder, CoreOperatorType.DIR_LISTING_SUB_SCAN_VALUE,  ScanOperator.Metric.class);
    register(builder, CoreOperatorType.MANIFEST_WRITER_VALUE, ParquetRecordWriter.Metric.class);
    register(builder, CoreOperatorType.WRITER_COMMITTER_VALUE, WriterCommitterOperator.Metric.class);
    CORE_OPERATOR_TYPE_METRICS_MAP = builder.build();
  }

  private static void register(CoreOperatorTypeMetricsMap.Builder builder, final Integer operatorType, final Class<? extends MetricDef> metricDef) {
    final MetricDef[] enumConstants = metricDef.getEnumConstants();
    populateMetricsInMap(builder, operatorType, Arrays.asList(enumConstants));
  }

  private static void register(CoreOperatorTypeMetricsMap.Builder builder, final Integer operatorType, final List< Class<? extends MetricDef>> metricsDef) {
    final ArrayList<MetricDef> enumConstants = new ArrayList<>();
    for (Class<? extends MetricDef> def: metricsDef) {
      Collections.addAll(enumConstants, def.getEnumConstants());
    }
    populateMetricsInMap(builder, operatorType, enumConstants);
  }

  private static void populateMetricsInMap(CoreOperatorTypeMetricsMap.Builder builder, final Integer operatorType, List<MetricDef> enumConstants) {
    MetricsDef.Builder metricsDefBuilder = builder.getMetricsDefBuilder(operatorType);
    if (enumConstants == null) {
      return;
    }
    final String[] names = new String[enumConstants.size()];
    for (int i = 0; i < enumConstants.size(); i++) {
      metricsDefBuilder.addMetricDef(UserBitShared.MetricDef.newBuilder()
        .setId(enumConstants.get(i).metricId())
        .setName(enumConstants.get(i).name()).build()
      );
      names[i] = enumConstants.get(i).name();
    }
    OPERATOR_METRICS_NAMES[operatorType] = names;
    builder.setMetricsDef(operatorType, metricsDefBuilder.build());
  }

  private static CoreOperatorTypeMetricsMap.Builder getMapBuilder() {
    CoreOperatorTypeMetricsMap.Builder builder = CoreOperatorTypeMetricsMap.newBuilder();
    for (int i = 0; i < CoreOperatorType.values().length; i++) {
      // Initalizing with metricsdef object with empty list => This can be modified later at the time of registration
      builder.addMetricsDef(MetricsDef.getDefaultInstance());
    }
    return builder;
  }

  public static CoreOperatorTypeMetricsMap getCoreOperatorTypeMetricsMap() {
    return CORE_OPERATOR_TYPE_METRICS_MAP;
  }

  private static Optional<MetricsDef> getMetricsDef(CoreOperatorTypeMetricsMap coreOperatorTypeMetricsMap, int operatorType) {
    if (operatorType < coreOperatorTypeMetricsMap.getMetricsDefCount()) {
      return Optional.ofNullable(coreOperatorTypeMetricsMap.getMetricsDef(operatorType));
    }
    return Optional.empty();
  }

  /**
   * @param coreOperatorTypeMetricsMap
   * @param operatorType
   * @return Array of Metric names if operator type is present in the map else empty list
   */
  public static String[] getMetricNames(CoreOperatorTypeMetricsMap coreOperatorTypeMetricsMap, int operatorType) {
    Optional<MetricsDef> metricsDef = getMetricsDef(coreOperatorTypeMetricsMap, operatorType);
    return metricsDef.map((metricDef) -> metricDef.getMetricDefList().stream().map(UserBitShared.MetricDef::getName).toArray(String[]::new)).orElse(ArrayUtils.EMPTY_STRING_ARRAY);
  }

  /**
   * @param operatorType
   * @return Array of Metric names if operator type is registered, else list
   */
  public static String[] getMetricNames(int operatorType) {
    return Optional.ofNullable(OPERATOR_METRICS_NAMES[operatorType]).orElse(ArrayUtils.EMPTY_STRING_ARRAY);
  }

  /**
   * @param coreOperatorTypeMetricsMap
   * @param operatorType
   * @return Array of Metric Ids if operator type is present in the map else empty list
   */
  public static Integer[] getMetricIds(CoreOperatorTypeMetricsMap coreOperatorTypeMetricsMap, int operatorType) {
    Optional<MetricsDef> metricsDef = getMetricsDef(coreOperatorTypeMetricsMap, operatorType);
    return metricsDef.map((metricDef) -> metricDef.getMetricDefList().stream().map(UserBitShared.MetricDef::getId).toArray(Integer[]::new)).orElse(ArrayUtils.EMPTY_INTEGER_OBJECT_ARRAY);
  }

  /**
   * @param coreOperatorTypeMetricsMap
   * @param operatorType
   * @param metricId
   * @return Optional<MetricDef> containing metric defination if present in the map
   */
  public static Optional<UserBitShared.MetricDef> getMetricById(CoreOperatorTypeMetricsMap coreOperatorTypeMetricsMap, int operatorType, int metricId) {
    Optional<MetricsDef> metricsDef = getMetricsDef(coreOperatorTypeMetricsMap, operatorType);
    if (metricsDef.isPresent() && metricId < metricsDef.get().getMetricDefCount()) {
      return Optional.ofNullable(metricsDef.get().getMetricDef(metricId));
    }
    return Optional.empty();
  }


  public static boolean isRegistered(int operatorType, int metricId){
    CoreOperatorTypeMetricsMap coreOperatorTypeMetricsMap = getCoreOperatorTypeMetricsMap();
    return getMetricById(coreOperatorTypeMetricsMap, operatorType, metricId).isPresent();
  }

  // to prevent instantiation
  private OperatorMetricRegistry() {
  }
}
