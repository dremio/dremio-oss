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
package com.dremio.exec.ops;

import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.store.parquet.ParquetRecordWriter;
import com.dremio.sabot.exec.context.MetricDef;
import com.dremio.sabot.op.common.hashtable.HashTableStats;
import com.dremio.sabot.op.receiver.merging.MergingReceiverOperator;
import com.dremio.sabot.op.receiver.unordered.UnorderedReceiverOperator;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.sabot.op.screen.ScreenOperator;
import com.dremio.sabot.op.sender.broadcast.BroadcastOperator;
import com.dremio.sabot.op.sender.partition.PartitionSenderOperator;
import com.dremio.sabot.op.sender.roundrobin.RoundRobinOperator;
import com.dremio.sabot.op.sender.single.SingleSenderOperator;
import com.dremio.sabot.op.sort.external.ExternalSortOperator;

/**
 * Registry of operator metrics.
 */
public class OperatorMetricRegistry {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(OperatorMetricRegistry.class);

  // Mapping: operator type --> metric id --> metric name
  private static final String[][] OPERATOR_METRICS = new String[CoreOperatorType.values().length][];

  static {
    register(CoreOperatorType.SCREEN_VALUE, ScreenOperator.Metric.class);
    register(CoreOperatorType.SINGLE_SENDER_VALUE, SingleSenderOperator.Metric.class);
    register(CoreOperatorType.BROADCAST_SENDER_VALUE, BroadcastOperator.Metric.class);
    register(CoreOperatorType.ROUND_ROBIN_SENDER_VALUE, RoundRobinOperator.Metric.class);
    register(CoreOperatorType.HASH_PARTITION_SENDER_VALUE, PartitionSenderOperator.Metric.class);
    register(CoreOperatorType.MERGING_RECEIVER_VALUE, MergingReceiverOperator.Metric.class);
    register(CoreOperatorType.UNORDERED_RECEIVER_VALUE, UnorderedReceiverOperator.Metric.class);
    register(CoreOperatorType.HASH_AGGREGATE_VALUE, HashTableStats.Metric.class);
    register(CoreOperatorType.HASH_JOIN_VALUE, HashTableStats.Metric.class);
    register(CoreOperatorType.EXTERNAL_SORT_VALUE, ExternalSortOperator.Metric.class);
    register(CoreOperatorType.HIVE_SUB_SCAN_VALUE, ScanOperator.Metric.class);
    register(CoreOperatorType.PARQUET_ROW_GROUP_SCAN_VALUE, ScanOperator.Metric.class);
    register(CoreOperatorType.PARQUET_WRITER_VALUE, ParquetRecordWriter.Metric.class);
  }

  private static void register(final int operatorType, final Class<? extends MetricDef> metricDef) {
    // Currently registers a metric def that has enum constants
    final MetricDef[] enumConstants = metricDef.getEnumConstants();
    if (enumConstants != null) {
      final String[] names = new String[enumConstants.length];
      for (int i = 0; i < enumConstants.length; i++) {
        names[i] = enumConstants[i].name();
      }
      OPERATOR_METRICS[operatorType] = names;
    }
  }

  /**
   * Given an operator type, this method returns an array of metric names (indexable by metric id).
   *
   * @param operatorType the operator type
   * @return metric names if operator was registered, null otherwise
   */
  public static String[] getMetricNames(final int operatorType) {
    return OPERATOR_METRICS[operatorType];
  }

  // to prevent instantiation
  private OperatorMetricRegistry() {
  }
}
