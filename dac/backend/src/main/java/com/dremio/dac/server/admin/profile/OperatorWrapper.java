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
package com.dremio.dac.server.admin.profile;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.lang3.tuple.ImmutablePair;

import com.dremio.exec.ops.OperatorMetricRegistry;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.proto.UserBitShared.CoreOperatorTypeMetricsMap;
import com.dremio.exec.proto.UserBitShared.MetricDef;
import com.dremio.exec.proto.UserBitShared.MetricValue;
import com.dremio.exec.proto.UserBitShared.OperatorProfile;
import com.dremio.exec.proto.UserBitShared.StreamProfile;
import com.dremio.sabot.op.aggregate.vectorized.HashAggStats;
import com.google.common.base.Preconditions;

/**
 * Wrapper class for profiles of ALL operator instances of the same operator type within a major fragment.
 */
public class OperatorWrapper {

  private final int major;
  private final List<ImmutablePair<OperatorProfile, Integer>> ops; // operator profile --> minor fragment number
  private final OperatorProfile firstProfile;
  private final CoreOperatorType operatorType;
  private final String operatorName;
  private final int size;
  private final CoreOperatorTypeMetricsMap coreOperatorTypeMetricsMap;

  public OperatorWrapper(int major, List<ImmutablePair<OperatorProfile, Integer>> ops, CoreOperatorTypeMetricsMap coreOperatorTypeMetricsMap) {
    Preconditions.checkArgument(ops.size() > 0);
    this.major = major;
    firstProfile = ops.get(0).getLeft();
    operatorType = CoreOperatorType.valueOf(firstProfile.getOperatorType());
    operatorName = operatorType == null ? "UNKNOWN_OPERATOR" : operatorType.toString();
    this.coreOperatorTypeMetricsMap = Optional.ofNullable(coreOperatorTypeMetricsMap).orElse(OperatorMetricRegistry.getCoreOperatorTypeMetricsMap());
    this.ops = ops;
    size = ops.size();
  }

  public String getDisplayName() {
    final String path = new OperatorPathBuilder().setMajor(major).setOperator(firstProfile).build();
    return String.format("%s - %s", path, operatorName);
  }

  public String getId() {
    return String.format("operator-%d-%d", major, ops.get(0).getLeft().getOperatorId());
  }

  public static final String[] OPERATOR_COLUMNS = {"Thread", "Setup Time", "Process Time", "Wait Time",
    "Max Batches", "Max Records", "Peak Memory"};

  public String getContent() {
    TableBuilder builder = new TableBuilder(OPERATOR_COLUMNS);

    for (ImmutablePair<OperatorProfile, Integer> ip : ops) {
      int minor = ip.getRight();
      OperatorProfile op = ip.getLeft();

      String path = new OperatorPathBuilder().setMajor(major).setMinor(minor).setOperator(op).build();
      builder.appendCell(path, null);
      builder.appendNanos(op.getSetupNanos());
      builder.appendNanos(op.getProcessNanos());
      builder.appendNanos(op.getWaitNanos());

      long maxBatches = Long.MIN_VALUE;
      long maxRecords = Long.MIN_VALUE;
      for (StreamProfile sp : op.getInputProfileList()) {
        maxBatches = Math.max(sp.getBatches(), maxBatches);
        maxRecords = Math.max(sp.getRecords(), maxRecords);
      }

      builder.appendFormattedInteger(maxBatches, null);
      builder.appendFormattedInteger(maxRecords, null);
      builder.appendBytes(op.getPeakLocalMemoryAllocated(), null);
    }
    return builder.build();
  }

  public static final String[] OPERATORS_OVERVIEW_COLUMNS = {"SqlOperatorImpl ID", "Type", "Min Setup Time", "Avg Setup Time",
    "Max Setup Time", "Min Process Time", "Avg Process Time", "Max Process Time", "Min Wait Time", "Avg Wait Time",
    "Max Wait Time", "Avg Peak Memory", "Max Peak Memory"};

  public void addSummary(TableBuilder tb) {

    String path = new OperatorPathBuilder().setMajor(major).setOperator(firstProfile).build();
    tb.appendCell(path, null);
    tb.appendCell(operatorName, null);

    double setupSum = 0.0;
    double processSum = 0.0;
    double waitSum = 0.0;
    double memSum = 0.0;
    for (ImmutablePair<OperatorProfile, Integer> ip : ops) {
      OperatorProfile profile = ip.getLeft();
      setupSum += profile.getSetupNanos();
      processSum += profile.getProcessNanos();
      waitSum += profile.getWaitNanos();
      memSum += profile.getPeakLocalMemoryAllocated();
    }

    final ImmutablePair<OperatorProfile, Integer> shortSetup = Collections.min(ops, Comparators.setupTime);
    final ImmutablePair<OperatorProfile, Integer> longSetup = Collections.max(ops, Comparators.setupTime);
    tb.appendNanos(shortSetup.getLeft().getSetupNanos());
    tb.appendNanos(Math.round(setupSum / size));
    tb.appendNanos(longSetup.getLeft().getSetupNanos());

    final ImmutablePair<OperatorProfile, Integer> shortProcess = Collections.min(ops, Comparators.processTime);
    final ImmutablePair<OperatorProfile, Integer> longProcess = Collections.max(ops, Comparators.processTime);
    tb.appendNanos(shortProcess.getLeft().getProcessNanos());
    tb.appendNanos(Math.round(processSum / size));
    tb.appendNanos(longProcess.getLeft().getProcessNanos());

    final ImmutablePair<OperatorProfile, Integer> shortWait = Collections.min(ops, Comparators.waitTime);
    final ImmutablePair<OperatorProfile, Integer> longWait = Collections.max(ops, Comparators.waitTime);
    tb.appendNanos(shortWait.getLeft().getWaitNanos());
    tb.appendNanos(Math.round(waitSum / size));
    tb.appendNanos(longWait.getLeft().getWaitNanos());

    final ImmutablePair<OperatorProfile, Integer> peakMem = Collections.max(ops, Comparators.operatorPeakMemory);
    tb.appendBytes(Math.round(memSum / size), null);
    tb.appendBytes(peakMem.getLeft().getPeakLocalMemoryAllocated(), null);
  }

  public String getMetricsTable() {
    if (operatorType == null) {
      return "";
    }

    final String[] metricNames = OperatorMetricRegistry.getMetricNames(operatorType.getNumber());
    final Integer[] metricIds = OperatorMetricRegistry.getMetricIds(coreOperatorTypeMetricsMap, operatorType.getNumber());

    if (metricIds.length == 0) {
      return "";
    }

    final String[] metricsTableColumnNames = new String[metricIds.length + 1];
    metricsTableColumnNames[0] = "Thread";
    Map<Integer, Integer> metricIdToMetricTableColumnIndex = new HashMap<Integer, Integer>();
    int i = 1;
    for (final int metricId : metricIds) {
      Optional<MetricDef> metric = OperatorMetricRegistry.getMetricById(coreOperatorTypeMetricsMap, operatorType.getNumber(), metricId);
      assert metric.isPresent(); // Since metric id was retrieved from map, doing a reverse lookup shouldn't fail.
      metricIdToMetricTableColumnIndex.put(metricId, i - 1);
      metricsTableColumnNames[i++] = metric.get().getName();
    }
    final TableBuilder builder = new TableBuilder(metricsTableColumnNames);
    for (final ImmutablePair<OperatorProfile, Integer> ip : ops) {
      final OperatorProfile op = ip.getLeft();

      builder.appendCell(
        new OperatorPathBuilder()
          .setMajor(major)
          .setMinor(ip.getRight())
          .setOperator(op)
          .build(),
        null);


      final boolean isHashAgg = operatorType.getNumber() == CoreOperatorType.HASH_AGGREGATE_VALUE;
      final boolean toSkip = isHashAgg && renderingOldProfiles(op);
      final Number[] values = new Number[metricIds.length];
      for (final MetricValue metric : op.getMetricList()) {
        int metricId = metric.getMetricId();
        if (toSkip) {
          /* working with older profiles (that have more stat columns), so
           * get the correct ordinal to index the values array
           * for storing the metric IDs for rendering.
           */
          metricId = HashAggStats.getCorrectOrdinalForOlderProfiles(metricId);
          if (metric.hasLongValue()) {
            values[metricId] = metric.getLongValue();
          } else if (metric.hasDoubleValue()) {
            values[metricId] = metric.getDoubleValue();
          }
        } else {
          Optional<Integer> columnIndex = Optional.ofNullable(metricIdToMetricTableColumnIndex.get(metric.getMetricId()));
          columnIndex.ifPresent(index -> {
            if (metric.hasLongValue()) {
              values[index] = metric.getLongValue();
            } else if (metric.hasDoubleValue()) {
              values[index] = metric.getDoubleValue();
            }
          });
        }
      }

      int count = 0;
      for (final Number value : values) {
        if (value != null) {
          if (isHashAgg && metricNames[count].contains("TIME")) {
            /* format elapsed time related metrics correctly as string (using hrs, mins, secs, us, ns as applicable) */
            builder.appendNanosWithUnit(value.longValue());
          } else {
            builder.appendFormattedNumber(value, null);
          }
        } else {
          builder.appendCell("", null);
        }

        count++;
      }
    }
    return builder.build();
  }

  private boolean renderingOldProfiles(OperatorProfile op) {
    final List<MetricValue> metricValues = op.getMetricList();
    for (MetricValue metric : metricValues) {
      final int metricId = metric.getMetricId();
      if (metricId == HashAggStats.SKIP_METRIC_START) {
        /* if the ordinal (metric id) to skip is indeed present
         * in the serialized profile that we are trying to render
         * then we are definitely working with new profiles
         */
        return false;
      }
    }
    return true;
  }
}
