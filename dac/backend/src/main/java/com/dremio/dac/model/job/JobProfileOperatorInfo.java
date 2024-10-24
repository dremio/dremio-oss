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
package com.dremio.dac.model.job;

import static com.dremio.exec.ops.OperatorMetricRegistry.getMetricById;

import com.dremio.dac.model.job.diagnostics.ProfileObservationUtil;
import com.dremio.dac.util.Filters;
import com.dremio.dac.util.OperatorMetricsUtil;
import com.dremio.dac.util.QueryProfileUtil;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.service.job.proto.OperationType;
import com.dremio.service.jobAnalysis.proto.BaseMetrics;
import com.dremio.service.jobAnalysis.proto.OperatorSpecificDetails;
import com.dremio.service.jobAnalysis.proto.ThreadData;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.Collections2;
import com.google.common.math.StatsAccumulator;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.DoubleSummaryStatistics;
import java.util.HashMap;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import javax.ws.rs.NotFoundException;
import org.apache.commons.text.WordUtils;

public class JobProfileOperatorInfo {

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(JobProfileOperatorInfo.class);

  private final String phaseId;
  private final String operatorId;
  private final String operatorName;
  private final int operatorType;
  private final long setupTime;
  private final long waitTime;
  private final List<Double> waitTimeSkew;
  private final long peakMemory;
  private final long processTime;
  private final List<Double> wallClockTimeSkew;
  private final long bytesProcessed;
  private final long batchesProcessed;
  private final List<Double> batchesProcessedSkew;
  private final long recordsProcessed;
  private final Map<String, String> operatorMetricsMap;
  private final OperatorSpecificDetails operatorSpecificDetails;
  private final long numberOfThreads;
  private final long outputRecords;
  private final long outputBytes;
  private final long sleepingDuration;
  private final long cpuWaitTime;
  private final JobProfileOperatorHealth jpOperatorHealth;
  private final Map<String, JobProfileMetricDetails> metricsDetailsMap;

  @JsonSerialize(using = JobProfileRelNodeInfoSerializer.class)
  private final UserBitShared.RelNodeInfo attributes;

  @WithSpan
  public JobProfileOperatorInfo(UserBitShared.QueryProfile profile, int phaseId, int operatorId) {
    if (profile == null) {
      throw new NotFoundException("Profile is not available");
    }
    if (profile.getFragmentProfileList().isEmpty()) {
      throw new NotFoundException("Profile Fragment is not available");
    }
    List<ThreadData> threadLevelMetricsList = new ArrayList<>();
    BaseMetrics baseMetrics = new BaseMetrics();
    UserBitShared.MajorFragmentProfile majorFragmentProfile =
        ProfileObservationUtil.getPhaseDetails(profile, phaseId);
    if (majorFragmentProfile == null) {
      throw new NotFoundException("Major Profile Fragment is not available");
    }
    int operatorType = getOperatorTypeHelper(operatorId, majorFragmentProfile);
    OperatorSpecificDetails opsDetails = new OperatorSpecificDetails();
    JobProfileOperatorHealth jpOperatorHealth = new JobProfileOperatorHealth();
    Map<String, JobProfileMetricDetails> metricsDetailsMap = new HashMap<>();
    Map<String, String> operatorMetricMap =
        getOperatorMetricMap(
            profile.getOperatorTypeMetricsMap(),
            operatorId,
            majorFragmentProfile,
            operatorType,
            opsDetails,
            metricsDetailsMap); // get OperatorMetricsMap
    String operatorName = String.valueOf(UserBitShared.CoreOperatorType.forNumber(operatorType));
    long batchesProcessed = getBatchSize(majorFragmentProfile, operatorId);

    long bytesProcessed =
        operatorMetricMap.entrySet().stream()
            .filter(name -> name.getKey().toUpperCase().contains("BYTES_"))
            .map(Map.Entry::getValue)
            .collect(Collectors.summarizingLong(Long::parseLong))
            .getMax();

    bytesProcessed = (bytesProcessed > -1) ? bytesProcessed : -1;

    // Below Function with build ThreadLevel Metrics of required fields
    QueryProfileUtil.buildTheadLevelMetrics(majorFragmentProfile, threadLevelMetricsList);

    // below function build the BaseMetrics information.
    threadLevelMetricsList.stream()
        .filter(operator -> Integer.parseInt(operator.getOperatorId()) == operatorId)
        .collect(Collectors.groupingBy(thread -> thread.getOperatorId(), Collectors.toList()))
        .forEach(
            (opId, threadLevelMetrics) -> {
              QueryProfileUtil.buildBaseMetrics(threadLevelMetrics, baseMetrics);
              QueryProfileUtil.getJobProfileOperatorHealth(
                  threadLevelMetrics, jpOperatorHealth, baseMetrics);
            });
    this.phaseId = QueryProfileUtil.getStringIds(phaseId);
    this.operatorId = QueryProfileUtil.getStringIds(operatorId);
    this.operatorName = operatorName;
    this.operatorType = operatorType;
    this.setupTime = baseMetrics.getSetupTime();
    this.waitTime = baseMetrics.getIoWaitTime();
    this.waitTimeSkew = getTotalBlockedOrWaitTimeSkew(majorFragmentProfile);
    this.peakMemory = baseMetrics.getPeakMemory();
    this.processTime = baseMetrics.getProcessingTime();
    this.wallClockTimeSkew = getWallClockTimeSkewFromAllMinorFragments(majorFragmentProfile);
    this.bytesProcessed = bytesProcessed;
    this.batchesProcessed = batchesProcessed;
    this.batchesProcessedSkew =
        getBatchSizesFromAllMinorFragments(majorFragmentProfile, operatorId);
    this.recordsProcessed = baseMetrics.getRecordsProcessed();
    this.operatorMetricsMap = operatorMetricMap;
    this.operatorSpecificDetails = opsDetails;
    this.numberOfThreads = majorFragmentProfile.getMinorFragmentProfileCount();
    this.outputRecords = baseMetrics.getOutputRecords();
    this.outputBytes = baseMetrics.getOutputBytes();
    this.jpOperatorHealth = jpOperatorHealth;
    this.metricsDetailsMap = metricsDetailsMap;
    this.attributes = QueryProfileUtil.getOperatorAttributes(profile, phaseId, operatorId);
    this.sleepingDuration = getMaxBlockedTime(majorFragmentProfile);
    this.cpuWaitTime = getCPUWaitTime(majorFragmentProfile, operatorId);
  }

  public String getPhaseId() {
    return phaseId;
  }

  public String getOperatorId() {
    return operatorId;
  }

  public String getOperatorName() {
    return operatorName;
  }

  public int getOperatorType() {
    return operatorType;
  }

  public long getSetupTime() {
    return setupTime;
  }

  public long getWaitTime() {
    return waitTime;
  }

  public long getPeakMemory() {
    return peakMemory;
  }

  public long getProcessTime() {
    return processTime;
  }

  public long getBytesProcessed() {
    return bytesProcessed;
  }

  public long getBatchesProcessed() {
    return batchesProcessed;
  }

  public long getRecordsProcessed() {
    return recordsProcessed;
  }

  public Map<String, String> getOperatorMetricsMap() {
    return operatorMetricsMap;
  }

  public Map<String, JobProfileMetricDetails> getMetricsDetailsMap() {
    return metricsDetailsMap;
  }

  public OperatorSpecificDetails getOperatorSpecificDetails() {
    return operatorSpecificDetails;
  }

  public long getNumberOfThreads() {
    return numberOfThreads;
  }

  public long getOutputRecords() {
    return outputRecords;
  }

  public long getOutputBytes() {
    return outputBytes;
  }

  public List<Double> getWaitTimeSkew() {
    return waitTimeSkew;
  }

  public List<Double> getWallClockTimeSkew() {
    return wallClockTimeSkew;
  }

  public List<Double> getBatchesProcessedSkew() {
    return batchesProcessedSkew;
  }

  public long getSleepingDuration() {
    return sleepingDuration;
  }

  public long getCpuWaitTime() {
    return cpuWaitTime;
  }

  public JobProfileOperatorHealth getJpOperatorHealth() {
    return jpOperatorHealth;
  }

  public UserBitShared.RelNodeInfo getAttributes() {
    return attributes;
  }

  /** This Method Will return operator Type for given Operator ID */
  private int getOperatorTypeHelper(int nodeId, UserBitShared.MajorFragmentProfile major) {
    int operatorType = -1;
    if (!major.getMinorFragmentProfileList().isEmpty()) {
      UserBitShared.MinorFragmentProfile minor = major.getMinorFragmentProfile(0);
      for (int operatorIndex = 0;
          operatorIndex < minor.getOperatorProfileCount();
          operatorIndex++) {
        if (nodeId == minor.getOperatorProfile(operatorIndex).getOperatorId()) {
          UserBitShared.OperatorProfile operatorProfile = minor.getOperatorProfile(operatorIndex);
          operatorType = operatorProfile.getOperatorType();
        }
      }
    }
    if (operatorType == -1) {
      throw new IllegalArgumentException(
          "OperatorId : " + nodeId + " not exists in Phase " + major.getMajorFragmentId());
    }
    return operatorType;
  }

  /** This Method will build the OperatorSpecificDetails */
  private void addOperatorSpecificDetails(
      UserBitShared.OperatorProfile operatorProfile, OperatorSpecificDetails opsDetails) {
    OperationType operationType =
        com.dremio.service.job.proto.OperationType.valueOf(operatorProfile.getOperatorType());
    operatorProfile.getDetails().getSlowIoInfosList().stream()
        .forEach(
            slowIOInfo -> {
              opsDetails.setIoTimeNs(slowIOInfo.getIoTime());
              opsDetails.setOffset(slowIOInfo.getIoOffset());
              opsDetails.setIoSize(slowIOInfo.getIoSize());
              opsDetails.setFilePath(slowIOInfo.getFilePath());
              opsDetails.setOperationType(String.valueOf(operationType));
            });
  }

  /** This method to add the information to MetricsValue to Add all the Operators */
  private void buildMetricsValueList(
      UserBitShared.CoreOperatorTypeMetricsMap coreOperatorTypeMetricsMap,
      int operatorType,
      List<UserBitShared.MetricValue> tempMetricsList,
      Integer id,
      List<UserBitShared.MetricValue> metricValueList) {
    String metricName = "";
    // Long metricValue = -1L;
    Optional<UserBitShared.MetricDef> mapMetricsDef =
        getMetricById(coreOperatorTypeMetricsMap, operatorType, id);
    if (mapMetricsDef.isPresent()) {
      metricName = mapMetricsDef.get().getName();
      String[] nodeValues = getOperatorSpecificMetrics(operatorType);
      List<String> validMetricsList =
          Arrays.asList(nodeValues).stream().map(n -> n.toLowerCase()).collect(Collectors.toList());
      String listString =
          validMetricsList.stream().map(Object::toString).collect(Collectors.joining(", "));
      if (containsName(validMetricsList, metricName.toLowerCase()) || listString.isEmpty()) {
        long metricLong = 0L;
        double metricDouble = 0D;
        for (UserBitShared.MetricValue metricValue : metricValueList) {
          if (metricValue.hasDoubleValue()) {
            metricDouble += metricValue.getDoubleValue();
          } else if (metricValue.hasLongValue()) {
            metricLong += metricValue.getLongValue();
          }
        }

        UserBitShared.MetricValue metricValue;
        if (metricDouble != 0) {
          metricValue =
              UserBitShared.MetricValue.newBuilder()
                  .setMetricId(id)
                  .setDoubleValue(metricDouble)
                  .build();
        } else {
          metricValue =
              UserBitShared.MetricValue.newBuilder()
                  .setMetricId(id)
                  .setLongValue(metricLong)
                  .build();
        }
        tempMetricsList.add(metricValue);
      }
    }
  }

  /** This method will return an array of Operator Specific Metrics */
  private String[] getOperatorSpecificMetrics(Integer operatorType) {
    String[] metricList = OperatorMetricsUtil.operatorSpecificMetrics.get(operatorType);
    if (metricList == null) {
      metricList = new String[] {""};
    }
    return metricList;
  }

  /**
   * This Method Will return a Indicator which specifies if Metrics information need to send to
   * response
   */
  private boolean containsName(final List<String> columnList, final String name) {
    return columnList.stream().anyMatch(names -> names.equals(name));
  }

  /** This is the Method which will return Sum/min/Max Operator Metrics value */
  public static Map<String, String> buildOperatorMetricsMap(
      List<UserBitShared.MetricValue> tempMetricsList,
      int operatorType,
      UserBitShared.CoreOperatorTypeMetricsMap coreOperatorTypeMetricsMap,
      Map<String, JobProfileMetricDetails> metricsDetailsMap) {
    Map<String, String> operatorMetricsMap = new HashMap<>();
    tempMetricsList.stream()
        .collect(Collectors.groupingBy(metricId -> metricId.getMetricId()))
        .forEach(
            (id, metric) -> {
              Optional<UserBitShared.MetricDef> mapMetricsDef =
                  getMetricById(coreOperatorTypeMetricsMap, operatorType, id);
              if (mapMetricsDef.isPresent()) {
                String name = mapMetricsDef.get().getName();
                String metricValue = "";
                UserBitShared.MetricDef.DisplayType displayType =
                    mapMetricsDef.get().getDisplayType();
                if (displayType == UserBitShared.MetricDef.DisplayType.DISPLAY_BY_DEFAULT) {
                  if (metric.get(0).hasDoubleValue()) {
                    DoubleSummaryStatistics value =
                        metric.stream()
                            .collect(
                                Collectors.summarizingDouble(
                                    UserBitShared.MetricValue::getDoubleValue));
                    UserBitShared.MetricDef.AggregationType aggregationType =
                        mapMetricsDef.get().getAggregationType();
                    if (aggregationType == UserBitShared.MetricDef.AggregationType.SUM) {
                      metricValue = String.valueOf(value.getSum());
                    } else if (aggregationType == UserBitShared.MetricDef.AggregationType.MAX) {
                      metricValue = String.valueOf(value.getMax());
                    } else {
                      logger.warn(
                          "Aggregation value not provided for operator type {} and metric id {} ",
                          operatorType,
                          id);
                    }
                  } else {
                    LongSummaryStatistics value =
                        metric.stream()
                            .collect(
                                Collectors.summarizingLong(
                                    UserBitShared.MetricValue::getLongValue));
                    UserBitShared.MetricDef.AggregationType aggregationType =
                        mapMetricsDef.get().getAggregationType();
                    if (aggregationType == UserBitShared.MetricDef.AggregationType.SUM) {
                      metricValue = String.valueOf(value.getSum());
                    } else if (aggregationType == UserBitShared.MetricDef.AggregationType.MAX) {
                      metricValue = String.valueOf(value.getMax());
                    } else {
                      logger.warn(
                          "Aggregation value not provided for operator type {} and metric id {} ",
                          operatorType,
                          id);
                    }
                  }
                  String displayCode = mapMetricsDef.get().getDisplayCode();
                  operatorMetricsMap.put(WordUtils.capitalizeFully(name, '_'), metricValue);
                  metricsDetailsMap.put(
                      WordUtils.capitalizeFully(name, '_'),
                      new JobProfileMetricDetails(true, displayCode));
                }
              } else {
                logger.warn(
                    "Metric details are not available in coreOperatorTypeMetricsMap for operator type {} and metric id {} ",
                    operatorType,
                    id);
              }
            });
    return operatorMetricsMap;
  }

  /** This Method Will return Metric List for operator */
  private Map<String, String> getOperatorMetricMap(
      UserBitShared.CoreOperatorTypeMetricsMap coreOperatorTypeMetricsMap,
      int nodeId,
      UserBitShared.MajorFragmentProfile major,
      int operatorType,
      OperatorSpecificDetails opsDetails,
      Map<String, JobProfileMetricDetails> metricsDetailsMap) {
    List<UserBitShared.MetricValue> tempMetricsList = new ArrayList<>();
    major.getMinorFragmentProfileList().stream()
        .flatMap(
            minorFragmentProfile ->
                minorFragmentProfile.getOperatorProfileList().stream()
                    .filter(minor -> minor.getOperatorId() == nodeId))
        .forEach(
            operatorProfile -> {
              addOperatorSpecificDetails(operatorProfile, opsDetails);
              operatorProfile.getMetricList().stream()
                  .collect(Collectors.groupingBy(id -> id.getMetricId(), Collectors.toList()))
                  .forEach(
                      (id, metricValueList) -> {
                        buildMetricsValueList(
                            coreOperatorTypeMetricsMap,
                            operatorType,
                            tempMetricsList,
                            id,
                            metricValueList);
                      });
            });

    return buildOperatorMetricsMap(
        tempMetricsList, operatorType, coreOperatorTypeMetricsMap, metricsDetailsMap);
  }

  /** This Method Will return batch Size of an Operator */
  private Long getBatchSize(UserBitShared.MajorFragmentProfile major, int operatorId) {
    List<Long> maxBatchSizeList = new ArrayList<>();
    major.getMinorFragmentProfileList().stream()
        .forEach(
            minor -> {
              minor.getOperatorProfileList().stream()
                  .filter(op -> op.getOperatorId() == operatorId)
                  .forEach(
                      ops -> {
                        Long batchSize = -1L;
                        if (!ops.getMetricList().isEmpty()) {
                          batchSize =
                              ops.getInputProfileList().stream()
                                  .collect(Collectors.summarizingLong(bt -> bt.getBatches()))
                                  .getMax();
                        }
                        maxBatchSizeList.add(batchSize);
                      });
            });
    long totalBatchSize =
        maxBatchSizeList.stream()
            .filter(value -> !value.equals(-1L))
            .collect(Collectors.summarizingLong(Long::longValue))
            .getSum();
    return totalBatchSize;
  }

  private Long getCPUWaitTime(UserBitShared.MajorFragmentProfile major, int operatorId) {

    AtomicLong cpuWaitTime = new AtomicLong();
    major.getMinorFragmentProfileList().stream()
        .forEach(
            minor -> {
              minor.getOperatorProfileList().stream()
                  .filter(op -> op.getOperatorId() == operatorId)
                  .forEach(
                      ops -> {
                        cpuWaitTime.set(Math.max(cpuWaitTime.get(), ops.getWaitNanos()));
                      });
            });

    return cpuWaitTime.get();
  }

  private Long getMaxBlockedTime(UserBitShared.MajorFragmentProfile major) {
    AtomicLong maxBlockedTime = new AtomicLong();
    major.getMinorFragmentProfileList().stream()
        .forEach(
            minor -> {
              if (minor.hasSleepingDuration()) {
                maxBlockedTime.set(Math.max(minor.getBlockedDuration(), maxBlockedTime.get()));
              }
            });
    return maxBlockedTime.get();
  }

  private List<Double> getWallClockTimeSkewFromAllMinorFragments(
      UserBitShared.MajorFragmentProfile major) {
    // Use only minor fragments that have complete profiles
    // Complete iff the fragment profile has at least one operator profile, and start and end times.
    final List<UserBitShared.MinorFragmentProfile> complete =
        new ArrayList<>(
            Collections2.filter(major.getMinorFragmentProfileList(), Filters.hasOperatorsAndTimes));

    int n = complete.size();
    double[] deviation = new double[n];
    for (int i = 0; i < n; i++) {
      final UserBitShared.MinorFragmentProfile minor = complete.get(i);
      final long wallClockTime = minor.getEndTime() - minor.getStartTime();
      deviation[i] = wallClockTime;
    }
    List<Double> result = getSDFromOperatorMetrics(deviation);
    return result;
  }

  /**
   * Calculate the deviation between batch sizes from all minor fragments of the same operator. Each
   * operator profile can have more than one input profile so it is taking the max.
   *
   * @param major
   * @param operatorId
   * @return
   */
  private List<Double> getBatchSizesFromAllMinorFragments(
      UserBitShared.MajorFragmentProfile major, int operatorId) {

    double[] deviation = new double[major.getMinorFragmentProfileList().size()];

    for (int i = 0; i < major.getMinorFragmentProfileList().size(); i++) {
      UserBitShared.MinorFragmentProfile minor = major.getMinorFragmentProfileList().get(i);

      for (int j = 0; j < minor.getOperatorProfileList().size(); j++) {

        UserBitShared.OperatorProfile operatorProfile = minor.getOperatorProfileList().get(j);
        if (operatorProfile.getOperatorId() == operatorId) {

          long batchSize =
              operatorProfile.getInputProfileList().stream()
                  .collect(Collectors.summarizingLong(bt -> bt.getBatches()))
                  .getMax();
          deviation[i] = batchSize;
        }
      }
    }
    List<Double> result = getSDFromOperatorMetrics(deviation);
    return result;
  }

  private List<Double> getTotalBlockedOrWaitTimeSkew(UserBitShared.MajorFragmentProfile major) {

    double[] deviation = new double[major.getMinorFragmentProfileList().size()];

    for (int i = 0; i < major.getMinorFragmentProfileList().size(); i++) {
      deviation[i] = major.getMinorFragmentProfileList().get(i).getBlockedOnMemoryDuration();
    }
    List<Double> result = getSDFromOperatorMetrics(deviation);
    return result;
  }

  private List<Double> getSDFromOperatorMetrics(double[] metricArray) {
    StatsAccumulator statsAccumulator = new StatsAccumulator();

    List<Double> deviation = new ArrayList<>();

    for (int i = 0; i < metricArray.length; i++) {
      statsAccumulator.add(metricArray[i]);
      deviation.add(metricArray[i]);
    }

    double mean = statsAccumulator.mean();
    double sd = statsAccumulator.populationStandardDeviation();
    for (int i = 0; i < deviation.size(); i++) {
      deviation.set(i, sd == 0 ? 0 : (deviation.get(i) - mean) / sd);
    }
    return deviation;
  }
}
