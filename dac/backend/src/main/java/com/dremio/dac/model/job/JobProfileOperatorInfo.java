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
import static java.lang.String.format;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.ws.rs.NotFoundException;

import org.apache.commons.text.WordUtils;

import com.dremio.dac.util.OperatorMetricsUtil;
import com.dremio.dac.util.QueryProfileUtil;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.service.job.proto.OperationType;
import com.dremio.service.jobAnalysis.proto.BaseMetrics;
import com.dremio.service.jobAnalysis.proto.MetricValue;
import com.dremio.service.jobAnalysis.proto.OperatorSpecificDetails;
import com.dremio.service.jobAnalysis.proto.ThreadData;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class JobProfileOperatorInfo {

  private final String phaseId;
  private final String operatorId;
  private final String operatorName;
  private final int operatorType;
  private final long setupTime;
  private final long waitTime;
  private final long peakMemory;
  private final long processTime;
  private final long bytesProcessed;
  private final long batchesProcessed;
  private final long recordsProcessed;
  private final Map<String,Long> operatorMetricsMap;
  private final OperatorSpecificDetails operatorSpecificDetails;

  @JsonCreator
  public JobProfileOperatorInfo(
    @JsonProperty("phaseId") String phaseId,
    @JsonProperty("operatorId") String operatorId,
    @JsonProperty("operatorName") String operatorName,
    @JsonProperty("operatorType") Integer operatorType,
    @JsonProperty("setupTime") Long setupTime,
    @JsonProperty("waitTime") Long waitTime,
    @JsonProperty("peakMemory") Long peakMemory,
    @JsonProperty("processTime") Long processTime,
    @JsonProperty("bytesProcessed") Long bytesProcessed,
    @JsonProperty("batchesProcessed") Long batchesProcessed,
    @JsonProperty("recordsProcessed") Long recordsProcessed,
    @JsonProperty("operatorMetricsMap") Map<String,Long> operatorMetricsMap,
    @JsonProperty("operatorSpecificDetails") OperatorSpecificDetails operatorSpecificDetails) {
    super();
    this.phaseId = phaseId;
    this.operatorId = operatorId;
    this.operatorName = operatorName;
    this.operatorType = operatorType;
    this.setupTime = setupTime;
    this.waitTime = waitTime;
    this.peakMemory = peakMemory;
    this.processTime = processTime;
    this.bytesProcessed = bytesProcessed;
    this.batchesProcessed = batchesProcessed;
    this.recordsProcessed = recordsProcessed;
    this.operatorMetricsMap = operatorMetricsMap;
    this.operatorSpecificDetails = operatorSpecificDetails;
  }

  public JobProfileOperatorInfo(String phaseId, String operatorId, String operatorName, int operatorType, long setupTime, long waitTime, long peakMemory, long processTime, long bytesProcessed, long batchesProcessed, long recordsProcessed, Map<String, Long> operatorMetricsMap, OperatorSpecificDetails operatorSpecificDetails) {
    this.phaseId = phaseId;
    this.operatorId = operatorId;
    this.operatorName = operatorName;
    this.operatorType = operatorType;
    this.setupTime = setupTime;
    this.waitTime = waitTime;
    this.peakMemory = peakMemory;
    this.processTime = processTime;
    this.bytesProcessed = bytesProcessed;
    this.batchesProcessed = batchesProcessed;
    this.recordsProcessed = recordsProcessed;
    this.operatorMetricsMap = operatorMetricsMap;
    this.operatorSpecificDetails = operatorSpecificDetails;
  }


  public JobProfileOperatorInfo(UserBitShared.QueryProfile profile, int phaseId, int operatorId) {

    if (!profile.getFragmentProfileList().isEmpty()) {
      List<ThreadData> threadLevelMetricsList = new ArrayList<>();
      BaseMetrics baseMetrics = new BaseMetrics();
      UserBitShared.MajorFragmentProfile majorFragmentProfile = getPhaseDetails(profile, phaseId);
      int operatorType = getOperatorTypeHelper(operatorId, majorFragmentProfile);
      OperatorSpecificDetails opsDetails = new OperatorSpecificDetails();
       Map<String,Long> operatorMetricMap = getOperatorMetricMap(profile.getOperatorTypeMetricsMap(), operatorId, majorFragmentProfile, operatorType,opsDetails); // get OperatorMetricsMap

      long batchesProcessed = getBatchSize(majorFragmentProfile, operatorId);

      long bytesProcessed = operatorMetricMap.entrySet().stream().filter(name->name.getKey().toUpperCase().contains("BYTES_")).collect(Collectors.summarizingLong(bytes->bytes.getValue())).getMax();

      bytesProcessed = (bytesProcessed > -1) ? bytesProcessed : -1;

      //Below Function with build ThreadLevel Metrics of required fields
      QueryProfileUtil.buildTheadLevelMetrics(majorFragmentProfile, threadLevelMetricsList);

      //below function build the BaseMetrics information.
      threadLevelMetricsList.stream().filter(operator->Integer.parseInt(operator.getOperatorId()) == operatorId)
        .collect(Collectors.groupingBy(thread -> thread.getOperatorId(), Collectors.toList())).forEach(
        (opId, threadLevelMetrics) -> {
          QueryProfileUtil.buildBaseMetrics(threadLevelMetrics, baseMetrics);
        }
      );

      this.phaseId = QueryProfileUtil.getStringIds(phaseId);
      this.operatorId = QueryProfileUtil.getStringIds(operatorId);
      this.operatorName = String.valueOf(UserBitShared.CoreOperatorType.forNumber(operatorType));
      this.operatorType = operatorType;
      this.setupTime = baseMetrics.getSetupTime();
      this.waitTime = baseMetrics.getIoWaitTime();
      this.peakMemory = baseMetrics.getPeakMemory();
      this.processTime = baseMetrics.getProcessingTime();
      this.bytesProcessed = bytesProcessed;
      this.batchesProcessed = batchesProcessed;
      this.recordsProcessed = baseMetrics.getRecordsProcessed();
      this.operatorMetricsMap = operatorMetricMap;
      this.operatorSpecificDetails = opsDetails;
    } else {
      throw new NotFoundException(format("Profile Fragment is not available"));
    }
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

  public Map<String, Long> getOperatorMetricsMap() {
    return operatorMetricsMap;
  }

  public OperatorSpecificDetails getOperatorSpecificDetails() {
    return operatorSpecificDetails;
  }

  /**
   * This Method Will return Fragment information from Profile Object for requested phase Id
   */
  private UserBitShared.MajorFragmentProfile getPhaseDetails(UserBitShared.QueryProfile profile, int phaseId) {
    UserBitShared.MajorFragmentProfile major;
    try {
      major = profile.getFragmentProfile(phaseId);
    } catch (Exception ex) {
      throw new IllegalArgumentException("Phase Id : " + phaseId + " not Exists in Profile");
    }
    return major;
  }

  /**
   * This Method Will return operator Type for given Operator ID
   */
  private int getOperatorTypeHelper(int nodeId, UserBitShared.MajorFragmentProfile major) {
    int operatorType = -1;
    UserBitShared.MinorFragmentProfile minor = major.getMinorFragmentProfile(0);
    for (int operatorIndex = 0; operatorIndex < minor.getOperatorProfileCount(); operatorIndex++) {
      if (nodeId == minor.getOperatorProfile(operatorIndex).getOperatorId()) {
        UserBitShared.OperatorProfile operatorProfile = minor.getOperatorProfile(operatorIndex);
        operatorType = operatorProfile.getOperatorType();
      }
    }
    if (operatorType == -1) {
      throw new IllegalArgumentException("OperatorId : " + nodeId + "not exists in Phase " + major.getMajorFragmentId());
    }
    return operatorType;
  }

  /**
   * This Method will build the OperatorSpecificDetails
   */
  private void addOperatorSpecificDetails(UserBitShared.OperatorProfile operatorProfile, OperatorSpecificDetails opsDetails ) {
    OperationType operationType = com.dremio.service.job.proto.OperationType.valueOf(operatorProfile.getOperatorType());
    operatorProfile.getDetails().getSlowIoInfosList().stream().forEach(
      slowIOInfo -> {
        opsDetails.setIoTimeNs(slowIOInfo.getIoTime());
        opsDetails.setOffset(slowIOInfo.getIoOffset());
        opsDetails.setIoSize(slowIOInfo.getIoSize());
        opsDetails.setFilePath(slowIOInfo.getFilePath());
        opsDetails.setOperationType(String.valueOf(operationType));
      }
    );
  }
  /**
   * This method to add the information to MetricsValue to Add all the Operators
   */
  private void buildMetricsValueList(UserBitShared.CoreOperatorTypeMetricsMap coreOperatorTypeMetricsMap, int operatorType, List<MetricValue> tempMetricsList, Integer id, List<UserBitShared.MetricValue> metricValueList) {
    String metricName = "";
    Long metricValue = -1L;
    Optional<UserBitShared.MetricDef> mapMetricsDef = getMetricById(coreOperatorTypeMetricsMap, operatorType, id);
    if (mapMetricsDef.isPresent()) {
      metricName = mapMetricsDef.get().getName();
      String[] nodeValues = getOperatorSpecificMetrics(operatorType);
      List<String> validMetricsList = Arrays.asList(nodeValues).stream().map(n -> n.toLowerCase()).collect(Collectors.toList());
      String listString = validMetricsList.stream().map(Object::toString).collect(Collectors.joining(", "));
      if (containsName(validMetricsList, metricName.toLowerCase()) || listString.isEmpty()) {
        metricValue = metricValueList.stream().collect(Collectors.summarizingLong(metricsValue -> metricsValue.getLongValue())).getSum();
        tempMetricsList.add(new MetricValue(id, WordUtils.capitalizeFully(metricName, '_'), metricValue));
      }
    }
  }

  /**
   * This method will return an array of Operator Specific Metrics
   */
  private String[] getOperatorSpecificMetrics(Integer operatorType) {
    String[] metricList = OperatorMetricsUtil.operatorSpecificMetrics.get(operatorType);
    if (metricList == null) {
      metricList = new String[]{""};
    }
    return metricList;
  }

  /**
   * This Method Will return a Indicator which specifies if Metrics information need to send to response
   */
  private boolean containsName(final List<String> columnList, final String name) {
    return columnList.stream().anyMatch(names->names.equals(name));
  }

  /**
   * This is the Method which will return Sum/min/Max Operator Metrics value
   */
  public static Map<String, Long> buildOperatorMetricsMap(List<MetricValue> tempMetricsList) {
    Map<String,Long> operatorMetricsMap = new HashMap<>();
    tempMetricsList.stream().collect(Collectors.groupingBy(metricName -> metricName.getMetricName(), Collectors.summarizingLong(value -> value.getMetricValue()))).forEach(
      (name, value) -> {
        operatorMetricsMap.put(name, value.getSum());
      }
    );
    return operatorMetricsMap;
  }

  /**
   * This Method Will return Metric List for operator
   */
  private Map<String,Long> getOperatorMetricMap(UserBitShared.CoreOperatorTypeMetricsMap coreOperatorTypeMetricsMap, int nodeId, UserBitShared.MajorFragmentProfile major, int operatorType,OperatorSpecificDetails opsDetails) {
    List<MetricValue> tempMetricsList = new ArrayList<>();
    major.getMinorFragmentProfileList().stream()
      .flatMap(minorFragmentProfile -> minorFragmentProfile.getOperatorProfileList().stream().filter(minor -> minor.getOperatorId() == nodeId))
      .forEach(
        operatorProfile -> {
          addOperatorSpecificDetails(operatorProfile,opsDetails );
          operatorProfile.getMetricList().stream().collect(Collectors.groupingBy(id -> id.getMetricId(), Collectors.toList()))
            .forEach(
              (id, metricValueList) -> {
                buildMetricsValueList(coreOperatorTypeMetricsMap, operatorType, tempMetricsList, id, metricValueList);
              }
            );
        }
      );

    return buildOperatorMetricsMap(tempMetricsList);
  }

  /**
   * This Method Will return batch Size of an Operator
   */
  private Long getBatchSize(UserBitShared.MajorFragmentProfile major, int operatorId) {
    List<Long> maxBatchSizeList = new ArrayList<>();
    major.getMinorFragmentProfileList().stream().forEach(
      minor -> {
        minor.getOperatorProfileList().stream().filter(op -> op.getOperatorId() == operatorId).forEach(
          ops -> {
            Long batchSize = -1L;
            if (!ops.getMetricList().isEmpty()) {
              batchSize = ops.getInputProfileList().stream().collect(Collectors.summarizingLong(bt -> bt.getBatches())).getMax();
            }
            maxBatchSizeList.add(batchSize);
          });
      });
    long totalBatchSize = maxBatchSizeList.stream().filter(value -> !value.equals(-1L)).collect(Collectors.summarizingLong(Long::longValue)).getSum();
    return totalBatchSize;
  }
}
