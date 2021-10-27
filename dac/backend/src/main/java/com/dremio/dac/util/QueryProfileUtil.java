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
package com.dremio.dac.util;

import static com.dremio.exec.ops.OperatorMetricRegistry.getMetricById;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;


import com.dremio.exec.proto.UserBitShared;
import com.dremio.service.jobAnalysis.proto.BaseMetrics;
import com.dremio.service.jobAnalysis.proto.MetricValue;
import com.dremio.service.jobAnalysis.proto.Node;
import com.dremio.service.jobAnalysis.proto.SuccessorNodes;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

/**
 * class for building Node data from Profile Object
 */
public class QueryProfileUtil {

  UserBitShared.QueryProfile profile;
  List<Node> nodeList = new ArrayList<>();

  public QueryProfileUtil(UserBitShared.QueryProfile profile) {
    this.profile = profile;
  }

  public List<Node> getNodeList() {
    return nodeList;
  }

  /**
   * Method to extract the all the information to build a graph in Query Profile page.
   */
  public List<Node> getNodeDetails() throws JsonProcessingException, ClassNotFoundException {
    String profileJson = profile.getJsonPlan();
    Map<String, Object> phaseNodeMap = new Gson().fromJson(
      profileJson, new TypeToken<HashMap<String, Object>>() {
      }.getType()
    );
    boolean isFragmentToProcess = isProfileFragmentToProcess();
    List<Node> nodeList  = getNodes(phaseNodeMap, isFragmentToProcess);
    return nodeList;
  }

  /**
   * This Method will return Metric Information  on operator Level such as Bytes Sent etc
   */
  public List<MetricValue> getMetricsData(UserBitShared.CoreOperatorTypeMetricsMap metricsMap, UserBitShared.OperatorProfile operatorData) {
    int operatorId  = operatorData.getOperatorType();
    List<MetricValue> metricData = new ArrayList<>();
    for (int metricsIndex = 0; metricsIndex < operatorData.getMetricList().size(); metricsIndex++) {
      int metricId = operatorData.getMetric(metricsIndex).getMetricId();
      String metricName = QueryProfileConstant.DEFAULT_NULL;
      long metricValue = QueryProfileConstant.DEFAULT_LONG;
      Optional<UserBitShared.MetricDef> mapMetricsDef = getMetricById(metricsMap, operatorId, metricId);
      if (mapMetricsDef.isPresent()) {
        metricName = mapMetricsDef.get().getName();
        metricValue = operatorData.getMetric(metricsIndex).getLongValue();
      }
      metricData.add(new MetricValue(metricId, metricName, metricValue));
    }
    return metricData;
  }

  /**
   * This Method will return Node Metric information
   */
  public BaseMetrics getNodeMetrics(UserBitShared.MajorFragmentProfile phaseData, int OperatorId, UserBitShared.CoreOperatorTypeMetricsMap metricsMap) {
    long processingTime = -1;
    long memory = -1;
    long waitTime = -1;
    long setupTime = -1;
    long totalRowCount = -1;
    long totalBytesProcessed = -1;
    long numThreads = -1L;

    if (phaseData.getMinorFragmentProfileCount() > 0) {
      for (int minorIndex = 0; minorIndex < phaseData.getMinorFragmentProfileCount(); minorIndex++) {
        if (phaseData.getMinorFragmentProfile(minorIndex).getOperatorProfileCount() > 0) {
          for (int operatorIndex = 0; operatorIndex < phaseData.getMinorFragmentProfile(minorIndex).getOperatorProfileCount(); operatorIndex++) {
            UserBitShared.OperatorProfile operatorData = phaseData.getMinorFragmentProfile(minorIndex).getOperatorProfile(operatorIndex);
            if (operatorData.getOperatorId() == OperatorId) {
              processingTime = (operatorData.getProcessNanos() > processingTime) ? operatorData.getProcessNanos() : processingTime;
              memory = (operatorData.getPeakLocalMemoryAllocated() > memory) ? operatorData.getPeakLocalMemoryAllocated() : memory;
              waitTime = (operatorData.getWaitNanos() > waitTime) ? operatorData.getWaitNanos() : waitTime;
              setupTime = (operatorData.getSetupNanos() > setupTime) ? operatorData.getSetupNanos() : setupTime;
              if (totalRowCount >= 0) {
                totalRowCount = totalRowCount + getThreadRowCount(operatorData);
              } else {
                totalRowCount = getThreadRowCount(operatorData);
              }
              long tempTotalBytesProcessed = 0;
              if (operatorData.getMetricCount() > 0) {
                LongSummaryStatistics summaryTotalBytesProcessed = getMetricsData(metricsMap, operatorData)
                  .stream().filter(operator -> operator.getMetricName().contains(QueryProfileConstant.BYTES))
                  .collect(Collectors.summarizingLong(metricvalue -> metricvalue.getMetricValue()));
                tempTotalBytesProcessed = summaryTotalBytesProcessed.getSum();
              }
              totalBytesProcessed = (tempTotalBytesProcessed > totalBytesProcessed) ? tempTotalBytesProcessed : totalBytesProcessed;
            }
          }
        }
      }
    }
    BaseMetrics baseMetrics = new BaseMetrics();
    baseMetrics.setProcessingTime(processingTime);
    baseMetrics.setPeakMemory(memory);
    baseMetrics.setIoWaitTime(waitTime);
    baseMetrics.setSetupTime(setupTime);
    baseMetrics.setRecordsProcessed(totalRowCount);
    baseMetrics.setBytesProcessed(totalBytesProcessed);
    baseMetrics.setNumThreads(numThreads);
    return baseMetrics;
  }
// OperatorMetric [id, value]
  // Porfile.CoreOperatorMetricsMap
  /**
   * This Method will return List of Node information
   */
  private List<Node> getNodes(Map<String, Object> phaseNodeMap,boolean isFragmentToProcess) {


    // For each operator of each phase
    for (Map.Entry<String, Object> keySet : phaseNodeMap.entrySet()) {  //PhaseId-OperatorIndex combination like 02-07 for Phase 2 OperatorIndex 7
      String keySetValue = keySet.getKey();
      Map<String, Object> phaseNode = (Map<String, Object>) phaseNodeMap.get(keySetValue);  //select one record from the PhaseNode map.
      String nodeName = getGraphNodeName(phaseNode);

      // build successor List for PhaseNode
      List<String> successorList = getSuccessorIdList(phaseNode);
      SuccessorNodes successorNodes = new SuccessorNodes();
      successorNodes.setSuccessorIdList(successorList);

      // extract mejorFragmentId and Operator Id
      int phaseIdStartIndex = keySetValue.indexOf(QueryProfileConstant.DOUBLE_QUOTE) + 1;
      int phaseIdEndIndex = keySetValue.indexOf(QueryProfileConstant.HYPHEN);
      int operatorIdStartIndex = keySetValue.indexOf(QueryProfileConstant.HYPHEN) + 1;
      int operatorIdEndIndex = keySetValue.lastIndexOf(QueryProfileConstant.DOUBLE_QUOTE);
      String phaseId = keySetValue.substring(phaseIdStartIndex, phaseIdEndIndex);
      String nodeId = keySetValue.substring(operatorIdStartIndex, operatorIdEndIndex);
      int majorFragmentId = Integer.parseInt(phaseId);
      int operatorId = Integer.parseInt(nodeId);
      int operatorType = -1;
      UserBitShared.MajorFragmentProfile phaseData ;
      if(isFragmentToProcess) {
         phaseData = profile.getFragmentProfile(majorFragmentId);  //extract major Fragment profile for Phase
         operatorType = getOperatorTypeFromId(operatorId, phaseData);
      }
      else {
         phaseData = null;
      }

      // This indicator specifies if there is phase change and we need to add hidden node information
      boolean isHiddenNodeHandlingRequired = isPhaseChanged(keySetValue, phaseId, successorList);

      addNodeDetails(nodeList, nodeName, successorList, successorNodes, phaseId, nodeId, operatorId, phaseData, operatorType, isHiddenNodeHandlingRequired);
    }
    return nodeList;
  }

  /**
   * This Method add Details to nodeList based on custom logic
   */
  private void addNodeDetails(List<Node> nodeList, String nodeName, List<String> successorList, SuccessorNodes successorNode, String phaseId, String nodeId, int operatorId, UserBitShared.MajorFragmentProfile phaseData, int operatorType, boolean isHiddenNodeHandlingRequired) {
    String currentOperatorName = getOperatorName(nodeName, operatorType);
    if (isHiddenNodeHandlingRequired) {
      int nextOperatorId = QueryProfileConstant.DEFAULT_INDEX;
      String nextNodeId = QueryProfileConstant.DEFAULT_NODEID;
      String nextPhaseId = getNextPhaseId(successorList);
      UserBitShared.MajorFragmentProfile nextPhaseData = profile.getFragmentProfile(Integer.parseInt(nextPhaseId));
      int nextOperatorType = getOperatorTypeFromId(0, nextPhaseData);
      String nextOperatorName = String.valueOf(UserBitShared.CoreOperatorType.forNumber(nextOperatorType));
      SuccessorNodes nextSuccessorNodes = new SuccessorNodes();
      nextSuccessorNodes.setSuccessorIdList(Arrays.asList(nextPhaseId + QueryProfileConstant.HYPHEN + QueryProfileConstant.DEFAULT_NODEID));

      // Add current Node details
      addNodeList(nodeList, nextSuccessorNodes, phaseId, nodeId, operatorId,
        phaseData, operatorType, QueryProfileConstant.BLANK_SPACE, currentOperatorName);

      //Add Hidden Node details
      addNodeList(nodeList, successorNode, nextPhaseId, nextNodeId,
        nextOperatorId, nextPhaseData, nextOperatorType, nodeName, nextOperatorName);
    } else {
      addNodeList(nodeList, successorNode, phaseId, nodeId, operatorId, phaseData, operatorType, QueryProfileConstant.BLANK_SPACE, currentOperatorName);
    }
  }

  /**
   * This Method Will Add Node Records to NodeList
   */
  private void addNodeList( List<Node> nodeList,
                            SuccessorNodes successorNode, String phaseId, String nodeId,
                            int operatorId, UserBitShared.MajorFragmentProfile phaseData,
                            int operatorType, String nodeMergeName, String operatorName) {


    UserBitShared.CoreOperatorTypeMetricsMap metricsMap = profile.getOperatorTypeMetricsMap();
    BaseMetrics baseMetrics = new BaseMetrics() ;
    if (profile.getFragmentProfileCount() > 0) {
      baseMetrics = getNodeMetrics(phaseData, operatorId, metricsMap);
    }
    nodeList.add(new Node(
      nodeId,
      phaseId,
      operatorName,
      operatorType,
      baseMetrics,
      nodeMergeName,
      successorNode
    ));
  }

  /**
   * This Method will return Thread RowCount for each node information
   */
  private long getThreadRowCount(UserBitShared.OperatorProfile operatorData) {
    long rowCount = QueryProfileConstant.DEFAULT_LONG;
    if (operatorData.getInputProfileCount() >0) {
      for (int inputIndex = QueryProfileConstant.DEFAULT_INDEX; inputIndex < operatorData.getInputProfileCount(); inputIndex++) {
        long tempRowCount = operatorData.getInputProfile(inputIndex).getRecords();
        if (tempRowCount > rowCount) {
          rowCount = tempRowCount;
        }
      }
    }
    return rowCount;
  }

  /**
   * This Method Is used to get the Next PhaseId
   * This Method is called only when Phase Changed so we can add missing/ hidden nodes from Graph
   */
  private String getNextPhaseId(List<String> successorList) {
    int phaseIdStartIndex = successorList.get(0).indexOf(QueryProfileConstant.DOUBLE_QUOTE) + 1;
    int phaseIdEndIndex = successorList.get(0).indexOf(QueryProfileConstant.HYPHEN);
    String nextPhaseId = QueryProfileConstant.BLANK_SPACE;
    if (!successorList.get(QueryProfileConstant.DEFAULT_INDEX).isEmpty()) {
      nextPhaseId = successorList.get(QueryProfileConstant.DEFAULT_INDEX).substring(phaseIdStartIndex , phaseIdEndIndex);
    }
    return nextPhaseId;
  }

  /**
   * This Method is Used to get the Actual Operator Name
   */
  private String getOperatorName(String nodeName, int operatorType) {
    String OperatorName = nodeName;
    if (!(operatorType == -1)) {
      OperatorName = String.valueOf(UserBitShared.CoreOperatorType.forNumber(operatorType));
    }
    return OperatorName;
  }

  /**
   * This Method add Successor Id List
   */
  private List<String> getSuccessorIdList(Map<String, Object> successorNodes) {
    return Arrays.asList((successorNodes.get(QueryProfileConstant.INPUTS).toString()
      .replaceAll(QueryProfileConstant.OPEN_BRACKET, QueryProfileConstant.BLANK_SPACE)
      .replaceAll(QueryProfileConstant.CLOSED_BRACKET, QueryProfileConstant.BLANK_SPACE).trim()).split(QueryProfileConstant.COMMA_SPACE_SEPERATOR));
  }

  /**
   * This Method will Extract node Name from Graph
   */
  private String getGraphNodeName(Map<String, Object> phaseNode) {
    int indexFrom = phaseNode.get(QueryProfileConstant.NODE_NAME).toString().lastIndexOf(QueryProfileConstant.DOT) + 1;
    return phaseNode.get(QueryProfileConstant.NODE_NAME).toString().substring(indexFrom).replace(QueryProfileConstant.PREL, QueryProfileConstant.BLANK_SPACE);
  }

  /**
   * This Method will return List of Node information Based Type of Profile Object
   */
  private boolean isProfileFragmentToProcess() {
    boolean isFragmentToProcess = true;
    if (profile.getJsonPlan().isEmpty() && profile.getFragmentProfileCount() > 0) { // when Query plan is empty and Fragment info is available will return Empty Array
      throw new IndexOutOfBoundsException("Query is not Planned");
    } else if (!(profile.getJsonPlan().isEmpty()) && profile.getFragmentProfileCount() == 0) { // when Query plan info is Available and Fragment info is empty
      isFragmentToProcess = false ;
    }
    return isFragmentToProcess;
  }

  /**
   * This Method is Used to get the Operator Type from Operator Id
   */
  private int getOperatorTypeFromId(int operatorId, UserBitShared.MajorFragmentProfile phaseData) {
    int operatorType = -1;
    if(phaseData.getMinorFragmentProfileList().size()>0) {
      operatorType = phaseData.getMinorFragmentProfile(QueryProfileConstant.DEFAULT_INDEX).getOperatorProfileList().stream()
        .collect(Collectors.groupingBy(operator -> operator.getOperatorId(), Collectors.toList())).get(operatorId).get(0).getOperatorType();
    }
    return operatorType;
  }

  /**
   * This Method will return a flag to say Next Node Merge Indicator
   */
  private boolean isPhaseChanged(String keySetValue, String phaseId, List<String> successorList) {
    boolean changeSuccessorId = QueryProfileConstant.DEFAULT_IND;
    if (!successorList.get(0).isEmpty()) {
      String nextPhaseId = successorList.get(QueryProfileConstant.DEFAULT_INDEX).substring(keySetValue.indexOf(QueryProfileConstant.DOUBLE_QUOTE), keySetValue.indexOf(QueryProfileConstant.HYPHEN) - 1);
      if (!nextPhaseId.equals(phaseId)) {
        changeSuccessorId = true;
      }
    }
    return changeSuccessorId;
  }
}
