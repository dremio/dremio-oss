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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.dremio.dac.util.QueryProfileConstant;
import com.dremio.dac.util.QueryProfileUtil;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.service.jobAnalysis.proto.BaseMetrics;
import com.dremio.service.jobAnalysis.proto.GraphNodeDetails;
import com.dremio.service.jobAnalysis.proto.OperatorData;
import com.dremio.service.jobAnalysis.proto.OperatorDataList;
import com.dremio.service.jobAnalysis.proto.PhaseData;
import com.dremio.service.jobAnalysis.proto.PhaseNode;
import com.dremio.service.jobAnalysis.proto.SuccessorNodes;
import com.dremio.service.jobAnalysis.proto.ThreadData;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

/**
 * class for building profile Phase data
 */
public class JobProfileVisualizerUI {

  //Class level variable declaration
  Map<String, Object> graphObjectMap = new HashMap<>();
  List<PhaseData> phaseData = new ArrayList<>();
  UserBitShared.QueryProfile profile;
  Map<String, Object> nodeMap = new HashMap<>();

  //constructor
  public JobProfileVisualizerUI(UserBitShared.QueryProfile profile) {
    this.profile = profile;
  }

  /**
   * This Method will return JobProfile Information
   */
  public List<PhaseData> getJobProfileInfo() {
    List<PhaseData> phaseData = new ArrayList<>();
    buildQueryProfileGraph(); // get details to build a Graph
    if(!graphObjectMap.isEmpty()) {
      phaseData = getPhaseDetails();
    }
    return phaseData;
  }

  /**
   * This Method will build Create map of Profile.Json and calling function to build the Graph details
   */
  private void buildQueryProfileGraph() {
    String profileJson = profile.getJsonPlan();
    if (!profileJson.isEmpty()) {
      Map<String, Object> phaseNodeMap = new Gson().fromJson(
        profileJson, new TypeToken<HashMap<String, Object>>() {
        }.getType()
      );
      buildGraphDetails(phaseNodeMap);
    }
  }

  /**
   * This Method will build "graphObjectMap" map to add MergeNode Name and SuccessorIdList features.
   */
  private void buildGraphDetails(Map<String, Object> phaseNodeMap) {
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

      // This indicator specifies if there is phase change and we need to add hidden node information
      boolean isHiddenNodeHandlingRequired = isPhaseChanged(keySetValue, phaseId, successorList);
      if (isHiddenNodeHandlingRequired) {
        buildHiddenNodeSuccessorIdDetails(nodeName, successorList, successorNodes, phaseId, nodeId);
      } else {
        buildSuccessorIdList(phaseId, nodeId, successorNodes, "", nodeName);
      }
    }
  }

  /**
   * This Method will Extract node Name from Graph
   */
  private String getGraphNodeName(Map<String, Object> phaseNode) {
    int indexFrom = phaseNode.get(QueryProfileConstant.NODE_NAME).toString().lastIndexOf(QueryProfileConstant.DOT) + 1;
    return phaseNode.get(QueryProfileConstant.NODE_NAME).toString().substring(indexFrom).replace(QueryProfileConstant.PREL, QueryProfileConstant.BLANK_SPACE);
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

  /**
   * This Method will get Hidden/customSuccessorIds when Phase Change
   */
  private void buildHiddenNodeSuccessorIdDetails(String nodeName, List<String> successorList, SuccessorNodes successorNode, String phaseId, String nodeId) {
    String nextNodeId = QueryProfileConstant.DEFAULT_NODEID;
    String nextPhaseId = getNextPhaseId(successorList);
    SuccessorNodes nextSuccessorNodes = new SuccessorNodes();
    nextSuccessorNodes.setSuccessorIdList(Arrays.asList(nextPhaseId + QueryProfileConstant.HYPHEN + QueryProfileConstant.DEFAULT_NODEID));

    buildSuccessorIdList(phaseId, nodeId, nextSuccessorNodes, "", nodeName);
    buildSuccessorIdList(nextPhaseId, nextNodeId, successorNode, nodeName, nodeName);
  }

  private void buildSuccessorIdList(String phaseId, String OperatorId, SuccessorNodes successorNode, String mergeNodeName, String nodeName) {
    GraphNodeDetails graphNodeDetails = new GraphNodeDetails();
    graphNodeDetails.setNodeName(nodeName);
    graphNodeDetails.setSuccessorId(successorNode);
    graphNodeDetails.setMergeNodeName(mergeNodeName);
    String mapId = phaseId + "_" + OperatorId;
    graphObjectMap.put(mapId, graphNodeDetails);
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
      nextPhaseId = successorList.get(QueryProfileConstant.DEFAULT_INDEX).substring(phaseIdStartIndex, phaseIdEndIndex);
    }
    return nextPhaseId;
  }

  private List<PhaseData> getPhaseDetails() {
    if (!profile.getFragmentProfileList().isEmpty()) {
      profile.getFragmentProfileList().stream().forEach(
        major -> {
          buildPhaseData(major);
        }
      );
    } else {
      buildPhaseDataOnlyForGraph();
    }
    return phaseData;
  }

  private void buildPhaseData(UserBitShared.MajorFragmentProfile major) {
    int majorId = major.getMajorFragmentId();
    String phaseId = QueryProfileUtil.getStringIds(majorId);
    OperatorDataList operatorDataList = new OperatorDataList();
    List<OperatorData> operatorData = new ArrayList<>();
    List<ThreadData> threadLevelMetricsList = new ArrayList<>();

    //Below Function with build ThreadLevel Metrics of required fields
    buildTheadLevelMetrics(major, threadLevelMetricsList);

    threadLevelMetricsList.stream().collect(Collectors.groupingBy(thread -> thread.getOperatorId(), Collectors.toList())).forEach(
      (operatorId, ThreadLevelMetrics) -> {
        BaseMetrics baseMetrics = new BaseMetrics();
        long processTime = ThreadLevelMetrics.stream().collect(Collectors.summarizingLong(pt -> pt.getProcessingTime())).getMax();
        long peakMemory = ThreadLevelMetrics.stream().collect(Collectors.summarizingLong(memory -> memory.getPeakMemory())).getMax();
        long waitTime = ThreadLevelMetrics.stream().collect(Collectors.summarizingLong(wtTime -> wtTime.getIoWaitTime())).getMax();
        long setupTime = ThreadLevelMetrics.stream().collect(Collectors.summarizingLong(stTime -> stTime.getSetupTime())).getMax();
        long recordsProcessed = ThreadLevelMetrics.stream().collect(Collectors.summarizingLong(row -> row.getRecordsProcessed())).getSum();
        int operatorType = ThreadLevelMetrics.stream().findFirst().get().getOperatorType();

        baseMetrics.setNumThreads(-1L);
        baseMetrics.setProcessingTime(processTime);
        baseMetrics.setRecordsProcessed(recordsProcessed);
        baseMetrics.setPeakMemory(peakMemory);
        baseMetrics.setSetupTime(setupTime);
        baseMetrics.setIoWaitTime(waitTime);

        buildOperatorDataList(phaseId, operatorId, operatorType, baseMetrics, operatorDataList, operatorData, graphObjectMap);
      }
    );
    long processTime = threadLevelMetricsList.stream().collect(Collectors.summarizingLong(pt -> pt.getProcessingTime())).getSum();
    long peakMemory = major.getNodePhaseProfileList().stream().collect(Collectors.summarizingLong(memory -> memory.getMaxMemoryUsed())).getSum();
    long recordsProcessed = operatorDataList.getOperatorDataList().stream().collect(Collectors.summarizingLong(row -> row.getBaseMetrics().getRecordsProcessed())).getSum();

    phaseData.add(new PhaseData(
      phaseId, processTime, peakMemory, recordsProcessed, -1L, operatorDataList
    ));
  }

  private void buildTheadLevelMetrics(UserBitShared.MajorFragmentProfile major, List<ThreadData> threadLevelMetricsList) {
    major.getMinorFragmentProfileList().stream().forEach(
      minor -> {
        minor.getOperatorProfileList().stream().forEach(
          operatorProfile -> {
            long maxThreadLevelRecords = operatorProfile.getInputProfileList().stream().collect(Collectors.summarizingLong(row -> row.getRecords())).getMax();
            threadLevelMetricsList.add(new ThreadData(
              QueryProfileUtil.getStringIds(operatorProfile.getOperatorId()),
              getOperatorName("", operatorProfile.getOperatorType()),
              operatorProfile.getOperatorType(),
              operatorProfile.getWaitNanos(),
              operatorProfile.getPeakLocalMemoryAllocated(),
              operatorProfile.getProcessNanos(),
              operatorProfile.getSetupNanos(),
              maxThreadLevelRecords
            ));
          }
        );
      }
    );
  }

  /**
   * This Method will build the OperatorDataList
   */
  private static void buildOperatorDataList(String phaseId, String operatorId, int operatorType, BaseMetrics baseMetrics, OperatorDataList operatorDataList, List<OperatorData> operatorData, Map<String, Object> stringObjectMap) {
    String mapIndex = phaseId + "_" + operatorId;
    GraphNodeDetails graphNodeDetails = (GraphNodeDetails) stringObjectMap.get(mapIndex);

    operatorData.add(new OperatorData(
      operatorId,
      getOperatorName("", operatorType),
      operatorType,
      baseMetrics,
      graphNodeDetails.getMergeNodeName(),
      graphNodeDetails.getSuccessorId()
    ));
    operatorDataList.setOperatorDataList(operatorData);
  }

  /**
   * This Method is Used to get the Actual Operator Name
   */
  public static String getOperatorName(String nodeName, int operatorType) {
    String OperatorName = nodeName;
    if (!(operatorType == -1)) {
      OperatorName = String.valueOf(UserBitShared.CoreOperatorType.forNumber(operatorType));
    }
    return OperatorName;
  }

  /**
   * This Method will build PhaseData if the query was planned but not executed.
   */
  private void buildPhaseDataOnlyForGraph() {
    List<PhaseNode> phaseNodeList = new ArrayList<>();
    buildPhaseNodeList(phaseNodeList); // This method will build PhaseNodeList which will be used to calculate PhaseData.
    addPhaseDataForOnlyPlannedQuery(phaseNodeList);
  }

  /**
   * This Method will only use when Query was planned and not executed. (regression scenario)
   * This method will build the Data when Graph data is available and Fragment is Empty -rare scenario.
   */
  private void buildPhaseNodeList(List<PhaseNode> phaseNodeList) {
    graphObjectMap.forEach(
      (phaseNodeId, graphData) -> {
        String phaseId = phaseNodeId.substring(0, phaseNodeId.lastIndexOf("_"));
        String nodeId = phaseNodeId.substring(phaseNodeId.lastIndexOf("_") + 1);
        GraphNodeDetails graphNodeDetails = (GraphNodeDetails) graphObjectMap.get(phaseNodeId);
        phaseNodeList.add(new PhaseNode(nodeId, phaseId, graphNodeDetails.getNodeName(), -1,
          new BaseMetrics()
          , graphNodeDetails.getMergeNodeName(), graphNodeDetails.getSuccessorId()));
      }
    );
  }

  /**
   * This Method will build PhaseData to only show middle panel/GraphData for query which was planned but not executed.
   */
  private void addPhaseDataForOnlyPlannedQuery(List<PhaseNode> phaseNodeList) {
    long default_Long = -1l;
    phaseNodeList.stream().collect(Collectors.groupingBy(phaseId -> phaseId.getPhaseId(), Collectors.toList())).forEach(
      (phaseId, nodeDetails) -> {
        OperatorDataList operatorData = new OperatorDataList();
        List<OperatorData> operatorDataList = new ArrayList<>();
        nodeDetails.stream().forEach(
          nodeData -> {
            operatorDataList.add(new OperatorData(
              nodeData.getNodeId(), nodeData.getOperatorName(), nodeData.getOperatorType(), nodeData.getBaseMetrics(),
              nodeData.getMergeNodeName(), nodeData.getSuccessorId()));
          }
        );
        operatorData.setOperatorDataList(operatorDataList);
        phaseData.add(new PhaseData(
          phaseId, default_Long, default_Long, default_Long, default_Long, operatorData
        ));
      }
    );
  }
}
