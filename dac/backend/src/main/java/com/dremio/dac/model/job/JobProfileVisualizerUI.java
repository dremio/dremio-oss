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

import static com.dremio.dac.server.admin.profile.HostProcessingRateUtil.computeRecordProcRateAtPhaseHostLevel;
import static com.dremio.dac.server.admin.profile.HostProcessingRateUtil.computeRecordProcRateAtPhaseOperatorHostLevel;
import static com.dremio.dac.util.QueryProfileConstant.REFLECTION_PREFIX;
import static java.lang.String.format;

import com.dremio.common.utils.PathUtils;
import com.dremio.dac.model.job.diagnostics.ProfileObservationUtil;
import com.dremio.dac.server.admin.profile.AccelerationWrapper;
import com.dremio.dac.server.admin.profile.HostProcessingRate;
import com.dremio.dac.util.QueryProfileConstant;
import com.dremio.dac.util.QueryProfileUtil;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.service.accelerator.AccelerationDetailsUtils;
import com.dremio.service.accelerator.proto.AccelerationDetails;
import com.dremio.service.jobAnalysis.proto.BaseMetrics;
import com.dremio.service.jobAnalysis.proto.GraphNodeDetails;
import com.dremio.service.jobAnalysis.proto.OperatorData;
import com.dremio.service.jobAnalysis.proto.PhaseData;
import com.dremio.service.jobAnalysis.proto.PhaseNode;
import com.dremio.service.jobAnalysis.proto.SuccessorNodes;
import com.dremio.service.jobAnalysis.proto.ThreadData;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.ImmutablePair;

/** class for building profile Phase data */
@SuppressWarnings("checkstyle:VisibilityModifier")
public class JobProfileVisualizerUI {

  // Class level variable declaration
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(JobProfileVisualizerUI.class);
  Map<String, Object> graphObjectMap = new HashMap<>();
  List<PhaseData> phaseDataList = new ArrayList<>();
  UserBitShared.QueryProfile profile;
  private static final Splitter splitter =
      Splitter.on(CharMatcher.is(',')).trimResults().omitEmptyStrings();
  private static Map<String, String> operatorToTable;
  private final ObjectMapper mapper;
  private static Map<String, List<String>>
      reflectionDatasetMap; // ReflectionID -> [ReflectionName, DatasetPath]

  // constructor
  public JobProfileVisualizerUI(UserBitShared.QueryProfile profile) {
    this.profile = profile;
    mapper = new ObjectMapper();
    mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    operatorToTable = Maps.newHashMap();
    reflectionDatasetMap = Maps.newHashMap();
  }

  /** This Method will return JobProfile Information */
  @WithSpan
  public List<PhaseData> getJobProfileInfo() {
    List<PhaseData> phaseData = new ArrayList<>();
    buildQueryProfileGraph(); // get details to build a Graph
    if (!graphObjectMap.isEmpty()) {
      phaseData = getPhaseDetails();
    }
    return phaseData;
  }

  /**
   * This Method will build Create map of Profile.Json and calling function to build the Graph
   * details
   */
  @WithSpan
  private void buildQueryProfileGraph() {
    String profileJson = profile.getJsonPlan();
    if (!profileJson.isEmpty()) {
      Map<String, Object> phaseNodeMap =
          new Gson().fromJson(profileJson, new TypeToken<HashMap<String, Object>>() {}.getType());
      buildGraphDetails(phaseNodeMap);
    }
  }

  /**
   * This Method will build "graphObjectMap" map to add MergeNode Name and SuccessorIdList features.
   */
  @WithSpan
  private void buildGraphDetails(Map<String, Object> phaseNodeMap) {
    for (Map.Entry<String, Object> keySet :
        phaseNodeMap
            .entrySet()) { // PhaseId-OperatorIndex combination like 02-07 for Phase 2 OperatorIndex
      // 7
      String keySetValue = keySet.getKey();
      Map<String, Object> phaseNode =
          (Map<String, Object>)
              phaseNodeMap.get(keySetValue); // select one record from the PhaseNode map.
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

      // This indicator specifies if there is phase change and we need to add hidden node
      // information
      boolean isHiddenNodeHandlingRequired = isPhaseChanged(keySetValue, phaseId, successorList);
      if (isHiddenNodeHandlingRequired) {
        buildHiddenNodeSuccessorIdDetails(nodeName, successorList, successorNodes, phaseId, nodeId);
      } else {
        buildSuccessorIdList(phaseId, nodeId, successorNodes, "", nodeName);
      }
    }
  }

  /** This Method will Extract node Name from Graph */
  private String getGraphNodeName(Map<String, Object> phaseNode) {
    int indexFrom =
        phaseNode
                .get(QueryProfileConstant.NODE_NAME)
                .toString()
                .lastIndexOf(QueryProfileConstant.DOT)
            + 1;
    return phaseNode
        .get(QueryProfileConstant.NODE_NAME)
        .toString()
        .substring(indexFrom)
        .replace(QueryProfileConstant.PREL, QueryProfileConstant.BLANK_SPACE);
  }

  /** This Method add Successor Id List */
  private List<String> getSuccessorIdList(Map<String, Object> successorNodes) {
    return Arrays.asList(
        (successorNodes
                .get(QueryProfileConstant.INPUTS)
                .toString()
                .replaceAll(QueryProfileConstant.OPEN_BRACKET, QueryProfileConstant.BLANK_SPACE)
                .replaceAll(QueryProfileConstant.CLOSED_BRACKET, QueryProfileConstant.BLANK_SPACE)
                .trim())
            .split(QueryProfileConstant.COMMA_SPACE_SEPERATOR));
  }

  /** This Method will return a flag to say Next Node Merge Indicator */
  private boolean isPhaseChanged(String keySetValue, String phaseId, List<String> successorList) {
    boolean changeSuccessorId = QueryProfileConstant.DEFAULT_IND;
    if (!successorList.get(0).isEmpty()) {
      String nextPhaseId =
          successorList
              .get(QueryProfileConstant.DEFAULT_INDEX)
              .substring(
                  keySetValue.indexOf(QueryProfileConstant.DOUBLE_QUOTE),
                  keySetValue.indexOf(QueryProfileConstant.HYPHEN) - 1);
      if (!nextPhaseId.equals(phaseId)) {
        changeSuccessorId = true;
      }
    }
    return changeSuccessorId;
  }

  /** This Method will get Hidden/customSuccessorIds when Phase Change */
  private void buildHiddenNodeSuccessorIdDetails(
      String nodeName,
      List<String> successorList,
      SuccessorNodes successorNode,
      String phaseId,
      String nodeId) {
    String nextNodeId = QueryProfileConstant.DEFAULT_NODEID;
    String nextPhaseId = getNextPhaseId(successorList);
    SuccessorNodes nextSuccessorNodes = new SuccessorNodes();
    nextSuccessorNodes.setSuccessorIdList(
        Arrays.asList(
            nextPhaseId + QueryProfileConstant.HYPHEN + QueryProfileConstant.DEFAULT_NODEID));

    buildSuccessorIdList(phaseId, nodeId, nextSuccessorNodes, "", nodeName);
    buildSuccessorIdList(nextPhaseId, nextNodeId, successorNode, nodeName, nodeName);
  }

  private void buildSuccessorIdList(
      String phaseId,
      String OperatorId,
      SuccessorNodes successorNode,
      String mergeNodeName,
      String nodeName) {
    GraphNodeDetails graphNodeDetails = new GraphNodeDetails();
    graphNodeDetails.setNodeName(nodeName);
    graphNodeDetails.setSuccessorId(successorNode);
    graphNodeDetails.setMergeNodeName(mergeNodeName);
    String mapId = phaseId + "_" + OperatorId;
    graphObjectMap.put(mapId, graphNodeDetails);
  }

  /**
   * This Method Is used to get the Next PhaseId This Method is called only when Phase Changed so we
   * can add missing/ hidden nodes from Graph
   */
  private String getNextPhaseId(List<String> successorList) {
    int phaseIdStartIndex = successorList.get(0).indexOf(QueryProfileConstant.DOUBLE_QUOTE) + 1;
    int phaseIdEndIndex = successorList.get(0).indexOf(QueryProfileConstant.HYPHEN);
    String nextPhaseId = QueryProfileConstant.BLANK_SPACE;
    if (!successorList.get(QueryProfileConstant.DEFAULT_INDEX).isEmpty()) {
      nextPhaseId =
          successorList
              .get(QueryProfileConstant.DEFAULT_INDEX)
              .substring(phaseIdStartIndex, phaseIdEndIndex);
    }
    return nextPhaseId;
  }

  private void checkIsAssignable(String field, Class<?> target, Class<?> expected)
      throws IOException {
    if (!expected.isAssignableFrom(target)) {
      throw new IOException(
          format("Invalid field %s, expected type %s, found %s", field, expected, target));
    }
  }

  private void parseJsonPlan() throws IOException {
    if (profile.getJsonPlan() == null || profile.getJsonPlan().isEmpty()) {
      return;
    }
    // Parse the plan and map tables to major fragment and operator ids.
    final Map<String, Object> plan = mapper.readValue(profile.getJsonPlan(), Map.class);
    for (Map.Entry<String, Object> entry : plan.entrySet()) {
      checkIsAssignable(entry.getKey(), entry.getValue().getClass(), Map.class);
      final Map<String, Object> operatorInfo = (Map) entry.getValue();
      final String operator = (String) operatorInfo.get("\"op\"");
      if (operator != null
          && (operator.contains("Scan") || operator.contains("TableFunction"))
          && operatorInfo.containsKey("\"values\"")) {
        // Get table name
        checkIsAssignable(
            entry.getKey() + ": values", operatorInfo.get("\"values\"").getClass(), Map.class);
        final Map<String, Object> values = (Map) operatorInfo.get("\"values\"");
        if (values.containsKey("\"table\"")) {
          final String tokens = ((String) values.get("\"table\"")).replaceAll("^\\[|\\]$", "");
          final String tablePath =
              PathUtils.constructFullPath(splitter.splitToList(tokens)).replaceAll("\"", "");
          operatorToTable.put(entry.getKey().replaceAll("\"", ""), tablePath);
        }
      }
    }
  }

  private void createReflectionsDatasetMap() {
    UserBitShared.AccelerationProfile accelerationProfile = profile.getAccelerationProfile();
    List<UserBitShared.LayoutMaterializedViewProfile> layoutProfilesList =
        accelerationProfile.getLayoutProfilesList();
    AccelerationDetails details = null;
    AccelerationWrapper wrapper = null;
    try {
      details =
          AccelerationDetailsUtils.deserialize(
              profile.getAccelerationProfile().getAccelerationDetails());
      if (details != null) {
        wrapper = new AccelerationWrapper(details);
      }
    } catch (Exception e) {
      details = new AccelerationDetails();
      logger.warn("Failed to deserialize acceleration details", e);
    }
    AccelerationWrapper accelerationDetails = wrapper;
    for (UserBitShared.LayoutMaterializedViewProfile viewProfile : layoutProfilesList) {
      String reflectionDatasetPath =
          accelerationDetails.getReflectionDatasetPath(viewProfile.getLayoutId());
      List<String> datasetNameReflectionName = new ArrayList<String>();
      datasetNameReflectionName.add(viewProfile.getName());
      datasetNameReflectionName.add(reflectionDatasetPath.replaceAll("\"", ""));
      reflectionDatasetMap.put(viewProfile.getLayoutId(), datasetNameReflectionName);
    }
  }

  @WithSpan
  private List<PhaseData> getPhaseDetails() {
    if (!profile.getFragmentProfileList().isEmpty()) {
      try {
        parseJsonPlan();
        createReflectionsDatasetMap();
      } catch (IOException e) {
        logger.error(
            "Failed to parse physical plan for query {}, plan {}",
            profile.getId(),
            profile.getJsonPlan(),
            e);
      }
      profile.getFragmentProfileList().stream()
          .forEach(
              major -> {
                buildPhaseData(major);
              });
    } else {
      buildPhaseDataOnlyForGraph();
    }
    return phaseDataList;
  }

  private void buildPhaseData(UserBitShared.MajorFragmentProfile major) {
    int majorId = major.getMajorFragmentId();
    String phaseId = QueryProfileUtil.getStringIds(majorId);
    List<OperatorData> operatorData = new ArrayList<>();
    List<ThreadData> threadLevelMetricsList = new ArrayList<>();
    final Map<
            ImmutablePair<Integer, Integer>,
            List<ImmutablePair<UserBitShared.OperatorProfile, Integer>>>
        opmap = new HashMap<>();
    Table<Integer, Integer, String> majorMinorHostTable = HashBasedTable.create();

    // Below Function with build ThreadLevel Metrics of required fields
    QueryProfileUtil.buildTheadLevelMetrics(major, threadLevelMetricsList);

    Map<Integer, Set<HostProcessingRate>> majorHostProcRateSetMap =
        getMajorHostProcRateSetMap(majorId, opmap, majorMinorHostTable);

    Set<HostProcessingRate> unAggregatedSetForMajor =
        majorHostProcRateSetMap.get(major.getMajorFragmentId());
    Set<HostProcessingRate> hostProcessingRateSet =
        computeRecordProcRateAtPhaseHostLevel(major.getMajorFragmentId(), unAggregatedSetForMajor);

    threadLevelMetricsList.stream()
        .collect(Collectors.groupingBy(thread -> thread.getOperatorId(), Collectors.toList()))
        .forEach(
            (operatorId, threadLevelMetrics) -> {
              int operatorType = threadLevelMetrics.stream().findFirst().get().getOperatorType();
              BaseMetrics baseMetrics = new BaseMetrics();
              QueryProfileUtil.buildBaseMetrics(
                  threadLevelMetrics, baseMetrics); // build BaseMetrics

              // This will build OperatorDataList for Phase.
              operatorData.add(
                  operatorDataList(phaseId, operatorId, operatorType, baseMetrics, graphObjectMap));
            });

    long processTime =
        hostProcessingRateSet.stream()
            .collect(
                Collectors.summarizingLong(
                    processTiming -> Long.valueOf(String.valueOf(processTiming.getProcessNanos()))))
            .getMax();
    long runTime =
        major.getMinorFragmentProfileList().stream()
            .mapToLong(minor -> minor.getRunDuration())
            .max()
            .orElse(0);
    long peakMemory =
        major.getMinorFragmentProfileList().stream()
            .mapToLong(minor -> minor.getMaxMemoryUsed())
            .max()
            .orElse(0);
    long totalMemory =
        major.getMinorFragmentProfileList().stream()
            .mapToLong(minor -> minor.getMaxMemoryUsed())
            .sum();
    long recordsProcessed =
        hostProcessingRateSet.stream()
            .collect(
                Collectors.summarizingLong(
                    records -> Long.valueOf(String.valueOf(records.getNumRecords()))))
            .getSum();
    long totalBufferForIncomingMemory =
        major.getMinorFragmentProfileList().stream()
            .mapToLong(minor -> minor.getMaxIncomingMemoryUsed())
            .sum();

    List<PhaseData.PhaseObservationType> notableObservationAtPhaseLevelList =
        ProfileObservationUtil.getNotableObservationAtPhaseLevel(major, profile);
    PhaseData phaseData =
        new PhaseData(
            phaseId,
            processTime,
            peakMemory,
            recordsProcessed,
            -1L,
            TimeUnit.MILLISECONDS.toNanos(runTime),
            totalMemory,
            totalBufferForIncomingMemory);
    phaseData.setPhaseObservationTypeList(notableObservationAtPhaseLevelList);
    phaseDataList.add(phaseData);
    phaseDataList.get(phaseDataList.size() - 1).setOperatorDataList(operatorData);
  }

  Comparator<UserBitShared.MinorFragmentProfile> minorIdComparator =
      new Comparator<UserBitShared.MinorFragmentProfile>() {
        @Override
        public int compare(
            final UserBitShared.MinorFragmentProfile o1,
            final UserBitShared.MinorFragmentProfile o2) {
          return Long.compare(o1.getMinorFragmentId(), o2.getMinorFragmentId());
        }
      };

  Comparator<UserBitShared.OperatorProfile> operatorIdComparator =
      new Comparator<UserBitShared.OperatorProfile>() {
        @Override
        public int compare(
            final UserBitShared.OperatorProfile o1, final UserBitShared.OperatorProfile o2) {
          return Long.compare(o1.getOperatorId(), o2.getOperatorId());
        }
      };

  /** This Method will return executor level ProcessingTime, Records processed and ThreadCount */
  private Map<Integer, Set<HostProcessingRate>> getMajorHostProcRateSetMap(
      int majorId,
      Map<
              ImmutablePair<Integer, Integer>,
              List<ImmutablePair<UserBitShared.OperatorProfile, Integer>>>
          opmap,
      Table<Integer, Integer, String> majorMinorHostTable) {
    UserBitShared.MajorFragmentProfile majorFragmentProfile = profile.getFragmentProfile(majorId);

    List<UserBitShared.MinorFragmentProfile> minorFragmentProfileList =
        new ArrayList<>(majorFragmentProfile.getMinorFragmentProfileList());

    Collections.sort(minorFragmentProfileList, minorIdComparator);
    for (UserBitShared.MinorFragmentProfile minorProfile :
        majorFragmentProfile.getMinorFragmentProfileList()) {
      majorMinorHostTable.put(
          majorId, minorProfile.getMinorFragmentId(), minorProfile.getEndpoint().getAddress());
      List<UserBitShared.OperatorProfile> ops =
          new ArrayList<>(minorProfile.getOperatorProfileList());
      Collections.sort(ops, operatorIdComparator);
      for (UserBitShared.OperatorProfile operatorProfile : ops) {
        final ImmutablePair<Integer, Integer> ip =
            new ImmutablePair<>(majorId, operatorProfile.getOperatorId());
        if (!opmap.containsKey(ip)) {
          final List<ImmutablePair<UserBitShared.OperatorProfile, Integer>> l = new ArrayList<>();
          opmap.put(ip, l);
        }
        opmap.get(ip).add(new ImmutablePair<>(operatorProfile, minorProfile.getMinorFragmentId()));
      }
    }

    final List<ImmutablePair<Integer, Integer>> keys = new ArrayList<>(opmap.keySet());
    Collections.sort(keys);
    Map<Integer, Set<HostProcessingRate>> majorHostProcRateSetMap = new HashMap<>();
    for (final ImmutablePair<Integer, Integer> ip : keys) {
      Set<HostProcessingRate> hostProcessingRateSet =
          computeRecordProcRateAtPhaseOperatorHostLevel(
              majorId, opmap.get(ip), majorMinorHostTable);
      Set<HostProcessingRate> phaseLevelSet = new HashSet<>();
      if (majorHostProcRateSetMap.containsKey(majorId)) {
        phaseLevelSet = majorHostProcRateSetMap.get(majorId);
      }
      phaseLevelSet.addAll(hostProcessingRateSet);
      majorHostProcRateSetMap.put(majorId, phaseLevelSet);
    }
    return majorHostProcRateSetMap;
  }

  /** This Method will build the OperatorDataList */
  static String getDatasetNameForOperator(
      String operatorId, UserBitShared.CoreOperatorType operatorType) {

    String datasetName = null;
    switch (operatorType) {
      case AVRO_SUB_SCAN:
      case DIRECT_SUB_SCAN:
      case HBASE_SUB_SCAN:
      case HIVE_SUB_SCAN:
      case INFO_SCHEMA_SUB_SCAN:
      case JSON_SUB_SCAN:
      case MOCK_SUB_SCAN:
      case PARQUET_ROW_GROUP_SCAN:
      case SYSTEM_TABLE_SCAN:
      case TEXT_SUB_SCAN:
      case ELASTICSEARCH_AGGREGATOR_SUB_SCAN:
      case ELASTICSEARCH_SUB_SCAN:
      case MONGO_SUB_SCAN:
      case EXCEL_SUB_SCAN:
      case ARROW_SUB_SCAN:
      case JDBC_SUB_SCAN:
      case FLIGHT_SUB_SCAN:
      case ICEBERG_SUB_SCAN:
      case DELTALAKE_SUB_SCAN:
      case DIR_LISTING_SUB_SCAN:
      case TABLE_FUNCTION:
        if (operatorToTable.containsKey(operatorId)) {
          datasetName = operatorToTable.get(operatorId);
        }
        break;
      default:
        break;
    }
    return datasetName;
  }

  static ArrayList<String> getReflectionNameForOperator(String datasetName) {

    ArrayList<String> reflectionPath =
        new ArrayList<String>(Arrays.asList(datasetName.split("[.]")));
    String reflectionID =
        reflectionPath.size() > 1 ? reflectionPath.get(1).replaceAll("\"", "") : null;
    String reflectionName = null, dataset = null;
    if (reflectionDatasetMap.containsKey(reflectionID)) {

      reflectionName = reflectionDatasetMap.get(reflectionID).get(0);
      dataset = reflectionDatasetMap.get(reflectionID).get(1);
    }
    return new ArrayList<>(Arrays.asList(reflectionName, dataset));
  }

  private OperatorData operatorDataList(
      String phaseId,
      String operatorId,
      int operatorType,
      BaseMetrics baseMetrics,
      Map<String, Object> stringObjectMap) {
    String mapIndex = phaseId + "_" + operatorId;
    GraphNodeDetails graphNodeDetails = (GraphNodeDetails) stringObjectMap.get(mapIndex);
    String key = phaseId + "-" + operatorId;
    String datasetName =
        getDatasetNameForOperator(key, UserBitShared.CoreOperatorType.valueOf(operatorType));
    String reflectionName = null;
    if (datasetName != null && datasetName.startsWith(REFLECTION_PREFIX)) {
      ArrayList<String> list = getReflectionNameForOperator(datasetName);
      reflectionName = list.get(0);
      datasetName = list.get(1);
    }

    OperatorData operatorData =
        new OperatorData(
            operatorId,
            getOperatorName("", operatorType),
            operatorType,
            baseMetrics,
            graphNodeDetails.getMergeNodeName(),
            graphNodeDetails.getSuccessorId(),
            datasetName,
            reflectionName);
    List<OperatorData.OperatorObservationType> notableObservationAtOperatorLevelList =
        ProfileObservationUtil.getNotableObservationsAtOperatorLevel(
            profile, Integer.parseInt(phaseId), Integer.parseInt(operatorId));
    operatorData.setOperatorObservationTypeList(notableObservationAtOperatorLevelList);

    return operatorData;
  }

  /** This Method is Used to get the Actual Operator Name */
  public static String getOperatorName(String nodeName, int operatorType) {
    String OperatorName = nodeName;
    if (!(operatorType == -1)) {
      OperatorName = String.valueOf(UserBitShared.CoreOperatorType.forNumber(operatorType));
    }
    return OperatorName;
  }

  /** This Method will build PhaseData if the query was planned but not executed. */
  private void buildPhaseDataOnlyForGraph() {
    List<PhaseNode> phaseNodeList = new ArrayList<>();
    buildPhaseNodeList(
        phaseNodeList); // This method will build PhaseNodeList which will be used to calculate
    // PhaseData.
    addPhaseDataForOnlyPlannedQuery(phaseNodeList);
  }

  /**
   * This Method will only use when Query was planned and not executed. (regression scenario) This
   * method will build the Data when Graph data is available and Fragment is Empty -rare scenario.
   */
  private void buildPhaseNodeList(List<PhaseNode> phaseNodeList) {
    graphObjectMap.forEach(
        (phaseNodeId, graphData) -> {
          String phaseId = phaseNodeId.substring(0, phaseNodeId.lastIndexOf("_"));
          String nodeId = phaseNodeId.substring(phaseNodeId.lastIndexOf("_") + 1);
          GraphNodeDetails graphNodeDetails = (GraphNodeDetails) graphObjectMap.get(phaseNodeId);
          phaseNodeList.add(
              new PhaseNode(
                  nodeId,
                  phaseId,
                  graphNodeDetails.getNodeName(),
                  -1,
                  new BaseMetrics(),
                  graphNodeDetails.getMergeNodeName(),
                  graphNodeDetails.getSuccessorId()));
        });
  }

  /**
   * This Method will build PhaseData to only show middle panel/GraphData for query which was
   * planned but not executed.
   */
  private void addPhaseDataForOnlyPlannedQuery(List<PhaseNode> phaseNodeList) {
    long default_Long = -1L;
    phaseNodeList.stream()
        .collect(Collectors.groupingBy(phaseId -> phaseId.getPhaseId(), Collectors.toList()))
        .forEach(
            (phaseId, nodeDetails) -> {
              List<OperatorData> operatorDataList = new ArrayList<>();
              nodeDetails.stream()
                  .forEach(
                      nodeData -> {
                        operatorDataList.add(
                            new OperatorData(
                                nodeData.getNodeId(),
                                nodeData.getOperatorName(),
                                nodeData.getOperatorType(),
                                nodeData.getBaseMetrics(),
                                nodeData.getMergeNodeName(),
                                nodeData.getSuccessorId(),
                                null,
                                null));
                      });
              phaseDataList.add(
                  new PhaseData(
                      phaseId,
                      default_Long,
                      default_Long,
                      default_Long,
                      default_Long,
                      default_Long,
                      default_Long,
                      default_Long));
              phaseDataList.get(phaseDataList.size() - 1).setOperatorDataList(operatorDataList);
            });
  }
}
