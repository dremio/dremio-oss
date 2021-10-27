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

import static com.dremio.dac.util.JobsConstant.ACCELERATOR;
import static com.dremio.dac.util.JobsConstant.AGGREGATION;
import static com.dremio.dac.util.JobsConstant.ALGEBRAIC_REFLECTIONS;
import static com.dremio.dac.util.JobsConstant.DATASETGRAPH;
import static com.dremio.dac.util.JobsConstant.DATASET_GRAPH_ERROR;
import static com.dremio.dac.util.JobsConstant.DOT_BACKSLASH;
import static com.dremio.dac.util.JobsConstant.EXTERNAL_QUERY;
import static com.dremio.dac.util.JobsConstant.OTHERS;
import static com.dremio.dac.util.JobsConstant.PDS;
import static com.dremio.dac.util.JobsConstant.QUOTES;
import static com.dremio.dac.util.JobsConstant.REFLECTION;
import static com.dremio.dac.util.JobsConstant.__ACCELERATOR;
import static com.dremio.service.accelerator.AccelerationDetailsUtils.deserialize;

import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.calcite.util.Util;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import com.dremio.dac.api.CatalogEntity;
import com.dremio.dac.api.CatalogItem;
import com.dremio.dac.obfuscate.ObfuscationUtils;
import com.dremio.dac.service.catalog.CatalogServiceHelper;
import com.dremio.dac.service.reflection.ReflectionServiceHelper;
import com.dremio.dac.util.JobUtil;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.service.accelerator.proto.AccelerationDetails;
import com.dremio.service.accelerator.proto.ReflectionRelationship;
import com.dremio.service.accelerator.proto.SubstitutionState;
import com.dremio.service.job.JobDetails;
import com.dremio.service.job.RequestType;
import com.dremio.service.job.proto.DataSet;
import com.dremio.service.job.proto.DatasetGraph;
import com.dremio.service.job.proto.DurationDetails;
import com.dremio.service.job.proto.JobAttempt;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobProtobuf;
import com.dremio.service.job.proto.JobProtobuf.JobInfo;
import com.dremio.service.job.proto.JobProtobuf.QueryType;
import com.dremio.service.job.proto.Reflection;
import com.dremio.service.job.proto.ReflectionMatchingType;
import com.dremio.service.job.proto.ReflectionType;
import com.dremio.service.job.proto.ScannedDataset;
import com.dremio.service.jobs.JobsProtoUtil;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetCommonProtobuf;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.ParentDataset;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.ProtocolStringList;

/**
 * Builds response of Job Details page
 */
public class JobInfoDetailsUI {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JobInfoDetailsUI.class);
  private String id;
  private QueryType queryType;
  private String queryUser;
  private String queryText;
  private String wlmQueue;
  private Long startTime;
  private Long endTime;
  private long waitInClient;
  private boolean isAccelerated;
  private Long inputBytes;
  private Long inputRecords;
  private Long outputBytes;
  private Long outputRecords;
  private Long duration;
  private List<DurationDetails> durationDetails;
  private int nrReflectionsConsidered;
  private int nrReflectionsMatched;
  private int nrReflectionsUsed;
  private List<Reflection> reflectionsMatched = new ArrayList<>();
  private List<Reflection> reflectionsUsed = new ArrayList<>();
  private List<Reflection> reflections = new ArrayList<>();
  private List<DataSet> queriedDatasets = new ArrayList<>();
  private List<ScannedDataset> scannedDatasets = new ArrayList<>();
  private List<DatasetGraph> datasetGraph = new ArrayList<>();
  private String jobStatus;
  private boolean isStarFlakeAccelerated;
  private Map<String, Reflection> remainingReflections = new HashMap<>();
  private List<DataSet> algebraicReflectionsDataset = new ArrayList<>();
  private boolean spilled = false;
  private Map<FieldDescriptor, Object> spilledJobDetails;
  private List<AttemptDetailsUI> attemptDetails;
  private String attemptsSummary;
  private String description;
  private RequestType requestType;
  private Map<String, String> exceptionsMap = new HashMap<>();
  private boolean isAccessException = Boolean.FALSE;
  private boolean isGraphException = Boolean.FALSE;
  private boolean isAlgebraicException = Boolean.FALSE;
  private JobFailureInfo failureInfo;
  private JobCancellationInfo cancellationInfo;
  private Map<String, Reflection> reflectionsMap = new HashMap<>();
  private Map<String, Reflection> reflectionsUsedMap = new HashMap<>();
  private Map<String, Reflection> reflectionsMatchedMap = new HashMap<>();
  private String datasetVersion;
  private boolean resultsAvailable;

  public JobInfoDetailsUI() {
  }

  @JsonCreator
  public JobInfoDetailsUI(
    @JsonProperty("id") String id,
    @JsonProperty("jobStatus") String jobStatus,
    @JsonProperty("queryType") QueryType queryType,
    @JsonProperty("queryUser") String queryUser,
    @JsonProperty("queryText") String queryText,
    @JsonProperty("wlmQueue") String wlmQueue,
    @JsonProperty("startTime") Long startTime,
    @JsonProperty("endTime") Long endTime,
    @JsonProperty("waitInClient") Long waitInClient,
    @JsonProperty("isAccelerated") boolean isAccelerated,
    @JsonProperty("inputBytes") Long inputBytes,
    @JsonProperty("inputRecords") Long inputRecords,
    @JsonProperty("outputBytes") Long outputBytes,
    @JsonProperty("outputRecords") Long outputRecords,
    @JsonProperty("duration") Long duration,
    @JsonProperty("durationDetails") List<DurationDetails> durationDetails,
    @JsonProperty("nrReflectionsConsidered") int nrReflectionsConsidered,
    @JsonProperty("nrReflectionsMatched") int nrReflectionsMatched,
    @JsonProperty("nrReflectionsUsed") int nrReflectionsUsed,
    @JsonProperty("reflectionsMatched") List<Reflection> reflectionsMatched,
    @JsonProperty("reflectionsUsed") List<Reflection> reflectionsUsed,
    @JsonProperty("reflections") List<Reflection> reflections,
    @JsonProperty("queriedDatasets") List<DataSet> queriedDatasets,
    @JsonProperty("scannedDataset") List<ScannedDataset> scannedDatasets,
    @JsonProperty("DatasetGraph") List<DatasetGraph> datasetGraph,
    @JsonProperty("spilled") boolean spilled,
    @JsonProperty("spillDetails") Map<FieldDescriptor, Object> spilledJobDetails,
    @JsonProperty("algebraicReflectionsDataset") List<DataSet> algebraicReflectionsDataset,
    @JsonProperty("isStarFlakeAccelerated") boolean isStarFlakeAccelerated,
    @JsonProperty("attemptDetails;") List<AttemptDetailsUI> attemptDetails,
    @JsonProperty("attemptsSummary;") String attemptsSummary,
    @JsonProperty("description") String description,
    @JsonProperty("requestType") RequestType requestType,
    @JsonProperty("exceptionsMap") Map<String, String> exceptionsMap,
    @JsonProperty("failureInfo") JobFailureInfo failureInfo,
    @JsonProperty("cancellationInfo") JobCancellationInfo cancellationInfo,
    @JsonProperty("datasetVersion") String datasetVersion,
    @JsonProperty("resultsAvailable") Boolean resultsAvailable) {
    this.id = id;
    this.jobStatus = jobStatus;
    this.queryType = queryType;
    this.queryUser = queryUser;
    this.queryText = queryText;
    this.wlmQueue = wlmQueue;
    this.startTime = startTime;
    this.endTime = endTime;
    this.waitInClient = waitInClient;
    this.isAccelerated = isAccelerated;
    this.inputBytes = inputBytes;
    this.inputRecords = inputRecords;
    this.outputBytes = outputBytes;
    this.outputRecords = outputRecords;
    this.duration = duration;
    this.durationDetails = durationDetails;
    this.nrReflectionsConsidered = nrReflectionsConsidered;
    this.nrReflectionsMatched = nrReflectionsMatched;
    this.nrReflectionsUsed = nrReflectionsUsed;
    this.reflectionsMatched = reflectionsMatched;
    this.reflectionsUsed = reflectionsUsed;
    this.reflections = reflections;
    this.queriedDatasets = queriedDatasets;
    this.scannedDatasets = scannedDatasets;
    this.datasetGraph = datasetGraph;
    this.spilled = spilled;
    this.spilledJobDetails = spilledJobDetails;
    this.isStarFlakeAccelerated = isStarFlakeAccelerated;
    this.algebraicReflectionsDataset = algebraicReflectionsDataset;
    this.attemptDetails = attemptDetails;
    this.attemptsSummary = attemptsSummary;
    this.description = description;
    this.requestType = requestType;
    this.exceptionsMap = exceptionsMap;
    this.failureInfo = failureInfo;
    this.cancellationInfo = cancellationInfo;
    this.datasetVersion = datasetVersion;
    this.resultsAvailable = resultsAvailable;
  }

  public JobInfoDetailsUI of(JobDetails jobDetails, UserBitShared.QueryProfile profile, CatalogServiceHelper catalogServiceHelper, ReflectionServiceHelper reflectionServiceHelper, NamespaceService namespaceService, int detailLevel, int attemptIndex) throws NamespaceException {
    JobProtobuf.JobAttempt jobAttempt = jobDetails.getAttemptsList().get(attemptIndex);
    final List<JobAttempt> attempts = jobDetails.getAttemptsList().stream().map(JobsProtoUtil::toStuff).collect(Collectors.toList());
    final JobInfo jobInfo = jobDetails.getAttempts(attemptIndex).getInfo();
    final JobId jobId = JobsProtoUtil.toStuff(jobDetails.getJobId());
    final JobAttempt lastJobAttempt = Util.last(attempts);
    AccelerationDetails accelerationDetails = null;
    try {
      accelerationDetails = deserialize(jobAttempt.getAccelerationDetails());
    } catch (Exception e) {
      accelerationDetails = new AccelerationDetails();
      logger.warn("Failed to deserialize acceleration details", e);
    }
    id = jobId.getId();
    jobStatus = jobDetails.getAttempts(attemptIndex).getState().toString();
    queryType = jobInfo.getQueryType();
    queryUser = jobInfo.getUser();
    queryText = jobInfo.getSql();
    wlmQueue = jobInfo.getResourceSchedulingInfo().getQueueName();
    startTime = jobDetails.getAttempts(0).getInfo().getStartTime();
    endTime = jobDetails.getAttempts(attemptIndex).getInfo().getFinishTime();
    waitInClient = jobAttempt.getDetails().getWaitInClient();
    inputBytes = jobAttempt.getStats().getInputBytes();
    inputRecords = jobAttempt.getStats().getInputRecords();
    outputBytes = jobAttempt.getStats().getOutputBytes();
    outputRecords = jobAttempt.getStats().getOutputRecords();
    duration = JobUtil.getTotalDuration(jobDetails, attemptIndex);
    durationDetails = JobUtil.buildDurationDetails(jobAttempt.getStateListList());
    requestType = RequestType.valueOf(jobInfo.getRequestType().toString());
    description = jobInfo.getDescription();
    attemptDetails = AttemptsUIHelper.fromAttempts(jobId, attempts);
    attemptsSummary = AttemptsUIHelper.constructSummary(attempts);
    if(queryType != QueryType.ACCELERATOR_DROP) {
      queriedDatasets = JobUtil.getQueriedDatasets(JobsProtoUtil.toStuff(jobAttempt.getInfo()), requestType);
      if (detailLevel == 1) {
        if (profile != null) {
          fetchReflectionsMatchedOrUsed(accelerationDetails, jobInfo);
          convertReflectionListToMap(reflectionsUsed, reflectionsMatched);
          scannedDatasets = buildScannedDatasets(jobInfo.getParentsList(), jobInfo.getGrandParentsList(), jobAttempt.getDetails().getTableDatasetProfilesList());
          if (queryType != QueryType.UI_EXPORT && queryType != QueryType.UNKNOWN) {
            if (!isAccessException) {
              datasetGraph = buildDataSetGraph(jobInfo, catalogServiceHelper, reflectionServiceHelper, namespaceService, profile);
              segregateExpansionAlgebraicReflections(reflectionsUsed, reflectionsMatched);
              algebraicReflectionsDataset = buildAlgebraicReflections(catalogServiceHelper, remainingReflections);
            }
            if(isGraphException || isAlgebraicException) {
              if (isGraphException) {
                datasetGraph.clear();
                datasetGraph.add(new DatasetGraph().setDescription(DATASET_GRAPH_ERROR));
              }
              algebraicReflectionsDataset.clear();
            }
          }
        }
      }
    }
    nrReflectionsConsidered = (profile != null && profile.getAccelerationProfile() != null) ? profile.getAccelerationProfile().getLayoutProfilesCount() : 0;
    nrReflectionsMatched = reflectionsMatched.size();
    nrReflectionsUsed = reflectionsUsed.size();
    isAccelerated = nrReflectionsUsed > 0;
    isStarFlakeAccelerated = this.isAccelerated && JobUtil.isSnowflakeAccelerated(accelerationDetails);
    spilledJobDetails = jobInfo.getSpillJobDetails().getAllFields();
    spilled = spilledJobDetails.size() > 0;
    failureInfo = JobUtil.toJobFailureInfo(lastJobAttempt.getInfo().getFailureInfo(), lastJobAttempt.getInfo().getDetailedFailureInfo());
    cancellationInfo = JobUtil.toJobCancellationInfo(lastJobAttempt.getState(), lastJobAttempt.getInfo().getCancellationInfo());
    datasetVersion = lastJobAttempt.getInfo().getDatasetVersion();
    final String currentUser = jobDetails.getAttempts(0).getInfo().getUser();
    resultsAvailable = jobDetails.getHasResults() && currentUser.equals(queryUser);
    return new JobInfoDetailsUI(
      id,
      jobStatus,
      queryType,
      queryUser,
      queryText,
      wlmQueue,
      startTime,
      endTime,
      waitInClient,
      isAccelerated,
      inputBytes,
      inputRecords,
      outputBytes,
      outputRecords,
      duration,
      durationDetails,
      nrReflectionsConsidered,
      nrReflectionsMatched,
      nrReflectionsUsed,
      ObfuscationUtils.obfuscate(reflectionsMatched, ObfuscationUtils::obfuscate),
      ObfuscationUtils.obfuscate(reflectionsUsed, ObfuscationUtils::obfuscate),
      reflections,
      ObfuscationUtils.obfuscate(queriedDatasets, ObfuscationUtils::obfuscate),
      scannedDatasets,
      datasetGraph,
      spilled,
      spilledJobDetails,
      ObfuscationUtils.obfuscate(algebraicReflectionsDataset, ObfuscationUtils::obfuscate),
      isStarFlakeAccelerated,
      attemptDetails,
      attemptsSummary,
      description,
      requestType,
      exceptionsMap,
      failureInfo,
      cancellationInfo,
      datasetVersion,
      resultsAvailable
    );
  }


  public String getDatasetVersion() {
    return datasetVersion;
  }

  public boolean isResultsAvailable() {
    return resultsAvailable;
  }

  public JobCancellationInfo getCancellationInfo() {
    return cancellationInfo;
  }

  public JobFailureInfo getFailureInfo() {
    return failureInfo;
  }

  public Map<String, String> getExceptionsMap() {
    return exceptionsMap;
  }

  public String getDescription() {
    return description;
  }

  public RequestType getRequestType() {
    return requestType;
  }

  public List<DataSet> getAlgebraicReflectionsDataset() {
    return algebraicReflectionsDataset;
  }

  public boolean isSpilled() {
    return spilled;
  }

  public Map<FieldDescriptor, Object> getSpilledJobDetails() {
    return spilledJobDetails;
  }

  public List<ScannedDataset> getScannedDatasets() {
    return scannedDatasets;
  }

  public List<DatasetGraph> getDatasetGraph() {
    return datasetGraph;
  }

  public QueryType getQueryType() {
    return queryType;
  }

  public String getQueryUser() {
    return queryUser;
  }

  public String getQueryText() {
    return queryText;
  }

  public String getWlmQueue() {
    return wlmQueue;
  }

  public Long getStartTime() {
    return startTime;
  }

  public Long getEndTime() {
    return endTime;
  }

  public long getWaitInClient() {
    return waitInClient;
  }

  public boolean isAccelerated() {
    return isAccelerated;
  }

  public Long getDuration() {
    return duration;
  }

  public List<DurationDetails> getDurationDetails() {
    return durationDetails;
  }

  public int getNrReflectionsConsidered() {
    return nrReflectionsConsidered;
  }

  public int getNrReflectionsMatched() {
    return nrReflectionsMatched;
  }

  public int getNrReflectionsUsed() {
    return nrReflectionsUsed;
  }

  public List<Reflection> getReflectionsMatched() {
    return reflectionsMatched;
  }

  public List<Reflection> getReflectionsUsed() {
    return reflectionsUsed;
  }

  public List<Reflection> getReflections() {
    return reflections;
  }

  public List<DataSet> getQueriedDatasets() {
    return queriedDatasets;
  }

  public String getId() {
    return id;
  }

  public Long getInputBytes() {
    return inputBytes;
  }

  public Long getInputRecords() {
    return inputRecords;
  }

  public Long getOutputBytes() {
    return outputBytes;
  }

  public Long getOutputRecords() {
    return outputRecords;
  }

  public boolean isStarFlakeAccelerated() {
    return isStarFlakeAccelerated;
  }

  public String getJobStatus() {
    return jobStatus;
  }

  public List<AttemptDetailsUI> getAttemptDetails() {
    return attemptDetails;
  }

  public String getAttemptsSummary() {
    return attemptsSummary;
  }


  private void convertReflectionListToMap(List<Reflection> reflectionsUsed, List<Reflection> reflectionsMatched) {
    reflectionsUsedMap = reflectionsUsed.stream().collect(Collectors.toMap(reflection -> reflection.getReflectionID(), reflection -> reflection));
    reflectionsMatchedMap = reflectionsMatched.stream().collect(Collectors.toMap(reflection -> reflection.getReflectionID(), reflection -> reflection));
    reflectionsMap.putAll(reflectionsUsedMap);
    reflectionsMap.putAll(reflectionsMatchedMap);
  }

  private List<ScannedDataset> buildScannedDatasets(List<JobProtobuf.ParentDatasetInfo> parentsList, List<DatasetCommonProtobuf.ParentDataset> grandParentsList, List<JobProtobuf.TableDatasetProfile> tableDatasetProfiles) {
    List<ScannedDataset> scannedDatasetList = new ArrayList<>();
    tableDatasetProfiles.stream().forEach(
      dataset -> {
        ScannedDataset scannedDataset = new ScannedDataset();
        String datasetName = "";
        String scanDescription = "";
        List<String> pathList = new ArrayList<>();
        List<String> paths = getDatasetPath(dataset);
        paths.forEach(s -> pathList.add(s.replaceAll(QUOTES, "")));
        if (CollectionUtils.isNotEmpty(pathList)) {
          boolean isReflection = pathList.get(0).equals(__ACCELERATOR);
          if (isReflection) {
            final String reflectionId = pathList.get(1);
            if (reflectionsMap.containsKey(reflectionId)) {
              Reflection reflection = reflectionsMap.get(reflectionId);
              datasetName = reflection.getReflectionName() + " (" + reflection.getDatasetName() + ")";
            }
          } else {
            datasetName = getScannedDatasetName(parentsList, grandParentsList, dataset);
          }
          String type = isReflection ? REFLECTION : PDS;
          scannedDataset.setDatasetType(type);
          scannedDataset.setNrScanThreads(dataset.getDatasetProfile().getParallelism());
          scannedDataset.setNrScannedRows(dataset.getDatasetProfile().getRecordsRead());
          scannedDataset.setIoWaitDurationMs(dataset.getDatasetProfile().getWaitOnSource());
          scannedDataset.setName(datasetName);
          scannedDataset.setDescription(scanDescription != null && !scanDescription.isEmpty() ? scanDescription : datasetName);
          scannedDatasetList.add(scannedDataset);
        }
      }
    );
    return scannedDatasetList;
  }

  // To get custom datasetname in case of query on non reflection datasets.
  private String getScannedDatasetName(List<JobProtobuf.ParentDatasetInfo> parentsList, List<DatasetCommonProtobuf.ParentDataset> grandParentsList, JobProtobuf.TableDatasetProfile dataset) {
    String datasetFullPath = StringUtils.join(dataset.getDatasetProfile().getDatasetPathsList().get(0).getDatasetPathList(),".").replaceAll(QUOTES,"");
    // Access grandparents list in case of query on VDS
    for (int grandparentIndex = 0; grandparentIndex < grandParentsList.size(); grandparentIndex++) {
      DatasetCommonProtobuf.ParentDataset grandParentDataset = grandParentsList.get(grandparentIndex);
      String grandParentsPath = StringUtils.join(grandParentsList.get(grandparentIndex).getDatasetPathList(), ".");
      if (datasetFullPath.equals(grandParentsPath)) {
        return grandParentDataset.getDatasetPathList().get(grandParentDataset.getDatasetPathList().size() - 1);
      }
    }
    // Access parents list in case of query on PDS
    for (int parentIndex = 0; parentIndex < parentsList.size(); parentIndex++) {
      JobProtobuf.ParentDatasetInfo parentDataset = parentsList.get(parentIndex);
      String parentsPath = StringUtils.join(parentsList.get(parentIndex).getDatasetPathList(), ".");
      if (datasetFullPath.equals(parentsPath)) {
        return parentDataset.getDatasetPathList().get(parentDataset.getDatasetPathList().size() - 1);
      }
    }
    return "";
  }

  private List<String> getDatasetPath(JobProtobuf.TableDatasetProfile dataset) {
    int datasetPathSize = dataset.getDatasetProfile().getDatasetPathsList().get(0).getDatasetPathList().size();
    return Arrays.asList(dataset.getDatasetProfile().getDatasetPathsList().get(0).getDatasetPathList().get(datasetPathSize - 1).split(DOT_BACKSLASH));
  }

  private void segregateExpansionAlgebraicReflections(List<Reflection> reflectionsUsed, List<Reflection> reflectionsMatched) {
    remainingReflections.putAll(reflectionsUsed.stream().filter(s -> s.getReflectionMatchingType() == null).map(s -> s.setReflectionMatchingType(ReflectionMatchingType.ALGEBRAIC)).collect(Collectors.toMap(reflection -> reflection.getReflectionID(), reflection -> reflection)));
    remainingReflections.putAll(reflectionsMatched.stream().filter(s -> s.getReflectionMatchingType() == null).map(s -> s.setReflectionMatchingType(ReflectionMatchingType.ALGEBRAIC)).collect(Collectors.toMap(reflection -> reflection.getReflectionID(), reflection -> reflection)));
  }

  private List<DatasetGraph> buildDataSetGraph(JobProtobuf.JobInfo jobInfo, CatalogServiceHelper catalogServiceHelper, ReflectionServiceHelper reflectionServiceHelper, NamespaceService namespaceService, UserBitShared.QueryProfile profile) throws NamespaceException {
    List<JobProtobuf.ParentDatasetInfo> parents = jobInfo.getParentsList();
    String sqlQuery = jobInfo.getSql();
    try {
      for (JobProtobuf.ParentDatasetInfo parent : parents) {
        List<String> pathList = new ArrayList<>();
        pathList.addAll(parent.getDatasetPathList());
        buildTree(catalogServiceHelper, reflectionServiceHelper, namespaceService, profile, sqlQuery, pathList, datasetGraph, null);
      }
    } catch (Exception ex) {
      isAccessException = Boolean.TRUE;
      isGraphException = Boolean.TRUE;
      exceptionsMap.put(DATASETGRAPH, ex.getMessage());
    }
    return datasetGraph;
  }

  private void buildTree(CatalogServiceHelper catalogServiceHelper, ReflectionServiceHelper reflectionServiceHelper, NamespaceService namespaceService, UserBitShared.QueryProfile profile, String sqlQuery, List<String> pathList, List<DatasetGraph> dGraph, String dbId) throws Exception {
    try {
      Optional<CatalogEntity> entity = catalogServiceHelper.getCatalogEntityByPath(pathList, new ArrayList<>(), new ArrayList<>());
      String datasetId = entity.get().getId();
      List<CatalogItem> parentsList;
      Optional<DatasetConfig> datasetConfig = catalogServiceHelper.getDatasetById(datasetId);
      if (datasetConfig.isPresent()) {
        DatasetConfig dataset = datasetConfig.get();
        parentsList = getParentsForDataset(dataset, namespaceService);
        dGraph.add(addNextDatasetGraph(dataset, parentsList, reflectionServiceHelper, profile));
        if (CollectionUtils.isNotEmpty(parentsList)) {
          buildDatasetGraphTree(reflectionServiceHelper, catalogServiceHelper, namespaceService, parentsList, dGraph, profile, sqlQuery);
        }
      }
    } catch (IllegalArgumentException ex) {
      handleCatalogEntityByPathException(dGraph, pathList, sqlQuery, dbId, null);
    } catch (Exception e) {
      throw e;
    }
  }

  private void handleCatalogEntityByPathException(List<DatasetGraph> dGraph, List<String> pathList, String sqlQuery, String uuid, String cItem) {
    AtomicBoolean isExternalQuery = getIsExternalQuery(pathList);
    String tempId = uuid != null ? uuid : cItem;
    if (isExternalQuery.get()) {
      dGraph.add(buildExternalQueryDataset(sqlQuery, pathList, OTHERS, EXTERNAL_QUERY, tempId));
    } else {
      dGraph.add(buildExternalQueryDataset(sqlQuery, pathList, OTHERS, OTHERS, tempId));
    }
  }

  private AtomicBoolean getIsExternalQuery(List<String> pathList) {
    AtomicBoolean isExternalQuery = new AtomicBoolean(false);
    pathList.stream().forEach((path) -> {
      if (path.contains(EXTERNAL_QUERY)) {
        isExternalQuery.set(true);
      }
    });
    return isExternalQuery;
  }

  private DatasetGraph buildExternalQueryDataset(String sqlQuery, List<String> pathList, String datasetType, String datasetName, String uuid) {
    String tempId = uuid != null ? uuid : String.valueOf(UUID.randomUUID());
    DatasetGraph datasetGraph = new DatasetGraph();
    DataSet dataSet = new DataSet();
    dataSet.setDatasetType(datasetType);
    dataSet.setDatasetID(tempId);
    dataSet.setDatasetName(datasetName);
    dataSet.setDatasetPath(String.join(".", pathList));
    datasetGraph.setDataSet(dataSet);
    datasetGraph.setSql(sqlQuery);
    datasetGraph.setId(tempId);
    datasetGraph.setParentNodeIdList(new ArrayList<>());
    return datasetGraph;
  }

  private List<DatasetGraph> buildDatasetGraphTree(ReflectionServiceHelper reflectionServiceHelper, CatalogServiceHelper catalogServiceHelper, NamespaceService namespaceService, List<CatalogItem> parentsList, List<DatasetGraph> graph, UserBitShared.QueryProfile profile, String sqlQuery) throws Exception {
    for (CatalogItem parent : parentsList) {
      List pathList = parent.getPath();
      try {
        buildTree(catalogServiceHelper, reflectionServiceHelper, namespaceService, profile, sqlQuery, pathList, graph, parent.getId());
      } catch (Exception e) {
        throw e;
      }
    }
    return graph;
  }

  private DatasetGraph addNextDatasetGraph(DatasetConfig dataset, List<CatalogItem> parents, ReflectionServiceHelper reflectionServiceHelper, UserBitShared.QueryProfile profile) {
    DataSet sampleDataset = new DataSet();
    DatasetGraph datasetGraph = new DatasetGraph();
    datasetGraph.setId(dataset.getId().getId());
    List<String> parentsId = new ArrayList<>();
    parents.forEach(p -> parentsId.add(p.getId()));
    datasetGraph.setParentNodeIdList(parentsId);
    sampleDataset.setDatasetType(JobUtil.getDatasetType(String.valueOf(dataset.getType())));
    sampleDataset.setDatasetID(dataset.getId().getId());
    sampleDataset.setDatasetName(JobUtil.extractDatasetConfigName(dataset));
    sampleDataset.setDatasetPath(String.join(".", dataset.getFullPathList()));
    sampleDataset = addReflections(reflectionServiceHelper, sampleDataset);
    datasetGraph.setDataSet(sampleDataset);
    java.util.Optional<UserBitShared.DatasetProfile> datasetProfileOptional = profile.getDatasetProfileList().stream().filter(datasetProfile ->
      datasetProfile.getDatasetPath().equals(String.join(".", dataset.getFullPathList()))).findFirst();
    if (datasetProfileOptional.isPresent()) {
      datasetGraph.setSql(datasetProfileOptional.get().getSql());
    }
    return datasetGraph;
  }

  private DataSet addReflections(ReflectionServiceHelper reflectionServiceHelper, DataSet dataSet) {
    List<Reflection> reflectionList = new ArrayList<>();
    reflectionsMap.entrySet().stream().forEach(goalMap -> {
      if (goalMap.getValue().getDatasetId().equals(dataSet.getDatasetID())) {
        Reflection goal = goalMap.getValue();
        Reflection reflection = buildReflections(goal, ReflectionMatchingType.EXPANSION);
        if (reflectionsUsedMap.containsKey(goal.getReflectionID())) {
          reflection.setIsUsed(Boolean.TRUE);
          reflectionsUsedMap.get(goal.getReflectionID()).setReflectionMatchingType(ReflectionMatchingType.EXPANSION);
        } else {
          reflection.setIsUsed(Boolean.FALSE);
          reflectionsMatchedMap.get(goal.getReflectionID()).setReflectionMatchingType(ReflectionMatchingType.EXPANSION);
        }
        reflectionList.add(reflection);
      }
    });
    dataSet.setReflectionsDefinedList(reflectionList);
    return dataSet;
  }

  private Reflection buildReflections(Reflection goal, ReflectionMatchingType reflectionMatchingType) {
    Reflection reflection = new Reflection();
    reflection.setReflectionID(goal.getReflectionID());
    reflection.setReflectionName(goal.getReflectionName());
    reflection.setReflectionType(goal.getReflectionType());
    reflection.setReflectionCreated(goal.getReflectionCreated());
    reflection.setReflectionLastRefreshed(goal.getReflectionLastRefreshed());
    reflection.setReflectionStatus(goal.getReflectionStatus());
    reflection.setReflectionMatchingType(reflectionMatchingType);
    return reflection;
  }

  private List<CatalogItem> getParentsForDataset(DatasetConfig datasetConfig, NamespaceService namespaceService) throws NamespaceException {
    // only virtual datasets have parents
    if (datasetConfig.getType() != DatasetType.VIRTUAL_DATASET) {
      return Collections.emptyList();
    }
    final List<CatalogItem> parents = new ArrayList<>();
    final List<ParentDataset> parentsList = datasetConfig.getVirtualDataset().getParentsList();
    // Parents may not exist.  For example "select 1".
    if (CollectionUtils.isNotEmpty(parentsList)) {
      parentsList.stream().forEach((parent) -> {
        try {
          DatasetConfig tempDatasetConfig = null;
          AtomicBoolean isExternalQuery = getIsExternalQuery(parent.getDatasetPathList());
          final NameSpaceContainer entity = namespaceService.getEntities(Collections.singletonList(new NamespaceKey(parent.getDatasetPathList()))).get(0);
          if (entity != null) {
            parents.add(CatalogItem.fromDatasetConfig(entity.getDataset(), null));
          } else {
            tempDatasetConfig = new DatasetConfig().setId(new EntityId(String.valueOf(UUID.randomUUID()))).setType(DatasetType.OTHERS).setFullPathList(parent.getDatasetPathList());
            if (isExternalQuery.get()) { // test condition to handle external query scenario.
              tempDatasetConfig.setName(EXTERNAL_QUERY);
            } else {  // test condition to handle all others scenario.
              tempDatasetConfig.setName(OTHERS);
            }
            parents.add(CatalogItem.fromDatasetConfig(tempDatasetConfig, null));
          }
        } catch (NamespaceException e) {
          e.printStackTrace();
        }
      });
    }
    return parents;
  }

  private List<DataSet> buildAlgebraicReflections(CatalogServiceHelper catalogServiceHelper, Map<String, Reflection> remainingReflections) {
    List<DataSet> algebraicDatasets = new ArrayList<>();
    try {
      algebraicDatasets = buildAlgebraicReflectionsDatasets(catalogServiceHelper, remainingReflections);
    } catch (AccessControlException ace) {
      isAccessException = Boolean.TRUE;
      isAlgebraicException = Boolean.TRUE;
      exceptionsMap.put(ALGEBRAIC_REFLECTIONS, ace.getMessage());
    } catch (Exception e) {
      isAccessException = Boolean.TRUE;
      isAlgebraicException = Boolean.TRUE;
      exceptionsMap.put(ALGEBRAIC_REFLECTIONS, e.getMessage());
    }
    return algebraicDatasets;
  }

  private List<DataSet> buildAlgebraicReflectionsDatasets(CatalogServiceHelper catalogServiceHelper, Map<String, Reflection> remainingReflections) {
    Map<String, DataSet> datasetsMap = new HashMap<>();
    for (Map.Entry<String, Reflection> refMap : remainingReflections.entrySet()) {
      Reflection reflectionGoal = refMap.getValue();
      List<Reflection> reflectionsList = new ArrayList<>();
      DataSet tempDataset = null;
      Reflection reflection = null;
      if(!datasetsMap.containsKey(reflectionGoal.getDatasetId())) {
        DatasetConfig datasetConfig = catalogServiceHelper.getDatasetById(reflectionGoal.getDatasetId()).get();
        tempDataset = new DataSet();
        tempDataset.setDatasetType(JobUtil.getDatasetType(String.valueOf(datasetConfig.getType())));
        tempDataset.setDatasetID(datasetConfig.getId().getId());
        tempDataset.setDatasetName(JobUtil.extractDatasetConfigName(datasetConfig));
        tempDataset.setDatasetPath(String.join(".", datasetConfig.getFullPathList()));
      } else {
        tempDataset = datasetsMap.get(reflectionGoal.getDatasetId());
        reflectionsList = tempDataset.getReflectionsDefinedList();
      }
      reflection = buildReflections(reflectionGoal, ReflectionMatchingType.ALGEBRAIC);
      if (reflectionsUsedMap.containsKey(reflectionGoal.getReflectionID())) {
        reflection.setIsUsed(Boolean.TRUE);
      } else {
        reflection.setIsUsed(Boolean.FALSE);
      }
      reflectionsList.add(reflection);
      tempDataset.setReflectionsDefinedList(reflectionsList);
      datasetsMap.put(reflectionGoal.getDatasetId(), tempDataset);
    }
    List<DataSet> datasetsList = new ArrayList<>(datasetsMap.values());
    return datasetsList;
  }

  private void fetchReflectionsMatchedOrUsed(AccelerationDetails accelerationDetails, JobInfo jobInfo) {
    if (accelerationDetails.getReflectionRelationshipsList() != null && accelerationDetails.getReflectionRelationshipsList().size() > 0) {
        extractReflectionFromReflectionRelationship(accelerationDetails);
    }
    if (jobInfo.getQueryType().toString().contains(ACCELERATOR) && !Strings.isNullOrEmpty(jobInfo.getMaterializationFor().getReflectionId())) {
      createReflectionDetails(jobInfo.getMaterializationFor(), jobInfo.getDatasetPathList(), reflections);
    }
  }

  private void createReflectionDetails(JobProtobuf.MaterializationSummary materializationFor, ProtocolStringList datasetPathList, List<Reflection> reflections) {
    Reflection reflection = new Reflection();
    reflection.setReflectionID(materializationFor.getReflectionId());
    reflection.setReflectionName(materializationFor.getReflectionName());
    ReflectionType reflectionType = (materializationFor.getReflectionType() != null && !materializationFor.getReflectionType().isEmpty()) ? (materializationFor.getReflectionType().equalsIgnoreCase(AGGREGATION) ? ReflectionType.AGGREGATE : ReflectionType.valueOf(materializationFor.getReflectionType())) : null;
    reflection.setReflectionType(reflectionType);
    reflection.setReflectionDatasetPath(String.join(".", datasetPathList));
    reflections.add(reflection);
  }

  private void extractReflectionFromReflectionRelationship(AccelerationDetails accelerationDetails) {
    accelerationDetails.getReflectionRelationshipsList().stream().forEach(reflectionRelationship -> {
      if (reflectionRelationship.getState() == SubstitutionState.CHOSEN) {
        addReflectionFromAccelerationDetails(reflectionRelationship, Boolean.TRUE, reflectionsUsed);
      } else {
        addReflectionFromAccelerationDetails(reflectionRelationship, Boolean.FALSE, reflectionsMatched);
      }
    });
  }

  private void addReflectionFromAccelerationDetails(ReflectionRelationship reflectionRelationship, Boolean isUsed, List<Reflection> reflectionList) {
    Reflection reflection = new Reflection();
    reflection.setReflectionID(reflectionRelationship.getReflection().getId().getId());
    reflection.setReflectionName(reflectionRelationship.getReflection().getName());
    reflection.setIsStarFlake(reflectionRelationship.getSnowflake());
    reflection.setIsUsed(isUsed);
    reflection.setReflectionType(ReflectionType.valueOf(reflectionRelationship.getReflectionType().getNumber()));
    reflection.setDatasetName(reflectionRelationship.getDataset().getPathList().get(reflectionRelationship.getDataset().getPathList().size() - 1));
    reflection.setDatasetId(reflectionRelationship.getDataset().getId());
    reflection.setReflectionDatasetPath(String.join(".", reflectionRelationship.getDataset().getPathList()));
    reflection.setReflectionCreated(reflectionRelationship.getMaterialization().getRefreshChainStartTime().toString());
    reflectionList.add(reflection);
  }
}
