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
import static com.dremio.dac.util.JobsConstant.DOT_BACKSLASH;
import static com.dremio.dac.util.JobsConstant.EXTERNAL_QUERY;
import static com.dremio.dac.util.JobsConstant.OTHERS;
import static com.dremio.dac.util.JobsConstant.PDS;
import static com.dremio.dac.util.JobsConstant.QUOTES;
import static com.dremio.dac.util.JobsConstant.REFLECTION;
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

import com.dremio.dac.api.CatalogEntity;
import com.dremio.dac.api.CatalogItem;
import com.dremio.dac.service.catalog.CatalogServiceHelper;
import com.dremio.dac.service.reflection.ReflectionServiceHelper;
import com.dremio.dac.util.JobUtil;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.proto.model.attempts.Attempts.RequestType;
import com.dremio.service.accelerator.proto.AccelerationDetails;
import com.dremio.service.accelerator.proto.ReflectionRelationship;
import com.dremio.service.accelerator.proto.SubstitutionState;
import com.dremio.service.job.JobDetails;
import com.dremio.service.job.JobSummary;
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
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.ParentDataset;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.reflection.proto.ReflectionGoal;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.protobuf.Descriptors.FieldDescriptor;

/**
 * Builds response of Job Details page
 */
public class JobInfoDetailsUI {
  private String id;
  private QueryType queryType;
  private String queryUser;
  private String queryText;
  private String wlmQueue;
  private Long startTime;
  private Long endTime;
  private long waitInClient;
  private boolean isAccelerated;
  private String input;
  private String output;
  private Long duration;
  private List<DurationDetails> durationDetails;
  private int nrReflectionsConsidered;
  private int nrReflectionsMatched;
  private int nrReflectionsUsed;
  private List<Reflection> reflectionsMatched = new ArrayList<>();
  private List<Reflection> reflectionsUsed = new ArrayList<>();
  private List<DataSet> queriedDatasets = new ArrayList<>();
  private List<ScannedDataset> scannedDatasets = new ArrayList<>();
  private List<DatasetGraph> datasetGraph = new ArrayList<>();
  private String jobStatus;
  private boolean isStarFlakeAccelerated;
  private List<Reflection> remainingReflections = new ArrayList<>();
  private List<DataSet> algebraicReflectionsDataset = new ArrayList<>();
  private boolean spilled = false;
  private Map<FieldDescriptor, Object> spilledJobDetails;
  private List<AttemptDetailsUI> attemptDetails;
  private String attemptsSummary;
  private List<ReflectionGoal> queryReflectionGoals = new ArrayList<>();
  private String description;
  private RequestType requestType;
  private Map<String, String> exceptionsMap = new HashMap<>();
  private boolean isAccessException = Boolean.FALSE;
  private JobFailureInfo failureInfo;
  private JobCancellationInfo cancellationInfo;

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
    @JsonProperty("input") String input,
    @JsonProperty("output") String output,
    @JsonProperty("duration") Long duration,
    @JsonProperty("durationDetails") List<DurationDetails> durationDetails,
    @JsonProperty("nrReflectionsConsidered") int nrReflectionsConsidered,
    @JsonProperty("nrReflectionsMatched") int nrReflectionsMatched,
    @JsonProperty("nrReflectionsUsed") int nrReflectionsUsed,
    @JsonProperty("reflectionsMatched") List<Reflection> reflectionsMatched,
    @JsonProperty("reflectionsUsed") List<Reflection> reflectionsUsed,
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
    @JsonProperty("cancellationInfo") JobCancellationInfo cancellationInfo) {
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
    this.input = input;
    this.output = output;
    this.duration = duration;
    this.durationDetails = durationDetails;
    this.nrReflectionsConsidered = nrReflectionsConsidered;
    this.nrReflectionsMatched = nrReflectionsMatched;
    this.nrReflectionsUsed = nrReflectionsUsed;
    this.reflectionsMatched = reflectionsMatched;
    this.reflectionsUsed = reflectionsUsed;
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
  }

  public JobInfoDetailsUI of(JobDetails jobDetails, UserBitShared.QueryProfile profile, JobSummary summary, CatalogServiceHelper catalogServiceHelper, ReflectionServiceHelper reflectionServiceHelper, NamespaceService namespaceService, int detailLevel, int attemptIndex) throws NamespaceException {
    JobProtobuf.JobAttempt jobAttempt = jobDetails.getAttemptsList().get(attemptIndex);
    //JobProtobuf.JobInfo jobInfo = jobAttempt.getInfo();
    final List<JobAttempt> attempts = jobDetails.getAttemptsList().stream().map(JobsProtoUtil::toStuff).collect(Collectors.toList());
    final JobInfo jobInfo = jobDetails.getAttempts(attemptIndex).getInfo();
    final JobId jobId = new JobId(jobDetails.getJobId().getId());

    id = jobId.getId();
    jobStatus = jobDetails.getAttempts(attemptIndex).getState().toString();
    queryType = jobInfo.getQueryType();
    queryUser = jobInfo.getUser();
    queryText = jobInfo.getSql();
    wlmQueue = jobInfo.getResourceSchedulingInfo().getQueueName();
    startTime = summary != null ? summary.getStartTime() : jobDetails.getAttempts(0).getInfo().getStartTime();
    endTime = summary != null ? summary.getEndTime() : jobDetails.getAttempts(attemptIndex).getInfo().getFinishTime();
    waitInClient = jobAttempt.getDetails().getWaitInClient();
    this.input = JobUtil.getConvertedBytes(jobAttempt.getStats().getInputBytes()) + " / " +
      jobAttempt.getStats().getInputRecords() + " Records";
    this.output = JobUtil.getConvertedBytes(jobAttempt.getStats().getOutputBytes()) + " / " +
      jobAttempt.getStats().getOutputRecords() + " Records";
    duration = JobUtil.getTotalDuration(jobDetails, attemptIndex);
    durationDetails = JobUtil.buildDurationDetails(jobAttempt.getStateListList());
    queriedDatasets = JobUtil.buildQueriedDatasets(jobAttempt.getInfo());
    final AccelerationDetails accelerationDetails = deserialize(jobAttempt.getAccelerationDetails());
    isStarFlakeAccelerated = this.isAccelerated && JobUtil.isSnowflakeAccelerated(accelerationDetails);
    attemptDetails = AttemptsUIHelper.fromAttempts(jobId, attempts);
    attemptsSummary = AttemptsUIHelper.constructSummary(attempts);
    requestType = jobInfo.getRequestType();
    description = jobInfo.getDescription();

    if (detailLevel == 1) {
      if (profile != null) {
        fetchReflectionsMatchedOrUsed(profile.getAccelerationProfile().getLayoutProfilesList(), reflectionServiceHelper, catalogServiceHelper, accelerationDetails);
        scannedDatasets = buildScannedDatasets(jobAttempt.getDetails().getTableDatasetProfilesList(), catalogServiceHelper, reflectionServiceHelper, profile);
        if (queryType != QueryType.UI_EXPORT && queryType != QueryType.UNKNOWN) {
          if (!isAccessException) {
            datasetGraph = buildDataSetGraph(jobInfo, catalogServiceHelper, reflectionServiceHelper, namespaceService, profile);
            segregateExpansionAlgebraicReflections(reflectionsUsed, reflectionsMatched, profile, reflectionServiceHelper);
            algebraicReflectionsDataset = buildAlgebraicReflections(catalogServiceHelper, reflectionServiceHelper, remainingReflections);
          }
          if (isAccessException) {
            datasetGraph.clear();
            datasetGraph.add(new DatasetGraph().setDescription("Something went wrong while accessing one or more components of Dataset Graph. Please check your privileges."));
            algebraicReflectionsDataset.clear();
          }
        }
      }
    }
    nrReflectionsConsidered = (profile != null && profile.getAccelerationProfile() != null) ? profile.getAccelerationProfile().getLayoutProfilesCount() : 0;
    nrReflectionsMatched = reflectionsMatched.size();
    nrReflectionsUsed = reflectionsUsed.size();
    isAccelerated = summary != null ? summary.getAccelerated() : (reflectionsUsed.size() > 0 ? Boolean.TRUE : Boolean.FALSE);
    spilledJobDetails = jobInfo.getSpillJobDetails().getAllFields();
    spilled = summary != null ? summary.getSpilled() : (spilledJobDetails.isEmpty() ? Boolean.FALSE : Boolean.TRUE);
    final JobAttempt lastJobAttempt = Util.last(attempts);
    failureInfo = JobUtil.toJobFailureInfo(lastJobAttempt.getInfo().getFailureInfo(), lastJobAttempt.getInfo().getDetailedFailureInfo());
    cancellationInfo = JobUtil.toJobCancellationInfo(lastJobAttempt.getState(), lastJobAttempt.getInfo().getCancellationInfo());
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
      input,
      output,
      duration,
      durationDetails,
      nrReflectionsConsidered,
      nrReflectionsMatched,
      nrReflectionsUsed,
      reflectionsMatched,
      reflectionsUsed,
      queriedDatasets,
      scannedDatasets,
      datasetGraph,
      spilled,
      spilledJobDetails,
      algebraicReflectionsDataset,
      isStarFlakeAccelerated,
      attemptDetails,
      attemptsSummary,
      description,
      requestType,
      exceptionsMap,
      failureInfo,
      cancellationInfo
    );
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

  public List<DataSet> getQueriedDatasets() {
    return queriedDatasets;
  }

  public String getId() {
    return id;
  }

  public String getInput() {
    return input;
  }

  public String getOutput() {
    return output;
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

  private List<ScannedDataset> buildScannedDatasets(List<JobProtobuf.TableDatasetProfile> tableDatasetProfiles, CatalogServiceHelper catalogServiceHelper, ReflectionServiceHelper reflectionServiceHelper, UserBitShared.QueryProfile profile) {
    List<ScannedDataset> scannedDatasetList = new ArrayList<>();
    tableDatasetProfiles.stream().forEach(
      dataset -> {
        ScannedDataset scannedDataset = new ScannedDataset();
        String datasetName = "";
        String scanDescription = "";
        List<String> pathList = new ArrayList<>();
        List<String> paths = getDatasetPath(dataset);
        paths.forEach(s -> pathList.add(s.replaceAll(QUOTES, "")));
        if (pathList.size() > 0) {
          boolean isReflection = pathList.get(0).equals(ACCELERATOR);
          if (isReflection) {
            final String reflectionId = pathList.get(1);
            try {
              Optional<ReflectionGoal> reflectionGoalOptional = reflectionServiceHelper.getReflectionById(pathList.get(1));
              if (reflectionGoalOptional.isPresent()) {
                ReflectionGoal reflectionGoal = reflectionGoalOptional.get();
                String datasetId = reflectionGoal.getDatasetId();
                DatasetConfig datasetConfig = catalogServiceHelper.getDatasetById(datasetId).get();
                datasetName = JobUtil.extractDatasetConfigName(datasetConfig).replaceAll(QUOTES, "");
                datasetName = reflectionGoal.getName() + " (" + datasetName + ")";
              } else {
                datasetName = extractDatasetName(profile, pathList, reflectionId);
              }
            } catch (AccessControlException ace) {
              isAccessException = Boolean.TRUE;
              datasetName = extractDatasetName(profile, pathList, reflectionId);
              scanDescription = "You do not have sufficient privileges to view Reflection with id (" + reflectionId + ") or its Dataset";
              exceptionsMap.put("ScannedDataset", ace.getMessage());
            } catch (Exception ex) {
              datasetName = extractDatasetName(profile, pathList, reflectionId);
              scanDescription = "Something went wrong while accessing Reflection with id (" + reflectionId + ") or its Dataset.";
              exceptionsMap.put("ScannedDataset", ex.getMessage());
            }
          } else {
            datasetName = pathList.get(pathList.size() - 1);
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

  private String extractDatasetName(UserBitShared.QueryProfile profile, List<String> pathList, String reflectionId) {
    String datasetName;
    String reflectionName = profile.getAccelerationProfile().getLayoutProfilesList().stream().filter(layoutProfile -> layoutProfile.getLayoutId().equals(pathList.get(1))).findFirst().get().getName();
    datasetName = reflectionName + " (" + reflectionId + ")";
    return datasetName;
  }

  private List<String> getDatasetPath(JobProtobuf.TableDatasetProfile dataset) {
    int datasetPathSize = dataset.getDatasetProfile().getDatasetPathsList().get(0).getDatasetPathList().size();
    return Arrays.asList(dataset.getDatasetProfile().getDatasetPathsList().get(0).getDatasetPathList().get(datasetPathSize - 1).split(DOT_BACKSLASH));
  }

  private void segregateExpansionAlgebraicReflections(List<Reflection> reflectionsUsed, List<Reflection> reflectionsMatched, UserBitShared.QueryProfile profile, ReflectionServiceHelper reflectionServiceHelper) {
    reflectionsUsed.stream().filter(s -> s.getReflectionMatchingType() == null).forEach(s -> s.setReflectionMatchingType(ReflectionMatchingType.ALGEBRAIC));
    reflectionsMatched.stream().filter(s -> s.getReflectionMatchingType() == null).forEach(s -> s.setReflectionMatchingType(ReflectionMatchingType.ALGEBRAIC));
    remainingReflections.addAll(reflectionsUsed.stream().filter(s -> s.getReflectionMatchingType() == ReflectionMatchingType.ALGEBRAIC).collect(Collectors.toList()));
    remainingReflections.addAll(reflectionsMatched.stream().filter(s -> s.getReflectionMatchingType() == ReflectionMatchingType.ALGEBRAIC).collect(Collectors.toList()));
  }

  private List<DatasetGraph> buildDataSetGraph(JobProtobuf.JobInfo jobInfo, CatalogServiceHelper catalogServiceHelper, ReflectionServiceHelper reflectionServiceHelper, NamespaceService namespaceService, UserBitShared.QueryProfile profile) throws NamespaceException {
    List<JobProtobuf.ParentDatasetInfo> parents = jobInfo.getParentsList();
    String sqlQuery = jobInfo.getSql();
    try {
      for (JobProtobuf.ParentDatasetInfo parent : parents) {
        List<String> pathList = new ArrayList<>();
        pathList.addAll(parent.getDatasetPathList());
        queryReflectionGoals = getQueryReflectionGoals(reflectionServiceHelper, profile);
        buildTree(catalogServiceHelper, reflectionServiceHelper, namespaceService, profile, sqlQuery, pathList, queryReflectionGoals, datasetGraph, null);
      }
    } catch (Exception ex) {
      isAccessException = Boolean.TRUE;
      exceptionsMap.put("DatasetGraph", ex.getMessage());
    }
    return datasetGraph;
  }

  private void buildTree(CatalogServiceHelper catalogServiceHelper, ReflectionServiceHelper reflectionServiceHelper, NamespaceService namespaceService, UserBitShared.QueryProfile profile, String sqlQuery, List<String> pathList, List<ReflectionGoal> queryReflectionGoals, List<DatasetGraph> dGraph, String dbId) throws Exception {
    try {
      Optional<CatalogEntity> entity = catalogServiceHelper.getCatalogEntityByPath(pathList, new ArrayList<>(), new ArrayList<>());
      String datasetId = entity.get().getId();
      List<CatalogItem> parentsList;
      if (catalogServiceHelper.getDatasetById(datasetId).isPresent()) {
        DatasetConfig dataset = catalogServiceHelper.getDatasetById(datasetId).get();
        parentsList = getParentsForDataset(dataset, namespaceService);
        dGraph.add(addNextDatasetGraph(dataset, parentsList, reflectionServiceHelper, queryReflectionGoals, profile));
        if (parentsList.size() > 0) {
          buildDatasetGraphTree(reflectionServiceHelper, catalogServiceHelper, namespaceService, parentsList, dGraph, queryReflectionGoals, profile, sqlQuery);
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

  private List<ReflectionGoal> getQueryReflectionGoals(ReflectionServiceHelper reflectionServiceHelper, UserBitShared.QueryProfile profile) {
    Iterable<ReflectionGoal> reflectionGoals = reflectionServiceHelper.getAllReflections();
    List<UserBitShared.LayoutMaterializedViewProfile> layoutProfilesList = profile.getAccelerationProfile().getLayoutProfilesList();
    List<ReflectionGoal> filteredReflectionGoals = new ArrayList<>();
    reflectionGoals.forEach(
      reflectionGoal -> {
        layoutProfilesList.stream().forEach(
          layoutProfile -> {
            if (layoutProfile.getLayoutId().equals(reflectionGoal.getId().getId())) {
              filteredReflectionGoals.add(reflectionGoal);
            }
          }
        );
      }
    );
    return filteredReflectionGoals;
  }

  private List<DatasetGraph> buildDatasetGraphTree(ReflectionServiceHelper reflectionServiceHelper, CatalogServiceHelper catalogServiceHelper, NamespaceService namespaceService, List<CatalogItem> parentsList, List<DatasetGraph> graph, List<ReflectionGoal> queryReflectionGoals, UserBitShared.QueryProfile profile, String sqlQuery) throws Exception {
    for (CatalogItem parent : parentsList) {
      List pathList = parent.getPath();
      try {
        buildTree(catalogServiceHelper, reflectionServiceHelper, namespaceService, profile, sqlQuery, pathList, queryReflectionGoals, graph, parent.getId());
      } catch (Exception e) {
        throw e;
      }
    }
    return graph;
  }

  private DatasetGraph addNextDatasetGraph(DatasetConfig dataset, List<CatalogItem> parents, ReflectionServiceHelper reflectionServiceHelper, List<ReflectionGoal> queryReflectionGoals, UserBitShared.QueryProfile profile) {
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
    sampleDataset = addReflections(reflectionServiceHelper, sampleDataset, queryReflectionGoals);
    datasetGraph.setDataSet(sampleDataset);
    java.util.Optional<UserBitShared.DatasetProfile> datasetProfileOptional = profile.getDatasetProfileList().stream().filter(datasetProfile ->
      datasetProfile.getDatasetPath().equals(String.join(".", dataset.getFullPathList()))).findFirst();
    if (datasetProfileOptional.isPresent()) {
      datasetGraph.setSql(datasetProfileOptional.get().getSql());
    }
    return datasetGraph;
  }

  private DataSet addReflections(ReflectionServiceHelper reflectionServiceHelper, DataSet dataSet, List<ReflectionGoal> queryReflectionGoals) {
    List<Reflection> reflectionList = new ArrayList<>();
    queryReflectionGoals.stream().forEach(
      goal -> {
        if (dataSet.getDatasetID().equals(goal.getDatasetId())) {
          Reflection reflection = buildReflections(reflectionServiceHelper, goal, ReflectionMatchingType.EXPANSION);
          if (reflectionsUsed.stream().anyMatch(s -> s.getReflectionID().equals(goal.getId().getId()))) {
            reflection.setIsUsed(Boolean.TRUE);
            reflectionsUsed.stream().filter(s -> s.getReflectionID().equals(goal.getId().getId())).findFirst().ifPresent(s -> s.setReflectionMatchingType(ReflectionMatchingType.EXPANSION));
          } else {
            reflection.setIsUsed(Boolean.FALSE);
            reflectionsMatched.stream().filter(s -> s.getReflectionID().equals(goal.getId().getId())).findFirst().ifPresent(s -> s.setReflectionMatchingType(ReflectionMatchingType.EXPANSION));
          }
          reflectionList.add(reflection);
        }
      }
    );
    dataSet.setReflectionsDefinedList(reflectionList);
    return dataSet;
  }

  private Reflection buildReflections(ReflectionServiceHelper reflectionServiceHelper, ReflectionGoal goal, ReflectionMatchingType reflectionMatchingType) {
    Reflection reflection = new Reflection();
    reflection.setReflectionID(goal.getId().getId());
    reflection.setReflectionName(goal.getName());
    reflection.setReflectionType(ReflectionType.valueOf(goal.getType().getNumber()));
    reflection.setReflectionCreated(goal.getCreatedAt().toString());
    reflection.setReflectionLastRefreshed(goal.getModifiedAt().toString());
    reflection.setReflectionStatus(reflectionServiceHelper.getStatusForReflection(goal.getId().getId()).getCombinedStatus().toString());
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

  private List<DataSet> buildAlgebraicReflections(CatalogServiceHelper catalogServiceHelper, ReflectionServiceHelper reflectionServiceHelper, List<Reflection> remainingReflections) {
    List<ReflectionGoal> algebraicReflectionGoals = new ArrayList<>();
    List<DataSet> algebraicDatasets = new ArrayList<>();
    queryReflectionGoals.stream().forEach(
      reflectionGoal -> {
        remainingReflections.stream().forEach(
          reflection -> {
            if (reflection.getReflectionID().equals(reflectionGoal.getId().getId())) {
              algebraicReflectionGoals.add(reflectionGoal);
            }
          }
        );
      }
    );
    try {
      algebraicDatasets = buildAlgebraicReflectionsDatasets(catalogServiceHelper, reflectionServiceHelper, algebraicReflectionGoals);
    } catch (AccessControlException ace) {
      isAccessException = Boolean.TRUE;
      exceptionsMap.put("AlgebraicReflections", ace.getMessage());
    } catch (Exception e) {
      isAccessException = Boolean.TRUE;
      exceptionsMap.put("AlgebraicReflections", e.getMessage());
    }
    return algebraicDatasets;
  }

  private List<DataSet> buildAlgebraicReflectionsDatasets(CatalogServiceHelper catalogServiceHelper, ReflectionServiceHelper reflectionServiceHelper, List<ReflectionGoal> algebraicReflectionGoals) {
    List<DataSet> datasetsList = new ArrayList<>();
    for (ReflectionGoal reflectionGoal : algebraicReflectionGoals) {
      List<Reflection> reflectionsList = new ArrayList<>();
      DatasetConfig datasetConfig = catalogServiceHelper.getDatasetById(reflectionGoal.getDatasetId()).get();
      DataSet tempDataset = new DataSet();
      tempDataset.setDatasetType(JobUtil.getDatasetType(String.valueOf(datasetConfig.getType())));
      tempDataset.setDatasetID(datasetConfig.getId().getId());
      tempDataset.setDatasetName(JobUtil.extractDatasetConfigName(datasetConfig));
      tempDataset.setDatasetPath(String.join(".", datasetConfig.getFullPathList()));
      Reflection reflection = buildReflections(reflectionServiceHelper, reflectionGoal, ReflectionMatchingType.ALGEBRAIC);
      if (reflectionsUsed.stream().anyMatch(s -> s.getReflectionID().equals(reflectionGoal.getId().getId()))) {
        reflection.setIsUsed(Boolean.TRUE);
      } else {
        reflection.setIsUsed(Boolean.FALSE);
      }
      reflectionsList.add(reflection);
      tempDataset.setReflectionsDefinedList(reflectionsList);
      datasetsList.add(tempDataset);
    }
    return datasetsList;
  }

  private void fetchReflectionsMatchedOrUsed(List<UserBitShared.LayoutMaterializedViewProfile> layoutProfilesList, ReflectionServiceHelper reflectionServiceHelper, CatalogServiceHelper catalogServiceHelper, AccelerationDetails accelerationDetails) {
    layoutProfilesList.stream().forEach(
      viewProfile -> {
        int refUsedTemp = viewProfile.getNumUsed();
        try {
          if (refUsedTemp > 0) {
            addReflectionDetails(reflectionServiceHelper, reflectionsUsed, viewProfile, Boolean.TRUE, catalogServiceHelper);
          }
          if (refUsedTemp <= 0) {
            addReflectionDetails(reflectionServiceHelper, reflectionsMatched, viewProfile, Boolean.FALSE, catalogServiceHelper);
          }
        } catch (AccessControlException ace) {
          isAccessException = Boolean.TRUE;
          extractReflectionFromReflectionRelationship(accelerationDetails, viewProfile);
          exceptionsMap.put("ReflectionsMatchedOrUsed", ace.getMessage());
        } catch (Exception ex) {
          extractReflectionFromReflectionRelationship(accelerationDetails, viewProfile);
          exceptionsMap.put("ReflectionsMatchedOrUsed", ex.getMessage());
        }
      }
    );
  }

  private void extractReflectionFromReflectionRelationship(AccelerationDetails accelerationDetails, UserBitShared.LayoutMaterializedViewProfile viewProfile) {
    accelerationDetails.getReflectionRelationshipsList().stream().forEach(reflectionRelationship -> {
      if (reflectionRelationship.getReflection().getId().getId().equals(viewProfile.getLayoutId())) {
        if (reflectionRelationship.getState() == SubstitutionState.CHOSEN) {
          addReflectionFromAccelerationDetails(reflectionRelationship, Boolean.TRUE, reflectionsUsed);
        } else {
          addReflectionFromAccelerationDetails(reflectionRelationship, Boolean.FALSE, reflectionsMatched);
        }
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
    reflection.setReflectionDatasetPath(String.join(".", reflectionRelationship.getDataset().getPathList()));
    reflection.setReflectionCreated(reflectionRelationship.getMaterialization().getRefreshChainStartTime().toString());
    reflectionList.add(reflection);
  }

  private void addReflectionDetails(ReflectionServiceHelper reflectionServiceHelper, List<Reflection> reflectionsMatched, UserBitShared.LayoutMaterializedViewProfile viewProfile, Boolean isUsed, CatalogServiceHelper catalogServiceHelper) {
    Reflection reflection = new Reflection();
    reflection.setReflectionID(viewProfile.getLayoutId());
    reflection.setReflectionName(Strings.isNullOrEmpty(viewProfile.getName()) ? viewProfile.getLayoutId() : viewProfile.getName());
    reflection.setReflectionType(ReflectionType.valueOf(viewProfile.getType().getNumber()));
    reflection.setIsStarFlake(viewProfile.getSnowflake());
    Optional<ReflectionGoal> reflectionGoal = reflectionServiceHelper.getReflectionById(viewProfile.getLayoutId());
    if (reflectionGoal.isPresent()) {
      reflection.setReflectionStatus(reflectionServiceHelper.getStatusForReflection(reflectionGoal.get().getId().getId()).getCombinedStatus().toString());
      reflection.setReflectionCreated(reflectionGoal.get().getCreatedAt().toString());
      reflection.setReflectionLastRefreshed(reflectionGoal.get().getModifiedAt().toString());
      reflection.setReflectionSizeRows(reflectionServiceHelper.getTotalSize(reflectionGoal.get().getId().getId()));
      reflection.setIsUsed(isUsed);
      DatasetConfig dataset = catalogServiceHelper.getDatasetById(reflectionGoal.get().getDatasetId()).get();
      reflection.setDatasetName(JobUtil.extractDatasetConfigName(dataset));
      reflection.setReflectionDatasetPath(String.join(".", dataset.getFullPathList()));
      reflectionsMatched.add(reflection);
    }
  }
}
