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

import static com.dremio.service.jobs.JobsConstant.DATASET_GRAPH_ERROR;
import static com.dremio.service.jobs.JobsConstant.EXTERNAL_QUERY;
import static com.dremio.service.jobs.JobsConstant.OTHERS;
import static com.dremio.service.jobs.JobsConstant.QUOTES;

import com.dremio.dac.api.CatalogEntity;
import com.dremio.dac.api.CatalogItem;
import com.dremio.dac.service.catalog.CatalogServiceHelper;
import com.dremio.dac.util.JobUtil;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.service.accelerator.proto.AccelerationDetails;
import com.dremio.service.job.JobDetails;
import com.dremio.service.job.proto.DataSet;
import com.dremio.service.job.proto.DatasetGraph;
import com.dremio.service.job.proto.JobProtobuf;
import com.dremio.service.job.proto.Reflection;
import com.dremio.service.job.proto.ReflectionMatchingType;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.ParentDataset;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.reflection.ReflectionUtils;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.collections4.CollectionUtils;

public class JobDatasetGraphUI {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(JobDatasetGraphUI.class);
  private List<DatasetGraph> datasetGraph = new ArrayList<>();
  private final Map<String, Reflection> reflectionsMap = new HashMap<>();
  private boolean isGraphException = Boolean.FALSE;
  private String graphExceptionTable;

  public JobDatasetGraphUI() {}

  @JsonCreator
  public JobDatasetGraphUI(@JsonProperty("DatasetGraph") List<DatasetGraph> datasetGraph) {
    this.datasetGraph = datasetGraph;
  }

  @WithSpan
  public JobDatasetGraphUI of(
      JobDetails jobDetails,
      UserBitShared.QueryProfile profile,
      CatalogServiceHelper catalogServiceHelper,
      NamespaceService namespaceService,
      int attemptIndex)
      throws NamespaceException {
    final JobProtobuf.JobAttempt jobAttempt = jobDetails.getAttempts(attemptIndex);
    final JobProtobuf.JobInfo jobInfo = jobAttempt.getInfo();
    AccelerationDetails accelerationDetails = ReflectionUtils.getAccelerationDetails(jobAttempt);
    List<Reflection> reflections = ReflectionUtils.getReflections(accelerationDetails);
    addToReflectionsMap(reflections);
    datasetGraph = buildDataSetGraph(jobInfo, catalogServiceHelper, namespaceService, profile);
    if (isGraphException) {
      datasetGraph.clear();
      datasetGraph.add(
          new DatasetGraph().setDescription(DATASET_GRAPH_ERROR + graphExceptionTable));
    }
    return new JobDatasetGraphUI(datasetGraph);
  }

  public List<DatasetGraph> getDatasetGraph() {
    return datasetGraph;
  }

  private void addToReflectionsMap(List<Reflection> reflections) {
    for (Reflection reflection : reflections) {
      reflectionsMap.put(reflection.getReflectionID(), reflection);
    }
  }

  private List<DatasetGraph> buildDataSetGraph(
      JobProtobuf.JobInfo jobInfo,
      CatalogServiceHelper catalogServiceHelper,
      NamespaceService namespaceService,
      UserBitShared.QueryProfile profile) {
    List<JobProtobuf.ParentDatasetInfo> parents = jobInfo.getParentsList();
    String sqlQuery = jobInfo.getSql();
    try {
      for (JobProtobuf.ParentDatasetInfo parent : parents) {
        List<String> pathList = new ArrayList<>(parent.getDatasetPathList());
        buildTree(
            catalogServiceHelper,
            namespaceService,
            profile,
            sqlQuery,
            pathList,
            datasetGraph,
            null);
      }
    } catch (Exception ex) {
      logger.error("Error while building DataSetGraph", ex);
      isGraphException = Boolean.TRUE;
    }
    return datasetGraph;
  }

  private void buildTree(
      CatalogServiceHelper catalogServiceHelper,
      NamespaceService namespaceService,
      UserBitShared.QueryProfile profile,
      String sqlQuery,
      List<String> pathList,
      List<DatasetGraph> dGraph,
      String dbId)
      throws Exception {
    try {
      Optional<CatalogEntity> entity =
          catalogServiceHelper.getCatalogEntityByPath(
              pathList, new ArrayList<>(), new ArrayList<>());
      String datasetId = entity.get().getId();
      List<CatalogItem> parentsList;
      Optional<DatasetConfig> datasetConfig = catalogServiceHelper.getDatasetById(datasetId);
      if (datasetConfig.isPresent()) {
        DatasetConfig dataset = datasetConfig.get();
        parentsList = getParentsForDataset(dataset, namespaceService);
        dGraph.add(addNextDatasetGraph(dataset, parentsList, profile));
        if (CollectionUtils.isNotEmpty(parentsList)) {
          buildDatasetGraphTree(
              catalogServiceHelper, namespaceService, parentsList, dGraph, profile, sqlQuery);
        }
      }
    } catch (IllegalArgumentException ex) {
      handleCatalogEntityByPathException(dGraph, pathList, sqlQuery, dbId, null);
    } catch (Exception e) {
      graphExceptionTable = String.join(".", pathList);
      throw e;
    }
  }

  private List<CatalogItem> getParentsForDataset(
      DatasetConfig datasetConfig, NamespaceService namespaceService) throws NamespaceException {
    // only virtual datasets have parents
    if (datasetConfig.getType() != DatasetType.VIRTUAL_DATASET) {
      return Collections.emptyList();
    }
    final List<CatalogItem> parents = new ArrayList<>();
    final List<ParentDataset> parentsList = datasetConfig.getVirtualDataset().getParentsList();
    // Parents may not exist.  For example "select 1".
    if (CollectionUtils.isNotEmpty(parentsList)) {
      parentsList.forEach(
          (parent) -> {
            DatasetConfig tempDatasetConfig = null;
            AtomicBoolean isExternalQuery = getIsExternalQuery(parent.getDatasetPathList());
            final NameSpaceContainer entity =
                namespaceService
                    .getEntities(
                        Collections.singletonList(new NamespaceKey(parent.getDatasetPathList())))
                    .get(0);
            if (entity != null) {
              parents.add(CatalogItem.fromDatasetConfig(entity.getDataset(), null));
            } else {
              tempDatasetConfig =
                  new DatasetConfig()
                      .setId(new EntityId(String.valueOf(UUID.randomUUID())))
                      .setType(DatasetType.OTHERS)
                      .setFullPathList(parent.getDatasetPathList());
              if (isExternalQuery.get()) { // test condition to handle external query scenario.
                tempDatasetConfig.setName(EXTERNAL_QUERY);
              } else { // test condition to handle all others scenario.
                tempDatasetConfig.setName(OTHERS);
              }
              parents.add(CatalogItem.fromDatasetConfig(tempDatasetConfig, null));
            }
          });
    }
    return parents;
  }

  private DatasetGraph addNextDatasetGraph(
      DatasetConfig dataset, List<CatalogItem> parents, UserBitShared.QueryProfile profile) {
    DataSet sampleDataset = new DataSet();
    DatasetGraph datasetGraph = new DatasetGraph();
    datasetGraph.setId(dataset.getId().getId());
    List<String> parentsId = new ArrayList<>();
    parents.forEach(parent -> parentsId.add(parent.getId()));
    datasetGraph.setParentNodeIdList(parentsId);
    sampleDataset.setDatasetType(JobUtil.getDatasetType(String.valueOf(dataset.getType())));
    sampleDataset.setDatasetID(dataset.getId().getId());
    sampleDataset.setDatasetName(JobUtil.extractDatasetConfigName(dataset));
    sampleDataset.setDatasetPath(String.join(".", dataset.getFullPathList()));
    sampleDataset = addReflections(sampleDataset);
    datasetGraph.setDataSet(sampleDataset);
    java.util.Optional<UserBitShared.DatasetProfile> datasetProfileOptional =
        profile.getDatasetProfileList().stream()
            .filter(
                datasetProfile ->
                    datasetProfile
                        .getDatasetPath()
                        .replaceAll(QUOTES, "")
                        .equals(String.join(".", dataset.getFullPathList())))
            .findFirst();
    datasetProfileOptional.ifPresent(
        datasetProfile -> datasetGraph.setSql(datasetProfile.getSql()));
    return datasetGraph;
  }

  private List<DatasetGraph> buildDatasetGraphTree(
      CatalogServiceHelper catalogServiceHelper,
      NamespaceService namespaceService,
      List<CatalogItem> parentsList,
      List<DatasetGraph> graph,
      UserBitShared.QueryProfile profile,
      String sqlQuery)
      throws Exception {
    for (CatalogItem parent : parentsList) {
      List pathList = parent.getPath();
      try {
        buildTree(
            catalogServiceHelper,
            namespaceService,
            profile,
            sqlQuery,
            pathList,
            graph,
            parent.getId());
      } catch (Exception e) {
        throw e;
      }
    }
    return graph;
  }

  private void handleCatalogEntityByPathException(
      List<DatasetGraph> dGraph,
      List<String> pathList,
      String sqlQuery,
      String uuid,
      String cItem) {
    AtomicBoolean isExternalQuery = getIsExternalQuery(pathList);
    String tempId = uuid != null ? uuid : cItem;
    if (isExternalQuery.get()) {
      sqlQuery = !dGraph.isEmpty() ? "" : sqlQuery;
      dGraph.add(buildExternalQueryDataset(sqlQuery, pathList, OTHERS, EXTERNAL_QUERY, tempId));
    } else {
      dGraph.add(buildExternalQueryDataset(sqlQuery, pathList, OTHERS, OTHERS, tempId));
    }
  }

  private AtomicBoolean getIsExternalQuery(List<String> pathList) {
    AtomicBoolean isExternalQuery = new AtomicBoolean(false);
    pathList.forEach(
        (path) -> {
          if (path.contains(EXTERNAL_QUERY)) {
            isExternalQuery.set(true);
          }
        });
    return isExternalQuery;
  }

  private DatasetGraph buildExternalQueryDataset(
      String sqlQuery, List<String> pathList, String datasetType, String datasetName, String uuid) {
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

  private DataSet addReflections(DataSet dataSet) {
    List<Reflection> reflectionList = new ArrayList<>();
    reflectionsMap.forEach(
        (key, goal) -> {
          if (goal.getDatasetId().equals(dataSet.getDatasetID())) {
            Reflection reflection = buildReflections(goal, ReflectionMatchingType.EXPANSION);
            Reflection usedOrMatchedReflection = reflectionsMap.get(goal.getReflectionID());
            reflection.setIsUsed(usedOrMatchedReflection.getIsUsed());
            usedOrMatchedReflection.setReflectionMatchingType(ReflectionMatchingType.EXPANSION);
            reflectionList.add(reflection);
          }
        });
    dataSet.setReflectionsDefinedList(reflectionList);
    return dataSet;
  }

  private Reflection buildReflections(
      Reflection goal, ReflectionMatchingType reflectionMatchingType) {
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
}
