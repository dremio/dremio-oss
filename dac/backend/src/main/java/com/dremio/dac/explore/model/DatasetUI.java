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
package com.dremio.dac.explore.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.dremio.common.utils.PathUtils;
import com.dremio.dac.model.folder.FolderPath;
import com.dremio.dac.model.folder.SourceFolderPath;
import com.dremio.dac.model.job.JobFilters;
import com.dremio.dac.model.sources.PhysicalDatasetPath;
import com.dremio.dac.model.spaces.HomeName;
import com.dremio.dac.proto.model.dataset.Derivation;
import com.dremio.dac.proto.model.dataset.VirtualDatasetUI;
import com.dremio.dac.util.DatasetsUtil;
import com.dremio.file.FilePath;
import com.dremio.file.SourceFilePath;
import com.dremio.service.jobs.JobIndexKeys;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.ParentDataset;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;


/**
 * Minimal info of Dataset needed by UI.
 */
public class DatasetUI {

  private final String id;
  private final String sql;
  private final List<String> context;
  // full path to use when making transforms on this dataset like transforms
  // this is the name that the dataset is saved into the KV store with
  private final List<String> fullPath;
  // display full path mostly tracks fullPath, the case where it differs is when
  // a user opens up the default "select * from foo.bar". This field allows foo.bar
  // to displayed even though the query that was created is actually internally
  // considered an untitled new virtual dataset
  private final List<String> displayFullPath;
  private final Long version;
  private final DatasetVersion datasetVersion;
  private final Integer jobCount;
  private final Integer descendants;
  private final Boolean canReapply;
  private final DatasetType datasetType;
  private final Map<String, String> links;
  private final Map<String, String> apiLinks;

  public static DatasetUI newInstance(VirtualDatasetUI vds, DatasetVersion tipVersion) throws NamespaceException {
    Boolean isUnsaved = (vds.getIsNamed() != null) ? !vds.getIsNamed() : null;
    List<String> fullPath = vds.getFullPathList();
    List<String> displayFullPath;
    DatasetType datasetType;

    boolean isDerivedDirectly = DatasetsUtil.isCreatedFromParent(vds.getLastTransform());
    boolean isUnsavedDirectPhysicalDataset = isUnsaved && vds.getDerivation() == Derivation.DERIVED_PHYSICAL && isDerivedDirectly;

    boolean atHistoryTip = tipVersion == null || tipVersion.equals(vds.getVersion());
    if (isUnsavedDirectPhysicalDataset && atHistoryTip) { // example select * mongo.yelp.review
      ParentDataset parentDataset = vds.getParentsList().get(0);
      displayFullPath = parentDataset.getDatasetPathList(); // There is always going to be one parent since its tmp dataset created directly from a physical dataset.
      datasetType = parentDataset.getType();
    } else if(isUnsaved && vds.getDerivation() == Derivation.DERIVED_PHYSICAL) {
      displayFullPath = fullPath; // this is going to be tmp.UNTITLED
      datasetType = DatasetType.VIRTUAL_DATASET;
    } else {
      if (isUnsaved && vds.getDerivation() == Derivation.DERIVED_VIRTUAL) { // its tmp.UNTITLED select * from virtual dataset
        displayFullPath = vds.getParentsList().get(0).getDatasetPathList();
      } else {
        displayFullPath = fullPath;
      }
      datasetType = DatasetType.VIRTUAL_DATASET;
    }

    final Boolean canReapply = isUnsaved && vds.getDerivation() == Derivation.DERIVED_VIRTUAL;
    String sql = vds.getSql();
    List<String> context = vds.getState().getContextList();

    return new DatasetUI(vds.getId(), sql, context, fullPath, displayFullPath, vds.getSavedVersion(), vds.getVersion(),
        null, null, canReapply, datasetType,
        createLinks(fullPath, displayFullPath, vds.getVersion(), isUnsavedDirectPhysicalDataset), createApiLinks(fullPath, displayFullPath, datasetType, vds.getVersion(), isUnsaved, isDerivedDirectly));
  }

  @JsonCreator
  public DatasetUI(
      @JsonProperty("id") String id,
      @JsonProperty("sql") String sql,
      @JsonProperty("context") List<String> context,
      @JsonProperty("fullPath") List<String> fullPath,
      @JsonProperty("displayFullPath") List<String> displayFullPath,
      @JsonProperty("version") Long version,
      @JsonProperty("datasetVersion") DatasetVersion datasetVersion,
      @JsonProperty("jobCount") Integer jobCount,
      @JsonProperty("descendants") Integer descendants,
      @JsonProperty("canReapply") Boolean canReapply,
      @JsonProperty("datasetType") DatasetType datasetType,
      @JsonProperty("links") Map<String, String> links,
      @JsonProperty("apiLinks") Map<String, String> apiLinks) {
    this.id = id;
    this.sql = sql;
    this.context = context;
    this.fullPath = fullPath;
    this.displayFullPath = displayFullPath;
    this.version = version;
    this.datasetVersion = datasetVersion;
    this.jobCount = jobCount;
    this.descendants = descendants;
    this.canReapply = canReapply;
    this.datasetType = datasetType;
    this.links = links != null ? ImmutableMap.copyOf(links) : ImmutableMap.<String, String> of();
    this.apiLinks = apiLinks != null ? ImmutableMap.copyOf(apiLinks) : ImmutableMap.<String, String> of();
  }

  /**
   * SQL of the dataset definition.
   * @return
   */
  public String getSql() {
    return sql;
  }

  /**
   * Context where this dataset is created. Ex. mongo.yelp. => [ "mongo", "yelp ]
   * @return
   */
  public List<String> getContext() {
    return context;
  }

  /**
   * Dataset full path. Last component is the Dataset name.
   * Ex. myspace.subspace."my.Dataset" => ["myspace", "subspace", "my.Dataset"]
   * @return
   */
  public List<String> getFullPath() {
    return fullPath;
  }

  public List<String> getDisplayFullPath() {
    return displayFullPath;
  }

  /**
   * Saved version of the dataset.
   * @return
   */
  public Long getVersion() {
    return version;
  }

  /**
   * Dataset version.
   * @return
   */
  public DatasetVersion getDatasetVersion() {
    return datasetVersion;
  }

  /**
   * Number of jobs related to this dataset.
   * @return
   */
  public Integer getJobCount() {
    return jobCount;
  }

  /**
   * Number of descendant datasets derived from this dataset.
   * @return
   */
  public Integer getDescendants() {
    return descendants;
  }

  public String getId() {
    return id;
  }

  @JsonProperty("canReapply")
  public Boolean canReapply(){
    return canReapply;
  }

  public DatasetType getDatasetType() {
    return datasetType;
  }

  public Map<String, String> getLinks() {
    return links;
  }

  public Map<String, String> getApiLinks() {
    return apiLinks;
  }

  public static Map<String, String> createLinks(List<String> fullPath, List<String> displayFullPath, DatasetVersion datasetVersion, boolean isUnsavedDirectPhysicalDataset) {
    String dottedFullPath = PathUtils.constructFullPath(fullPath);
    String queryUrlPath;
    if (isUnsavedDirectPhysicalDataset) {
      if (displayFullPath.get(0).startsWith(HomeName.HOME_PREFIX)) {
        queryUrlPath = new DatasetPath(displayFullPath).getQueryUrlPath();
      } else {
        queryUrlPath = new PhysicalDatasetPath(displayFullPath).getQueryUrlPath();
      }
    } else {
      queryUrlPath = new DatasetPath(displayFullPath).getQueryUrlPath();
    }
    Map<String, String> links = new HashMap<>();
    links.put("self", queryUrlPath + "?version=" + datasetVersion);
    links.put("edit", queryUrlPath + "?mode=edit&version=" + datasetVersion);
    final JobFilters jobFilters = new JobFilters()
      .addFilter(JobIndexKeys.ALL_DATASETS, dottedFullPath)
      .addFilter(JobIndexKeys.QUERY_TYPE, JobIndexKeys.UI, JobIndexKeys.EXTERNAL);
    links.put("jobs", jobFilters.toUrl());
    links.put("context", "/datasets/context" + queryUrlPath);

    return links;
  }

  public static Map<String, String> createApiLinks(List<String> fullPath, List<String> displayFullPath, DatasetType datasetType,
      DatasetVersion datasetVersion, boolean isUnsaved, boolean isDerivedDirectly)  {
    String dottedFullPath = new NamespaceKey(fullPath).toUrlEncodedString();

    Map<String, String> links = new HashMap<>();
    links.put("self", "/dataset/" + dottedFullPath + "/version/" + datasetVersion);
    if (!isUnsaved || isDerivedDirectly) {
      links.put("namespaceEntity", getNamespaceEntityUrlPath(displayFullPath, datasetType));
    }

    return links;
  }

  private static String getNamespaceEntityUrlPath(List<String> displayFullPath, DatasetType datasetType) {
    switch(datasetType) {
    case VIRTUAL_DATASET:
      return new DatasetPath(displayFullPath).toUrlPath();
    case PHYSICAL_DATASET:
      return new PhysicalDatasetPath(displayFullPath).toUrlPath();
    case PHYSICAL_DATASET_SOURCE_FILE:
      return new SourceFilePath(displayFullPath).toUrlPath();
    case PHYSICAL_DATASET_SOURCE_FOLDER:
      return new SourceFolderPath(displayFullPath).toUrlPath();
    case PHYSICAL_DATASET_HOME_FILE:
      return new FilePath(displayFullPath).toUrlPath();
    case PHYSICAL_DATASET_HOME_FOLDER:
      return new FolderPath(displayFullPath).toUrlPath(); // this should not happen. can't query folder in home
    default:
      return null;
    }
  }
}
