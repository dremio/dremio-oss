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

import java.util.Collections;
import java.util.List;

import com.dremio.dac.explore.DatasetTool;
import com.dremio.dac.util.JSONUtil;
import com.dremio.service.job.JobSummary;
import com.dremio.service.job.RequestType;
import com.dremio.service.job.proto.JobProtobuf;
import com.dremio.service.job.proto.ParentDatasetInfo;
import com.dremio.service.jobs.JobsProtoUtil;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.proto.NameSpaceContainer.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

/**
 * List of jobs provided to jobs page.
 */
public class JobsUI {
  private final ImmutableList<JobListItem> jobs;
  private final String next;

  // do not provide types for these so ui hides overlay.
  private static final ParentDatasetInfo METADATA = new ParentDatasetInfo().setDatasetPathList(Collections.singletonList("Catalog"));
  private static final ParentDatasetInfo UNKNOWN = new ParentDatasetInfo().setDatasetPathList(Collections.singletonList("Unavailable"));

  private static DatasetType getType(NamespaceService service, List<String> namespacePath){
    try{
      List<NameSpaceContainer> containers = service.getEntities(Collections.singletonList(new NamespaceKey(namespacePath)));
      if(containers != null && !containers.isEmpty()){
        NameSpaceContainer container = containers.get(0);
        if(container.getType() == Type.DATASET){
          return container.getDataset().getType();
        }
      }
    }catch(Exception ex){

    }

    return null;
  }

  public static List<String> asTruePathOrNull(List<String> datasetPathList) {
    if(isTruePath(datasetPathList)){
      return datasetPathList;
    }
    return null;
  }

  public static boolean isTruePath(List<String> datasetPathList) {
    if(
        datasetPathList != null
        && !datasetPathList.equals(DatasetTool.TMP_DATASET_PATH.toPathList())
        && !datasetPathList.isEmpty()
        && !datasetPathList.get(0).equals("UNKNOWN")){
      return true;
    }

    return false;
  }

  public static ParentDatasetInfo getDatasetToDisplay(JobSummary jobSummary, NamespaceService service) {
    if (jobSummary.getRequestType() == RequestType.INVALID_REQUEST_TYPE) {
      return UNKNOWN;
    }

    switch(jobSummary.getRequestType()){
    case GET_CATALOGS:
    case GET_COLUMNS:
    case GET_SCHEMAS:
    case GET_TABLES:
      return METADATA;
    default:
    }

    final List<String> datasetPathList = jobSummary.getDatasetPathList();
    if(isTruePath(datasetPathList)) {
      // return a parent path.
      return new ParentDatasetInfo().setDatasetPathList(datasetPathList).setType(getType(service, datasetPathList));
    }
    // return one of the parents.
    if (jobSummary.hasParent()) {
      final JobProtobuf.ParentDatasetInfo parentDatasetInfo = jobSummary.getParent();
      return new ParentDatasetInfo()
        .setDatasetPathList(parentDatasetInfo.getDatasetPathList())
        .setType(JobsProtoUtil.toStuff(parentDatasetInfo.getType()));
    }
    return UNKNOWN;
  }

  public JobsUI(
    final NamespaceService service,
    final List<JobSummary> jobs,
    final String next) {
    this.jobs = FluentIterable.from(jobs)
      .transform(new Function<JobSummary, JobListItem>() {
        @Override
        public JobListItem apply(JobSummary input) {
          final ParentDatasetInfo displayInfo = getDatasetToDisplay(input, service);
          return new JobListItem(input, displayInfo);
        }
      }).toList();
    this.next = next;
  }

  @JsonCreator
  public JobsUI(
      @JsonProperty("jobs") List<JobListItem> jobs,
      @JsonProperty("next") String next) {
    this.jobs = ImmutableList.copyOf(jobs);
    this.next = next;
  }

  public ImmutableList<JobListItem> getJobs() {
    return jobs;
  }

  public String getNext() {
    return next;
  }

  @Override
  public String toString() {
    return JSONUtil.toString(this);
  }
}
