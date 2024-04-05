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
package com.dremio.service.jobs;

import static com.dremio.common.utils.Protos.listNotNull;
import static com.dremio.service.jobs.JobIndexKeys.ALL_DATASETS;
import static com.dremio.service.jobs.JobIndexKeys.CHOSEN_REFLECTION_IDS;
import static com.dremio.service.jobs.JobIndexKeys.CONSIDERED_REFLECTION_IDS;
import static com.dremio.service.jobs.JobIndexKeys.DATASET;
import static com.dremio.service.jobs.JobIndexKeys.DATASET_VERSION;
import static com.dremio.service.jobs.JobIndexKeys.DURATION;
import static com.dremio.service.jobs.JobIndexKeys.END_TIME;
import static com.dremio.service.jobs.JobIndexKeys.JOBID;
import static com.dremio.service.jobs.JobIndexKeys.JOB_STATE;
import static com.dremio.service.jobs.JobIndexKeys.JOB_TTL_EXPIRY;
import static com.dremio.service.jobs.JobIndexKeys.MATCHED_REFLECTION_IDS;
import static com.dremio.service.jobs.JobIndexKeys.PARENT_DATASET;
import static com.dremio.service.jobs.JobIndexKeys.QUERY_TYPE;
import static com.dremio.service.jobs.JobIndexKeys.QUEUE_NAME;
import static com.dremio.service.jobs.JobIndexKeys.SPACE;
import static com.dremio.service.jobs.JobIndexKeys.SQL;
import static com.dremio.service.jobs.JobIndexKeys.START_TIME;
import static com.dremio.service.jobs.JobIndexKeys.USER;

import com.dremio.common.utils.PathUtils;
import com.dremio.datastore.api.DocumentConverter;
import com.dremio.datastore.api.DocumentWriter;
import com.dremio.datastore.api.LegacyIndexedStore;
import com.dremio.datastore.api.LegacyIndexedStoreCreationFunction;
import com.dremio.datastore.api.LegacyStoreBuildingFactory;
import com.dremio.datastore.format.Format;
import com.dremio.service.job.proto.JobAttempt;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobInfo;
import com.dremio.service.job.proto.JobResult;
import com.dremio.service.job.proto.ParentDatasetInfo;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.FieldOrigin;
import com.dremio.service.namespace.dataset.proto.Origin;
import com.dremio.service.namespace.dataset.proto.ParentDataset;
import com.google.common.base.Joiner;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Creator for jobs. */
public class JobsStoreCreator implements LegacyIndexedStoreCreationFunction<JobId, JobResult> {
  public static final String JOBS_NAME = "jobs";

  @SuppressWarnings("unchecked")
  @Override
  public LegacyIndexedStore<JobId, JobResult> build(LegacyStoreBuildingFactory factory) {
    return factory
        .<JobId, JobResult>newStore()
        .name(JOBS_NAME)
        .keyFormat(Format.wrapped(JobId.class, JobId::getId, JobId::new, Format.ofString()))
        .valueFormat(Format.ofProtostuff(JobResult.class))
        .buildIndexed(new JobConverter());
  }

  private static final class JobConverter implements DocumentConverter<JobId, JobResult> {
    private Integer version = 2;

    @Override
    public Integer getVersion() {
      return version;
    }

    @Override
    public void convert(DocumentWriter writer, JobId key, JobResult job) {
      final Set<NamespaceKey> allDatasets = new HashSet<>();
      // we only care about the last attempt
      final int numAttempts = job.getAttemptsList().size();
      final JobAttempt jobAttempt = job.getAttemptsList().get(numAttempts - 1);
      final JobInfo jobInfo = jobAttempt.getInfo();
      final NamespaceKey datasetPath = new NamespaceKey(jobInfo.getDatasetPathList());
      allDatasets.add(datasetPath);

      writer.write(JOBID, key.getId());
      writer.write(USER, jobInfo.getUser());
      writer.write(SPACE, jobInfo.getSpace());
      writer.write(SQL, jobInfo.getSql());
      writer.write(QUERY_TYPE, jobInfo.getQueryType().name());
      writer.write(JOB_STATE, jobAttempt.getState().name());
      writer.write(DATASET, datasetPath.getSchemaPath());
      writer.write(DATASET_VERSION, jobInfo.getDatasetVersion());
      writer.write(START_TIME, jobInfo.getStartTime());
      writer.write(END_TIME, jobInfo.getFinishTime());
      if (jobInfo.getResourceSchedulingInfo() != null
          && jobInfo.getResourceSchedulingInfo().getQueueName() != null) {
        writer.write(QUEUE_NAME, jobInfo.getResourceSchedulingInfo().getQueueName());
      }

      final Long duration =
          jobInfo.getStartTime() == null || jobInfo.getFinishTime() == null
              ? null
              : jobInfo.getFinishTime() - jobInfo.getStartTime();
      writer.write(DURATION, duration);

      Set<NamespaceKey> parentDatasets = new HashSet<>();
      List<ParentDatasetInfo> parentsList = jobInfo.getParentsList();
      if (parentsList != null) {
        for (ParentDatasetInfo parentDatasetInfo : parentsList) {
          List<String> pathList = listNotNull(parentDatasetInfo.getDatasetPathList());
          final NamespaceKey parentDatasetPath = new NamespaceKey(pathList);
          parentDatasets.add(parentDatasetPath);
          allDatasets.add(parentDatasetPath);
        }
      }
      List<FieldOrigin> fieldOrigins = jobInfo.getFieldOriginsList();
      if (fieldOrigins != null) {
        for (FieldOrigin fieldOrigin : fieldOrigins) {
          for (Origin origin : listNotNull(fieldOrigin.getOriginsList())) {
            List<String> tableList = listNotNull(origin.getTableList());
            if (!tableList.isEmpty()) {
              final NamespaceKey parentDatasetPath = new NamespaceKey(tableList);
              parentDatasets.add(parentDatasetPath);
              allDatasets.add(parentDatasetPath);
            }
          }
        }
      }

      for (NamespaceKey parentDatasetPath : parentDatasets) {
        // DX-5119 Index unquoted dataset names along with quoted ones.
        // TODO (Amit H): DX-1563 We should be using analyzer to match both rather than indexing
        // twice.
        writer.write(
            PARENT_DATASET,
            parentDatasetPath.getSchemaPath(),
            Joiner.on(PathUtils.getPathDelimiter()).join(parentDatasetPath.getPathComponents()));
      }
      // index all datasets accessed by this job
      for (ParentDataset parentDataset : listNotNull(jobInfo.getGrandParentsList())) {
        allDatasets.add(new NamespaceKey(parentDataset.getDatasetPathList()));
      }
      for (NamespaceKey allDatasetPath : allDatasets) {
        // DX-5119 Index unquoted dataset names along with quoted ones.
        // TODO (Amit H): DX-1563 We should be using analyzer to match both rather than indexing
        // twice.
        writer.write(
            ALL_DATASETS,
            allDatasetPath.getSchemaPath(),
            Joiner.on(PathUtils.getPathDelimiter()).join(allDatasetPath.getPathComponents()));
      }

      for (String id : listNotNull(jobInfo.getConsideredReflectionIdsList())) {
        writer.write(CONSIDERED_REFLECTION_IDS, id);
      }

      for (String id : listNotNull(jobInfo.getMatchedReflectionIdsList())) {
        writer.write(MATCHED_REFLECTION_IDS, id);
      }

      for (String id : listNotNull(jobInfo.getChosenReflectionIdsList())) {
        writer.write(CHOSEN_REFLECTION_IDS, id);
      }
      if (jobInfo.getTtlExpireAt() != null) {
        writer.writeTTLExpireAt(JOB_TTL_EXPIRY, jobInfo.getTtlExpireAt());
      }
    }
  }
}
