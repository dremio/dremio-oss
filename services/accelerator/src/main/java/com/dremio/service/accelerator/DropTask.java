/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.service.accelerator;

import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;

import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.exec.work.user.SubstitutionSettings;
import com.dremio.service.accelerator.proto.Acceleration;
import com.dremio.service.accelerator.proto.Materialization;
import com.dremio.service.accelerator.proto.MaterializationState;
import com.dremio.service.job.proto.MaterializationSummary;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.Job;
import com.dremio.service.jobs.JobStatusListener;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.google.common.base.Preconditions;

/**
 * An async task that drops given materialization.
 */
public class DropTask extends AsyncTask {
  private static final Logger logger = LoggerFactory.getLogger(DropTask.class);

  private final Acceleration acceleration;
  private final Materialization materialization;
  private final JobsService jobsService;

  private final DatasetConfig dataset;

  public DropTask(final Acceleration acceleration,
      final Materialization materialization,
      final JobsService jobsService,
      final DatasetConfig dataset) {
    Preconditions.checkArgument(materialization.getState() == MaterializationState.DONE,
        "cannot drop a non-completed materialization");
    this.acceleration = acceleration;
    this.materialization = materialization;
    this.jobsService = jobsService;
    this.dataset = dataset;
  }

  @Override
  public void doRun() {
    final String pathString = AccelerationUtils.makePathString(materialization.getPathList());
    final String query = String.format("DROP TABLE IF EXISTS %s", pathString);
    NamespaceKey datasetPathList = (dataset !=null)?new NamespaceKey(dataset.getFullPathList()):null;
    DatasetVersion datasetVersion = new DatasetVersion(acceleration.getContext().getDataset().getVersion());
    MaterializationSummary materializationSummary = new MaterializationSummary()
        .setAccelerationId(acceleration.getId().getId())
        .setLayoutId(materialization.getLayoutId().getId())
        .setLayoutVersion(materialization.getLayoutVersion())
        .setMaterializationId(materialization.getId().getId());
    final Job job = jobsService.submitJobWithExclusions(new SqlQuery(query, SYSTEM_USERNAME), QueryType.ACCELERATOR_DROP,
        datasetPathList, datasetVersion, JobStatusListener.NONE, materializationSummary, SubstitutionSettings.of());
    try {
      job.getData();
    } catch (final Exception e) {
      logger.warn("Failed to delete acceleration table with {}", query, e);
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || this.getClass() != obj.getClass()) {
      return false;
    }

    final DropTask that = (DropTask) obj;
    return (materialization == that.materialization) &&
           (jobsService == that.jobsService);
  }

  @Override
  public int hashCode() {
    return Objects.hash(materialization, jobsService);
  }
}
