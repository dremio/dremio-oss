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
package com.dremio.service.reflection.refresh;

import java.util.UUID;

import com.dremio.service.job.proto.JobId;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.reflection.ReflectionManager.WakeUpCallback;
import com.dremio.service.reflection.ReflectionUtils;
import com.dremio.service.reflection.WakeUpManagerWhenJobDone;
import com.dremio.service.reflection.proto.Materialization;
import com.dremio.service.reflection.proto.MaterializationId;
import com.dremio.service.reflection.proto.MaterializationState;
import com.dremio.service.reflection.proto.ReflectionEntry;
import com.dremio.service.reflection.proto.ReflectionId;
import com.dremio.service.reflection.store.MaterializationStore;
import com.google.common.base.Preconditions;

/**
 * called when a materialization job is started
 */
public class RefreshStartHandler {
  protected static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RefreshStartHandler.class);

  private final NamespaceService namespaceService;
  private final JobsService jobsService;
  private final MaterializationStore materializationStore;

  private final ReflectionEntry entry;

  private final ReflectionId reflectionId;
  private final WakeUpCallback wakeUpCallback;

  public RefreshStartHandler(ReflectionEntry entry, NamespaceService namespaceService, JobsService jobsService,
      MaterializationStore materializationStore, WakeUpCallback wakeUpCallback) {
    this.entry = Preconditions.checkNotNull(entry, "entry required");
    this.namespaceService = Preconditions.checkNotNull(namespaceService, "namespace service required");
    this.jobsService = Preconditions.checkNotNull(jobsService, "jobs service required");
    this.materializationStore = Preconditions.checkNotNull(materializationStore, "materialization store required");
    this.reflectionId = entry.getId();
    this.wakeUpCallback = Preconditions.checkNotNull(wakeUpCallback, "wakeup callback required");

  }

  public JobId startJob(final long jobSubmissionTime) {
    final MaterializationId id = new MaterializationId(UUID.randomUUID().toString());
    logger.debug("starting refresh for materialization {}/{}", reflectionId.getId(), id.getId());
    final Materialization materialization = new Materialization()
        .setId(id)
        .setInitRefreshSubmit(jobSubmissionTime)
        .setState(MaterializationState.RUNNING)
        .setLastRefreshFromPds(0L)
        .setReflectionGoalVersion(entry.getGoalVersion())
        .setReflectionGoalHash(entry.getReflectionGoalHash())
        .setReflectionId(reflectionId)
        .setArrowCachingEnabled(entry.getArrowCachingEnabled());
    // this is getting convoluted, but we need to make sure we save the materialization before we run the CTAS
    // as the MaterializedView will need it to extract the logicalPlan
    materializationStore.save(materialization);

    final String sql = String.format("REFRESH REFLECTION '%s' AS '%s'", reflectionId.getId(), materialization.getId().getId());

    final JobId jobId = ReflectionUtils.submitRefreshJob(jobsService, namespaceService, entry, materialization, sql,
      new WakeUpManagerWhenJobDone(wakeUpCallback, "materialization job done"));

    logger.debug("starting materialization job {}", jobId.getId());

    materialization.setInitRefreshJobId(jobId.getId());

    materializationStore.save(materialization);

    return jobId;
  }

}
