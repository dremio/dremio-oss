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

import com.dremio.exec.store.CatalogService;
import com.dremio.options.OptionManager;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.reflection.ReflectionManager;
import com.dremio.service.reflection.ReflectionManager.WakeUpCallback;
import com.dremio.service.reflection.ReflectionUtils;
import com.dremio.service.reflection.WakeUpManagerWhenJobDone;
import com.dremio.service.reflection.proto.Materialization;
import com.dremio.service.reflection.proto.MaterializationId;
import com.dremio.service.reflection.proto.MaterializationState;
import com.dremio.service.reflection.proto.ReflectionEntry;
import com.dremio.service.reflection.proto.ReflectionId;
import com.dremio.service.reflection.proto.Refresh;
import com.dremio.service.reflection.store.MaterializationStore;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.UUID;

/** called when a materialization job is started */
public class RefreshStartHandler {
  protected static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(RefreshStartHandler.class);

  private final CatalogService catalogService;
  private final JobsService jobsService;
  private final MaterializationStore materializationStore;
  private final WakeUpCallback wakeUpCallback;
  private final OptionManager optionManager;

  public RefreshStartHandler(
      CatalogService catalogService,
      JobsService jobsService,
      MaterializationStore materializationStore,
      WakeUpCallback wakeUpCallback,
      OptionManager optionManager) {
    this.catalogService = Preconditions.checkNotNull(catalogService, "Catalog service required");
    this.jobsService = Preconditions.checkNotNull(jobsService, "jobs service required");
    this.materializationStore =
        Preconditions.checkNotNull(materializationStore, "materialization store required");
    this.wakeUpCallback = Preconditions.checkNotNull(wakeUpCallback, "wakeup callback required");
    this.optionManager = Preconditions.checkNotNull(optionManager, "option manager required");
  }

  @WithSpan
  public JobId startJob(
      ReflectionEntry entry, long jobSubmissionTime, Long previousIcebergSnapshot) {
    ReflectionId reflectionId = entry.getId();

    final MaterializationId id = new MaterializationId(UUID.randomUUID().toString());
    final Materialization materialization =
        new Materialization()
            .setId(id)
            .setInitRefreshSubmit(jobSubmissionTime)
            .setState(MaterializationState.RUNNING)
            .setLastRefreshFromPds(0L)
            .setReflectionGoalVersion(entry.getGoalVersion())
            .setReflectionGoalHash(entry.getReflectionGoalHash())
            .setReflectionId(reflectionId)
            .setArrowCachingEnabled(entry.getArrowCachingEnabled())
            .setIsIcebergDataset(true)
            .setPreviousIcebergSnapshot(previousIcebergSnapshot);
    setIcebergReflectionAttributes(reflectionId, materialization);
    // this is getting convoluted, but we need to make sure we save the materialization before we
    // run the CTAS
    // as the MaterializedView will need it to extract the logicalPlan
    materializationStore.save(materialization);

    final String sql =
        String.format(
            "REFRESH REFLECTION '%s' AS '%s'",
            reflectionId.getId(), materialization.getId().getId());

    final JobId jobId =
        ReflectionUtils.submitRefreshJob(
            jobsService,
            catalogService,
            entry,
            materialization.getId(),
            sql,
            QueryType.ACCELERATOR_CREATE,
            new WakeUpManagerWhenJobDone(wakeUpCallback, "materialization job done"),
            optionManager);

    logger.debug(
        "Submitted REFRESH REFLECTION job {} for {}",
        jobId.getId(),
        ReflectionUtils.getId(entry, materialization));

    materialization.setInitRefreshJobId(jobId.getId());

    materializationStore.save(materialization);
    ReflectionManager.setSpanAttributes(entry, materialization);

    return jobId;
  }

  private void setIcebergReflectionAttributes(
      ReflectionId reflectionId, Materialization materialization) {
    FluentIterable<Refresh> refreshes =
        materializationStore.getRefreshesByReflectionId(reflectionId);
    if (refreshes.isEmpty()) {
      // no previous refresh exists. nothing to set.
      return;
    }

    Refresh latestRefresh = refreshes.get(refreshes.size() - 1);
    // current refresh is using iceberg.
    if (latestRefresh.getIsIcebergRefresh() == null || !latestRefresh.getIsIcebergRefresh()) {
      // last refresh did not use Iceberg, hence forcing full refresh
      materialization.setForceFullRefresh(true);
    }
    if (latestRefresh.getIsIcebergRefresh() != null && latestRefresh.getIsIcebergRefresh()) {
      // current refresh is iceberg, and last refresh was also iceberg
      // set base path so that incremental refresh can insert into Iceberg table at base path
      materialization.setBasePath(latestRefresh.getBasePath());
    }
  }
}
