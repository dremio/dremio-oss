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

import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.store.CatalogService;
import com.dremio.options.OptionManager;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.jobs.JobsService;
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

/**
 * called when a materialization job is started
 */
public class RefreshStartHandler {
  protected static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RefreshStartHandler.class);

  private final CatalogService catalogService;
  private final JobsService jobsService;
  private final MaterializationStore materializationStore;
  private final WakeUpCallback wakeUpCallback;

  public RefreshStartHandler(CatalogService catalogService,
                             JobsService jobsService,
                             MaterializationStore materializationStore, WakeUpCallback wakeUpCallback) {
    this.catalogService = Preconditions.checkNotNull(catalogService, "Catalog service required");
    this.jobsService = Preconditions.checkNotNull(jobsService, "jobs service required");
    this.materializationStore = Preconditions.checkNotNull(materializationStore, "materialization store required");
    this.wakeUpCallback = Preconditions.checkNotNull(wakeUpCallback, "wakeup callback required");
  }

  public JobId startJob(ReflectionEntry entry, long jobSubmissionTime, OptionManager optionManager, Long previousIcebergSnapshot) {
    ReflectionId reflectionId = entry.getId();

    final MaterializationId id = new MaterializationId(UUID.randomUUID().toString());
    boolean icebergDataset = isIcebergDataset(optionManager);
    final Materialization materialization = new Materialization()
        .setId(id)
        .setInitRefreshSubmit(jobSubmissionTime)
        .setState(MaterializationState.RUNNING)
        .setLastRefreshFromPds(0L)
        .setReflectionGoalVersion(entry.getGoalVersion())
        .setReflectionGoalHash(entry.getReflectionGoalHash())
        .setReflectionId(reflectionId)
        .setArrowCachingEnabled(entry.getArrowCachingEnabled())
        .setIsIcebergDataset(icebergDataset)
        .setPreviousIcebergSnapshot(previousIcebergSnapshot);
    setIcebergReflectionAttributes(reflectionId, materialization, icebergDataset);
    // this is getting convoluted, but we need to make sure we save the materialization before we run the CTAS
    // as the MaterializedView will need it to extract the logicalPlan
    materializationStore.save(materialization);

    final String sql = String.format("REFRESH REFLECTION '%s' AS '%s'", reflectionId.getId(), materialization.getId().getId());

    final JobId jobId = ReflectionUtils.submitRefreshJob(jobsService, catalogService, entry, materialization, sql,
      new WakeUpManagerWhenJobDone(wakeUpCallback, "materialization job done"));

    logger.debug("Submitted REFRESH REFLECTION job {} for {}", jobId.getId(), ReflectionUtils.getId(entry, materialization));

    materialization.setInitRefreshJobId(jobId.getId());

    materializationStore.save(materialization);

    return jobId;
  }

  private boolean isIcebergDataset(OptionManager optionManager) {
    return optionManager.getOption(PlannerSettings.UNLIMITED_SPLITS_SUPPORT) &&
      optionManager.getOption(ExecConstants.ENABLE_ICEBERG);
  }

  private void setIcebergReflectionAttributes(ReflectionId reflectionId, Materialization materialization, boolean icebergDataset) {
    FluentIterable<Refresh> refreshes = materializationStore.getRefreshesByReflectionId(reflectionId);
    if (refreshes.isEmpty()) {
      // no previous refresh exists. nothing to set.
      return;
    }

    Refresh latestRefresh = refreshes.get(refreshes.size() - 1);
    if (icebergDataset) {
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
    } else if ((latestRefresh.getIsIcebergRefresh() != null && latestRefresh.getIsIcebergRefresh())) {
      // current refresh is not using iceberg
      // last refresh used Iceberg, hence forcing full refresh.
      materialization.setForceFullRefresh(true);
    }
  }
}
