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
package com.dremio.exec.planner.plancache;

import static org.slf4j.LoggerFactory.getLogger;

import com.dremio.common.AutoCloseables;
import com.dremio.common.utils.protos.AttemptId;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.DelegatingCatalog;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.server.SabotContext;
import com.dremio.options.OptionResolver;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.ParentDataset;
import com.dremio.service.reflection.UserSessionUtils;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.LinkedList;
import java.util.Queue;
import org.slf4j.Logger;

/**
 * Plan cache helper that takes care of releasing the query context when closed. Caller must close
 * the helper when done using the converter
 */
public class PlanCacheInvalidationHelper implements AutoCloseable {
  private static final Logger LOGGER = getLogger(PlanCacheInvalidationHelper.class);

  private final LegacyPlanCache planCache;
  private final OptionResolver optionResolver;
  private final QueryContext context;
  private final Catalog catalog;

  PlanCacheInvalidationHelper(SabotContext sabotContext, LegacyPlanCache legacyPlanCache) {
    final UserSession session = UserSessionUtils.systemSession(sabotContext.getOptionManager());
    this.context = new QueryContext(session, sabotContext, new AttemptId().toQueryId());
    this.catalog = context.getCatalog();
    this.optionResolver = context.getOptions();
    this.planCache = legacyPlanCache;
  }

  @Override
  public void close() {
    AutoCloseables.closeNoChecked(context);
  }

  @WithSpan
  public void invalidateReflectionAssociatedPlanCache(String datasetId) {
    Span.current().setAttribute("dremio.plan_cache.dataset_id", datasetId);
    if (!isPlanCacheEnabled()) {
      return;
    } else if (!(catalog instanceof DelegatingCatalog)) {
      return;
    }
    Queue<DatasetConfig> configQueue = new LinkedList<>();
    DremioTable rootTable = catalog.getTable(datasetId);
    if (rootTable == null) {
      LOGGER.info("Can't find dataset {}. Won't invalidate the plan cache", datasetId);
      return;
    }
    configQueue.add(rootTable.getDatasetConfig());
    while (!configQueue.isEmpty()) {
      DatasetConfig config = configQueue.remove();
      if (config.getType().getNumber() > 1) { // physical dataset types
        planCache.invalidateCacheOnDataset(config.getId().getId());
      } else if (config.getType() == DatasetType.VIRTUAL_DATASET
          && config.getVirtualDataset().getParentsList() != null) {
        for (ParentDataset parent : config.getVirtualDataset().getParentsList()) {
          try {
            DremioTable table = catalog.getTable(new NamespaceKey(parent.getDatasetPathList()));
            if (table != null) {
              configQueue.add(table.getDatasetConfig());
            } else {
              LOGGER.info("Can't find parent dataset {}", parent.getDatasetPathList());
            }
          } catch (Exception exception) {
            LOGGER.warn("Can't find parent dataset {}", parent.getDatasetPathList(), exception);
          }
        }
      }
    }
  }

  public long getCacheEntryCount() {
    return planCache.getCachePlans().size();
  }

  public void invalidatePlanCache() {
    planCache.getCachePlans().invalidateAll();
  }

  public boolean isPlanCacheEnabled() {
    return optionResolver.getOption(PlannerSettings.QUERY_PLAN_CACHE_ENABLED);
  }
}
