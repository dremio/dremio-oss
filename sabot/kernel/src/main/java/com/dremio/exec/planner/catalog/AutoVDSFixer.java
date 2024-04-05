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
package com.dremio.exec.planner.catalog;

import static com.dremio.exec.planner.common.PlannerMetrics.VIEW_SCHEMA_LEARNING;

import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.dotfile.View;
import com.dremio.exec.physical.base.ViewOptions;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.common.PlannerMetrics;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.sql.CalciteArrowHelper;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.options.OptionResolver;
import com.dremio.sabot.rpc.user.UserSession;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Metrics;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AutoVDSFixer updates the view schema when it detects that the validated row type does not match
 * the view's schema. The main reason for doing this is so that the catalog shows the latest view
 * schema. Whether the view is updated successfully or not doesn't affect planning.
 *
 * <p>The validated row type is the row type of the view's SQL after validation through Calcite.
 * Complex types may be unknown as this time and so will return as ANY. ANY type will get resolved
 * to Arrow types during execution and we save this as the view's latest schema. Validation can also
 * determine type precision and nullability whereas execution loses precision and (not) nullability.
 */
public class AutoVDSFixer {
  private static final Logger LOGGER = LoggerFactory.getLogger(AutoVDSFixer.class);
  private final Catalog catalog;
  private final OptionResolver optionResolver;
  private final UserSession userSession;

  private final Meter.MeterProvider<Counter> counter;

  public AutoVDSFixer(Catalog catalog, OptionResolver optionResolver, UserSession userSession) {
    this.catalog = catalog;
    this.optionResolver = optionResolver;
    this.userSession = userSession;
    counter =
        Counter.builder(PlannerMetrics.createName(PlannerMetrics.PREFIX, VIEW_SCHEMA_LEARNING))
            .description("Counter for view schema learning")
            .withRegistry(Metrics.globalRegistry);
  }

  public void autoFixVds(ViewTable viewTable, RelRoot root) {
    if (!optionResolver.getOption(PlannerSettings.VDS_AUTO_FIX)
        || viewTable.getView().hasDeclaredFieldNames()) {
      return; // No auto fixing - view schema learning.
    }

    // Given the cost and complexity to auto fix, limit how often we do try to do this.
    if (viewTable.getDatasetConfig() != null
        && viewTable.getDatasetConfig().getLastModified()
            > System.currentTimeMillis()
                - optionResolver.getOption(PlannerSettings.VDS_AUTO_FIX_THRESHOLD) * 1000) {
      return;
    }

    RelDataType validatedRowType = root.validatedRowType;
    RelDataType viewRowType = viewTable.getView().getRowType(JavaTypeFactoryImpl.INSTANCE);
    if (MoreRelOptUtil.areRowTypesEqualForViewSchemaLearning(validatedRowType, viewRowType)) {
      return;
    }
    boolean success = false;
    try {
      View newView = viewTable.getView().withRowType(validatedRowType);

      String sourceName = viewTable.getPath().getRoot();
      Catalog updateViewCatalog = catalog;
      boolean versionedView =
          CatalogUtil.requestedPluginSupportsVersionedTables(sourceName, updateViewCatalog);

      // TODO(DX-48432) : Fix after DX-48432 is figured out - ownership chaining.
      if (viewTable.getViewOwner() != null) {
        updateViewCatalog = updateViewCatalog.resolveCatalog(viewTable.getViewOwner());
      }

      ViewOptions viewOptions = null;
      if (versionedView) {
        ResolvedVersionContext resolvedVersionContext =
            CatalogUtil.resolveVersionContext(
                updateViewCatalog, sourceName, userSession.getSessionVersionForSource(sourceName));
        // try to repair the view but only of the current versionContext is a branch
        if (resolvedVersionContext.getType() != ResolvedVersionContext.Type.BRANCH) {
          LOGGER.info(
              "Unable to update view {}. Versioned view update can only be performed when current version context is of type BRANCH."
                  + " The current version context is of type: {}.  Old schema (Arrow): {} New schema (Validated): {}",
              viewTable.getPath(),
              resolvedVersionContext.getType(),
              viewRowType.getFullTypeString(),
              validatedRowType.getFullTypeString());
          return;
        }
        viewOptions =
            new ViewOptions.ViewOptionsBuilder()
                .actionType(ViewOptions.ActionType.UPDATE_VIEW)
                .version(resolvedVersionContext)
                .batchSchema(CalciteArrowHelper.fromCalciteRowType(validatedRowType))
                .build();
      }

      updateViewCatalog.updateView(viewTable.getPath(), newView, viewOptions);
      success = true;
      LOGGER.info(
          "View {} was out of date and successfully updated. Old schema (Arrow): {} New schema (Validated): {}",
          viewTable.getPath(),
          viewRowType.getFullTypeString(),
          validatedRowType.getFullTypeString());

    } catch (Exception e) {
      LOGGER.error(
          "Failed to update view {}.  Old Schema (Arrow): {} New schema (Validated): {}",
          viewTable.getPath(),
          viewRowType.getFullTypeString(),
          validatedRowType.getFullTypeString(),
          e);
    } finally {
      PlannerMetrics.withOutcome(counter, success).increment();
    }
  }
}
