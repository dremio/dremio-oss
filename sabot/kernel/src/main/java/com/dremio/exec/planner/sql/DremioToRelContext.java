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
package com.dremio.exec.planner.sql;

import java.util.ConcurrentModificationException;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable.ToRelContext;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.commons.lang3.StringUtils;

import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.common.exceptions.ErrorHelper;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.dotfile.View;
import com.dremio.exec.ops.PlannerCatalog;
import com.dremio.exec.physical.base.ViewOptions;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.google.common.collect.ImmutableList;

public interface DremioToRelContext {
  final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DremioToRelContext.class);

  interface DremioQueryToRelContext extends ToRelContext{
    SqlValidatorAndToRelContext.Builder getSqlValidatorAndToRelContext();
    RelRoot expandView(ViewTable view);
  }

  interface DremioSerializationToRelContext extends ToRelContext {
  }

  static DremioSerializationToRelContext createSerializationContext(RelOptCluster relOptCluster) {
    return new DremioSerializationToRelContext() {
      @Override
      public RelOptCluster getCluster() {
        return relOptCluster;
      }

      @Override
      public List<RelHint> getTableHints() {
        return ImmutableList.of();
      }

      @Override
      public RelRoot expandView(RelDataType rowType,
        String queryString,
        List<String> schemaPath, List<String> viewPath) {
        throw new UnsupportedOperationException();
      }
    };
  }

  static DremioQueryToRelContext createQueryContext(final SqlConverter sqlConverter) {
    return new DremioQueryToRelContext() {

      private static final int MAX_RETRIES = 5;

      @Override
      public SqlValidatorAndToRelContext.Builder getSqlValidatorAndToRelContext() {
        return SqlValidatorAndToRelContext.builder(sqlConverter);
      }

      @Override
      public RelOptCluster getCluster() {
        return sqlConverter.getCluster();
      }

      @Override
      public List<RelHint> getTableHints() {
        return ImmutableList.of();
      }

      @Override
      public RelRoot expandView(ViewTable view) {
        final RelRoot root;

        try {
          root = DremioSqlToRelConverter.expandView(view,
            view.getView().getSql(),
            sqlConverter
          );
        } catch (Exception ex) {
          String message = String.format("Error while expanding view %s. ", view.getPath());
          final SqlValidatorException sve = ErrorHelper.findWrappedCause(ex, SqlValidatorException.class);
          if (sve != null && StringUtils.isNotBlank(sve.getMessage())) {
            // Expose reason why view expansion failed such as specific table or column not found
            message += String.format("%s. Verify the view’s SQL definition.", sve.getMessage());
          } else if (StringUtils.isNotBlank(ex.getMessage())){
            message += String.format("%s", ex.getMessage());
          } else {
            message += "Verify the view’s SQL definition.";
          }

          throw UserException.planError(ex)
            .message(message)
            .addContext("View SQL", view.getView().getSql())
            .build(logger);
        }

        // After expanding view, check if the view's schema (i.e. cached row type) does not match the validated row type.  If so,
        // then update the view's row type in the catalog so that it is consistent again.  A common example when a view
        // needs to be updated is when a view is defined as a select * on a table and the table's schema changes.
        RelDataType validatedRowType = root.validatedRowType;
        RelDataType viewRowType = view.getView().getRowType(sqlConverter.getCluster().getTypeFactory());
        if (sqlConverter.getSettings().getOptions().getOption(PlannerSettings.VDS_AUTO_FIX) && !view.getView().hasDeclaredFieldNames()) {
          // this functionality only works for views that without externally defined field names. This is consistent with how VDSs are defined. (only legacy views support this)
          if (!MoreRelOptUtil.areRowTypesEqualForViewSchemaLearning(validatedRowType, viewRowType))
          {
            try {
              View newView = view.getView().withRowType(validatedRowType);

              int count = 0;
              boolean versionedView = false;
              String sourceName = view.getPath().getRoot();
              final PlannerCatalog viewCatalog = sqlConverter.getPlannerCatalog();
              Catalog updateViewCatalog = viewCatalog.getMetadataCatalog();
              if (CatalogUtil.requestedPluginSupportsVersionedTables(sourceName, updateViewCatalog)) {
                versionedView = true;
              }
              while (true) {
                try {
                  //TODO(DX-48432) : Fix after DX-48432 is figured out - ownership chaining.
                  if (view.getViewOwner() != null) {
                    updateViewCatalog = updateViewCatalog.resolveCatalog(view.getViewOwner());
                  }

                  ViewOptions viewOptions = null;
                  if (versionedView) {
                    ResolvedVersionContext resolvedVersionContext = CatalogUtil.resolveVersionContext(
                      updateViewCatalog,
                      sourceName,
                      sqlConverter.getSession().getSessionVersionForSource(sourceName)
                    );
                    // try to repair the view but only of the current versionContext is a branch
                    if (resolvedVersionContext.getType() != ResolvedVersionContext.Type.BRANCH) {
                      logger.info("Unable to update view {}. Versioned view update can only be performed when current version context is of type BRANCH." +
                        " The current version context is of type: {}", view.getPath(), resolvedVersionContext.getType());
                      break;
                    }
                    viewOptions = new ViewOptions.ViewOptionsBuilder()
                      .actionType(ViewOptions.ActionType.UPDATE_VIEW)
                      .version(resolvedVersionContext)
                      .batchSchema(CalciteArrowHelper.fromCalciteRowType(validatedRowType))
                      .build();
                  }

                  updateViewCatalog.updateView(view.getPath(), newView, viewOptions);
                  logger.info("View {} was out of date and successfully updated.  Old schema: {}  New schema: {}",
                    view.getPath(), viewRowType.getFullTypeString(), validatedRowType.getFullTypeString());
                  break;
                } catch (ConcurrentModificationException ex) {
                  if (count++ >= MAX_RETRIES) {
                    throw ex;
                  }
                  logger.warn("Retrying view {} update whose row type {} does not match its validated row type {}.  Retry count: {}",
                    view.getPath(), viewRowType.getFullTypeString(), validatedRowType.getFullTypeString(), count, ex);
                }
              }
            } catch (Exception e) {
              logger.error("Failed to update view {} whose row type {} does not match its validated row type {}.",
                view.getPath(), viewRowType.getFullTypeString(), validatedRowType.getFullTypeString(), e);
            }
          }
        }
        return root;
      }

      @Override
      public RelRoot expandView(RelDataType rowType, String queryString, List<String> schemaPath, List<String> viewPath) {
        throw new IllegalStateException("This expander should not be used.");
      }
    };
  }
}
