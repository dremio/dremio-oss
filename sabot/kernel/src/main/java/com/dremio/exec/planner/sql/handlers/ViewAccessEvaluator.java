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
package com.dremio.exec.planner.sql.handlers;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.CatalogIdentity;
import com.dremio.exec.catalog.CatalogUser;
import com.dremio.exec.catalog.DremioCatalogReader;
import com.dremio.exec.catalog.DremioPrepareTable;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.planner.DremioVolcanoPlanner;
import com.dremio.exec.planner.ExceptionUtils;
import com.dremio.exec.planner.acceleration.ExpansionNode;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.planner.sql.SqlValidatorAndToRelContext;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.VirtualDataset;
import com.dremio.service.users.UserNotFoundException;

/**
 * Validates dataset access by traversing parent datasets.
 * This validation is needed in case a default raw reflection in substituted during convertToRel.
 * Since this uses the same caching catalog from convertToRel, many tables may already be cached.
 */
public interface ViewAccessEvaluator extends Runnable, AutoCloseable {

  /**
   * Returns a ViewAccessEvaluator that asynchronously checks view access only if a default raw reflection
   * was substituted into the query during convertToRel phase.
   *
   * @param config
   * @param convertedRelNode
   * @return
   */
  static ViewAccessEvaluator createAsyncEvaluator(SqlHandlerConfig config, ConvertedRelNode convertedRelNode) {
    if (config.getConverter().getViewExpansionContext().isSubstitutedWithDRR()) {
      final RelNode convertedRelWithExpansionNodes = ((DremioVolcanoPlanner) convertedRelNode.getConvertedNode().getCluster().getPlanner()).getOriginalRoot();
      ViewAccessEvaluator vae = new ViewAccessEvaluatorImpl(convertedRelWithExpansionNodes, config);
      config.getContext().getExecutorService().submit(vae);
      return vae;
    } else {
      return new ViewAccessEvaluatorNoOp();
    }
  }

  class ViewAccessEvaluatorNoOp implements ViewAccessEvaluator {
    @Override
    public void run() {}

    @Override
    public void close() throws Exception {}
  }

  class ViewAccessEvaluatorImpl implements ViewAccessEvaluator {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ViewAccessEvaluatorImpl.class);

    private final CountDownLatch latch;
    private final RelNode rel;
    private final SqlHandlerConfig config;
    private Exception exception;

    ViewAccessEvaluatorImpl(RelNode rel, SqlHandlerConfig config) {
      this.rel = rel;
      this.config = config;
      latch = new CountDownLatch(1);
      exception = null;
    }

    @Override
    public void run() {
      final Thread currentThread = Thread.currentThread();
      final String originalName = currentThread.getName();
      currentThread.setName(config.getContext().getQueryId() + ":foreman-access-evaluation");
      try {
        final List<List<String>> topExpansionPaths = new ArrayList<>();
        rel.accept(new RelShuttleImpl() {
          @Override
          public RelNode visit(RelNode other) {
            if (other instanceof ExpansionNode) {
              ExpansionNode expansionNode = ((ExpansionNode) other);
              topExpansionPaths.add(expansionNode.getPath().getPathComponents());
              return other;
            }
            return super.visit(other);
          }
        });
        final String queryUser = config.getContext().getQueryUserName();
        if (!topExpansionPaths.isEmpty()) {
          SqlValidatorAndToRelContext sqlValidatorAndToRelContext = SqlValidatorAndToRelContext.builder(config.getConverter()).build();
          final List<DremioTable> tables = new ArrayList<>();
          for (List<String> path : topExpansionPaths) {
            if (!supportsVersioning(path, sqlValidatorAndToRelContext.getDremioCatalogReader())) {
              DremioTable table = sqlValidatorAndToRelContext.getDremioCatalogReader().getTable(path).getTable();
              tables.add(table);
            } else {
              // RBAC not supported on versioned views, so we assume current query user can access any Arctic view
              logger.trace(String.format("Access control on versioned view %s for user %s not supported", path, queryUser));
            }
          }
          validateViewAccess(tables, sqlValidatorAndToRelContext.getDremioCatalogReader().withCheckValidity(false), queryUser);
        }
      } catch (Exception e) {
        exception = e;
      } finally {
        latch.countDown();
        currentThread.setName(originalName);
      }
    }

    /**
     * Validates view access by recursively checking access to parent datasets as the view owner.
     * Views store their parent datasets in the KV store and these datasets are fully qualified so
     * no context (if any on the view) needs to be applied to catalog when looking up these datasets.
     *
     * @param tables
     * @param catalogReader
     * @param queryUser
     */
    private void validateViewAccess(List<DremioTable> tables, DremioCatalogReader catalogReader, String queryUser) {
      for (DremioTable table : tables) {
        DatasetConfig datasetConfig = table.getDatasetConfig();
        if (datasetConfig != null && table instanceof ViewTable) {
          final CatalogIdentity viewOwner = ((ViewTable) table).getViewOwner();
          final DremioCatalogReader catalogReaderWithUser = viewOwner == null ? catalogReader :
            catalogReader.withSchemaPathAndUser(null, viewOwner, false);
          VirtualDataset vds = datasetConfig.getVirtualDataset();
          if (vds != null && vds.getParentsList() != null) {
            validateViewAccess(
              vds.getParentsList().stream()
                .map(parent -> {
                  DremioPrepareTable dremioTable;
                  try {
                    dremioTable = catalogReaderWithUser.getTable(parent.getDatasetPathList());
                  } catch (RuntimeException ex) {
                    if (!(ex.getCause() instanceof UserNotFoundException)) {
                      throw ex;
                    }
                    dremioTable = catalogReader.withSchemaPathAndUser(null, new CatalogUser(queryUser), false)
                      .getTable(parent.getDatasetPathList());
                  }
                  if (dremioTable != null) {
                    return dremioTable.getTable();
                  }
                  return null;
                }).filter(Objects::nonNull).collect(Collectors.toList()),
              catalogReader,
              queryUser);
          }
        }
      }
    }

    private boolean supportsVersioning(List<String> expansionPath, DremioCatalogReader catalogReader) {
      return catalogReader.supportsVersioning(new NamespaceKey(expansionPath));
    }

    /**
     * Awaits on the result of view access validation.  Should be called with a try-with-resource statement
     * that includes physical planning.
     *
     * @throws Exception - if SELECT permissions are missing
     */
    @Override
    public void close() throws Exception {
      if (latch.await(config.getContext().getPlannerSettings().getMaxPlanningPerPhaseMS(), TimeUnit.MILLISECONDS)) {
        if (exception != null) {
          throw exception; // Security exception
        }
      } else {
        // Waiting on latch timed out
        final long inSecs = TimeUnit.MILLISECONDS.toSeconds(config.getContext().getPlannerSettings().getMaxPlanningPerPhaseMS());
        ExceptionUtils.throwUserException(String.format("Query was cancelled because view access evaluation time exceeded %d seconds", inSecs),
          null, config.getContext().getPlannerSettings(), null, UserException.AttemptCompletionState.PLANNING_TIMEOUT, logger);
      }
    }
  }
}
