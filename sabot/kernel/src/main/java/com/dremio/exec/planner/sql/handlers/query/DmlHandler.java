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
package com.dremio.exec.planner.sql.handlers.query;

import static com.dremio.exec.planner.sql.handlers.query.DataAdditionCmdHandler.refreshDataset;
import static com.dremio.exec.planner.sql.parser.DmlUtils.getVersionContext;
import static com.dremio.exec.planner.sql.parser.DmlUtils.resolveVersionContextForDml;

import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.util.Pair;

import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.catalog.model.dataset.TableVersionType;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.ops.PlannerCatalog;
import com.dremio.exec.physical.PhysicalPlan;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.ScreenRel;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.sql.SqlExceptionHelper;
import com.dremio.exec.planner.sql.handlers.ConvertedRelNode;
import com.dremio.exec.planner.sql.handlers.DrelTransformer;
import com.dremio.exec.planner.sql.handlers.PrelTransformer;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.SqlHandlerUtil;
import com.dremio.exec.planner.sql.handlers.SqlToRelTransformer;
import com.dremio.exec.planner.sql.handlers.direct.SqlNodeUtil;
import com.dremio.exec.planner.sql.parser.DmlUtils;
import com.dremio.exec.planner.sql.parser.SqlDmlOperator;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.annotations.VisibleForTesting;

/**
 * An abstract class to be extended by classes that will handle future DML operations.
 * The current plan is to have DELETE, MERGE, and UPDATE to extend this class.
 *
 * Eventually, refactor DataAdditionCmdHandler such that INSERT and CTAS can be handled
 * through this abstract class as well. There are lots of shared code once we get to
 * implementing `getPlan`. Lots of those shared code can probably go into a utility
 * class or this base method.
 */
public abstract class DmlHandler extends TableManagementHandler {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DmlHandler.class);

  private String textPlan;
  private Rel drel;
  private Prel prel;

  @Override
  void checkValidations(Catalog catalog, SqlHandlerConfig config, NamespaceKey path, SqlNode sqlNode) throws Exception {
    validateDmlRequest(catalog, config, CatalogEntityKey.namespaceKeyToCatalogEntityKey(path, DmlUtils.getVersionContext((SqlDmlOperator) sqlNode)), getSqlOperator());
    validatePrivileges(catalog, path, sqlNode);
  }

  @VisibleForTesting
  @Override
  public PhysicalPlan getPlan(SqlHandlerConfig config, String sql, SqlNode sqlNode, NamespaceKey path) throws Exception {
    try {
      // Extends sqlNode's DML target table with system columns (e.g., file_name and row_index)
      SqlDmlOperator sqlDmlOperator = SqlNodeUtil.unwrap(sqlNode, SqlDmlOperator.class);
      sqlDmlOperator.extendTableWithDataFileSystemColumns();
      final ConvertedRelNode convertedRelNode = SqlToRelTransformer.validateAndConvertForDml(config, sqlNode, path.getRoot());
      final PlannerCatalog catalog = config.getConverter().getPlannerCatalog();
      final RelNode relNode = convertedRelNode.getConvertedNode();

      validateDmlVersioning(config.getContext().getCatalog(), sqlDmlOperator, path);

      drel = convertToDrel(config, sqlNode, path, catalog, relNode);
      final Pair<Prel, String> prelAndTextPlan = PrelTransformer.convertToPrel(config, drel);

      textPlan = prelAndTextPlan.getValue();
      prel = prelAndTextPlan.getKey();

      final PhysicalOperator pop = PrelTransformer.convertToPop(config, prel);
      final String sourceName = path.getRoot();
      final ResolvedVersionContext version = resolveVersionContextForDml(config, sqlDmlOperator, sourceName);
      CatalogUtil.validateResolvedVersionIsBranch(version);
      Runnable committer = !CatalogUtil.requestedPluginSupportsVersionedTables(path, config.getContext().getCatalog())
        ? () -> refreshDataset(config.getContext().getCatalog(), path, false)
        : null;
      // cleaner will call refreshDataset to avoid the issues like DX-49928
      Runnable cleaner = committer;
      // Metadata for non-versioned plugins happens via this call back. For versioned tables (currently
      // only applies to Nessie), the metadata update happens during the operation within NessieClientImpl).
      return PrelTransformer.convertToPlan(config, pop, committer, cleaner);
    } catch (CalciteContextException ex) {
      SqlDmlOperator sqlDmlOperator = SqlNodeUtil.unwrap(sqlNode, SqlDmlOperator.class);
      if (sqlDmlOperator.getSqlTableVersionSpec().getTableVersionSpec().getTableVersionType() != TableVersionType.NOT_SPECIFIED) {
        throw UserException.validationError(ex.getCause())
          .message("Version context for source table must be specified using AT SQL syntax")
          .buildSilently();
      }
    } catch (Exception e) {
      throw SqlExceptionHelper.coerceException(logger, sql, e, true);
    }
      return null;
  }

  @VisibleForTesting
  public Prel getPrel()
  {
    return prel;
  }

  @VisibleForTesting
  public static void validateDmlRequest(Catalog catalog, SqlHandlerConfig config, CatalogEntityKey catalogEntityKey, SqlOperator sqlOperator) {
    if (!config.getContext().getOptions().getOption(ExecConstants.ENABLE_ICEBERG_ADVANCED_DML)) {
      throw UserException.unsupportedError()
        .message("%s clause is not supported in the query for this source", sqlOperator)
        .buildSilently();
    }

    IcebergUtils.checkTableExistenceAndMutability(catalog, config, catalogEntityKey, sqlOperator, true);
  }

  @Override
  protected Rel convertToDrel(SqlHandlerConfig config, SqlNode sqlNode, NamespaceKey path, PlannerCatalog catalog, RelNode relNode) throws Exception {
    // Allow TableModifyCrel to access CreateTableEntry that can only be created now.
    SqlDmlOperator dmlOperator = (SqlDmlOperator) sqlNode;
    String sourceName = path.getRoot();
    DremioTable table = DmlUtils.getVersionContext(dmlOperator).getType() == TableVersionType.NOT_SPECIFIED ?
      catalog.getTableWithSchema(path) :
      catalog.getTableSnapshotWithSchema(path, getVersionContext(dmlOperator));
    final ResolvedVersionContext version = resolveVersionContextForDml(config, dmlOperator, sourceName);
    CreateTableEntry createTableEntry = IcebergUtils.getIcebergCreateTableEntry(config, config.getContext().getCatalog(),
      table, getSqlOperator(), null, version);
    Rel convertedRelNode = DrelTransformer.convertToDrel(config, rewriteCrel(relNode, createTableEntry));

    // below is for results to be returned to client - delete/update/merge operation summary output
    convertedRelNode = SqlHandlerUtil.storeQueryResultsIfNeeded(config.getConverter().getParserConfig(),
      config.getContext(), convertedRelNode);

    return new ScreenRel(convertedRelNode.getCluster(), convertedRelNode.getTraitSet(), convertedRelNode);
  }

  @Override
  public String getTextPlan() {
    return textPlan;
  }

  @Override
  public Rel getLogicalPlan() {
    return drel;
  }

  private void validateDmlVersioning(Catalog catalog, SqlDmlOperator sqlDmlOperator, NamespaceKey targetTableKey) {
    if (sqlDmlOperator.getSqlTableVersionSpec().getTableVersionSpec().getTableVersionType() == TableVersionType.NOT_SPECIFIED) {
      // No need to validate source table when there's no version for target table.
      return;
    }
    Stream<DremioTable> requestedTables = StreamSupport.stream(Spliterators.spliteratorUnknownSize(catalog.getAllRequestedTables().iterator(), Spliterator.ORDERED), false);
    requestedTables.forEach((requestedTable) -> {
        if (!requestedTable.hasAtSpecifier() && !requestedTable.getDataset().getName().equals(targetTableKey)) {
          throw UserException.validationError()
            .message(String.format("When specifying the version of the table to be modified, you must also specify the version of the source table %s using AT SQL syntax.", requestedTable.getDataset().getName()))
            .buildSilently();
        }
    });
  }
}
