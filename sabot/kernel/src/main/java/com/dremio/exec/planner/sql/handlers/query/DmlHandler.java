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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.util.Pair;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.physical.PhysicalPlan;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.ScreenRel;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.sql.SqlExceptionHelper;
import com.dremio.exec.planner.sql.handlers.ConvertedRelNode;
import com.dremio.exec.planner.sql.handlers.PrelTransformer;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.SqlHandlerUtil;
import com.dremio.exec.planner.sql.handlers.direct.SqlNodeUtil;
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

  @Override
  void checkValidations(Catalog catalog, SqlHandlerConfig config, NamespaceKey path, SqlNode sqlNode) throws Exception {
    validateDmlRequest(catalog, config, path, getSqlOperator());
    validatePrivileges(catalog, path, sqlNode);
  }

  @Override
  protected PhysicalPlan getPlan(Catalog catalog, SqlHandlerConfig config, String sql, SqlNode sqlNode, NamespaceKey path) throws Exception {
    try {
      final Prel prel = getNonPhysicalPlan(catalog, config, sqlNode, path);
      final PhysicalOperator pop = PrelTransformer.convertToPop(config, prel);
      Runnable committer = !CatalogUtil.requestedPluginSupportsVersionedTables(path, catalog)
        ? () -> refreshDataset(catalog, path, false)
        : null;
      // cleaner will call refreshDataset to avoid the issues like DX-49928
      Runnable cleaner = committer;
      // Metadata for non-versioned plugins happens via this call back. For versioned tables (currently
      // only applies to Nessie), the metadata update happens during the operation within NessieClientImpl).
      return PrelTransformer.convertToPlan(config, pop, committer, cleaner);
    } catch (Exception e) {
      throw SqlExceptionHelper.coerceException(logger, sql, e, true);
    }
  }

  @VisibleForTesting
  public Prel getNonPhysicalPlan(Catalog catalog, SqlHandlerConfig config, SqlNode sqlNode, NamespaceKey path) throws Exception{
    // Extends sqlNode's DML target table with system columns (e.g., file_name and row_index)
    SqlDmlOperator sqlDmlOperator = SqlNodeUtil.unwrap(sqlNode, SqlDmlOperator.class);
    sqlDmlOperator.extendTableWithDataFileSystemColumns();

    final ConvertedRelNode convertedRelNode = PrelTransformer.validateAndConvert(config, sqlNode);

    final RelNode relNode = convertedRelNode.getConvertedNode();

    final Rel drel = convertToDrel(config, sqlNode, path, catalog, relNode);
    final Pair<Prel, String> prelAndTextPlan = PrelTransformer.convertToPrel(config, drel);

    return prelAndTextPlan.getKey();
  }

  @VisibleForTesting
  public static void validateDmlRequest(Catalog catalog, SqlHandlerConfig config, NamespaceKey path, SqlOperator sqlOperator) {
    if (!config.getContext().getOptions().getOption(ExecConstants.ENABLE_ICEBERG_ADVANCED_DML)) {
      throw UserException.unsupportedError()
        .message("%s clause is not supported in the query for this source", sqlOperator)
        .buildSilently();
    }

    IcebergUtils.checkTableExistenceAndMutability(catalog, config, path, sqlOperator, false);
  }


  @Override
  protected Rel convertToDrel(SqlHandlerConfig config, SqlNode sqlNode, NamespaceKey path, Catalog catalog, RelNode relNode) throws Exception {
    // Allow TableModifyCrel to access CreateTableEntry that can only be created now.
    CreateTableEntry createTableEntry = IcebergUtils.getIcebergCreateTableEntry(config, catalog,
      catalog.getTable(path), getSqlOperator().getKind(), null);
    Rel convertedRelNode = PrelTransformer.convertToDrel(config, rewriteCrel(relNode, createTableEntry));

    // below is for results to be returned to client - delete/update/merge operation summary output
    convertedRelNode = SqlHandlerUtil.storeQueryResultsIfNeeded(config.getConverter().getParserConfig(),
      config.getContext(), convertedRelNode);

    return new ScreenRel(convertedRelNode.getCluster(), convertedRelNode.getTraitSet(), convertedRelNode);
  }




}
