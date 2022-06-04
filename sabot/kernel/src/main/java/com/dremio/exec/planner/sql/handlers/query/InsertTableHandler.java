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

import org.apache.calcite.sql.SqlNode;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.catalog.ResolvedVersionContext;
import com.dremio.exec.catalog.VersionContext;
import com.dremio.exec.physical.PhysicalPlan;
import com.dremio.exec.planner.sql.SqlExceptionHelper;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.direct.SqlNodeUtil;
import com.dremio.exec.planner.sql.parser.SqlInsertTable;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.annotations.VisibleForTesting;

public class InsertTableHandler extends DataAdditionCmdHandler {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(InsertTableHandler.class);

  @Override
  public PhysicalPlan getPlan(SqlHandlerConfig config, String sql, SqlNode sqlNode) throws Exception {
    try {
      final SqlInsertTable sqlInsertTable = SqlNodeUtil.unwrap(sqlNode, SqlInsertTable.class);
      final Catalog catalog = config.getContext().getCatalog();
      final NamespaceKey path = catalog.resolveSingle(sqlInsertTable.getPath());

      // TODO: fix parser to disallow this
      if (sqlInsertTable.isSingleWriter() &&
        !sqlInsertTable.getPartitionColumns(super.getDremioTable(catalog, path)).isEmpty()) {
        throw UserException.unsupportedError()
          .message("Cannot partition data and write to a single file at the same time.")
          .build(logger);
      }
      if (CatalogUtil.requestedPluginSupportsVersionedTables(path, catalog)) {
        return doVersionedInsert(catalog, config, path, sql, sqlNode, sqlInsertTable);
      } else {
        return doInsert(catalog, config, path, sql, sqlNode, sqlInsertTable);
      }
    }
    catch(Exception ex){
      throw SqlExceptionHelper.coerceException(logger, sql, ex, true);
    }
  }

  @VisibleForTesting
  public void validateInsertTableFormatOptions(Catalog catalog, SqlHandlerConfig config, NamespaceKey path) {
    validateTableFormatOptions(catalog, path, config.getContext().getOptions());
    IcebergUtils.checkTableExistenceAndMutability(catalog, config, path, false);
  }

  private PhysicalPlan doInsert(Catalog catalog, SqlHandlerConfig config, NamespaceKey path, String sql, SqlNode sqlNode, SqlInsertTable sqlInsertTable) throws Exception {
    validateInsertTableFormatOptions(catalog, config, path);
    PhysicalPlan plan = super.getPlan(catalog, path, config, sql, sqlInsertTable, null);
    super.validateIcebergSchemaForInsertCommand(sqlInsertTable.getFieldNames());
    return plan;
  }

  private PhysicalPlan doVersionedInsert(Catalog catalog, SqlHandlerConfig config, NamespaceKey path, String sql, SqlNode sqlNode, SqlInsertTable sqlInsertTable) throws Exception {
    final String sourceName = path.getRoot();
    final VersionContext sessionVersion = config.getContext().getSession().getSessionVersionForSource(sourceName);
    final ResolvedVersionContext version = CatalogUtil.resolveVersionContext(catalog, sourceName, sessionVersion);
    try {
      validateVersionedTableFormatOptions(catalog, path, config.getContext().getOptions());
      checkExistenceValidity(path, getDremioTable(catalog, path));
      return super.getPlan(catalog, path, config, sql, sqlInsertTable, version);
    } catch (Exception e) {
      throw SqlExceptionHelper.coerceException(logger, sql, e, true);
    }

  }

  @Override
  public boolean isCreate() {
    return false;
  }
}
