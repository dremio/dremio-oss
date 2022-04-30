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
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.ResolvedVersionContext;
import com.dremio.exec.catalog.VersionContext;
import com.dremio.exec.physical.PhysicalPlan;
import com.dremio.exec.planner.sql.SqlExceptionHelper;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.direct.SqlNodeUtil;
import com.dremio.exec.planner.sql.parser.SqlCreateTable;
import com.dremio.options.OptionManager;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.annotations.VisibleForTesting;

public class CreateTableHandler extends DataAdditionCmdHandler {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CreateTableHandler.class);

  @Override
  public PhysicalPlan getPlan(SqlHandlerConfig config, String sql, SqlNode sqlNode) throws Exception {
      final SqlCreateTable sqlCreateTable = SqlNodeUtil.unwrap(sqlNode, SqlCreateTable.class);
      final Catalog catalog = config.getContext().getCatalog();
      final NamespaceKey path = catalog.resolveSingle(sqlCreateTable.getPath());

      // TODO: fix parser to disallow this
      if (sqlCreateTable.isSingleWriter() && !sqlCreateTable.getPartitionColumns(null).isEmpty()) {
        throw UserException.unsupportedError()
          .message("Cannot partition data and write to a single file at the same time.")
          .build(logger);
      }

      // this map has properties specified using 'STORE AS' in sql
      // will be null if 'STORE AS' is not in query
    createStorageOptionsMap(sqlCreateTable.getFormatOptions());
    if (CatalogUtil.requestedPluginSupportsVersionedTables(path, catalog)) {
      return doVersionedCtas(config, path, catalog, sql, sqlCreateTable);
    }
    return doCtas(config, path, catalog, sql, sqlCreateTable);

  }

  private PhysicalPlan doVersionedCtas(SqlHandlerConfig config, NamespaceKey path, Catalog catalog, String sql, SqlCreateTable sqlCreateTable) throws Exception {
    final String sourceName = path.getRoot();
    final VersionContext sessionVersion = config.getContext().getSession().getSessionVersionForSource(sourceName);
    final ResolvedVersionContext version = CatalogUtil.resolveVersionContext(catalog, sourceName, sessionVersion);

    try {
      validateVersionedTableFormatOptions(catalog, path, config.getContext().getOptions());
      checkExistenceValidity(path, getDremioTable(catalog, path));

      return super.getPlan(catalog,path, config, sql, sqlCreateTable, version);
    } catch (Exception e) {
      throw SqlExceptionHelper.coerceException(logger, sql, e, true);
    }

  }

  private PhysicalPlan doCtas(SqlHandlerConfig config,
                              NamespaceKey path,
                              Catalog catalog,
                              String sql,
                              SqlCreateTable sqlCreateTable) throws Exception {
    validateCreateTableFormatOptions(catalog, path, config.getContext().getOptions());
    validateCreateTableLocation(catalog, path, sqlCreateTable);
    try {
      return super.getPlan(catalog, path, config, sql, sqlCreateTable, null);
    } catch (Exception e) {
      throw SqlExceptionHelper.coerceException(logger, sql, e, true);
    }

  }

  @VisibleForTesting
  public void validateCreateTableFormatOptions(Catalog catalog, NamespaceKey path, OptionManager options) {
    validateTableFormatOptions(catalog, path, options);
    DremioTable table = catalog.getTableNoResolve(path);
    if(table != null) {
      throw UserException.validationError()
        .message("A table or view with given name [%s] already exists.", path)
        .build(logger);
    }
  }

  @Override
  public boolean isCreate() {
    return true;
  }
}
