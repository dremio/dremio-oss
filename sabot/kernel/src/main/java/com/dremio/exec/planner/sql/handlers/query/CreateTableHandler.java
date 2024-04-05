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

import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.catalog.model.VersionContext;
import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.DatasetCatalog;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.physical.PhysicalPlan;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.planner.sql.SqlExceptionHelper;
import com.dremio.exec.planner.sql.SqlValidatorImpl;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.direct.SqlNodeUtil;
import com.dremio.exec.planner.sql.parser.ReferenceTypeUtils;
import com.dremio.exec.planner.sql.parser.SqlCreateTable;
import com.dremio.exec.planner.sql.parser.SqlGrant.Privilege;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.options.OptionManager;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import org.apache.calcite.sql.SqlNode;

public class CreateTableHandler extends DataAdditionCmdHandler {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(CreateTableHandler.class);

  @Override
  public PhysicalPlan getPlan(SqlHandlerConfig config, String sql, SqlNode sqlNode)
      throws Exception {
    SqlValidatorImpl.checkForFeatureSpecificSyntax(sqlNode, config.getContext().getOptions());
    final SqlCreateTable sqlCreateTable = SqlNodeUtil.unwrap(sqlNode, SqlCreateTable.class);
    final Catalog catalog = config.getContext().getCatalog();
    final NamespaceKey path = catalog.resolveSingle(sqlCreateTable.getPath());
    catalog.validatePrivilege(path, Privilege.CREATE_TABLE);

    // TODO: fix parser to disallow this
    if (sqlCreateTable.isSingleWriter() && !sqlCreateTable.getPartitionColumns(null).isEmpty()) {
      throw UserException.unsupportedError()
          .message("Cannot partition data and write to a single file at the same time.")
          .build(logger);
    }

    IcebergUtils.validateIcebergLocalSortIfDeclared(sql, config.getContext().getOptions());

    // this map has properties specified using 'STORE AS' in sql
    // will be null if 'STORE AS' is not in query
    createStorageOptionsMap(sqlCreateTable.getFormatOptions());

    final String sourceName = path.getRoot();
    VersionContext statementSourceVersion =
        ReferenceTypeUtils.map(sqlCreateTable.getRefType(), sqlCreateTable.getRefValue(), null);
    final VersionContext sessionVersion =
        config.getContext().getSession().getSessionVersionForSource(sourceName);
    VersionContext sourceVersion = statementSourceVersion.orElse(sessionVersion);
    CatalogEntityKey catalogEntityKey =
        CatalogEntityKey.newBuilder()
            .keyComponents(path.getPathComponents())
            .tableVersionContext(TableVersionContext.of(sourceVersion))
            .build();
    return doCtas(config, catalogEntityKey, catalog, sql, sqlCreateTable);
  }

  private PhysicalPlan doCtas(
      SqlHandlerConfig config,
      CatalogEntityKey catalogEntityKey,
      Catalog catalog,
      String sql,
      SqlCreateTable sqlCreateTable)
      throws Exception {
    ResolvedVersionContext resolvedVersionContext =
        getResolvedVersionContextIfVersioned(catalogEntityKey, catalog);

    validateCreateTableFormatOptions(
        catalog,
        catalogEntityKey,
        config.getContext().getOptions(),
        getResolvedVersionContextIfVersioned(catalogEntityKey, catalog));
    validateCreateTableLocation(catalog, catalogEntityKey.toNamespaceKey(), sqlCreateTable);
    try {
      return super.getPlan(
          catalog, catalogEntityKey, config, sql, sqlCreateTable, resolvedVersionContext);
    } catch (Exception e) {
      throw SqlExceptionHelper.coerceException(logger, sql, e, true);
    }
  }

  @VisibleForTesting
  public void validateCreateTableFormatOptions(
      Catalog catalog,
      CatalogEntityKey catalogEntityKey,
      OptionManager options,
      ResolvedVersionContext resolvedVersionContext) {
    validateTableFormatOptions(catalog, catalogEntityKey, options, resolvedVersionContext);
    DremioTable table = catalog.getTableNoResolve(catalogEntityKey);
    if (table != null) {
      throw UserException.validationError()
          .message(
              "A table or view with given name [%s] already exists.",
              catalogEntityKey.toNamespaceKey().toString())
          .build(logger);
    }
  }

  @Override
  public boolean isCreate() {
    return true;
  }

  public static CreateTableHandler create() {
    try {
      final Class<?> cl =
          Class.forName("com.dremio.exec.planner.sql.handlers.EnterpriseCreateTableHandler");
      final Constructor<?> ctor = cl.getConstructor();
      return (CreateTableHandler) ctor.newInstance();
    } catch (ClassNotFoundException e) {
      return new CreateTableHandler();
    } catch (InstantiationException
        | IllegalAccessException
        | NoSuchMethodException
        | InvocationTargetException e2) {
      throw Throwables.propagate(e2);
    }
  }

  @Override
  public void cleanUp(DatasetCatalog datasetCatalog, NamespaceKey key) {
    try {
      CreateTableEntry tableEntry = getTableEntry();
      String tableLocation =
          isIcebergTable()
              ? tableEntry.getIcebergTableProps().getTableLocation()
              : tableEntry.getLocation();
      FileSystem fs =
          tableEntry.getPlugin().createFS(tableLocation, tableEntry.getUserName(), null);

      // delete folders created by CTAS
      Path path = Path.of(tableLocation);
      if (!fs.supportsPathsWithScheme()) {
        path = Path.of(Path.getContainerSpecificRelativePath(path));
      }
      fs.delete(path, true);

      if (!(tableEntry.getPlugin() instanceof FileSystemPlugin)) {
        // try deleting table from hive and glue metastore
        datasetCatalog.dropTable(key, null);
      } else {
        // try to delete the table from catalog
        datasetCatalog.forgetTable(key);
      }
    } catch (Exception e) {
      logger.warn(String.format("cleanup failed for CTAS query + %s", e.getMessage()));
    }
  }
}
