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
package com.dremio.exec.planner.sql.handlers.direct;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.List;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.sql.SqlNode;

import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.catalog.model.VersionContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.TableMutationOptions;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.SqlHandlerUtil;
import com.dremio.exec.planner.sql.handlers.query.DataAdditionCmdHandler;
import com.dremio.exec.planner.sql.parser.DremioSqlColumnDeclaration;
import com.dremio.exec.planner.sql.parser.SqlAlterTableChangeColumn;
import com.dremio.exec.planner.sql.parser.SqlGrant;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.base.Throwables;

/**
 * Changes column name, type specified by {@link SqlAlterTableChangeColumn}
 */
public class ChangeColumnHandler extends SimpleDirectHandler {
  private final Catalog catalog;
  private final SqlHandlerConfig config;

  public ChangeColumnHandler(Catalog catalog, SqlHandlerConfig config) {
    this.catalog = catalog;
    this.config = config;
  }

  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode) throws Exception {
    SqlAlterTableChangeColumn sqlChangeColumn = SqlNodeUtil.unwrap(sqlNode, SqlAlterTableChangeColumn.class);

    NamespaceKey sqlPath = catalog.resolveSingle(sqlChangeColumn.getTable());
    final String sourceName = sqlPath.getRoot();
    VersionContext statementSourceVersion = sqlChangeColumn.getSqlTableVersionSpec().getTableVersionSpec().getTableVersionContext().asVersionContext();
    final VersionContext sessionVersion = config.getContext().getSession().getSessionVersionForSource(sourceName);
    VersionContext sourceVersion = statementSourceVersion.orElse(sessionVersion);
    ResolvedVersionContext resolvedVersionContext = CatalogUtil.resolveVersionContext(catalog, sourceName, sourceVersion);
    final CatalogEntityKey catalogEntityKey = CatalogUtil.getResolvedCatalogEntityKey(
        catalog,
        sqlPath,
        resolvedVersionContext);
    NamespaceKey path = new NamespaceKey(catalogEntityKey.getKeyComponents());

    catalog.validatePrivilege(path, SqlGrant.Privilege.ALTER);

    DremioTable table = CatalogUtil.getTableNoResolve(catalogEntityKey, catalog);

    SimpleCommandResult result = SqlHandlerUtil.validateSupportForDDLOperations(catalog, config, path, table);

    if (!result.ok) {
      return Collections.singletonList(result);
    }

    String currentColumnName = sqlChangeColumn.getColumnToChange();
    DremioSqlColumnDeclaration newColumnSpec = sqlChangeColumn.getNewColumnSpec();
    String columnNewName = newColumnSpec.getName().getSimple();

    if (!table.getSchema().findFieldIgnoreCase(currentColumnName).isPresent()) {
      throw UserException.validationError().message("Column [%s] is not present in table [%s]",
        currentColumnName, path).buildSilently();
    }

    if (CatalogUtil.isFSInternalIcebergTableOrJsonTableOrMongo(catalog, path, table.getDatasetConfig()) && !currentColumnName.equalsIgnoreCase(columnNewName)) {
      throw UserException.validationError().message("Column [%s] cannot be renamed",
        currentColumnName).buildSilently();
    }
    TableMutationOptions tableMutationOptions = TableMutationOptions.newBuilder()
      .setResolvedVersionContext(resolvedVersionContext)
      .build();

    if (!currentColumnName.equalsIgnoreCase(columnNewName) &&
      table.getSchema().findFieldIgnoreCase(columnNewName).isPresent()) {
      throw UserException.validationError().message("Column [%s] already present in table [%s]",
        columnNewName, path).buildSilently();
    }

    SqlHandlerUtil.checkNestedFieldsForDuplicateNameDeclarations(sql, newColumnSpec.getDataType().getTypeName());

    Field columnField = SqlHandlerUtil.fieldFromSqlColDeclaration(config, newColumnSpec, sql);

    catalog.changeColumn(path, table.getDatasetConfig(), sqlChangeColumn.getColumnToChange(), columnField, tableMutationOptions);

    if (!CatalogUtil.isFSInternalIcebergTableOrJsonTableOrMongo(catalog, path, table.getDatasetConfig()) && !(CatalogUtil.requestedPluginSupportsVersionedTables(path, catalog))) {
      //only fire the refresh dataset query when the query is on a native iceberg table
      DataAdditionCmdHandler.refreshDataset(catalog, path, false);
    }

    try {
      handleFineGrainedAccess(
        config.getContext(),
        path,
        sqlChangeColumn.getColumnToChange(),
        columnNewName);
    } catch (InvalidRelException ex) {
      return Collections.singletonList(SimpleCommandResult.successful(String.format("Column [%s] modified. However, policy attachments need to be manually corrected",
        currentColumnName)));
    }

    return Collections.singletonList(SimpleCommandResult.successful(String.format("Column [%s] modified",
        currentColumnName)));
  }

  public void handleFineGrainedAccess(QueryContext context, NamespaceKey key, String oldColumn, String newColumn) throws Exception {
  }

  public static ChangeColumnHandler create(Catalog catalog, SqlHandlerConfig config) {
    try {
      final Class<?> cl = Class.forName("com.dremio.exec.planner.sql.handlers.EnterpriseChangeColumnHandler");
      final Constructor<?> ctor = cl.getConstructor(Catalog.class, SqlHandlerConfig.class);
      return (ChangeColumnHandler) ctor.newInstance(catalog, config);
    } catch (ClassNotFoundException e) {
      return new ChangeColumnHandler(catalog, config);
    } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e2) {
      throw Throwables.propagate(e2);
    }
  }
}
