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
import com.dremio.exec.planner.sql.parser.SqlAlterTableDropColumn;
import com.dremio.exec.planner.sql.parser.SqlGrant;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.base.Throwables;

/**
 * Removes column from the table specified by {@link SqlAlterTableDropColumn}
 */
public class DropColumnHandler extends SimpleDirectHandler {
  private final Catalog catalog;
  private final SqlHandlerConfig config;

  public DropColumnHandler(Catalog catalog, SqlHandlerConfig config) {
    this.catalog = catalog;
    this.config = config;
  }

  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode) throws Exception {
    SqlAlterTableDropColumn sqlDropColumn = SqlNodeUtil.unwrap(sqlNode, SqlAlterTableDropColumn.class);

    NamespaceKey sqlPath = catalog.resolveSingle(sqlDropColumn.getTable());
    final String sourceName = sqlPath.getRoot();
    VersionContext statementSourceVersion = sqlDropColumn.getSqlTableVersionSpec().getTableVersionSpec().getTableVersionContext().asVersionContext();
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

    SimpleCommandResult validate = SqlHandlerUtil.validateSupportForDDLOperations(catalog, config, path, table);

    if (!validate.ok) {
      return Collections.singletonList(validate);
    }

    if (table.getSchema().getFields().stream()
        .noneMatch(field -> field.getName().equalsIgnoreCase(sqlDropColumn.getColumnToDrop()))) {
      throw UserException.validationError().message("Column [%s] is not present in table [%s]",
          sqlDropColumn.getColumnToDrop(), path).buildSilently();
    }

    if (table.getSchema().getFieldCount() == 1) {
      throw UserException.validationError().message("Cannot drop all columns of a table").buildSilently();
    }
    CatalogUtil.validateResolvedVersionIsBranch(resolvedVersionContext);
    TableMutationOptions tableMutationOptions = TableMutationOptions.newBuilder()
      .setResolvedVersionContext(resolvedVersionContext)
      .build();
    catalog.dropColumn(path, table.getDatasetConfig(), sqlDropColumn.getColumnToDrop(), tableMutationOptions);

    if (!CatalogUtil.isFSInternalIcebergTableOrJsonTableOrMongo(catalog, path, table.getDatasetConfig()) && !(CatalogUtil.requestedPluginSupportsVersionedTables(path, catalog))) {
      DataAdditionCmdHandler.refreshDataset(catalog, path, false);
    }

    try {
      handleFineGrainedAccess(config.getContext(), path, sqlDropColumn.getColumnToDrop());
    } catch (InvalidRelException ex) {
      return Collections.singletonList(SimpleCommandResult.successful(String.format("Column [%s] dropped. However, policy attachments need to be manually corrected",
        sqlDropColumn.getColumnToDrop())));
    }

    return Collections.singletonList(SimpleCommandResult.successful(String.format("Column [%s] dropped",
        sqlDropColumn.getColumnToDrop())));
  }

  public void handleFineGrainedAccess(QueryContext context, NamespaceKey key, String dropColumn) throws Exception {
  }

  public static DropColumnHandler create(Catalog catalog, SqlHandlerConfig config) {
    try {
      final Class<?> cl = Class.forName("com.dremio.exec.planner.sql.handlers.EnterpriseDropColumnHandler");
      final Constructor<?> ctor = cl.getConstructor(Catalog.class, SqlHandlerConfig.class);
      return (DropColumnHandler) ctor.newInstance(catalog, config);
    } catch (ClassNotFoundException e) {
      return new DropColumnHandler(catalog, config);
    } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e2) {
      throw Throwables.propagate(e2);
    }
  }
}
