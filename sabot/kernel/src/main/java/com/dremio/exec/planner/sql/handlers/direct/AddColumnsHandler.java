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

import java.util.Collections;
import java.util.List;

import org.apache.calcite.sql.SqlNode;

import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.catalog.model.VersionContext;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.TableMutationOptions;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.SqlHandlerUtil;
import com.dremio.exec.planner.sql.handlers.query.DataAdditionCmdHandler;
import com.dremio.exec.planner.sql.parser.DremioSqlColumnDeclaration;
import com.dremio.exec.planner.sql.parser.SqlAlterTableAddColumns;
import com.dremio.exec.planner.sql.parser.SqlGrant;
import com.dremio.exec.record.BatchSchema;
import com.dremio.service.namespace.NamespaceKey;
/**
 * Adds columns to the table specified using {@link SqlAlterTableAddColumns}
 */
public class AddColumnsHandler extends SimpleDirectHandler {
  private final Catalog catalog;
  private final SqlHandlerConfig config;

  public AddColumnsHandler(Catalog catalog, SqlHandlerConfig config) {
    this.catalog = catalog;
    this.config = config;
  }

  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode) throws Exception {
    SqlAlterTableAddColumns sqlAddColumns = SqlNodeUtil.unwrap(sqlNode, SqlAlterTableAddColumns.class);

    NamespaceKey sqlPath = catalog.resolveSingle(sqlAddColumns.getTable());
    final String sourceName = sqlPath.getRoot();
    VersionContext statementSourceVersion = sqlAddColumns.getSqlTableVersionSpec().getTableVersionSpec().getTableVersionContext().asVersionContext();
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

    List<DremioSqlColumnDeclaration> newColumns = SqlHandlerUtil.columnDeclarationsFromSqlNodes(sqlAddColumns.getColumnList(), sql);

    SqlHandlerUtil.checkForDuplicateColumns(newColumns, table.getSchema(), sql);
    CatalogUtil.validateResolvedVersionIsBranch(resolvedVersionContext);
    TableMutationOptions tableMutationOptions = TableMutationOptions.newBuilder()
      .setResolvedVersionContext(resolvedVersionContext)
      .build();
    BatchSchema deltaSchema = SqlHandlerUtil.batchSchemaFromSqlSchemaSpec(config, newColumns, sql);
    catalog.addColumns(path, table.getDatasetConfig(), deltaSchema.getFields(), tableMutationOptions);

    if (!CatalogUtil.isFSInternalIcebergTableOrJsonTableOrMongo(catalog, path, table.getDatasetConfig()) && !(CatalogUtil.requestedPluginSupportsVersionedTables(path, catalog))) {
      DataAdditionCmdHandler.refreshDataset(catalog, path, false);
    }
    return Collections.singletonList(SimpleCommandResult.successful("New columns added."));
  }
}
