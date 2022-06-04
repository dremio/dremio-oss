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

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.sql.SqlNode;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.ResolvedVersionContext;
import com.dremio.exec.catalog.TableMutationOptions;
import com.dremio.exec.catalog.VersionContext;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.SqlHandlerUtil;
import com.dremio.exec.planner.sql.handlers.query.DataAdditionCmdHandler;
import com.dremio.exec.planner.sql.parser.SqlAlterTableChangeColumn;
import com.dremio.exec.planner.sql.parser.SqlColumnDeclaration;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.NamespaceKey;

/**
 * Changes column name, type specified by {@link SqlAlterTableChangeColumn}
 */
public class ChangeColumnHandler extends SimpleDirectHandler {

  private static org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ChangeColumnHandler.class);

  private final Catalog catalog;
  private final SqlHandlerConfig config;

  public ChangeColumnHandler(Catalog catalog, SqlHandlerConfig config) {
    this.catalog = catalog;
    this.config = config;
  }

  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode) throws Exception {
    SqlAlterTableChangeColumn sqlChangeColumn = SqlNodeUtil.unwrap(sqlNode, SqlAlterTableChangeColumn.class);

    NamespaceKey path = catalog.resolveSingle(sqlChangeColumn.getTable());

    DremioTable table = catalog.getTableNoResolve(path);
    SimpleCommandResult result = SqlHandlerUtil.validateSupportForDDLOperations(catalog, config, path, table);

    if (!result.ok) {
      return Collections.singletonList(result);
    }

    String currentColumnName = sqlChangeColumn.getColumnToChange();
    SqlColumnDeclaration newColumnSpec = sqlChangeColumn.getNewColumnSpec();
    String columnNewName = newColumnSpec.getName().getSimple();

    if (!table.getSchema().findFieldIgnoreCase(currentColumnName).isPresent()) {
      throw UserException.validationError().message("Column [%s] is not present in table [%s]",
        currentColumnName, path).buildSilently();
    }

    if (DatasetHelper.isInternalIcebergTableOrJsonTable(table.getDatasetConfig()) && !currentColumnName.equalsIgnoreCase(columnNewName)) {
      throw UserException.validationError().message("Column [%s] cannot be renamed",
        currentColumnName).buildSilently();
    }

    final String sourceName = path.getRoot();
    final VersionContext sessionVersion = config.getContext().getSession().getSessionVersionForSource(sourceName);
    ResolvedVersionContext resolvedVersionContext = CatalogUtil.resolveVersionContext(catalog, sourceName, sessionVersion);
    CatalogUtil.validateResolvedVersionIsBranch(resolvedVersionContext, path.toString());
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

    catalog.changeColumn(path, sqlChangeColumn.getColumnToChange(), columnField, tableMutationOptions);

    if (!DatasetHelper.isInternalIcebergTableOrJsonTable(table.getDatasetConfig()) && !(CatalogUtil.requestedPluginSupportsVersionedTables(path, catalog))) {
      //only fire the refresh dataset query when the query is on a native iceberg table
      DataAdditionCmdHandler.refreshDataset(catalog, path, false);
    }
    return Collections.singletonList(SimpleCommandResult.successful(String.format("Column [%s] modified",
        currentColumnName)));
  }
}
