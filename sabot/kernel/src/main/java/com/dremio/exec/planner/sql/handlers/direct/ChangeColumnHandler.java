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
import java.util.Optional;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.sql.SqlNode;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.SqlHandlerUtil;
import com.dremio.exec.planner.sql.handlers.query.DataAdditionCmdHandler;
import com.dremio.exec.planner.sql.parser.SqlAlterTableChangeColumn;
import com.dremio.exec.store.iceberg.IcebergUtils;
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
    Optional<SimpleCommandResult> result = IcebergUtils.checkTableExistenceAndMutability(catalog, config,
        path, false);
    if (result.isPresent()) {
      return Collections.singletonList(result.get());
    }

    String currentColumnName = sqlChangeColumn.getColumnToChange();
    String columnNewName = sqlChangeColumn.getNewColumnSpec().getName().getSimple();

    DremioTable table = catalog.getTableNoResolve(path);
    if (!table.getSchema().findFieldIgnoreCase(currentColumnName).isPresent()) {
      throw UserException.validationError().message("Column [%s] is not present in table [%s]",
          currentColumnName, path).buildSilently();
    }

    if (!currentColumnName.equalsIgnoreCase(columnNewName) &&
        table.getSchema().findFieldIgnoreCase(columnNewName).isPresent()) {
      throw UserException.validationError().message("Column [%s] already present in table [%s]",
          columnNewName, path).buildSilently();
    }

    Field columnField = SqlHandlerUtil.fieldFromSqlColDeclaration(config, sqlChangeColumn.getNewColumnSpec(), sql);

    catalog.changeColumn(path, sqlChangeColumn.getColumnToChange(), columnField);

    DataAdditionCmdHandler.refreshDataset(catalog, path, false);
    return Collections.singletonList(SimpleCommandResult.successful(String.format("Column [%s] modified",
        currentColumnName)));
  }
}
