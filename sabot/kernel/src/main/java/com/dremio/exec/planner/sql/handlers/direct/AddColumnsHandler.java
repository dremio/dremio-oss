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

import org.apache.calcite.sql.SqlNode;

import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.SqlHandlerUtil;
import com.dremio.exec.planner.sql.handlers.query.DataAdditionCmdHandler;
import com.dremio.exec.planner.sql.parser.SqlAlterTableAddColumns;
import com.dremio.exec.planner.sql.parser.SqlColumnDeclaration;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.iceberg.IcebergTableOperations;
import com.dremio.service.namespace.NamespaceKey;

/**
 * Adds columns to the table specified using {@link SqlAlterTableAddColumns}
 */
public class AddColumnsHandler extends SimpleDirectHandler {

  private static org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AddColumnsHandler.class);

  private final Catalog catalog;
  private final SqlHandlerConfig config;

  public AddColumnsHandler(Catalog catalog, SqlHandlerConfig config) {
    this.catalog = catalog;
    this.config = config;
  }

  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode) throws Exception {
    SqlAlterTableAddColumns sqlAddColumns = SqlNodeUtil.unwrap(sqlNode, SqlAlterTableAddColumns.class);

    NamespaceKey path = catalog.resolveSingle(sqlAddColumns.getTable());

    Optional<SimpleCommandResult> validate = IcebergTableOperations.checkTableExistenceAndMutability(catalog, config,
        path, false);

    if (validate.isPresent()) {
      return Collections.singletonList(validate.get());
    }

    DremioTable table = catalog.getTableNoResolve(path);

    List<SqlColumnDeclaration> newColumns = SqlHandlerUtil.columnDeclarationsFromSqlNodes(sqlAddColumns.getColumnList(), sql);

    SqlHandlerUtil.checkForDuplicateColumns(newColumns, table.getSchema(), sql);

    BatchSchema deltaSchema = SqlHandlerUtil.batchSchemaFromSqlSchemaSpec(config, newColumns, sql);
    catalog.addColumns(path, deltaSchema.getFields());
    DataAdditionCmdHandler.refreshDataset(catalog, path, false);
    return Collections.singletonList(SimpleCommandResult.successful("New columns added."));
  }
}
