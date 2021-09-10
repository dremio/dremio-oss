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

import static com.dremio.exec.planner.sql.handlers.direct.AlterTableSetOptionHandler.createAttributeValue;
import static com.dremio.exec.planner.sql.handlers.direct.AlterTableSetOptionHandler.createIdentifierAttributeValue;

import java.util.Collections;
import java.util.List;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;

import com.dremio.common.exceptions.UserException;
import com.dremio.connector.metadata.AttributeValue;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.planner.sql.SqlExceptionHelper;
import com.dremio.exec.planner.sql.handlers.query.DataAdditionCmdHandler;
import com.dremio.exec.planner.sql.parser.SqlAlterTableChangeColumnSetOption;
import com.dremio.service.namespace.NamespaceKey;

/**
 * Alters column properties specified using {@link SqlAlterTableChangeColumnSetOption}
 */
public class AlterTableChangeColumnSetOptionHandler extends SimpleDirectHandler {

  private final Catalog catalog;

  public AlterTableChangeColumnSetOptionHandler(Catalog catalog) {
    super();
    this.catalog = catalog;
  }

  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode) throws Exception {
    final SqlAlterTableChangeColumnSetOption sqlColumnOption = SqlNodeUtil.unwrap(sqlNode, SqlAlterTableChangeColumnSetOption.class);
    final String optionName = sqlColumnOption.getName().toString().toLowerCase();

    NamespaceKey path = catalog.resolveSingle(sqlColumnOption.getTable());
    final String column = sqlColumnOption.getColumn();
    final SqlNode value = sqlColumnOption.getValue();
    if (value != null && !(value instanceof SqlLiteral) && !(value instanceof SqlIdentifier)) {
      throw SqlExceptionHelper.parseError("SET requires a literal value or identifier to be provided",
          sql, value.getParserPosition())
                              .buildSilently();
    }

    final String scope = sqlColumnOption.getScope();
    if (!scope.equalsIgnoreCase("COLUMN")) {
      throw UserException.validationError()
                         .message("[%s] is not supported", sql)
                         .buildSilently();
    }

    final DremioTable table = catalog.getTableNoResolve(path);

    if (table == null) {
      throw UserException.validationError()
                         .message("Table [%s] does not exist", path)
                         .buildSilently();
    }

    if (!table.getSchema().findFieldIgnoreCase(column).isPresent()) {
      throw UserException.validationError().message("Column [%s] is not present in table [%s]",
        column, path).buildSilently();
    }

    final AttributeValue optionValue;
    if (value != null) { // SET option
      if (value instanceof SqlIdentifier) {
        optionValue = createIdentifierAttributeValue((SqlIdentifier) value);
      } else {
        optionValue = createAttributeValue((SqlLiteral) value);
      }
    } else { // RESET option
      optionValue = null;
    }

    boolean changed = catalog.alterColumnOption(path, column, optionName, optionValue);
    String resultMessage;
    if (changed) {
      resultMessage = "Table [%s] column [%s] options updated";
      DataAdditionCmdHandler.refreshDataset(catalog, path, false);
    } else {
      resultMessage = "Table [%s] column [%s] options did not change";
    }

    return Collections.singletonList(SimpleCommandResult.successful(resultMessage, path, column));
  }
}
