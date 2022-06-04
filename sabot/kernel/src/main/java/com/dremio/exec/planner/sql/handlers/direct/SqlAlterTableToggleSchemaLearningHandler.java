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

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.parser.SqlAlterTableToggleSchemaLearning;
import com.dremio.service.namespace.NamespaceKey;

/**
 * Turns ON or OFF Schema Learning for the table specified by {@link SqlAlterTableToggleSchemaLearning}
 */
public class SqlAlterTableToggleSchemaLearningHandler extends SimpleDirectHandler {
  private static org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SqlAlterTableToggleSchemaLearningHandler.class);

  private final Catalog catalog;
  private final SqlHandlerConfig config;

  public SqlAlterTableToggleSchemaLearningHandler(Catalog catalog, SqlHandlerConfig config) {
    this.catalog = catalog;
    this.config = config;
  }

  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode) throws Exception {

    final SqlAlterTableToggleSchemaLearning sqlToggleSchemaLearning = SqlNodeUtil.unwrap(sqlNode, SqlAlterTableToggleSchemaLearning.class);

    NamespaceKey path = catalog.resolveSingle(sqlToggleSchemaLearning.getTable());
    DremioTable table = catalog.getTableNoResolve(path);

    if (table == null) {
      throw UserException.validationError()
        .message("Table [%s] not found", table)
        .buildSilently();
    }

    final boolean enableSchemaLearning = sqlToggleSchemaLearning.getEnableSchemaLearning();

    boolean isToggled = catalog.toggleSchemaLearning(path, enableSchemaLearning);
    if (!isToggled) {
      return Collections.singletonList(SimpleCommandResult.successful("Failed to toggle schema learning on table %s", path));
    }

    String message = String.format("Schema Learning on table %s is " + (enableSchemaLearning ? "enabled." : "disabled."), path);
    return Collections.singletonList(SimpleCommandResult.successful(message));
  }
}
