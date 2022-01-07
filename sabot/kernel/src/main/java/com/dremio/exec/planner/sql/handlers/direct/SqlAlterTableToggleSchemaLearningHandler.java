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

import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.planner.sql.parser.SqlAlterTableToggleSchemaLearning;

/**
 * Turns ON or OFF Schema Learning for the table specified by {@link SqlAlterTableToggleSchemaLearning}
 */
public class SqlAlterTableToggleSchemaLearningHandler extends SimpleDirectHandler {
  private static org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SqlAlterTableToggleSchemaLearningHandler.class);

  private final Catalog catalog;

  public SqlAlterTableToggleSchemaLearningHandler(Catalog catalog) {
    this.catalog = catalog;
  }

  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode) throws Exception {
    return Collections.singletonList(SimpleCommandResult.successful("Parsing Schema learning command is successful."));
  }
}
