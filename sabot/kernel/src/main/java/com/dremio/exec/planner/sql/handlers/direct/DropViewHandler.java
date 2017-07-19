/*
 * Copyright (C) 2017 Dremio Corporation
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

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlNode;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.planner.sql.SchemaUtilities;
import com.dremio.exec.planner.sql.handlers.SqlHandlerUtil;
import com.dremio.exec.planner.sql.parser.SqlDropView;
import com.dremio.exec.store.AbstractSchema;
import com.dremio.exec.store.dfs.SchemaMutability.MutationType;

/** Handler for Drop View [If Exists] DDL command. */
public class DropViewHandler implements SqlDirectHandler<SimpleCommandResult> {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DropViewHandler.class);

  private final SchemaPlus defaultSchema;
  private final boolean systemUser;

  public DropViewHandler(SchemaPlus defaultSchema, boolean systemUser) {
    super();
    this.defaultSchema = defaultSchema;
    this.systemUser = systemUser;
  }

  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode) throws Exception {
    final SqlDropView dropView = SqlNodeUtil.unwrap(sqlNode, SqlDropView.class);
    final String viewName = dropView.getName();
    final AbstractSchema schemaInstance =
        SchemaUtilities.resolveToMutableSchemaInstance(defaultSchema, dropView.getSchemaPath(), systemUser, MutationType.VIEW);

    final String schemaPath = schemaInstance.getFullSchemaName();

    final Table viewToDrop = SqlHandlerUtil.getTableFromSchema(schemaInstance, viewName);
    if (dropView.checkViewExistence()) {
      if (viewToDrop == null || viewToDrop.getJdbcTableType() != Schema.TableType.VIEW){
        return Collections.singletonList(new SimpleCommandResult(true,
            String.format("View [%s] not found in schema [%s].", viewName, schemaPath)));
      }
    } else {
      if (viewToDrop != null && viewToDrop.getJdbcTableType() != Schema.TableType.VIEW) {
        throw UserException.validationError()
            .message("[%s] is not a VIEW in schema [%s]", viewName, schemaPath)
            .build(logger);
      } else if (viewToDrop == null) {
        throw UserException.validationError()
            .message("Unknown view [%s] in schema [%s].", viewName, schemaPath)
            .build(logger);
      }
    }

    schemaInstance.dropView(viewName);
    return Collections.singletonList(SimpleCommandResult.successful("View [%s] deleted successfully from schema [%s].", viewName, schemaPath));

  }


  @Override
  public Class<SimpleCommandResult> getResultType() {
    return SimpleCommandResult.class;
  }

}
