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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.dotfile.View;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.sql.ParserConfig;
import com.dremio.exec.planner.sql.SchemaUtilities;
import com.dremio.exec.planner.sql.handlers.ConvertedRelNode;
import com.dremio.exec.planner.sql.handlers.PrelTransformer;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.SqlHandlerUtil;
import com.dremio.exec.planner.sql.parser.SqlCreateView;
import com.dremio.exec.store.AbstractSchema;
import com.dremio.exec.store.dfs.SchemaMutability.MutationType;

public class CreateViewHandler extends SimpleDirectHandler {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CreateViewHandler.class);

  private final SqlHandlerConfig config;
  private final QueryContext context;
  private final boolean systemUser;


  public CreateViewHandler(SqlHandlerConfig config, boolean systemUser) {
    super();
    this.config = config;
    this.context = config.getContext();
    this.systemUser = systemUser;
  }

  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode) throws Exception {
      SqlCreateView createView = SqlNodeUtil.unwrap(sqlNode, SqlCreateView.class);

      final String newViewName = createView.getName();

      // Store the viewSql as view def SqlNode is modified as part of the resolving the new table definition below.
      final String viewSql = createView.getQuery().toSqlString(new SqlDialect(SqlDialect.CALCITE.getDatabaseProduct(), SqlDialect.CALCITE.getDatabaseProduct().name(), ParserConfig.QUOTING.string)).getSql();
      final ConvertedRelNode convertedRelNode = PrelTransformer.validateAndConvert(config, createView.getQuery());
      final RelDataType validatedRowType = convertedRelNode.getValidatedRowType();
      final RelNode queryRelNode = convertedRelNode.getConvertedNode();

      final RelNode newViewRelNode = SqlHandlerUtil.resolveNewTableRel(true, createView.getFieldNames(), validatedRowType, queryRelNode);

      final SchemaPlus defaultSchema = context.getNewDefaultSchema();
      final AbstractSchema schemaInstance = SchemaUtilities.resolveToMutableSchemaInstance(defaultSchema, createView.getSchemaPath(), systemUser, MutationType.VIEW);

      final String schemaPath = schemaInstance.getFullSchemaName();
      final View view = new View(newViewName, viewSql, newViewRelNode.getRowType(), SchemaUtilities.getSchemaPathAsList(defaultSchema));

      final Table existingTable = SqlHandlerUtil.getTableFromSchema(schemaInstance, newViewName);

      if (existingTable != null) {
        if (existingTable.getJdbcTableType() != Schema.TableType.VIEW) {
          // existing table is not a view
          throw UserException.validationError()
              .message("A non-view table with given name [%s] already exists in schema [%s]",
                  newViewName, schemaPath
              )
              .build(logger);
        }

        if (existingTable.getJdbcTableType() == Schema.TableType.VIEW && !createView.getReplace()) {
          // existing table is a view and create view has no "REPLACE" clause
          throw UserException.validationError()
              .message("A view with given name [%s] already exists in schema [%s]",
                  newViewName, schemaPath
              )
              .build(logger);
        }
      }

      final boolean replaced = schemaInstance.createView(view);
      return Collections.singletonList(SimpleCommandResult.successful("View '%s' %s successfully in '%s' schema",
          createView.getName(), replaced ? "replaced" : "created", schemaPath));
  }
}
