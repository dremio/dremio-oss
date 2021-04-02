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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.dotfile.View;
import com.dremio.exec.planner.sql.handlers.ConvertedRelNode;
import com.dremio.exec.planner.sql.handlers.PrelTransformer;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.SqlHandlerUtil;
import com.dremio.exec.planner.sql.parser.SqlCreateView;
import com.dremio.service.namespace.NamespaceKey;

public class CreateViewHandler extends SimpleDirectHandler {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CreateViewHandler.class);

  private final SqlHandlerConfig config;


  public CreateViewHandler(SqlHandlerConfig config) {
    this.config = config;
  }

  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode) throws Exception {
      SqlCreateView createView = SqlNodeUtil.unwrap(sqlNode, SqlCreateView.class);
      final String newViewName = createView.getName();

      // Store the viewSql as view def SqlNode is modified as part of the resolving the new table definition below.
      final String viewSql = createView.getQuery().toSqlString(CalciteSqlDialect.DEFAULT).getSql();
      final ConvertedRelNode convertedRelNode = PrelTransformer.validateAndConvert(config, createView.getQuery());
      final RelDataType validatedRowType = convertedRelNode.getValidatedRowType();
      final RelNode queryRelNode = convertedRelNode.getConvertedNode();
      final RelNode newViewRelNode = SqlHandlerUtil.resolveNewTableRel(true, createView.getFieldNames(), validatedRowType, queryRelNode);
      Catalog catalog = config.getContext().getCatalog();
      NamespaceKey viewPath = catalog.resolveSingle(createView.getPath());
      NamespaceKey defaultSchema = catalog.getDefaultSchema();

      final DremioTable existingTable = config.getContext().getCatalog().getTableNoResolve(viewPath);
      List<String> viewContext = defaultSchema == null ? null : defaultSchema.getPathComponents();

      final View view = new View(newViewName, viewSql, newViewRelNode.getRowType(), createView.getFieldNames(), viewContext);

      boolean replaced = false;

      if (existingTable != null) {
        if (existingTable.getJdbcTableType() != Schema.TableType.VIEW) {
          // existing table is not a view
          throw UserException.validationError()
              .message("A non-view table with given name [%s] already exists in schema [%s]",
                  newViewName, viewPath.getParent()
              )
              .build(logger);
        }

        if (existingTable.getJdbcTableType() == Schema.TableType.VIEW && !createView.getReplace()) {
          // existing table is a view and create view has no "REPLACE" clause
          throw UserException.validationError()
              .message("A view with given name [%s] already exists in schema [%s]",
                  newViewName, viewPath.getParent()
              )
              .build(logger);
        }

        config.getContext().getCatalog().updateView(viewPath, view);
        replaced = true;
      } else {
        config.getContext().getCatalog().createView(viewPath, view);
      }

      return Collections.singletonList(SimpleCommandResult.successful("View '%s' %s successfully",
          viewPath, replaced ? "replaced" : "created"));
  }
}
