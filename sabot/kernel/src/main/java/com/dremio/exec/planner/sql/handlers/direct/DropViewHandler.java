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

import org.apache.calcite.schema.Schema.TableType;
import org.apache.calcite.sql.SqlNode;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.planner.sql.parser.SqlDropView;
import com.dremio.service.namespace.NamespaceKey;

/** Handler for Drop View [If Exists] DDL command. */
public class DropViewHandler implements SqlDirectHandler<SimpleCommandResult> {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DropViewHandler.class);

  private final Catalog catalog;

  public DropViewHandler(Catalog catalog) {
    this.catalog = catalog;
  }

  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode) throws Exception {
    final SqlDropView dropView = SqlNodeUtil.unwrap(sqlNode, SqlDropView.class);
    NamespaceKey path = catalog.resolveSingle(dropView.getPath());

    DremioTable table = catalog.getTableNoColumnCount(path);
    if (!dropView.checkViewExistence()) {
      if(table == null) {
        throw UserException.validationError()
          .message("Unknown view [%s].", path)
          .build(logger);
      } else if(table.getJdbcTableType() != TableType.VIEW) {
        throw UserException.validationError()
            .message("[%s] is not a VIEW", table.getPath())
            .build(logger);
      }
    } else if(table == null) {
      return Collections.singletonList(new SimpleCommandResult(true, String.format("View [%s] not found.", path)));
    } else if(table.getJdbcTableType() != TableType.VIEW) {
      return Collections.singletonList(new SimpleCommandResult(true, String.format("View [%s] not found.", path)));
    }

    catalog.dropView(path);
    return Collections.singletonList(SimpleCommandResult.successful("View [%s] deleted successfully.", path));
  }

  @Override
  public Class<SimpleCommandResult> getResultType() {
    return SimpleCommandResult.class;
  }

}
