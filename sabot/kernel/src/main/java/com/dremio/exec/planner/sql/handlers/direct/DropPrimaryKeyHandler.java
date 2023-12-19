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
import com.dremio.exec.planner.sql.parser.SqlAlterTableDropPrimaryKey;
import com.dremio.exec.planner.sql.parser.SqlGrant;
import com.dremio.service.namespace.NamespaceKey;

/**
 * Drops the primary key info from a table
 */
public class DropPrimaryKeyHandler extends SimpleDirectHandler {
  private final Catalog catalog;

  public DropPrimaryKeyHandler(Catalog catalog) {
    this.catalog = catalog;
  }

  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode) throws Exception {
    SqlAlterTableDropPrimaryKey sqlDropPrimaryKey = SqlNodeUtil.unwrap(sqlNode, SqlAlterTableDropPrimaryKey.class);

    NamespaceKey path = catalog.resolveSingle(sqlDropPrimaryKey.getTable());
    catalog.validatePrivilege(path, SqlGrant.Privilege.ALTER);

    catalog.dropPrimaryKey(path, sqlDropPrimaryKey.getSqlTableVersionSpec().getTableVersionSpec().getTableVersionContext().asVersionContext(), catalog);

    return Collections.singletonList(SimpleCommandResult.successful("Primary key dropped."));
  }
}
