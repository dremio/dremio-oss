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

import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.planner.sql.parser.SqlAlterTableAddPrimaryKey;
import com.dremio.exec.planner.sql.parser.SqlGrant;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;

/** Adds primary key info to the table metadata */
public class AddPrimaryKeyHandler extends SimpleDirectHandler {
  private final Catalog catalog;

  public AddPrimaryKeyHandler(Catalog catalog) {
    this.catalog = catalog;
  }

  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode) throws Exception {
    SqlAlterTableAddPrimaryKey sqlAddPrimaryKey =
        SqlNodeUtil.unwrap(sqlNode, SqlAlterTableAddPrimaryKey.class);

    NamespaceKey path = catalog.resolveSingle(sqlAddPrimaryKey.getTable());
    catalog.validatePrivilege(path, SqlGrant.Privilege.ALTER);

    catalog.addPrimaryKey(
        path,
        toStrings(sqlAddPrimaryKey.getColumnList()),
        sqlAddPrimaryKey
            .getSqlTableVersionSpec()
            .getTableVersionSpec()
            .getTableVersionContext()
            .asVersionContext());

    return Collections.singletonList(SimpleCommandResult.successful("Primary key added."));
  }

  private List<String> toStrings(SqlNodeList list) {
    if (list == null) {
      return ImmutableList.of();
    }
    List<String> columnNames = Lists.newArrayList();
    for (SqlNode node : list.getList()) {
      columnNames.add(node.toString());
    }
    return columnNames;
  }
}
