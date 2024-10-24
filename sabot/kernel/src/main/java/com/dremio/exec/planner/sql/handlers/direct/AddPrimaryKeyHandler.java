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

import com.dremio.catalog.model.VersionContext;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.planner.sql.parser.SqlAlterTableAddPrimaryKey;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;

/** Adds primary key info to the table metadata */
public class AddPrimaryKeyHandler extends SimpleDirectHandlerWithValidator {
  private final Catalog catalog;
  private final UserSession userSession;

  public AddPrimaryKeyHandler(Catalog catalog, UserSession userSession) {
    this.catalog = catalog;
    this.userSession = userSession;
  }

  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode) throws Exception {
    SqlAlterTableAddPrimaryKey sqlAddPrimaryKey =
        SqlNodeUtil.unwrap(sqlNode, SqlAlterTableAddPrimaryKey.class);

    NamespaceKey path = catalog.resolveSingle(sqlAddPrimaryKey.getTable());

    VersionContext statementSourceVersion =
        sqlAddPrimaryKey
            .getSqlTableVersionSpec()
            .getTableVersionSpec()
            .getTableVersionContext()
            .asVersionContext();
    final VersionContext sessionVersion = userSession.getSessionVersionForSource(path.getRoot());
    VersionContext sourceVersion = statementSourceVersion.orElse(sessionVersion);

    validate(path, sourceVersion);

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
