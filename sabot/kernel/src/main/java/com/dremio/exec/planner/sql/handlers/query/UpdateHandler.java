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
package com.dremio.exec.planner.sql.handlers.query;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;

import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.planner.sql.handlers.direct.SqlNodeUtil;
import com.dremio.exec.planner.sql.parser.SqlGrant.Privilege;
import com.dremio.exec.planner.sql.parser.SqlUpdateTable;
import com.dremio.service.namespace.NamespaceKey;

/**
 * Handles the UPDATE DML.
 */
public class UpdateHandler extends DmlHandler {

  @Override
  public NamespaceKey getTargetTablePath(SqlNode sqlNode) throws Exception {
    return SqlNodeUtil.unwrap(sqlNode, SqlUpdateTable.class).getPath();
  }

  @Override
  protected SqlOperator getSqlOperator() {
    return SqlUpdateTable.OPERATOR;
  }

  @Override
  protected void validatePrivileges(Catalog catalog, NamespaceKey path, SqlNode sqlNode) {
    catalog.validatePrivilege(path, Privilege.UPDATE);
    catalog.validatePrivilege(path, Privilege.SELECT);
  }
}
