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
import org.apache.calcite.sql.SqlOperator;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.TableMutationOptions;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.parser.SqlGrant;
import com.dremio.exec.planner.sql.parser.SqlRollbackTable;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;

public class RollbackHandler extends TableManagementDirectHandler {

  public RollbackHandler(Catalog catalog, SqlHandlerConfig config) {
    super(catalog, config);
  }

  @Override
  public NamespaceKey getTablePath(SqlNode sqlNode) throws Exception {
    return SqlNodeUtil.unwrap(sqlNode, SqlRollbackTable.class).getPath();
  }

  @Override
  protected SqlOperator getSqlOperator() {
    return SqlRollbackTable.OPERATOR;
  }

  @Override
  protected void validatePrivileges(Catalog catalog, NamespaceKey path, String identityName) throws Exception {
    // User has Admin or ALL privileges. Or User should have any DML privileges: i.e., INSERT, DELETE, UPDATE.
    // Overall, only needs to check whether a user has one of three DML privilege, as if user has Admin or ALL privileges,
    // this user automatically has any of those three DML privileges.
    if (!catalog.hasPrivilege(path, SqlGrant.Privilege.INSERT)
      && !catalog.hasPrivilege(path, SqlGrant.Privilege.DELETE)
      && !catalog.hasPrivilege(path, SqlGrant.Privilege.UPDATE) ) {
      throw UserException.permissionError()
        .message(String.format("Identity [%s] not authorized to ROLLBACK [%s].", identityName, path))
        .buildSilently();
    }
  }

  @Override
  protected void validateFeatureEnabled(SqlHandlerConfig config) {
    if (!config.getContext().getOptions().getOption(ExecConstants.ENABLE_ICEBERG_ROLLBACK)) {
      throw UserException.unsupportedError().message("ROLLBACK TABLE command is not supported.").buildSilently();
    }
  }

  @Override
  protected List<SimpleCommandResult> getCommandResult(NamespaceKey path) {
    return Collections.singletonList(SimpleCommandResult.successful("Table [%s] rollbacked", path));
  }

  @Override
  protected void execute(Catalog catalog,
                         SqlNode sqlNode,
                         NamespaceKey path,
                         DatasetConfig datasetConfig,
                         TableMutationOptions tableMutationOptions) throws Exception {
    final SqlRollbackTable rollbackTable = SqlNodeUtil.unwrap(sqlNode, SqlRollbackTable.class);
    catalog.rollbackTable(path, datasetConfig, rollbackTable.getRollbackOption(), tableMutationOptions);
  }
}
