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

import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.catalog.model.VersionContext;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.planner.sql.handlers.direct.SqlNodeUtil;
import com.dremio.exec.planner.sql.parser.DmlUtils;
import com.dremio.exec.planner.sql.parser.SqlDeleteFromTable;
import com.dremio.exec.planner.sql.parser.SqlGrant.Privilege;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.TableProperties;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.iceberg.RowLevelOperationMode;

/** Handles the DELETE DML. */
public class DeleteHandler extends DmlHandler {

  @Override
  public NamespaceKey getTargetTablePath(SqlNode sqlNode) throws Exception {
    return SqlNodeUtil.unwrap(sqlNode, SqlDeleteFromTable.class).getPath();
  }

  @Override
  protected SqlOperator getSqlOperator() {
    return SqlDeleteFromTable.OPERATOR;
  }

  @Override
  protected void validatePrivileges(
      Catalog catalog, CatalogEntityKey key, SqlNode sqlNode, VersionContext versionContext) {
    NamespaceKey path = key.toNamespaceKey();
    catalog.validatePrivilege(path, Privilege.DELETE);
    catalog.validatePrivilege(path, Privilege.SELECT);
  }

  @Override
  protected RowLevelOperationMode getRowLevelOperationMode(DremioTable table) {
    TableProperties deleteDmlWriteMode =
        DmlUtils.getDmlWriteProp(table, org.apache.iceberg.TableProperties.DELETE_MODE);
    return DmlUtils.getDmlWriteMode(deleteDmlWriteMode);
  }
}
