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
import com.dremio.exec.planner.sql.parser.SqlVacuum;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;

/**
 * Handler for vacuuming table.
 */
public class VacuumHandler extends TableManagementDirectHandler {

  public VacuumHandler(Catalog catalog, SqlHandlerConfig config) {
    super(catalog, config);
  }

  @Override
  public NamespaceKey getTablePath(SqlNode sqlNode) throws Exception {
    return SqlNodeUtil.unwrap(sqlNode, SqlVacuum.class).getPath();
  }

  @Override
  protected SqlOperator getSqlOperator() {
    return SqlVacuum.OPERATOR;
  }

  @Override
  protected void validatePrivileges(Catalog catalog, NamespaceKey path, String identityName) throws Exception {
    // User must be admin,owner of the table.
    catalog.validateOwnership(path);
  }

  @Override
  protected List<SimpleCommandResult> getCommandResult(NamespaceKey path) {
    return Collections.singletonList(SimpleCommandResult.successful("Table [%s] vacuumed", path));
  }

  @Override
  protected void execute(Catalog catalog,
                         SqlNode sqlNode,
                         NamespaceKey path,
                         DatasetConfig datasetConfig,
                         TableMutationOptions tableMutationOptions) throws Exception {
    final SqlVacuum sqlVacuum = SqlNodeUtil.unwrap(sqlNode, SqlVacuum.class);
    catalog.vacuumTable(path, datasetConfig, sqlVacuum.getVacuumOption(), tableMutationOptions);
  }

  @Override
  protected void validateFeatureEnabled(SqlHandlerConfig config) {
    if (!config.getContext().getOptions().getOption(ExecConstants.ENABLE_ICEBERG_VACUUM)) {
      throw UserException.unsupportedError().message("VACUUM command is not supported.").buildSilently();
    }
  }
}
