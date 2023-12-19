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


import static com.dremio.exec.planner.sql.handlers.query.DataAdditionCmdHandler.refreshDataset;

import java.util.Collections;
import java.util.List;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;

import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.catalog.model.VersionContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.TableMutationOptions;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.parser.SqlRollbackTable;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;

public class RollbackHandler extends SimpleDirectHandler {
  private final Catalog catalog;
  private final SqlHandlerConfig config;

  public RollbackHandler(Catalog catalog, SqlHandlerConfig config) {
    this.catalog = catalog;
    this.config = config;
  }

  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode) throws Exception {
    final NamespaceKey path = CatalogUtil.getResolvePathForTableManagement(catalog, getTablePath (sqlNode));
    CatalogEntityKey key = CatalogEntityKey.fromNamespaceKey(path);
    validateCommand(catalog, config, key);
    DremioTable table = catalog.getTable(path);

    final String sourceName = path.getRoot();
    final VersionContext sessionVersion = config.getContext().getSession().getSessionVersionForSource(sourceName);
    ResolvedVersionContext resolvedVersionContext = CatalogUtil.resolveVersionContext(catalog, sourceName, sessionVersion);
    CatalogUtil.validateResolvedVersionIsBranch(resolvedVersionContext);
    TableMutationOptions tableMutationOptions = TableMutationOptions.newBuilder()
      .setResolvedVersionContext(resolvedVersionContext)
      .build();

    execute(catalog, sqlNode, path, table.getDatasetConfig(), tableMutationOptions);

    // Table is modified and invalidate the cached plan that refers this table.
    String datasetId = table.getDataset().getDatasetConfig().getId().getId();
    config.getContext().getPlanCache().invalidateCacheOnDataset(datasetId);

    // Refresh the dataset to update table's metadata.
    refreshDataset(catalog, path, false);

    return getCommandResult(path);
  }

  protected void validateCommand(Catalog catalog, SqlHandlerConfig config, CatalogEntityKey key) throws Exception {
    validateFeatureEnabled(config);
    IcebergUtils.checkTableExistenceAndMutability(catalog, config, key, getSqlOperator(), true);
  }

  private NamespaceKey getTablePath(SqlNode sqlNode) throws Exception {
    return SqlNodeUtil.unwrap(sqlNode, SqlRollbackTable.class).getPath();
  }

  private SqlOperator getSqlOperator() {
    return SqlRollbackTable.OPERATOR;
  }

  private void validateFeatureEnabled(SqlHandlerConfig config) {
    if (!config.getContext().getOptions().getOption(ExecConstants.ENABLE_ICEBERG_ROLLBACK)) {
      throw UserException.unsupportedError().message("ROLLBACK TABLE command is not supported.").buildSilently();
    }
  }

  private List<SimpleCommandResult> getCommandResult(NamespaceKey path) {
    return Collections.singletonList(SimpleCommandResult.successful("Table [%s] rollbacked", path));
  }

  private void execute(Catalog catalog,
                         SqlNode sqlNode,
                         NamespaceKey path,
                         DatasetConfig datasetConfig,
                         TableMutationOptions tableMutationOptions) throws Exception {
    final SqlRollbackTable rollbackTable = SqlNodeUtil.unwrap(sqlNode, SqlRollbackTable.class);
    catalog.rollbackTable(path, datasetConfig, rollbackTable.getRollbackOption(), tableMutationOptions);
  }
}
