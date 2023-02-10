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

import java.util.List;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;

import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.ResolvedVersionContext;
import com.dremio.exec.catalog.TableMutationOptions;
import com.dremio.exec.catalog.VersionContext;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.parser.SqlRollbackTable;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;

public abstract class TableManagementDirectHandler extends SimpleDirectHandler {
  private final Catalog catalog;
  private final SqlHandlerConfig config;

  public TableManagementDirectHandler(Catalog catalog, SqlHandlerConfig config) {
    this.catalog = catalog;
    this.config = config;
  }

  protected abstract NamespaceKey getTablePath(SqlNode sqlNode) throws Exception;

  protected abstract SqlOperator getSqlOperator();

  protected abstract void validateFeatureEnabled(SqlHandlerConfig config);

  protected abstract void validatePrivileges(Catalog catalog, NamespaceKey path, String identityName) throws Exception;

  protected abstract List<SimpleCommandResult> getCommandResult(NamespaceKey path);

  protected abstract void execute(
    Catalog catalog,
    SqlNode sqlNode,
    NamespaceKey path,
    DatasetConfig datasetConfig,
    TableMutationOptions tableMutationOptions) throws Exception;

  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode) throws Exception {
    final NamespaceKey path = catalog.resolveSingle(getTablePath (sqlNode));
    checkValidations(catalog, config, path);
    DremioTable table = catalog.getTable(path);

    final String sourceName = path.getRoot();
    final VersionContext sessionVersion = config.getContext().getSession().getSessionVersionForSource(sourceName);
    ResolvedVersionContext resolvedVersionContext = CatalogUtil.resolveVersionContext(catalog, sourceName, sessionVersion);
    CatalogUtil.validateResolvedVersionIsBranch(resolvedVersionContext, path.toString());
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

  protected void checkValidations(Catalog catalog, SqlHandlerConfig config, NamespaceKey path) throws Exception {
    validateFeatureEnabled(config);
    validatePrivileges(catalog, path, config.getContext().getQueryUserName());
    IcebergUtils.checkTableExistenceAndMutability(catalog, config, path, SqlRollbackTable.OPERATOR, false);
  }
}
