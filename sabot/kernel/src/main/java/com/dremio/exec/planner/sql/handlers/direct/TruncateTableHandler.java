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
import java.util.Optional;

import org.apache.calcite.sql.SqlNode;

import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.catalog.ResolvedVersionContext;
import com.dremio.exec.catalog.TableMutationOptions;
import com.dremio.exec.catalog.VersionContext;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.query.DataAdditionCmdHandler;
import com.dremio.exec.planner.sql.parser.SqlTruncateTable;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.service.namespace.NamespaceKey;

public class TruncateTableHandler extends SimpleDirectHandler {

  private final Catalog catalog;
  private final SqlHandlerConfig config;

  public TruncateTableHandler(SqlHandlerConfig config) {
    this.catalog = config.getContext().getCatalog();
    this.config = config;
  }

  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode) throws Exception {
    SqlTruncateTable truncateTableNode = SqlNodeUtil.unwrap(sqlNode, SqlTruncateTable.class);
    NamespaceKey path = catalog.resolveSingle(truncateTableNode.getPath());

    Optional<SimpleCommandResult> result = IcebergUtils.checkTableExistenceAndMutability(catalog,
      config, path, truncateTableNode.checkTableExistence());
    if(result.isPresent()) {
      return Collections.singletonList(result.get());
    }

    final String sourceName = path.getRoot();
    final VersionContext sessionVersion = config.getContext().getSession().getSessionVersionForSource(sourceName);
    ResolvedVersionContext resolvedVersionContext = CatalogUtil.resolveVersionContext(catalog, sourceName, sessionVersion);
    CatalogUtil.validateResolvedVersionIsBranch(resolvedVersionContext, path.toString());
    TableMutationOptions tableMutationOptions = TableMutationOptions.newBuilder()
      .setResolvedVersionContext(resolvedVersionContext)
      .build();
    catalog.truncateTable(path, tableMutationOptions);
    if( !(CatalogUtil.requestedPluginSupportsVersionedTables(path, catalog))) {
      DataAdditionCmdHandler.refreshDataset(catalog, path, false);
    }

    return Collections.singletonList(SimpleCommandResult.successful("Table [%s] truncated", path));
  }
}
