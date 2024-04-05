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

import static com.dremio.exec.planner.sql.parser.SqlAlterTableProperties.Mode.SET;
import static com.dremio.exec.planner.sql.parser.SqlAlterTableProperties.Mode.UNSET;

import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.catalog.model.VersionContext;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.TableMutationOptions;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.sql.SqlValidatorImpl;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.SqlHandlerUtil;
import com.dremio.exec.planner.sql.handlers.query.DataAdditionCmdHandler;
import com.dremio.exec.planner.sql.parser.SqlAlterTableProperties;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.options.OptionManager;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.calcite.sql.SqlNode;

public class AlterTablePropertiesHandler extends SimpleDirectHandler {
  private final Catalog catalog;
  private final SqlHandlerConfig config;

  public AlterTablePropertiesHandler(Catalog catalog, SqlHandlerConfig config) {
    this.catalog = catalog;
    this.config = config;
  }

  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode) throws Exception {
    SqlAlterTableProperties sqlAlterTableProperties =
        SqlNodeUtil.unwrap(sqlNode, SqlAlterTableProperties.class);
    QueryContext context = Preconditions.checkNotNull(config.getContext());
    OptionManager optionManager = Preconditions.checkNotNull(context.getOptions());
    SqlValidatorImpl.checkForFeatureSpecificSyntax(sqlNode, optionManager);
    SqlAlterTableProperties.Mode mode = sqlAlterTableProperties.getMode();

    IcebergUtils.validateTablePropertiesRequest(optionManager);
    Map<String, String> tableProperties =
        IcebergUtils.convertTableProperties(
            sqlAlterTableProperties.getTablePropertyNameList(),
            sqlAlterTableProperties.getTablePropertyValueList(),
            mode == UNSET);

    NamespaceKey path =
        CatalogUtil.getResolvePathForTableManagement(catalog, sqlAlterTableProperties.getTable());

    DremioTable table = catalog.getTableNoResolve(path);
    SimpleCommandResult result =
        SqlHandlerUtil.validateSupportForDDLOperations(catalog, config, path, table);

    if (!result.ok) {
      return Collections.singletonList(result);
    }

    final String sourceName = path.getRoot();
    final VersionContext sessionVersion =
        config.getContext().getSession().getSessionVersionForSource(sourceName);
    ResolvedVersionContext resolvedVersionContext =
        CatalogUtil.resolveVersionContext(catalog, sourceName, sessionVersion);
    CatalogUtil.validateResolvedVersionIsBranch(resolvedVersionContext);
    TableMutationOptions tableMutationOptions =
        TableMutationOptions.newBuilder().setResolvedVersionContext(resolvedVersionContext).build();
    if (mode == UNSET) {
      catalog.updateTableProperties(
          path,
          table.getDatasetConfig(),
          table.getSchema(),
          tableProperties,
          tableMutationOptions,
          true);
    } else {
      catalog.updateTableProperties(
          path,
          table.getDatasetConfig(),
          table.getSchema(),
          tableProperties,
          tableMutationOptions,
          false);
    }

    DataAdditionCmdHandler.refreshDataset(catalog, path, false);
    String message = "";
    if (mode == SET) {
      for (Map.Entry<String, String> entry : tableProperties.entrySet()) {
        message +=
            String.format(
                "Table Property [%s] set with value [%s]. ", entry.getKey(), entry.getValue());
      }
    } else {
      for (Map.Entry<String, String> entry : tableProperties.entrySet()) {
        message += String.format("Table Property [%s] unset. ", entry.getKey());
      }
    }
    return Collections.singletonList(SimpleCommandResult.successful(message));
  }
}
