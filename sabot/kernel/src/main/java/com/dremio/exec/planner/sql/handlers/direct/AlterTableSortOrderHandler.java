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

import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.catalog.model.VersionContext;
import com.dremio.common.exceptions.UserRemoteException;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.TableMutationOptions;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.sql.SqlValidatorImpl;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.SqlHandlerUtil;
import com.dremio.exec.planner.sql.handlers.query.DataAdditionCmdHandler;
import com.dremio.exec.planner.sql.parser.SqlAlterTableSortOrder;
import com.dremio.exec.planner.sql.parser.SqlGrant;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.options.OptionManager;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.sql.SqlNode;

public class AlterTableSortOrderHandler extends SimpleDirectHandler {
  private final Catalog catalog;
  private final SqlHandlerConfig config;

  public AlterTableSortOrderHandler(Catalog catalog, SqlHandlerConfig config) {
    this.catalog = catalog;
    this.config = config;
  }

  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode) throws Exception {
    final SqlAlterTableSortOrder sqlAlterTableSortOrder =
        SqlNodeUtil.unwrap(sqlNode, SqlAlterTableSortOrder.class);
    QueryContext context = Preconditions.checkNotNull(config.getContext());
    OptionManager optionManager = Preconditions.checkNotNull(context.getOptions());
    SqlValidatorImpl.checkForFeatureSpecificSyntax(sqlNode, optionManager);
    NamespaceKey path =
        CatalogUtil.getResolvePathForTableManagement(catalog, sqlAlterTableSortOrder.getTable());

    IcebergUtils.validateIcebergLocalSortIfDeclared(sql, context.getOptions());

    catalog.validatePrivilege(path, SqlGrant.Privilege.ALTER);

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

    List<String> sortOrderColumns = sqlAlterTableSortOrder.getSortList();
    Set<String> fieldSet =
        table.getSchema().getFields().stream().map(Field::getName).collect(Collectors.toSet());

    for (String col : sortOrderColumns) {
      if (!fieldSet.contains(col)) {
        throw UserRemoteException.validationError()
            .message(String.format("Column '%s' does not exist in the table.", col))
            .buildSilently();
      }
    }
    catalog.alterSortOrder(
        path, table.getDatasetConfig(), table.getSchema(), sortOrderColumns, tableMutationOptions);

    DataAdditionCmdHandler.refreshDataset(catalog, path, false);
    String message = String.format("Sort order on table %s successfully updated", path);
    return Collections.singletonList(SimpleCommandResult.successful(message));
  }
}
