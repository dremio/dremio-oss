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

import org.apache.arrow.util.Preconditions;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlNode;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogOptions;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.catalog.ColumnCountTooLargeException;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.ResolvedVersionContext;
import com.dremio.exec.catalog.VersionContext;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.SqlHandlerUtil;
import com.dremio.exec.planner.sql.handlers.query.DataAdditionCmdHandler;
import com.dremio.exec.planner.sql.parser.SqlColumnDeclaration;
import com.dremio.exec.planner.sql.parser.SqlCreateEmptyTable;
import com.dremio.exec.record.BatchSchema;
import com.dremio.options.OptionManager;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.namespace.NamespaceKey;

public class CreateEmptyTableHandler extends SimpleDirectHandler {
  private static org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CreateEmptyTableHandler.class);

  private final Catalog catalog;
  private final SqlHandlerConfig config;
  private final OptionManager optionManager;
  private final UserSession userSession;

  public CreateEmptyTableHandler(Catalog catalog, SqlHandlerConfig config, UserSession userSession) {
    this.catalog = Preconditions.checkNotNull(catalog);
    this.config = Preconditions.checkNotNull(config);
    QueryContext context = Preconditions.checkNotNull(config.getContext());
    this.optionManager = Preconditions.checkNotNull(context.getOptions());
    this.userSession = Preconditions.checkNotNull(userSession);
  }

  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode) throws Exception {
    SqlCreateEmptyTable sqlCreateEmptyTable = SqlNodeUtil.unwrap(sqlNode, SqlCreateEmptyTable.class);

    NamespaceKey tableKey = catalog.resolveSingle(sqlCreateEmptyTable.getPath());

    if (CatalogUtil.requestedPluginSupportsVersionedTables(tableKey,catalog)) {
      return createVersionedEmptyTable(tableKey, sql, sqlCreateEmptyTable);
    }

    return createEmptyTable(tableKey, sql, sqlCreateEmptyTable);
  }

  private List<SimpleCommandResult> createEmptyTable(NamespaceKey key, String sql, SqlCreateEmptyTable sqlCreateEmptyTable) {

    if (!DataAdditionCmdHandler.isIcebergDMLFeatureEnabled(catalog, key, optionManager, null)) {
      throw UserException.unsupportedError()
        .message("Please contact customer support for steps to enable the iceberg tables feature.")
        .buildSilently();
    }

    // path is not valid
    if (!DataAdditionCmdHandler.validatePath(this.catalog, key)) {
      throw UserException.unsupportedError()
        .message(String.format("Invalid path. Given path, [%s] is not valid.", key))
        .buildSilently();
    }

    // path is valid but source is not valid
    if (!DataAdditionCmdHandler.validatePluginSupportForIceberg(this.catalog, key)) {
      throw UserException.unsupportedError()
        .message(String.format("Source [%s] does not support CREATE TABLE. Please use correct catalog", key.getRoot()))
        .buildSilently();
    }

    // validate if source supports providing table location
    DataAdditionCmdHandler.validateCreateTableLocation(this.catalog, key, sqlCreateEmptyTable);

    final long ringCount = optionManager.getOption(PlannerSettings.RING_COUNT);
    final String tableLocation = (SqlCharStringLiteral) sqlCreateEmptyTable.getLocation() != null ?
      ((SqlCharStringLiteral) sqlCreateEmptyTable.getLocation()).toValue() : null;

    final WriterOptions writerOptions = new WriterOptions(
      (int) ringCount,
      sqlCreateEmptyTable.getPartitionColumns(null),
      sqlCreateEmptyTable.getSortColumns(),
      sqlCreateEmptyTable.getDistributionColumns(),
      sqlCreateEmptyTable.getPartitionDistributionStrategy(),
      tableLocation,
      sqlCreateEmptyTable.isSingleWriter(),
      Long.MAX_VALUE,
      WriterOptions.IcebergWriterOperation.CREATE,
      null
    );

    DremioTable table = catalog.getTableNoResolve(key);
    if (table != null) {
      throw UserException.validationError()
        .message("A table or view with given name [%s] already exists.", key)
        .buildSilently();
    }
    List<SqlColumnDeclaration> columnDeclarations = SqlHandlerUtil.columnDeclarationsFromSqlNodes(sqlCreateEmptyTable.getFieldList(), sql);

    long maxColumnCount = optionManager.getOption(CatalogOptions.METADATA_LEAF_COLUMN_MAX);
    if (columnDeclarations.size() > maxColumnCount) {
      throw new ColumnCountTooLargeException((int) maxColumnCount);
    }

    SqlHandlerUtil.checkForDuplicateColumns(columnDeclarations, BatchSchema.of(), sql);
    catalog.createEmptyTable(key, SqlHandlerUtil.batchSchemaFromSqlSchemaSpec(config, columnDeclarations, sql), writerOptions);

    // do a refresh on the dataset to populate the kvstore.
    DataAdditionCmdHandler.refreshDataset(catalog, key, true);

    return Collections.singletonList(SimpleCommandResult.successful("Table created"));
  }

  private List<SimpleCommandResult> createVersionedEmptyTable(NamespaceKey key, String sql, SqlCreateEmptyTable sqlCreateEmptyTable) {
    // path is not valid
    if (!DataAdditionCmdHandler.validatePath(this.catalog, key)) {
      throw UserException.unsupportedError()
        .message(String.format("Invalid path. Given path, [%s] is not valid.", key))
        .buildSilently();
    }

    final long ringCount = optionManager.getOption(PlannerSettings.RING_COUNT);

    final String sourceName = key.getRoot();
    final VersionContext sessionVersion = userSession.getSessionVersionForSource(sourceName);
    final ResolvedVersionContext version = CatalogUtil.resolveVersionContext(catalog, sourceName, sessionVersion);
    CatalogUtil.validateResolvedVersionIsBranch(version, key.toString());
    final WriterOptions writerOptions = new WriterOptions(
      (int) ringCount,
      sqlCreateEmptyTable.getPartitionColumns(null),
      sqlCreateEmptyTable.getSortColumns(),
      sqlCreateEmptyTable.getDistributionColumns(),
      sqlCreateEmptyTable.getPartitionDistributionStrategy(),
      sqlCreateEmptyTable.isSingleWriter(),
      Long.MAX_VALUE,
      WriterOptions.IcebergWriterOperation.CREATE,
      null,
      version
    );

    List<SqlColumnDeclaration> columnDeclarations = SqlHandlerUtil.columnDeclarationsFromSqlNodes(sqlCreateEmptyTable.getFieldList(), sql);

    long maxColumnCount = optionManager.getOption(CatalogOptions.METADATA_LEAF_COLUMN_MAX);
    if (columnDeclarations.size() > maxColumnCount) {
      throw new ColumnCountTooLargeException((int) maxColumnCount);
    }

    SqlHandlerUtil.checkForDuplicateColumns(columnDeclarations, BatchSchema.of(), sql);
    try {
      catalog.createEmptyTable(key, SqlHandlerUtil.batchSchemaFromSqlSchemaSpec(config, columnDeclarations, sql), writerOptions);
    } catch (UserException ux) {
      throw ux;
    } catch (Exception ex) {
      throw UserException.validationError(ex)
        .message("Error while trying to create table [%s]", key)
        .buildSilently();
    }

    return Collections.singletonList(SimpleCommandResult.successful("Table created"));
  }
}
