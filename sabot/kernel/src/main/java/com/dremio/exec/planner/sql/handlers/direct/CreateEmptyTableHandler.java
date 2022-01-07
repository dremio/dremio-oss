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

import static com.dremio.service.nessie.NessieConfig.NESSIE_DEFAULT_BRANCH;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.calcite.sql.SqlNode;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.PathUtils;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogOptions;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.catalog.ColumnCountTooLargeException;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.VersionContext;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.SqlHandlerUtil;
import com.dremio.exec.planner.sql.handlers.query.DataAdditionCmdHandler;
import com.dremio.exec.planner.sql.parser.SqlColumnDeclaration;
import com.dremio.exec.planner.sql.parser.SqlCreateEmptyTable;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.VersionedDatasetAccessOptions;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.namespace.NamespaceKey;

public class CreateEmptyTableHandler extends SimpleDirectHandler {
  private static org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CreateEmptyTableHandler.class);
  private final Catalog catalog;
  private final SqlHandlerConfig config;
  private final UserSession userSession;

  public CreateEmptyTableHandler(Catalog catalog, SqlHandlerConfig config, UserSession userSession) {
    this.catalog = catalog;
    this.config = config;
    this.userSession = userSession;
  }

  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode) throws Exception {
    SqlCreateEmptyTable sqlCreateEmptyTable = SqlNodeUtil.unwrap(sqlNode, SqlCreateEmptyTable.class);

    NamespaceKey key = catalog.resolveSingle(sqlCreateEmptyTable.getPath());

    if (CatalogUtil.isVersionedDDPEntity(key)) {
      return createVersionedIcebergTable(key, sql, sqlCreateEmptyTable);
    }
    return createIcebergTable(key, sql, sqlCreateEmptyTable);

  }

  private List<SimpleCommandResult> createIcebergTable(NamespaceKey key, String sql, SqlCreateEmptyTable sqlCreateEmptyTable) {

    if (!DataAdditionCmdHandler.isIcebergFeatureEnabled(config.getContext().getOptions(), null)) {
      throw UserException.unsupportedError()
        .message("Please contact customer support for steps to enable " +
          "the iceberg tables feature.")
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

    final long ringCount = config.getContext().getOptions().getOption(PlannerSettings.RING_COUNT);

    final WriterOptions options = new WriterOptions(
      (int) ringCount,
      sqlCreateEmptyTable.getPartitionColumns(catalog, key),
      sqlCreateEmptyTable.getSortColumns(),
      sqlCreateEmptyTable.getDistributionColumns(),
      sqlCreateEmptyTable.getPartitionDistributionStrategy(),
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

    long maxColumnCount = config.getContext().getOptions().getOption(CatalogOptions.METADATA_LEAF_COLUMN_MAX);
    if (columnDeclarations.size() > maxColumnCount) {
      throw new ColumnCountTooLargeException((int) maxColumnCount);
    }

    SqlHandlerUtil.checkForDuplicateColumns(columnDeclarations, BatchSchema.of(), sql);
    catalog.createEmptyTable(key, SqlHandlerUtil.batchSchemaFromSqlSchemaSpec(config, columnDeclarations, sql), options);

    // do a refresh on the dataset to populate the kvstore.
    DataAdditionCmdHandler.refreshDataset(catalog, key, true);

    return Collections.singletonList(SimpleCommandResult.successful("Table created"));
  }


  private List<SimpleCommandResult> createVersionedIcebergTable(NamespaceKey key, String sql, SqlCreateEmptyTable sqlCreateEmptyTable) {

    Optional<VersionContext> versionContext = userSession.getVersionContext().isPresent() ?
      userSession.getVersionContext() :
      Optional.of(VersionContext.fromBranchName(NESSIE_DEFAULT_BRANCH));
    key = new NamespaceKey(PathUtils.parseFullPath(CatalogUtil.removeVersionedCatalogPrefix(key.toString())));

    // path is not valid
    if (!DataAdditionCmdHandler.validatePath(this.catalog, key)) {
      throw UserException.unsupportedError()
        .message(String.format("Invalid path. Given path, [%s] is not valid.", key))
        .buildSilently();
    }

    // path is valid but source is not valid
    if (!CatalogUtil.ensurePluginSupportForDDP(this.catalog, key)) {
      throw UserException.unsupportedError()
        .message(String.format("Source [%s] does not support CREATE TABLE. Please use correct source", key.getRoot()))
        .buildSilently();
    }

    final long ringCount = config.getContext().getOptions().getOption(PlannerSettings.RING_COUNT);

    final VersionedDatasetAccessOptions versionedDatasetAccessOptions = new VersionedDatasetAccessOptions.Builder()
      .setVersionedTableKeyPath(key.toString())
      .setVersionContext(versionContext)
      .build();

    final WriterOptions options = new WriterOptions(
      (int) ringCount,
      sqlCreateEmptyTable.getPartitionColumns(catalog, key),
      sqlCreateEmptyTable.getSortColumns(),
      sqlCreateEmptyTable.getDistributionColumns(),
      sqlCreateEmptyTable.getPartitionDistributionStrategy(),
      sqlCreateEmptyTable.isSingleWriter(),
      Long.MAX_VALUE,
      WriterOptions.IcebergWriterOperation.CREATE,
      null,
      versionedDatasetAccessOptions
    );

    List<SqlColumnDeclaration> columnDeclarations = SqlHandlerUtil.columnDeclarationsFromSqlNodes(sqlCreateEmptyTable.getFieldList(), sql);

    long maxColumnCount = config.getContext().getOptions().getOption(CatalogOptions.METADATA_LEAF_COLUMN_MAX);
    if (columnDeclarations.size() > maxColumnCount) {
      throw new ColumnCountTooLargeException((int) maxColumnCount);
    }

    SqlHandlerUtil.checkForDuplicateColumns(columnDeclarations, BatchSchema.of(), sql);
    try {
      catalog.createEmptyTable(key, SqlHandlerUtil.batchSchemaFromSqlSchemaSpec(config, columnDeclarations, sql), options);
    } catch (UserException u) {
      // Since we skipped the table existence check for verison tables, we check if we got a folder clash exception.
      // And then throw a meaningful exception here.
      if (u.getMessage().contains("Folder already exists at path")) {
        throw UserException.validationError()
          .message("A table or view with given name [%s] already exists.", key)
          .buildSilently();
      }
    }

    return Collections.singletonList(SimpleCommandResult.successful("Table created"));
  }

}
