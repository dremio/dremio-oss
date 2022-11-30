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

import static com.dremio.exec.store.iceberg.IcebergSerDe.serializedSchemaAsJson;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.List;

import org.apache.calcite.sql.SqlNode;
import org.apache.iceberg.PartitionSpec;

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
import com.dremio.exec.planner.sql.SqlValidatorImpl;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.SqlHandlerUtil;
import com.dremio.exec.planner.sql.handlers.query.DataAdditionCmdHandler;
import com.dremio.exec.planner.sql.parser.SqlColumnDeclaration;
import com.dremio.exec.planner.sql.parser.SqlCreateEmptyTable;
import com.dremio.exec.planner.sql.parser.SqlGrant.Privilege;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.exec.store.iceberg.IcebergSerDe;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.exec.store.iceberg.SchemaConverter;
import com.dremio.options.OptionManager;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import io.protostuff.ByteString;

public class CreateEmptyTableHandler extends SimpleDirectHandler {
  private static org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CreateEmptyTableHandler.class);

  private final Catalog catalog;
  private final SqlHandlerConfig config;
  private final OptionManager optionManager;
  private final UserSession userSession;
  private final boolean ifNotExists;

  public CreateEmptyTableHandler(Catalog catalog, SqlHandlerConfig config, UserSession userSession, boolean ifNotExists) {
    this.catalog = Preconditions.checkNotNull(catalog);
    this.config = Preconditions.checkNotNull(config);
    QueryContext context = Preconditions.checkNotNull(config.getContext());
    this.optionManager = Preconditions.checkNotNull(context.getOptions());
    this.userSession = Preconditions.checkNotNull(userSession);
    this.ifNotExists = ifNotExists;
  }

  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode) throws Exception {
    SqlCreateEmptyTable sqlCreateEmptyTable = SqlNodeUtil.unwrap(sqlNode, SqlCreateEmptyTable.class);

    NamespaceKey tableKey = catalog.resolveSingle(sqlCreateEmptyTable.getPath());
    catalog.validatePrivilege(tableKey, Privilege.CREATE_TABLE);

    List<SimpleCommandResult> result = CatalogUtil.requestedPluginSupportsVersionedTables(tableKey,catalog) ?
      createVersionedEmptyTable(tableKey, sql, sqlCreateEmptyTable) :
      createEmptyTable(tableKey, sql, sqlCreateEmptyTable);
    return handlePolicy(result, tableKey, sql, sqlCreateEmptyTable);
  }

  protected List<SimpleCommandResult> handlePolicy(
    List<SimpleCommandResult> createTableResult,
    NamespaceKey key,
    String sql,
    SqlCreateEmptyTable sqlCreateEmptyTable) throws Exception {
    return createTableResult;
  }

  protected SqlHandlerConfig getConfig() {
    return config;
  }

  protected Catalog getCatalog() {
    return catalog;
  }

  protected boolean isPolicyAllowed() {
    return false;
  }

  @VisibleForTesting
  public void callCatalogCreateEmptyTableWithCleanup(NamespaceKey key, BatchSchema batchSchema, WriterOptions options) {
    try {
      catalog.createEmptyTable(key, batchSchema, options);
    } catch (Exception ex) {
      cleanUpFromCatalogAndMetaStore(key);
      throw UserException.validationError(ex)
        .message(ex.getMessage())
        .buildSilently();
    }
  }

  protected List<SimpleCommandResult> createEmptyTable(NamespaceKey key, String sql, SqlCreateEmptyTable sqlCreateEmptyTable) throws Exception{
    SqlValidatorImpl.checkForFeatureSpecificSyntax(sqlCreateEmptyTable, optionManager);

    if (!IcebergUtils.isIcebergDMLFeatureEnabled(catalog, key, optionManager, null)) {
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
    if (!IcebergUtils.validatePluginSupportForIceberg(this.catalog, key)) {
      throw UserException.unsupportedError()
        .message(String.format("Source [%s] does not support CREATE TABLE. Please use correct catalog", key.getRoot()))
        .buildSilently();
    }

    // row access policy is not allowed
    if (!isPolicyAllowed() && sqlCreateEmptyTable.getPolicy() != null) {
      throw UserException.unsupportedError()
        .message("This Dremio edition doesn't support ADD ROW ACCESS POLICY")
        .buildSilently();
    }

    // validate if source supports providing table location
    DataAdditionCmdHandler.validateCreateTableLocation(this.catalog, key, sqlCreateEmptyTable);

    final long ringCount = optionManager.getOption(PlannerSettings.RING_COUNT);

    List<SqlColumnDeclaration> columnDeclarations = SqlHandlerUtil.columnDeclarationsFromSqlNodes(sqlCreateEmptyTable.getFieldList(), sql);

    if (!isPolicyAllowed() && columnDeclarations.stream().anyMatch(col -> col.getPolicy() != null)) {
      throw UserException.unsupportedError()
        .message("This Dremio edition doesn't support SET COLUMN MASKING")
        .buildSilently();
    }

    SqlHandlerUtil.checkForDuplicateColumns(columnDeclarations, BatchSchema.of(), sql);
    BatchSchema batchSchema = SqlHandlerUtil.batchSchemaFromSqlSchemaSpec(config, columnDeclarations, sql);
    PartitionSpec partitionSpec = IcebergUtils.getIcebergPartitionSpecFromTransforms(batchSchema, sqlCreateEmptyTable.getPartitionTransforms(null), null);
    IcebergTableProps icebergTableProps = new IcebergTableProps(ByteString.copyFrom(IcebergSerDe.serializePartitionSpec(partitionSpec)), serializedSchemaAsJson(SchemaConverter.getBuilder().build().toIcebergSchema(batchSchema)));

    final WriterOptions options = new WriterOptions(
      (int) ringCount,
      sqlCreateEmptyTable.getPartitionColumns(null),
      sqlCreateEmptyTable.getSortColumns(),
      sqlCreateEmptyTable.getDistributionColumns(),
      sqlCreateEmptyTable.getPartitionDistributionStrategy(config, null, null),
      sqlCreateEmptyTable.getLocation(),
      sqlCreateEmptyTable.isSingleWriter(),
      Long.MAX_VALUE,
      WriterOptions.IcebergWriterOperation.CREATE,
      null,
      icebergTableProps
    );

    DremioTable table = catalog.getTableNoResolve(key);
    if (table != null) {
      if(ifNotExists){
        return Collections.singletonList(new SimpleCommandResult(true, String.format("Table [%s] already exists.", key)));
      }
      else {
        throw UserException.validationError()
          .message("A table or view with given name [%s] already exists.", key)
          .buildSilently();
      }
    }

    long maxColumnCount = optionManager.getOption(CatalogOptions.METADATA_LEAF_COLUMN_MAX);
    if (columnDeclarations.size() > maxColumnCount) {
      throw new ColumnCountTooLargeException((int) maxColumnCount);
    }
    callCatalogCreateEmptyTableWithCleanup(key, batchSchema, options);

      // do a refresh on the dataset to populate the kvstore.
      //Will the orphanage cleanup logic remove files and folders if query fails during refreshDataset() function?
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

    // validate if source supports providing table location
    DataAdditionCmdHandler.validateCreateTableLocation(this.catalog, key, sqlCreateEmptyTable);

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
      sqlCreateEmptyTable.getPartitionDistributionStrategy(config, null, null),
      sqlCreateEmptyTable.getLocation(),
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
    callCatalogCreateEmptyTableWithCleanup(key, SqlHandlerUtil.batchSchemaFromSqlSchemaSpec(config, columnDeclarations, sql), writerOptions);

    return Collections.singletonList(SimpleCommandResult.successful("Table created"));
  }

  public static CreateEmptyTableHandler create(Catalog catalog, SqlHandlerConfig config, UserSession userSession, boolean ifNotExists) {
    try {
      final Class<?> cl = Class.forName("com.dremio.exec.planner.sql.handlers.EnterpriseCreateEmptyTableHandler");
      final Constructor<?> ctor = cl.getConstructor(Catalog.class, SqlHandlerConfig.class, UserSession.class, boolean.class);
      return (CreateEmptyTableHandler) ctor.newInstance(catalog, config, userSession, ifNotExists);
    } catch (ClassNotFoundException e) {
      return new CreateEmptyTableHandler(catalog, config, userSession, ifNotExists);
    } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e2) {
      throw Throwables.propagate(e2);
    }
  }

  private void cleanUpFromCatalogAndMetaStore(NamespaceKey key){
    try {
      if(catalog.getSource(key.getRoot()) instanceof FileSystemPlugin) {
        catalog.forgetTable(key);
      }else{
        catalog.dropTable(key, null);
      }
    }catch(Exception i){
      logger.warn("Failure during removing table from catalog and metastore. " + i.getMessage() );
    }
  }
}
