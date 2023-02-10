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

import static com.dremio.exec.planner.sql.handlers.query.DataAdditionCmdHandler.refreshDataset;

import java.util.List;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.util.Pair;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.calcite.logical.TableOptimizeCrel;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.physical.PhysicalPlan;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.ScreenRel;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.sql.SqlExceptionHelper;
import com.dremio.exec.planner.sql.handlers.ConvertedRelNode;
import com.dremio.exec.planner.sql.handlers.PrelTransformer;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.SqlHandlerUtil;
import com.dremio.exec.planner.sql.handlers.direct.SqlNodeUtil;
import com.dremio.exec.planner.sql.parser.SqlGrant;
import com.dremio.exec.planner.sql.parser.SqlOptimize;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.IcebergMetadata;
import com.google.common.annotations.VisibleForTesting;

/**
 * Handler for {@link SqlOptimize} command.
 */
public class OptimizeHandler extends TableManagementHandler {

  private static final Logger logger = LoggerFactory.getLogger(OptimizeHandler.class);

  private String textPlan;

  @Override
  public NamespaceKey getTargetTablePath(SqlNode sqlNode) throws Exception {
    return SqlNodeUtil.unwrap(sqlNode, SqlOptimize.class).getPath();
  }

  @Override
  public SqlOperator getSqlOperator() {
    return SqlOptimize.OPERATOR;
  }

  @Override
  protected void validatePrivileges(Catalog catalog, NamespaceKey path, SqlNode sqlNode) throws Exception {
    catalog.validatePrivilege(path, SqlGrant.Privilege.SELECT);
    catalog.validatePrivilege(path, SqlGrant.Privilege.UPDATE);
  }

  @VisibleForTesting
  @Override
  void checkValidations(Catalog catalog, SqlHandlerConfig config, NamespaceKey path, SqlNode sqlNode) throws Exception {
    validateOptimizeEnabled(config);
    validatePrivileges(catalog, path, sqlNode);
    validateWhereClause(((SqlOptimize)sqlNode).getCondition());
    validateCompatibleTableFormat(catalog, config, path, getSqlOperator());
  }

  @Override
  protected Rel convertToDrel(SqlHandlerConfig config, SqlNode sqlNode, NamespaceKey path, Catalog catalog, RelNode relNode) throws Exception {

    DremioTable table = catalog.getTable(path);
    List<String> partitionColumnsList = table.getDatasetConfig().getReadDefinition().getPartitionColumnsList();
    OptimizeOptions optimizeOptions = new OptimizeOptions(config.getContext().getOptions(), (SqlOptimize) sqlNode, CollectionUtils.isEmpty(partitionColumnsList));

    CreateTableEntry createTableEntry = IcebergUtils.getIcebergCreateTableEntry(config, catalog,
      table, getSqlOperator().getKind(), optimizeOptions);

    Rel convertedRelNode = PrelTransformer.convertToDrel(config, rewriteCrel(relNode, createTableEntry));
    convertedRelNode = SqlHandlerUtil.storeQueryResultsIfNeeded(config.getConverter().getParserConfig(),
      config.getContext(), convertedRelNode);
    return new ScreenRel(convertedRelNode.getCluster(), convertedRelNode.getTraitSet(), convertedRelNode);
  }

  @Override
  protected PhysicalPlan getPlan(Catalog catalog, SqlHandlerConfig config, String sql, SqlNode sqlNode, NamespaceKey path) throws Exception {
    try {

      Runnable refresh = null;
      if (!CatalogUtil.requestedPluginSupportsVersionedTables(path, catalog)) {
        refresh = () -> refreshDataset(catalog, path, false);
        //Always use the latest snapshot before optimize.
        refresh.run();
      }

      final Prel prel = getNonPhysicalPlan(catalog, config, sqlNode, path);
      final PhysicalOperator pop = PrelTransformer.convertToPop(config, prel);

      return PrelTransformer.convertToPlan(config, pop, refresh, refresh);
    } catch (Exception e) {
      throw SqlExceptionHelper.coerceException(logger, sql, e, true);
    }
  }

  public Prel getNonPhysicalPlan(Catalog catalog, SqlHandlerConfig config, SqlNode sqlNode, NamespaceKey path) throws Exception {
    final ConvertedRelNode convertedRelNode = PrelTransformer.validateAndConvert(config, sqlNode);
    final RelNode relNode = convertedRelNode.getConvertedNode();
    DremioTable table = catalog.getTable(path);
    List<String> partitionColumnsList = table.getDatasetConfig().getReadDefinition().getPartitionColumnsList();

    final RelNode optimizeRelNode = ((TableOptimizeCrel) relNode).createWith(new OptimizeOptions(config.getContext().getOptions(), (SqlOptimize) sqlNode, CollectionUtils.isEmpty(partitionColumnsList)));
    final Rel drel = convertToDrel(config, sqlNode, path, catalog, optimizeRelNode);
    final Pair<Prel, String> prelAndTextPlan = PrelTransformer.convertToPrel(config, drel);
    textPlan = prelAndTextPlan.getValue();
    return prelAndTextPlan.getKey();
  }

  @VisibleForTesting
  void validateWhereClause(SqlNode condition) {
    if (condition != null) {
      throw UserException.unsupportedError().message("OPTIMIZE TABLE does not support WHERE conditions.").buildSilently();
    }
  }

  private void validateOptimizeEnabled(SqlHandlerConfig config) {
    if (!config.getContext().getOptions().getOption(ExecConstants.ENABLE_ICEBERG_OPTIMIZE)) {
      throw UserException.unsupportedError().message("OPTIMIZE TABLE command is not supported.").buildSilently();
    }
  }

  private void validateCompatibleTableFormat(Catalog catalog, SqlHandlerConfig config, NamespaceKey namespaceKey, SqlOperator sqlOperator) {
    // Validate table exists and is Iceberg table
    IcebergUtils.checkTableExistenceAndMutability(catalog, config, namespaceKey, sqlOperator, false);
    // Validate table has no delete files
    IcebergMetadata icebergMetadata = catalog.getTableNoResolve(namespaceKey).getDatasetConfig().getPhysicalDataset().getIcebergMetadata();
    if (icebergMetadata.getDeleteManifestStats().getRecordCount() > 0) {
      throw UserException.unsupportedError().message("OPTIMIZE TABLE command does not support tables with delete files.").buildSilently();
    }
  }

  @Override
  public String getTextPlan() {
    return textPlan;
  }
}
