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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUtil;
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
import com.dremio.exec.planner.sql.parser.SqlVacuumTable;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.annotations.VisibleForTesting;

/**
 * Handler for {@link SqlVacuumTable} command.
 */
public class VacuumTableHandler extends TableManagementHandler {
  private static final Logger logger = LoggerFactory.getLogger(VacuumTableHandler.class);

  private String textPlan;
  private Rel drel;

  @Override
  public NamespaceKey getTargetTablePath(SqlNode sqlNode) throws Exception {
    return SqlNodeUtil.unwrap(sqlNode, SqlVacuumTable.class).getPath();
  }

  @Override
  public SqlOperator getSqlOperator() {
    return SqlVacuumTable.OPERATOR;
  }

  @Override
  protected void validatePrivileges(Catalog catalog, NamespaceKey path, SqlNode sqlNode) throws Exception {
    // User must be admin,owner of the table.
    catalog.validateOwnership(path);
  }

  private void validateFeatureEnabled(SqlHandlerConfig config) {
    if (!config.getContext().getOptions().getOption(ExecConstants.ENABLE_ICEBERG_VACUUM)) {
      throw UserException.unsupportedError().message("VACUUM TABLE command is not supported.").buildSilently();
    }
  }

  @VisibleForTesting
  @Override
  public void checkValidations(Catalog catalog, SqlHandlerConfig config, NamespaceKey path, SqlNode sqlNode) throws Exception {
    validateFeatureEnabled(config);
    validatePrivileges(catalog, path, sqlNode);
    validateTableExistenceAndMutability(catalog, config, path);
  }

  @Override
  protected Rel convertToDrel(SqlHandlerConfig config, SqlNode sqlNode, NamespaceKey path, Catalog catalog, RelNode relNode) throws Exception {
    CreateTableEntry createTableEntry = IcebergUtils.getIcebergCreateTableEntry(config, catalog,
      catalog.getTable(path), getSqlOperator(), null);
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
        //Always use the latest snapshot before vacuum.
        refresh.run();
      } else {
        throw UserException.unsupportedError()
          .message("VACUUM TABLE command is not supported for this source")
          .buildSilently();
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

    drel = convertToDrel(config, sqlNode, path, catalog, relNode);
    final Pair<Prel, String> prelAndTextPlan = PrelTransformer.convertToPrel(config, drel);
    textPlan = prelAndTextPlan.getValue();
    return prelAndTextPlan.getKey();
  }

  @Override
  public String getTextPlan() {
    return textPlan;
  }

  @Override
  public Rel getLogicalPlan() {
    return drel;
  }
}
