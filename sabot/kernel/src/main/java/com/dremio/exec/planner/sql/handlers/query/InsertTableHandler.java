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

import org.apache.calcite.sql.SqlNode;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.physical.PhysicalPlan;
import com.dremio.exec.planner.sql.SqlExceptionHelper;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.direct.SqlNodeUtil;
import com.dremio.exec.planner.sql.parser.SqlInsertTable;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.annotations.VisibleForTesting;

public class InsertTableHandler extends DataAdditionCmdHandler {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(InsertTableHandler.class);

  @Override
  public PhysicalPlan getPlan(SqlHandlerConfig config, String sql, SqlNode sqlNode) throws Exception {
    try {
      final SqlInsertTable sqlInsertTable = SqlNodeUtil.unwrap(sqlNode, SqlInsertTable.class);
      final Catalog catalog = config.getContext().getCatalog();
      final NamespaceKey path = catalog.resolveSingle(sqlInsertTable.getPath());

      // TODO: fix parser to disallow this
      if (sqlInsertTable.isSingleWriter() && !sqlInsertTable.getPartitionColumns(catalog, path).isEmpty()) {
        throw UserException.unsupportedError()
          .message("Cannot partition data and write to a single file at the same time.")
          .build(logger);
      }

      validateInsertTableFormatOptions(catalog, config, path);
      PhysicalPlan plan = super.getPlan(catalog, path, config, sql, sqlNode, sqlInsertTable);

      super.validateIcebergSchemaForInsertCommand(sqlInsertTable.getFieldNames());
      return plan;
    }
    catch(Exception ex){
      throw SqlExceptionHelper.coerceException(logger, sql, ex, true);
    }
  }

  @VisibleForTesting
  public void validateInsertTableFormatOptions(Catalog catalog, SqlHandlerConfig config, NamespaceKey path) {
    validateTableFormatOptions(catalog, path, config.getContext().getOptions());
    IcebergUtils.checkTableExistenceAndMutability(catalog, config, path, false);
  }

  @Override
  public boolean isCreate() {
    return false;
  }
}
