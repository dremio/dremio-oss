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
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.physical.PhysicalPlan;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.direct.SqlNodeUtil;
import com.dremio.exec.planner.sql.parser.SqlVacuumCatalog;
import com.dremio.service.namespace.NamespaceKey;

/**
 * Handler for {@link com.dremio.exec.planner.sql.parser.SqlVacuumCatalog} command.
 */
public class VacuumCatalogHandler implements SqlToPlanHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(VacuumCatalogHandler.class);

  @Override
  public PhysicalPlan getPlan(SqlHandlerConfig config, String sql, SqlNode sqlNode) throws Exception {
    final Catalog catalog = config.getContext().getCatalog();
    NamespaceKey path = SqlNodeUtil.unwrap(sqlNode, SqlVacuumCatalog.class).getPath();

    validate(catalog, config, path);

    throw new NotImplementedException();
  }

  private void validate(Catalog catalog, SqlHandlerConfig config, NamespaceKey path) {
    validateFeatureEnabled(config);
    validatePath(path);
    validateCompatibleCatalog(catalog, path);
  }

  private void validateFeatureEnabled(SqlHandlerConfig config) {
    if (!config.getContext().getOptions().getOption(ExecConstants.ENABLE_ICEBERG_VACUUM_CATALOG)) {
      throw UserException.unsupportedError().message("VACUUM CATALOG command is not supported.").buildSilently();
    }
  }

  private void validatePath(NamespaceKey path) {
    if (path.size() != 1) {
      throw UserException.parseError()
        .message("Catalog name cannot have multiple path components.")
        .buildSilently();
    }
  }

  private void validateCompatibleCatalog(Catalog catalog, NamespaceKey path) {
    if (!CatalogUtil.requestedPluginSupportsVersionedTables(path, catalog)) {
      throw UserException.unsupportedError()
        .message("VACUUM CATALOG is supported only on versioned sources.")
        .buildSilently();
    }
  }

  @Override
  public String getTextPlan() {
    throw new NotImplementedException();
  }

  @Override
  public Rel getLogicalPlan() {
    throw new NotImplementedException();
  }
}
