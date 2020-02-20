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

import static com.dremio.exec.planner.sql.handlers.direct.SimpleCommandResult.successful;
import static java.util.Collections.singletonList;

import java.util.ConcurrentModificationException;
import java.util.List;

import org.apache.calcite.schema.Schema.TableType;
import org.apache.calcite.sql.SqlNode;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.planner.sql.parser.SqlSetHiveVarcharCompatible;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;

/**
 * Handler for <code>ALTER TABLE ... ENABLE|DISABLE HIVE VARCHAR COMPATIBILITY</code> command.
 */
public class SetHiveVarcharCompatibilityHandler extends SimpleDirectHandler {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SetHiveVarcharCompatibilityHandler.class);

  private final Catalog catalog;
  private final NamespaceService namespaceService;

  public SetHiveVarcharCompatibilityHandler(Catalog catalog, NamespaceService namespaceService) {
    this.catalog = catalog;
    this.namespaceService = namespaceService;
  }

  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode) throws Exception {
    final SqlSetHiveVarcharCompatible sqlSetHiveVarcharCompatible = SqlNodeUtil.unwrap(sqlNode, SqlSetHiveVarcharCompatible.class);
    final NamespaceKey path = catalog.resolveSingle(sqlSetHiveVarcharCompatible.getPath());
    DremioTable table = catalog.getTableNoResolve(path);

    if (table == null) {
      throw UserException.validationError().message("Table [%s] not found", path, sqlNode.getParserPosition()).buildSilently();
    }

    if (table.getJdbcTableType() != TableType.TABLE) {
      throw UserException.validationError().message("[%s] is a %s", path, table.getJdbcTableType())
          .buildSilently();
    }

    StoragePlugin storagePlugin;
    try {
      storagePlugin = catalog.getSource(path.getRoot());
    } catch (UserException uex) {
      throw UserException.validationError().message("Could not get storage plugin for table [%s]", path).buildSilently();
    }

    if (!storagePlugin.getSourceCapabilities().getCapability(SourceCapabilities.VARCHARS_WITH_WIDTH)) {
      throw UserException.validationError().message("source [%s] doesn't support width property for varchars", path.getRoot()).buildSilently();
    }

    try {
      table = catalog.getTableNoResolve(path);
      storagePlugin = catalog.getSource(path.getRoot());
      DatasetConfig config = table.getDatasetConfig();

      boolean isChanged = storagePlugin.updateVarcharCompatibility(config, sqlSetHiveVarcharCompatible.isCompatible());

      if (isChanged) {
        namespaceService.addOrUpdateDataset(path, config);
        // metadata refresh
        Catalog.UpdateStatus updateStatus = catalog.refreshDataset(path, DatasetRetrievalOptions.DEFAULT);
        logger.info("refreshed dataset {} after {} Hive Varchar Compatibility, update status: {}", path,
            sqlSetHiveVarcharCompatible.isCompatible() ? "enabling" : "disabling", updateStatus.name());
        return singletonList(successful(String.format("Hive varchar compatibility is %s for %s",
            sqlSetHiveVarcharCompatible.isCompatible() ? "enabled" : "disabled", table.getPath())));
      }

      return singletonList(successful(String.format("Hive varchar compatibility is already %s for %s",
          sqlSetHiveVarcharCompatible.isCompatible() ? "enabled" : "disabled", table.getPath())));

    } catch (NamespaceNotFoundException ex) {
      throw UserException.parseError(ex).message("Failure modifying table %s.", path).buildSilently();
    } catch (ConcurrentModificationException e) {
      throw UserException.concurrentModificationError(e).buildSilently();
    }
  }
}
