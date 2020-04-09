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
package com.dremio.exec.store.iceberg;

import java.util.Optional;

import org.apache.calcite.schema.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.hadoop.HadoopTableOperations;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.direct.SimpleCommandResult;
import com.dremio.exec.planner.sql.handlers.query.DataAdditionCmdHandler;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.NamespaceKey;

public class IcebergTableOperations extends HadoopTableOperations {
  public IcebergTableOperations(Path location, Configuration conf) {
    super(location, conf);
  }

  /**
   * checks:
   * 1. if iceberg feature is enabled
   * 2. table exists
   * 3. plugin support for iceberg tables
   * 4. table is an iceberg dataset
   * May return non-empty optional if ifExistsCheck is true
   * @param catalog
   * @param config
   * @param path
   * @param ifExistsCheck
   * @return
   */
  public static Optional<SimpleCommandResult> checkTableExistenceAndMutability(Catalog catalog, SqlHandlerConfig config,
                                                                                     NamespaceKey path, boolean ifExistsCheck) {
    boolean icebergFeatureEnabled = DataAdditionCmdHandler.isIcebergFeatureEnabled(config.getContext().getOptions(),
        null);
    if (!icebergFeatureEnabled) {
      throw UserException.unsupportedError()
          .message("Please contact customer support for steps to enable " +
              "the iceberg tables feature.")
          .buildSilently();
    }

    DremioTable table = catalog.getTableNoResolve(path);
    if (table == null) {
      if (ifExistsCheck) {
        return Optional.of(SimpleCommandResult.successful("Table [%s] does not exist.", path));
      } else {
        throw UserException.validationError()
            .message("Table [%s] not found", path)
            .buildSilently();
      }
    }

    if (table.getJdbcTableType() != Schema.TableType.TABLE) {
      throw UserException.validationError()
          .message("[%s] is a %s", path, table.getJdbcTableType())
          .buildSilently();
    }

    if (!DataAdditionCmdHandler.validatePluginSupportForIceberg(catalog, path)) {
      throw UserException.unsupportedError()
          .message("Source [%s] does not support DML operations", path.getRoot())
          .buildSilently();
    }

    try {
      if (!DatasetHelper.isIcebergDataset(table.getDatasetConfig())) {
        throw UserException.unsupportedError()
            .message("Table [%s] is not configured to support DML operations", path)
            .buildSilently();
      }
    } catch (NullPointerException ex) {
      if (ifExistsCheck) {
        return Optional.of(SimpleCommandResult.successful("Table [%s] does not exist.", path));
      } else {
        throw UserException.validationError()
            .message("Table [%s] not found", path)
            .buildSilently();
      }
    }
    return Optional.empty();
  }
}