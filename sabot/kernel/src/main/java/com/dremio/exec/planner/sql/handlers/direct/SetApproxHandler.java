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

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.planner.sql.parser.SqlSetApprox;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import java.util.List;
import org.apache.calcite.schema.Schema.TableType;
import org.apache.calcite.sql.SqlNode;

/** Handler for <code>ALTER TABLE ... ENABLE|DISABLE APPROXIMATE STATS</code> command. */
public class SetApproxHandler extends SimpleDirectHandler {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(SetApproxHandler.class);

  private final Catalog catalog;

  public SetApproxHandler(Catalog catalog) {
    this.catalog = catalog;
  }

  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode) throws Exception {
    final SqlSetApprox sqlSetApprox = SqlNodeUtil.unwrap(sqlNode, SqlSetApprox.class);
    final NamespaceKey path = catalog.resolveSingle(sqlSetApprox.getPath());

    String root = path.getRoot();
    if ("sys".equalsIgnoreCase(root) || "INFORMATION_SCHEMA".equalsIgnoreCase(root)) {
      throw UserException.parseError()
          .message("System and Information Schema tables cannot be modified: %s", path)
          .build(logger);
    }

    while (true) {
      final DremioTable table = catalog.getTableNoResolve(path);

      if (table.getDatasetConfig().getPhysicalDataset() == null) {
        throw UserException.parseError()
            .message("Unable to approximate stats on virtual dataset %s", path)
            .build(logger);
      }
      if (table == null
          || table.getJdbcTableType() != TableType.TABLE
          || table.getDatasetConfig() == null) {
        throw UserException.parseError().message("Unable to find table %s.", path).build(logger);
      }

      try {
        DatasetConfig config = table.getDatasetConfig();
        config.getPhysicalDataset().setAllowApproxStats(sqlSetApprox.isEnable());
        catalog.addOrUpdateDataset(path, config);

        return singletonList(
            successful(
                String.format("Successfully updated table '%s' from namespace.", table.getPath())));
      } catch (NamespaceNotFoundException ex) {
        throw UserException.parseError(ex)
            .message("Failure modifying table %s.", path)
            .build(logger);
      }
    }
  }
}
