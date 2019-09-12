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
import com.dremio.exec.planner.sql.parser.SqlForgetTable;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.NamespaceService;

/**
 * Handler for <code>FORGET TABLE tblname</code> command.
 */
public class ForgetTableHandler extends SimpleDirectHandler {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ForgetTableHandler.class);

  private final static int MAX_RETRIES = 5;
  private final NamespaceService namespaceService;
  private final Catalog catalog;

  public ForgetTableHandler(Catalog catalog, NamespaceService namespaceService) {
    this.catalog = catalog;
    this.namespaceService = namespaceService;
  }

  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode) throws Exception {
    final SqlForgetTable sqlForgetTable = SqlNodeUtil.unwrap(sqlNode, SqlForgetTable.class);
    final NamespaceKey path = catalog.resolveSingle(sqlForgetTable.getPath());

    String root = path.getRoot();
    if(root.startsWith("@") || root.equalsIgnoreCase("sys") || root.equalsIgnoreCase("INFORMATION_SCHEMA")) {
      throw UserException.parseError().message("Unable to find table %s.", path).build(logger);
    }

    int count = 0;
    while(true) {
      final DremioTable table = catalog.getTableNoColumnCount(path);
      if(table == null || table.getJdbcTableType() != TableType.TABLE) {
        throw UserException.parseError().message("Unable to find table %s.", path).build(logger);
      }

      try {
        namespaceService.deleteDataset(table.getPath(), table.getVersion());
        return singletonList(successful(String.format("Successfully removed table '%s' from namespace.", table.getPath())));
      } catch (NamespaceNotFoundException ex) {
        logger.debug("Table to delete not found", ex);
      } catch (ConcurrentModificationException ex) {
        if (count++ < MAX_RETRIES) {
          logger.debug("Concurrent failure.", ex);
        } else {
          throw ex;
        }
      }
    }


  }
}
