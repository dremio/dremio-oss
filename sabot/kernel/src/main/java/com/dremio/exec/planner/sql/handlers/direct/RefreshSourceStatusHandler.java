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

import java.util.List;

import org.apache.calcite.sql.SqlNode;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.SourceCatalog;
import com.dremio.exec.planner.sql.parser.SqlGrant;
import com.dremio.exec.planner.sql.parser.SqlRefreshSourceStatus;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.SourceState;

/**
 * Handler for <code>SOURCE REFRESH STATUS</code> command.
 */
public class RefreshSourceStatusHandler extends SimpleDirectHandler {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ForgetTableHandler.class);

  private final SourceCatalog sourceCatalog;

  public RefreshSourceStatusHandler(SourceCatalog sourceCatalog) {
    this.sourceCatalog = sourceCatalog;
  }

  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode) throws Exception {
    final SqlRefreshSourceStatus sqlRefreshSourceStatus = SqlNodeUtil.unwrap(sqlNode, SqlRefreshSourceStatus.class);
    final NamespaceKey path = sqlRefreshSourceStatus.getPath();
    sourceCatalog.validatePrivilege(path, SqlGrant.Privilege.ALTER);

    String root = path.getRoot();
    if(root.startsWith("@") || root.equalsIgnoreCase("sys") || root.equalsIgnoreCase("INFORMATION_SCHEMA")) {
      throw UserException.parseError().message("Unable to find source %s.", path).build(logger);
    }

    final String source = sqlRefreshSourceStatus.getSource().toString();

    SourceState sourceState = sourceCatalog.refreshSourceStatus(path);
    SourceState.SourceStatus sourceStatus = sourceState.getStatus();

    final String state = " New status is: " + sourceState.toString();
    String message;
    switch(sourceStatus){
      case good:
        message = "Successfully refreshed status for source '%s'.";
        break;
      case bad:
        message = "Failed to refresh status for source '%s'.";
        break;
      case warn:
        message = "Successfully refreshed status for source '%s'.";
        break;
      default:
        throw new IllegalStateException();
    }
    message += state;
    return singletonList(successful(String.format(message, source)));
  }
}
