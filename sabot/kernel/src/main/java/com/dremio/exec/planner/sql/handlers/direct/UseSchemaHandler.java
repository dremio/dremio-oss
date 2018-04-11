/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import java.util.Collections;
import java.util.List;

import org.apache.calcite.sql.SqlNode;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.SqlUtils;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.planner.sql.parser.SqlUseSchema;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.namespace.NamespaceKey;

public class UseSchemaHandler extends SimpleDirectHandler {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UseSchemaHandler.class);

  private final UserSession session;
  private final Catalog catalog;

  public UseSchemaHandler(UserSession session, Catalog catalog) {
    this.session = session;
    this.catalog = catalog;
  }

  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode) throws Exception {
    final SqlUseSchema useSchema = SqlNodeUtil.unwrap(sqlNode, SqlUseSchema.class);
    // first we try locally.
    NamespaceKey orig = new NamespaceKey(useSchema.getSchema());
    NamespaceKey defaultPath = catalog.resolveToDefault(orig);
    NamespaceKey compoundPath = orig.size() == 1 && orig.getRoot().contains(".") ? new NamespaceKey(SqlUtils.parseSchemaPath(orig.getRoot())) : null;

    if(defaultPath != null && catalog.containerExists(defaultPath)) {
      session.setDefaultSchemaPath(defaultPath.getPathComponents(), catalog);
    } else if (catalog.containerExists(orig)) {
      session.setDefaultSchemaPath(orig.getPathComponents(), catalog);
    } else if(compoundPath != null && catalog.containerExists(compoundPath)) {
      // kept to support old compound use statements.
      session.setDefaultSchemaPath(compoundPath.getPathComponents(), catalog);
    } else {
      throw UserException.validationError().message("Schema [%s] is not valid with respect to either root schema or current default schema.", orig).build(logger);
    }

    return Collections.singletonList(SimpleCommandResult.successful("Default schema changed to [%s]", session.getDefaultSchemaPath()));
  }
}
