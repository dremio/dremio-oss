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

import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.SqlUtils;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.planner.sql.parser.SqlUseSchema;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.namespace.NamespaceKey;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.sql.SqlNode;

public class UseSchemaHandler extends SimpleDirectHandler {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(UseSchemaHandler.class);

  private final UserSession session;
  private final Catalog catalog;

  public UseSchemaHandler(UserSession session, Catalog catalog) {
    this.session = session;
    this.catalog = catalog;
  }

  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode) throws Exception {
    final SqlUseSchema useSchema = SqlNodeUtil.unwrap(sqlNode, SqlUseSchema.class);

    if (useSchema.getSchema() == null) {
      session.setDefaultSchemaPath(null);
      return Collections.singletonList(SimpleCommandResult.successful("Default schema cleared"));
    }

    // first we try locally.
    NamespaceKey orig = new NamespaceKey(useSchema.getSchema().names);
    NamespaceKey defaultPath = catalog.resolveToDefault(orig);
    NamespaceKey compoundPath =
        orig.size() == 1 && orig.getRoot().contains(".")
            ? new NamespaceKey(SqlUtils.parseSchemaPath(orig.getRoot()))
            : null;
    if ((defaultPath != null)
        && (catalog.containerExists(
            CatalogEntityKey.newBuilder()
                .keyComponents(defaultPath.getPathComponents())
                .tableVersionContext(
                    TableVersionContext.of(
                        session.getSessionVersionForSource(defaultPath.getRoot())))
                .build()))) {
      session.setDefaultSchemaPath(defaultPath.getPathComponents());
    } else if (catalog.containerExists(
        CatalogEntityKey.newBuilder()
            .keyComponents(orig.getPathComponents())
            .tableVersionContext(
                TableVersionContext.of(session.getSessionVersionForSource(orig.getRoot())))
            .build())) {
      session.setDefaultSchemaPath(orig.getPathComponents());
    } else if (compoundPath != null) {
      if (catalog.containerExists(
          CatalogEntityKey.newBuilder()
              .keyComponents(compoundPath.getPathComponents())
              .tableVersionContext(
                  TableVersionContext.of(
                      session.getSessionVersionForSource(compoundPath.getRoot())))
              .build())) {
        // kept to support old compound use statements.
        session.setDefaultSchemaPath(compoundPath.getPathComponents());
      }
    } else {
      throw UserException.validationError()
          .message(
              "Schema [%s] is not valid with respect to either root schema or current default schema.",
              orig)
          .build(logger);
    }

    return Collections.singletonList(
        SimpleCommandResult.successful(
            "Default schema changed to [%s]", session.getDefaultSchemaPath()));
  }
}
