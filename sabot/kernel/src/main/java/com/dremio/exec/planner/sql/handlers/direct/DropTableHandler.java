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

import static com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType.VALIDATION;
import static com.google.common.base.Preconditions.checkNotNull;

import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.catalog.model.VersionContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.catalog.TableMutationOptions;
import com.dremio.exec.planner.sql.parser.ReferenceTypeUtils;
import com.dremio.exec.planner.sql.parser.SqlDropTable;
import com.dremio.exec.planner.sql.parser.SqlGrant.Privilege;
import com.dremio.exec.work.foreman.ForemanSetupException;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.namespace.NamespaceKey;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

// Direct Handler for dropping a table.
public class DropTableHandler extends SimpleDirectHandler {
  private final Catalog catalog;
  private final UserSession userSession;

  public DropTableHandler(Catalog catalog, UserSession userSession) {
    this.catalog = catalog;
    this.userSession = userSession;
  }

  /**
   * Function resolves the schema and invokes the drop method (while IF EXISTS statement is used
   * function invokes the drop method only if table exists). Raises an exception if the schema is
   * immutable.
   *
   * @param sqlNode - SqlDropTable (SQL parse tree of drop table [if exists] query)
   * @return - Single row indicating drop succeeded or table does not exist while IF EXISTS
   *     statement is used, raise exception otherwise
   * @throws ValidationException
   * @throws RelConversionException
   * @throws IOException
   */
  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode)
      throws ValidationException, ForemanSetupException, RelConversionException, IOException {
    checkNotNull(userSession);
    final SqlDropTable dropTableNode = SqlNodeUtil.unwrap(sqlNode, SqlDropTable.class);
    NamespaceKey path;
    // skip getting resolved path for data plane with "AT BRANCH" - as
    // getResolvePathForTableManagement not support DDL with refType
    // also skip scratch table due to DX-55882
    if (dropTableNode.getRefType() != null
        || dropTableNode.getRefValue() != null
        || dropTableNode.getPath().getRoot().equals("$scratch")) {
      path = catalog.resolveSingle(dropTableNode.getPath());
    } else {
      try {
        path = CatalogUtil.getResolvePathForTableManagement(catalog, dropTableNode.getPath());
      } catch (UserException ue) {
        // when table not found but the query itself is "if exists"
        // fall back to use the original way, i.e. without resolving path, and let the code below to
        // handle the case
        if (ue.getErrorType() == VALIDATION && !dropTableNode.shouldErrorIfTableDoesNotExist()) {
          path = catalog.resolveSingle(dropTableNode.getPath());
        } else {
          throw ue;
        }
      }
    }
    catalog.validatePrivilege(path, Privilege.DROP);
    final String sourceName = path.getRoot();
    VersionContext statementSourceVersion =
        ReferenceTypeUtils.map(dropTableNode.getRefType(), dropTableNode.getRefValue(), null);
    final VersionContext sessionVersion = userSession.getSessionVersionForSource(sourceName);
    VersionContext sourceVersion = statementSourceVersion.orElse(sessionVersion);
    try {
      ResolvedVersionContext resolvedVersionContext =
          CatalogUtil.resolveVersionContext(catalog, sourceName, sourceVersion);
      CatalogUtil.validateResolvedVersionIsBranch(resolvedVersionContext);
      TableMutationOptions tableMutationOptions =
          TableMutationOptions.newBuilder()
              .setResolvedVersionContext(resolvedVersionContext)
              .build();
      catalog.dropTable(path, tableMutationOptions);
    } catch (UserException e) {
      if (e.getErrorType() == VALIDATION && !dropTableNode.shouldErrorIfTableDoesNotExist()) {
        return Collections.singletonList(
            new SimpleCommandResult(true, String.format("Table [%s] not found.", path)));
      }

      throw e;
    }

    return Collections.singletonList(SimpleCommandResult.successful("Table [%s] dropped", path));
  }
}
