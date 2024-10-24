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
import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.catalog.model.VersionContext;
import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.catalog.udf.UserDefinedFunctionCatalog;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.sql.parser.SqlDropFunction;
import com.dremio.service.namespace.NamespaceKey;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.calcite.sql.SqlNode;

/** DropFunctionHandler */
public class DropFunctionHandler extends SimpleDirectHandlerWithValidator {
  private final QueryContext context;

  public DropFunctionHandler(QueryContext context) {
    this.context = context;
  }

  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode) throws Exception {
    return Collections.singletonList(toResultImplementation(sql, sqlNode));
  }

  private SimpleCommandResult toResultImplementation(String sql, SqlNode sqlNode) throws Exception {
    final SqlDropFunction dropFunction = SqlNodeUtil.unwrap(sqlNode, SqlDropFunction.class);

    NamespaceKey functionKey = context.getCatalog().resolveSingle(dropFunction.getPath());
    CatalogEntityKey catalogEntityKey =
        CatalogEntityKeyUtil.buildCatalogEntityKey(
            functionKey.getPathComponents(),
            dropFunction.getSqlTableVersionSpec(),
            context.getSession().getSessionVersionForSource(functionKey.getRoot()));
    Optional<SimpleCommandResult> result = dropFunctionIfExist(catalogEntityKey);
    if (result.isPresent()) {
      return result.get();
    }

    // Try again but from the root context:
    if (!functionKey.equals(dropFunction.getPath())) {
      catalogEntityKey = CatalogEntityKey.fromNamespaceKey(dropFunction.getPath());
      result = dropFunctionIfExist(catalogEntityKey);
      if (result.isPresent()) {
        return result.get();
      }
    }

    if (dropFunction.isIfExists()) {
      return SimpleCommandResult.successful(
          "Function [%s] does not exist.", catalogEntityKey.toSql());
    }

    throw UserException.validationError()
        .message("Function [%s] does not exist.", catalogEntityKey.toSql())
        .buildSilently();
  }

  protected ResolvedVersionContext getResolvedVersionContext(
      String sourceName, VersionContext version) {
    return CatalogUtil.resolveVersionContext(context.getCatalog(), sourceName, version);
  }

  private Optional<SimpleCommandResult> dropFunctionIfExist(CatalogEntityKey catalogEntityKey) {
    UserDefinedFunctionCatalog userDefinedFunctionCatalog = context.getUserDefinedFunctionCatalog();

    // If the function does not exist, or an exception happens, this is a no-op.
    try {
      if (userDefinedFunctionCatalog.getFunction(catalogEntityKey) == null) {
        return Optional.empty();
      }
    } catch (Exception ignored) {
      return Optional.empty();
    }

    validate(
        catalogEntityKey.toNamespaceKey(),
        Objects.requireNonNullElse(
                catalogEntityKey.getTableVersionContext(), TableVersionContext.NOT_SPECIFIED)
            .asVersionContext());

    userDefinedFunctionCatalog.dropFunction(catalogEntityKey);
    return Optional.of(
        SimpleCommandResult.successful(
            String.format("Function [%s] has been dropped.", catalogEntityKey.toSql())));
  }
}
