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
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.catalog.udf.UserDefinedFunctionCatalog;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.sql.parser.SqlDropFunction;
import com.dremio.service.namespace.NamespaceKey;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.sql.SqlNode;

/** DropFunctionHandler */
public class DropFunctionHandler extends SimpleDirectHandler {
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
    final Catalog catalog = context.getCatalog();
    UserDefinedFunctionCatalog userDefinedFunctionCatalog = context.getUserDefinedFunctionCatalog();

    NamespaceKey functionKey = catalog.resolveSingle(dropFunction.getPath());
    CatalogEntityKey catalogEntityKey =
        CatalogEntityKeyUtil.buildCatalogEntityKey(
            functionKey,
            dropFunction.getSqlTableVersionSpec(),
            context.getSession().getSessionVersionForSource(functionKey.getRoot()));
    boolean functionExists = doesFunctionExist(userDefinedFunctionCatalog, catalogEntityKey);
    if (functionExists) {
      userDefinedFunctionCatalog.dropFunction(catalogEntityKey);
      return SimpleCommandResult.successful(
          String.format("Function [%s] has been dropped.", catalogEntityKey.toSql()));
    }

    // Try again but from the root context:
    if (functionKey.size() > 1) {
      functionKey = new NamespaceKey(functionKey.getLeaf());
      catalogEntityKey =
          CatalogEntityKeyUtil.buildCatalogEntityKey(
              functionKey,
              dropFunction.getSqlTableVersionSpec(),
              context.getSession().getSessionVersionForSource(functionKey.getRoot()));
      functionExists = doesFunctionExist(userDefinedFunctionCatalog, catalogEntityKey);
      if (functionExists) {
        userDefinedFunctionCatalog.dropFunction(CatalogEntityKey.fromNamespaceKey(functionKey));
        return SimpleCommandResult.successful(
            String.format("Function [%s] has been dropped.", catalogEntityKey.toSql()));
      }
    }

    if (dropFunction.isIfExists()) {
      return SimpleCommandResult.successful(
          "Function [%s] does not exists.", catalogEntityKey.toSql());
    }

    throw UserException.validationError()
        .message("Function [%s] does not exists.", catalogEntityKey.toSql())
        .buildSilently();
  }

  protected ResolvedVersionContext getResolvedVersionContext(
      String sourceName, VersionContext version) {
    return CatalogUtil.resolveVersionContext(context.getCatalog(), sourceName, version);
  }

  private static boolean doesFunctionExist(
      UserDefinedFunctionCatalog udfCatalog, CatalogEntityKey functionKey) {
    boolean exists;
    try {
      exists = udfCatalog.getFunction(functionKey) != null;
    } catch (Exception ignored) {
      exists = false;
    }

    return exists;
  }
}
