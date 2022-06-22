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

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.calcite.sql.SqlNode;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.sql.parser.SqlDropFunction;
import com.dremio.service.namespace.NamespaceKey;

/**
 * DropFunctionHandler
 */
public class DropFunctionHandler extends SimpleDirectHandler {

  private static org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DropFunctionHandler.class);

  private final QueryContext context;

  public DropFunctionHandler(QueryContext context) {
    this.context = context;
  }

  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode) throws Exception {
    final SqlDropFunction dropFunction = SqlNodeUtil.unwrap(sqlNode, SqlDropFunction.class);
    final Catalog catalog = context.getCatalog();
    final NamespaceKey functionKey = catalog.resolveSingle(dropFunction.getPath());

    boolean functionExists = checkFunctionExists(functionKey, catalog);

    if (functionExists) {
      catalog.dropFunction(functionKey);
      return Collections.singletonList(
        SimpleCommandResult.successful(String.format("Function, %s, is dropped.", functionKey)));
    } else if(dropFunction.isIfExists()) {
      return Collections.singletonList(
        SimpleCommandResult.successful("Function, %s, does not exists.", functionKey));
    } else {
      throw UserException.validationError()
        .message("Function, %s, does not exists.", functionKey)
        .buildSilently();
    }
  }

  private boolean checkFunctionExists (NamespaceKey functionKey, Catalog catalog) {
    try {
      return null != catalog.getFunction(functionKey);
    } catch (IOException ignored) {
      throw UserException.ioExceptionError(ignored)
        .buildSilently();
    }
  }
}
