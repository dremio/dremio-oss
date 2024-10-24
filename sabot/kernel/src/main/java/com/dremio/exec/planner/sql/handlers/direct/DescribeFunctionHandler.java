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
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.sql.parser.SqlDescribeFunction;
import com.dremio.exec.planner.sql.parser.SqlTableVersionSpec;
import com.dremio.exec.store.sys.udf.UserDefinedFunction;
import com.dremio.exec.work.foreman.ForemanSetupException;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.users.UserNotFoundException;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.security.AccessControlException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.RelConversionException;
import org.apache.commons.lang3.StringUtils;

public class DescribeFunctionHandler
    implements SqlDirectHandler<DescribeFunctionHandler.DescribeResult>, DirectHandlerValidator {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(DescribeFunctionHandler.class);

  protected final QueryContext context;

  public DescribeFunctionHandler(QueryContext context) {
    super();
    this.context = context;
  }

  @Override
  public List<DescribeResult> toResult(String sql, SqlNode sqlNode)
      throws RelConversionException, ForemanSetupException {
    final SqlDescribeFunction node = SqlNodeUtil.unwrap(sqlNode, SqlDescribeFunction.class);
    try {
      Catalog catalog = context.getCatalog();
      final SqlIdentifier functionSqlIdentifier = node.getFunction();

      CatalogEntityKey catalogEntityKey = null;
      UserDefinedFunction userDefinedFunction = null;
      // If there's a default schema, use it to try to retrieve the function. Ignore all errors.
      if (StringUtils.isNotBlank(context.getSession().getDefaultSchemaName())) {
        catalogEntityKey =
            buildCatalogEntityKey(
                catalog
                    .resolveToDefault(new NamespaceKey(functionSqlIdentifier.names))
                    .getPathComponents(),
                node.getSqlTableVersionSpec());
        userDefinedFunction = getUserDefinedFunction(catalogEntityKey, true);
      }

      // Try without the default schema applied if the function was not found.
      if (userDefinedFunction == null) {
        catalogEntityKey =
            buildCatalogEntityKey(functionSqlIdentifier.names, node.getSqlTableVersionSpec());
        userDefinedFunction = getUserDefinedFunction(catalogEntityKey, false);
      }

      //noinspection ConstantConditions
      DescribeResult describeResult =
          new DescribeResult(
              (userDefinedFunction.getName() != null) ? userDefinedFunction.getName() : "",
              (userDefinedFunction.getFunctionArgsList() != null)
                  ? userDefinedFunction.getFunctionArgsList().toString()
                  : null,
              userDefinedFunction.getReturnType().toString(),
              userDefinedFunction.getFunctionSql(),
              userDefinedFunction.getCreatedAt(),
              userDefinedFunction.getModifiedAt(),
              getOwner(catalogEntityKey));

      return ImmutableList.of(describeResult);
    } catch (AccessControlException e) {
      throw UserException.permissionError(e)
          .message("Not authorized to describe Function.")
          .build(logger);
    } catch (Exception ex) {
      throw UserException.planError(ex)
          .message("Error while rewriting DESCRIBE query: %s", ex.getMessage())
          .buildSilently();
    }
  }

  private CatalogEntityKey buildCatalogEntityKey(
      List<String> path, SqlTableVersionSpec sqlTableVersionSpec) {
    return CatalogEntityKeyUtil.buildCatalogEntityKey(
        path, sqlTableVersionSpec, context.getSession().getSessionVersionForSource(path.get(0)));
  }

  private UserDefinedFunction getUserDefinedFunction(
      CatalogEntityKey catalogEntityKey, boolean nullOnUserException) {
    validate(
        catalogEntityKey.toNamespaceKey(),
        Objects.requireNonNullElse(
                catalogEntityKey.getTableVersionContext(), TableVersionContext.NOT_SPECIFIED)
            .asVersionContext());

    try {
      return context.getUserDefinedFunctionCatalog().getFunction(catalogEntityKey);
    } catch (UserException e) {
      if (nullOnUserException) {
        return null;
      }
      throw e;
    }
  }

  public static class DescribeResult {
    public final String Name;
    public final String Input;
    public final String Returns;
    public final String Body;
    public final Timestamp Created_At;
    public final Timestamp Last_Modified_At;
    public final String Owner;

    public DescribeResult(
        String functionName,
        String argList,
        String returnType,
        String body,
        Timestamp created_at,
        Timestamp last_modified_at,
        String owner) {
      Name = functionName;
      Input = argList;
      Returns = returnType;
      Body = body;
      Created_At = created_at;
      Last_Modified_At = last_modified_at;
      Owner = owner;
    }
  }

  @Override
  public Class<DescribeResult> getResultType() {
    return DescribeResult.class;
  }

  @Nullable
  protected String getOwner(CatalogEntityKey key) throws NamespaceException, UserNotFoundException {
    return null;
  }

  public static DescribeFunctionHandler create(QueryContext queryContext) {
    try {
      final Class<?> cl =
          Class.forName("com.dremio.exec.planner.sql.handlers.EnterpriseDescribeFunctionHandler");
      final Constructor<?> ctor = cl.getConstructor(QueryContext.class);
      return (DescribeFunctionHandler) ctor.newInstance(queryContext);
    } catch (ClassNotFoundException e) {
      return new DescribeFunctionHandler(queryContext);
    } catch (InstantiationException
        | IllegalAccessException
        | NoSuchMethodException
        | InvocationTargetException e2) {
      throw Throwables.propagate(e2);
    }
  }
}
