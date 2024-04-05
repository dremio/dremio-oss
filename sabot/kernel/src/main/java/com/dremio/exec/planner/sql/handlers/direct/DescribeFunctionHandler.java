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

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.udf.UserDefinedFunctionCatalog;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.sql.parser.SqlDescribeFunction;
import com.dremio.exec.store.sys.udf.UserDefinedFunction;
import com.dremio.exec.work.foreman.ForemanSetupException;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.users.UserNotFoundException;
import com.google.common.base.Throwables;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.security.AccessControlException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.RelConversionException;

public class DescribeFunctionHandler
    implements SqlDirectHandler<DescribeFunctionHandler.DescribeResult> {
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
      UserDefinedFunctionCatalog userDefinedFunctionCatalog =
          context.getUserDefinedFunctionCatalog();
      final SqlIdentifier functionId = node.getFunction();
      final NamespaceKey namespaceKey = new NamespaceKey(functionId.names);
      final UserDefinedFunction function = userDefinedFunctionCatalog.getFunction(namespaceKey);
      if (function == null) {
        throw UserException.validationError()
            .message("Unknown function [%s]", functionId)
            .buildSilently();
      }

      DescribeResult describeResult =
          new DescribeResult(
              (function.getName() != null) ? function.getName() : "",
              (function.getFunctionArgsList() != null)
                  ? function.getFunctionArgsList().toString()
                  : null,
              function.getReturnType().toString(),
              function.getFunctionSql(),
              function.getCreatedAt(),
              function.getModifiedAt(),
              getOwner(namespaceKey));

      return Arrays.asList(describeResult);
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

  @Nullable
  protected String getOwner(NamespaceKey key) throws NamespaceException, UserNotFoundException {
    return null;
  }
}
