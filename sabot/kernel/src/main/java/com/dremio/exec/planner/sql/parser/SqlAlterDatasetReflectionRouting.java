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
package com.dremio.exec.planner.sql.parser;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.sql.handlers.direct.SimpleDirectHandler;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

public class SqlAlterDatasetReflectionRouting extends SqlSystemCall implements SimpleDirectHandler.Creator  {
  public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("REFLECTION_ROUTING", SqlKind.OTHER_DDL) {
    @Override
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
      Preconditions.checkArgument(operands.length == 4, "SqlAlterDatasetReflectionRouting.createCall() has to get 4 operands!");
      return new SqlAlterDatasetReflectionRouting(
        pos,
        (SqlIdentifier) operands[0],
        operands[1],
        operands[2],
        (SqlIdentifier) operands[3]
      );
    }
  };

  private final SqlIdentifier tblName;
  private final SqlNode isDefault;
  private final SqlNode isQueue;
  private final SqlIdentifier queueOrEngineName;

  public SqlAlterDatasetReflectionRouting(SqlParserPos pos, SqlIdentifier tblName, SqlNode isDefault, SqlNode isQueue, SqlIdentifier queueOrEngineName) {
    super(pos);
    this.tblName = tblName;
    this.isDefault = isDefault;
    this.isQueue = isQueue;
    this.queueOrEngineName = queueOrEngineName;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return Lists.newArrayList(tblName, isDefault, isQueue, queueOrEngineName);
  }

  public SqlIdentifier getTblName() {
    return tblName;
  }

  public boolean isDefault(){
    return ((SqlLiteral) this.isDefault).booleanValue();
  }

  public boolean isQueue() { return ((SqlLiteral) this.isQueue).booleanValue(); }

  public SqlIdentifier getQueueOrEngineName() {
    return queueOrEngineName;
  }

  @Override
  public SimpleDirectHandler toDirectHandler(QueryContext context) {
    try {
      final Class<?> cl = Class.forName("com.dremio.exec.planner.sql.handlers.DCSReflectionRoutingHandler");
      final Constructor<?> ctor = cl.getConstructor(QueryContext.class);
      return (SimpleDirectHandler) ctor.newInstance(context);
    } catch (ClassNotFoundException e) {
      // Assume failure to find class means that we aren't running DCS Edition
      try {
        final Class<?> cl = Class.forName("com.dremio.exec.planner.sql.handlers.EnterpriseReflectionRoutingHandler");
        final Constructor<?> ctor = cl.getConstructor(QueryContext.class);
        return (SimpleDirectHandler) ctor.newInstance(context);
      } catch (InstantiationException | IllegalAccessException | ClassNotFoundException | NoSuchMethodException | InvocationTargetException e2) {
        final UserException.Builder exceptionBuilder = UserException.unsupportedError()
          .message("This command is not supported in this edition of Dremio.");
        throw exceptionBuilder.buildSilently();
      }
    }  catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
      throw Throwables.propagate(e);
    }
  }
}
