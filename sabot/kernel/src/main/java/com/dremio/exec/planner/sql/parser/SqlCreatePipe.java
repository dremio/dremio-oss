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
import java.util.ArrayList;
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
import com.google.common.base.Throwables;

/**
 * Template implementation. To be extended as needed.
 */
public class SqlCreatePipe extends SqlCall implements SimpleDirectHandler.Creator {

  private final SqlIdentifier pipeName;
  private final SqlIdentifier notificationProvider;
  private final SqlIdentifier notificationQueueRef;
  private final SqlCopyIntoTable sqlCopyInto;

  public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("CREATE PIPE", SqlKind.OTHER) {
    @Override
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
      return new SqlCreatePipe(pos,
        (SqlIdentifier) operands[0],
        operands[1],
        (SqlIdentifier) operands[2],
        (SqlIdentifier) operands[3]
      );
    }
  };

  public SqlCreatePipe(SqlParserPos pos, SqlIdentifier pipeName, SqlNode sqlCopyInto, SqlIdentifier notificationProvider, SqlIdentifier notificationQueueRef) {
    super(pos);
    this.pipeName = pipeName;
    this.sqlCopyInto = (SqlCopyIntoTable) sqlCopyInto;
    this.notificationProvider = notificationProvider;
    this.notificationQueueRef = notificationQueueRef;
  }

  @Override
  public SimpleDirectHandler toDirectHandler(QueryContext context) {
    try {
      final Class<?> cl = Class.forName("com.dremio.exec.planner.sql.handlers.CreatePipeHandler");
      final Constructor<?> ctor = cl.getConstructor(QueryContext.class);
      return (SimpleDirectHandler) ctor.newInstance(context);
    } catch (ClassNotFoundException e) {
      // Assume failure to find class means that we aren't running Enterprise Edition
      throw UserException.unsupportedError(e)
        .message("CREATE PIPE is only supported in the Enterprise Edition.")
        .buildSilently();
    } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  public SqlIdentifier getPipeName() {
    return pipeName;
  }

  public SqlCopyIntoTable getSqlCopyInto() {
    return sqlCopyInto;
  }

  public SqlIdentifier getNotificationProvider() {
    return notificationProvider;
  }

  public SqlIdentifier getNotificationQueueRef() {
    return notificationQueueRef;
  }

  @Override
  public List<SqlNode> getOperandList() {
    List<SqlNode> operands = new ArrayList<>();
    operands.add(pipeName);
    operands.add(sqlCopyInto);
    operands.add(notificationProvider);
    operands.add(notificationQueueRef);
    return operands;
  }
}
