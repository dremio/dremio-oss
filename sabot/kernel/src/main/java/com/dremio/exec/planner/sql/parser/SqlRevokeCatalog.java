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
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.sql.handlers.direct.SimpleDirectHandler;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

public class SqlRevokeCatalog extends SqlCall implements SimpleDirectHandler.Creator{
  private final SqlNodeList privilegeList;
  private final SqlLiteral grantType;
  private final SqlIdentifier entity;
  private final SqlIdentifier grantee;
  private final SqlLiteral granteeType;

  public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("REVOKE", SqlKind.OTHER) {
    @Override
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
      Preconditions.checkArgument(operands.length == 5, "SqlRevokeCatalog.createCall() has to get 5 operands!");
      return new SqlRevokeCatalog(
        pos,
        (SqlNodeList) operands[0],
        (SqlLiteral) operands[1],
        (SqlIdentifier) operands[2],
        (SqlLiteral) operands[3],
        (SqlIdentifier) operands[4]
      );
    }
  };

  public SqlRevokeCatalog(SqlParserPos pos, SqlNodeList privilegeList, SqlLiteral grantType, SqlIdentifier entity,
                          SqlLiteral granteeType, SqlIdentifier grantee) {
    super(pos);
    this.privilegeList = privilegeList;
    this.grantType = grantType;
    this.entity = entity;
    this.grantee = grantee;
    this.granteeType = granteeType;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    List<SqlNode> ops = Lists.newArrayList();
    ops.add(privilegeList);
    ops.add(grantType);
    ops.add(entity);
    ops.add(grantee);
    ops.add(granteeType);
    return ops;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("REVOKE");
    privilegeList.unparse(writer, leftPrec, rightPrec);
    writer.keyword("ON");
    grantType.unparse(writer, 0, 0);
    writer.keyword("FROM");
    grantee.unparse(writer, leftPrec, rightPrec);
  }

  @Override
  public SimpleDirectHandler toDirectHandler(QueryContext context) {
    try {
      final Class<?> cl = Class.forName("com.dremio.exec.planner.sql.handlers.RevokeCatalogHandler");
      final Constructor<?> ctor = cl.getConstructor(QueryContext.class);
      return (SimpleDirectHandler) ctor.newInstance(context);
    } catch (ClassNotFoundException e) {
      throw UserException.unsupportedError(e)
        .message("REVOKE action is not supported")
        .buildSilently();
    } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
      throw Throwables.propagate(e);
    }
  }

  public SqlNodeList getPrivilegeList() {
    return privilegeList;
  }

  public SqlLiteral getGrantType() {
    return grantType;
  }

  public SqlIdentifier getEntity() {
    return entity;
  }

  public SqlIdentifier getGrantee() {
    return grantee;
  }

  public SqlLiteral getGranteeType() {
    return granteeType;
  }
}
