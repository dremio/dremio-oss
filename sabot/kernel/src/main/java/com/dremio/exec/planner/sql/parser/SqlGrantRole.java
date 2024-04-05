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

import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.sql.handlers.direct.SimpleDirectHandler;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
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
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * Implements SQL Role Grant to add a role to a role. Represents statements like: GRANT ROLE
 * role_name TO USER | ROLE grantee_role_name
 */
public class SqlGrantRole extends SqlCall implements SimpleDirectHandler.Creator {
  private final SqlIdentifier roleToGrant;
  private final SqlLiteral granteeType;
  private final SqlIdentifier grantee;

  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("GRANT", SqlKind.OTHER) {
        @Override
        public SqlCall createCall(
            SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
          Preconditions.checkArgument(
              operands.length == 3, "SqlGrantRole.createCall() has to get 3 operands!");
          return new SqlGrantRole(
              pos,
              (SqlIdentifier) operands[0],
              (SqlLiteral) operands[1],
              (SqlIdentifier) operands[2]);
        }
      };

  public SqlGrantRole(
      SqlParserPos pos, SqlIdentifier roleToGrant, SqlLiteral granteeType, SqlIdentifier grantee) {
    super(pos);
    this.roleToGrant = roleToGrant;
    this.granteeType = granteeType;
    this.grantee = grantee;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    List<SqlNode> ops = Lists.newArrayList();
    ops.add(roleToGrant);
    ops.add(granteeType);
    ops.add(grantee);

    return ops;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("GRANT");
    writer.keyword("ROLE");
    roleToGrant.unparse(writer, 0, 0);
    writer.keyword("TO");
    grantee.unparse(writer, leftPrec, rightPrec);
  }

  @Override
  public SimpleDirectHandler toDirectHandler(QueryContext context) {
    try {
      final Class<?> cl = Class.forName("com.dremio.exec.planner.sql.handlers.RoleGrantHandler");
      Constructor<?> ctor = cl.getConstructor(QueryContext.class);
      return (SimpleDirectHandler) ctor.newInstance(context);
    } catch (InstantiationException
        | IllegalAccessException
        | ClassNotFoundException
        | NoSuchMethodException
        | InvocationTargetException e) {
      throw Throwables.propagate(e);
    }
  }

  public SqlIdentifier getRoleToGrant() {
    return roleToGrant;
  }

  public SqlLiteral getGranteeType() {
    return granteeType;
  }

  public SqlIdentifier getGrantee() {
    return grantee;
  }
}
