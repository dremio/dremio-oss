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
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

/*
 * Implements SQL to revoke privileges on all folders under a catalog/folder for a given grantee.
 *
 * Represents statements like:
 * REVOKE priv1 [,...] ON ALL FOLDERS IN entity FROM granteeType grantee
 */
public class SqlRevokeOnAllFolders extends SqlCall implements SimpleDirectHandler.Creator {
  private final SqlNodeList privilegeList;
  private final SqlLiteral entityType;
  private final SqlIdentifier entity;
  private final SqlLiteral revokeeType;
  private final SqlIdentifier revokee;

  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("REVOKE", SqlKind.OTHER) {
        @Override
        public SqlCall createCall(
            SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
          Preconditions.checkArgument(
              operands.length == 5, "SqlRevokeOnAllFolders.createCall() has to get 5 operands!");

          return new SqlRevokeOnAllFolders(
              pos,
              (SqlNodeList) operands[0],
              (SqlLiteral) operands[1],
              (SqlIdentifier) operands[2],
              (SqlLiteral) operands[3],
              (SqlIdentifier) operands[4]);
        }
      };

  public SqlRevokeOnAllFolders(
      SqlParserPos pos,
      SqlNodeList privilegeList,
      SqlLiteral entityType,
      SqlIdentifier entity,
      SqlLiteral revokeeType,
      SqlIdentifier revokee) {
    super(pos);

    this.privilegeList = privilegeList;
    this.entityType = entityType;
    this.entity = entity;
    this.revokeeType = revokeeType;
    this.revokee = revokee;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    final List<SqlNode> ops = Lists.newArrayList();
    ops.add(privilegeList);
    ops.add(entityType);
    ops.add(entity);
    ops.add(revokeeType);
    ops.add(revokee);

    return ops;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("REVOKE");

    privilegeList.unparse(writer, leftPrec, rightPrec);

    writer.keyword("ON");
    writer.keyword("ALL");
    writer.keyword("FOLDERS");
    writer.keyword("IN");

    entityType.unparse(writer, 0, 0);
    writer.keyword("FROM");
    revokee.unparse(writer, leftPrec, rightPrec);
  }

  @Override
  public SimpleDirectHandler toDirectHandler(QueryContext context) {
    try {
      final Class<?> cl =
          Class.forName("com.dremio.exec.planner.sql.handlers.RevokeOnAllFoldersHandler");
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

  public SqlNodeList getPrivilegeList() {
    return privilegeList;
  }

  public SqlLiteral getEntityType() {
    return entityType;
  }

  public SqlIdentifier getEntity() {
    return entity;
  }

  public SqlLiteral getRevokeeType() {
    return revokeeType;
  }

  public SqlIdentifier getRevokee() {
    return revokee;
  }
}
