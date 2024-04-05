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
 * Implements SQL to revoke privilege on catalog entities {PDS/table, VDS/view, Folder/schema}.
 *
 * Represents statements like:
 * REVOKE priv1 [,...] ON entityType entity FROM revokeeType revokee
 */
public class SqlRevokeOnCatalog extends SqlCall implements SimpleDirectHandler.Creator {
  private static final SqlLiteral sqlLiteralNull = SqlLiteral.createNull(SqlParserPos.ZERO);
  private final SqlNodeList privilegeList;
  private final SqlLiteral entityType;
  private final SqlIdentifier entity;
  private final SqlLiteral revokeeType;
  private final SqlIdentifier revokee;
  private final ReferenceType refType;
  private final SqlIdentifier refValue;

  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("REVOKE", SqlKind.OTHER) {
        @Override
        public SqlCall createCall(
            SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
          Preconditions.checkArgument(
              operands.length == 7, "SqlRevokeOnEntity.createCall() has to get 7 operands!");

          return new SqlRevokeOnCatalog(
              pos,
              (SqlNodeList) operands[0],
              (SqlLiteral) operands[1],
              (SqlIdentifier) operands[2],
              (SqlLiteral) operands[3],
              (SqlIdentifier) operands[4],
              ((SqlLiteral) operands[5]).symbolValue(ReferenceType.class),
              (SqlIdentifier) operands[6]);
        }
      };

  public SqlRevokeOnCatalog(
      SqlParserPos pos,
      SqlNodeList privilegeList,
      SqlLiteral entityType,
      SqlIdentifier entity,
      SqlLiteral granteeType,
      SqlIdentifier grantee,
      ReferenceType refType,
      SqlIdentifier refValue) {
    super(pos);

    this.privilegeList = privilegeList;
    this.entityType = entityType;
    this.entity = entity;
    this.revokeeType = granteeType;
    this.revokee = grantee;
    this.refType = refType;
    this.refValue = refValue;
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
    ops.add(
        (refType == null) ? sqlLiteralNull : SqlLiteral.createSymbol(refType, SqlParserPos.ZERO));
    ops.add(refValue);

    return ops;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("REVOKE");
    privilegeList.unparse(writer, leftPrec, rightPrec);
    writer.keyword("ON");
    entityType.unparse(writer, 0, 0);

    if (refType != null && refValue != null) {
      writer.keyword("AT");
      writer.keyword(refType.toString());
      refValue.unparse(writer, leftPrec, rightPrec);
    }

    writer.keyword("FROM");
    revokee.unparse(writer, leftPrec, rightPrec);
  }

  @Override
  public SimpleDirectHandler toDirectHandler(QueryContext context) {
    try {
      final Class<?> cl =
          Class.forName("com.dremio.exec.planner.sql.handlers.CatalogRevokeHandler");
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

  public ReferenceType getRefType() {
    return refType;
  }

  public SqlIdentifier getRefValue() {
    return refValue;
  }
}
