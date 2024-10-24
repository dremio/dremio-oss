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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
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

/** Implements SQL Ownership Grants. */
public class SqlGrantOwnership extends SqlCall {
  private final SqlIdentifier entity;
  private final SqlLiteral entityType;
  private final SqlIdentifier grantee;
  private final SqlLiteral granteeType;
  private final ReferenceType refType;
  private final SqlIdentifier refValue;

  public enum GrantType {
    USER,
    VDS,
    PDS,
    PROJECT,
    ORG,
    CLOUD,
    ENGINE,
    ROLE,
    IDENTITY_PROVIDER,
    OAUTH_APPLICATION,
    EXTERNAL_TOKENS_PROVIDER,
    FOLDER,
    SOURCE,
    SPACE,
    FUNCTION,
    ARCTIC_CATALOG,
    BRANCH,
    TAG
  }

  public enum GranteeType {
    USER,
    ROLE
  }

  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("GRANT OWNERSHIP", SqlKind.OTHER) {
        @Override
        public SqlCall createCall(
            SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
          Preconditions.checkArgument(
              operands.length == 6, "SqlGrantOwnership.createCall() has to get 6 operands!");
          return new SqlGrantOwnership(
              pos,
              (SqlIdentifier) operands[0],
              (SqlLiteral) operands[1],
              (SqlIdentifier) operands[2],
              (SqlLiteral) operands[3],
              ((SqlLiteral) operands[4]).symbolValue(ReferenceType.class),
              (SqlIdentifier) operands[5]);
        }
      };

  public SqlGrantOwnership(
      SqlParserPos pos,
      SqlIdentifier entity,
      SqlLiteral entityType,
      SqlIdentifier grantee,
      SqlLiteral granteeType,
      ReferenceType refType,
      SqlIdentifier refValue) {
    super(pos);
    this.entity = entity;
    this.entityType = entityType;
    this.grantee = grantee;
    this.granteeType = granteeType;
    this.refType = refType;
    this.refValue = refValue;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    List<SqlNode> ops = Lists.newArrayList();
    ops.add(entity);
    ops.add(entityType);
    ops.add(grantee);
    ops.add(granteeType);
    ops.add(
        (refType == null)
            ? SqlLiteral.createNull(SqlParserPos.ZERO)
            : SqlLiteral.createSymbol(refType, SqlParserPos.ZERO));
    ops.add(refValue);

    return ops;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("GRANT");
    writer.keyword("OWNERSHIP");
    writer.keyword("ON");
    entityType.unparse(writer, 0, 0);

    if (refType != null && refValue != null) {
      writer.keyword("AT");
      writer.keyword(refType.toString());
      refValue.unparse(writer, leftPrec, rightPrec);
    }

    writer.keyword("TO");
    granteeType.unparse(writer, 0, 0);
    grantee.unparse(writer, leftPrec, rightPrec);
  }

  public SqlIdentifier getGrantee() {
    return grantee;
  }

  public SqlIdentifier getEntity() {
    return entity;
  }

  public SqlLiteral getEntityType() {
    return entityType;
  }

  public SqlLiteral getGranteeType() {
    return granteeType;
  }

  public ReferenceType getRefType() {
    return refType;
  }

  public SqlIdentifier getRefValue() {
    return refValue;
  }

  public static class Grant {
    private final SqlLiteral type;

    public Grant(SqlLiteral type) {
      this.type = type;
    }

    public SqlLiteral getType() {
      return type;
    }
  }
}
