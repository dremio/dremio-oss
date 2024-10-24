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
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

/*
 * Implements SQL to grant privilege on reference for catalog.
 *
 * Represents statements like:
 * GRANT priv1 [,...] ON BRANCH/TAG refValue IN CATALOG catalogName TO granteeType grantee
 */
public final class SqlGrantOnReference extends SqlCall {
  private static final SqlLiteral sqlLiteralNull = SqlLiteral.createNull(SqlParserPos.ZERO);
  private final SqlNodeList privilegeList;
  private final ReferenceType refType;
  private final SqlIdentifier refValue;
  private final SqlIdentifier catalogName;
  private final SqlLiteral granteeType;
  private final SqlIdentifier grantee;

  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("GRANT", SqlKind.OTHER) {
        @Override
        public SqlCall createCall(
            SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
          Preconditions.checkArgument(
              operands.length == 6, "SqlGrantOnReference.createCall() has to get 7 operands!");

          return new SqlGrantOnReference(
              pos,
              (SqlNodeList) operands[0],
              ((SqlLiteral) operands[1]).symbolValue(ReferenceType.class),
              (SqlIdentifier) operands[2],
              (SqlIdentifier) operands[3],
              (SqlLiteral) operands[4],
              (SqlIdentifier) operands[5]);
        }
      };

  public SqlGrantOnReference(
      SqlParserPos pos,
      SqlNodeList privilegeList,
      ReferenceType refType,
      SqlIdentifier refValue,
      SqlIdentifier catalogName,
      SqlLiteral granteeType,
      SqlIdentifier grantee) {
    super(pos);

    this.privilegeList = privilegeList;
    this.refType = refType;
    this.refValue = refValue;
    this.catalogName = catalogName;
    this.granteeType = granteeType;
    this.grantee = grantee;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    final List<SqlNode> ops = Lists.newArrayList();
    ops.add(privilegeList);
    ops.add(SqlLiteral.createSymbol(refType, SqlParserPos.ZERO));
    ops.add(refValue);
    ops.add(catalogName);
    ops.add(granteeType);
    ops.add(grantee);

    return ops;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("GRANT");
    privilegeList.unparse(writer, leftPrec, rightPrec);

    writer.keyword("ON");
    writer.keyword(refType.toString());
    refValue.unparse(writer, 0, 0);

    writer.keyword("IN");
    writer.keyword("CATALOG");
    catalogName.unparse(writer, 0, 0);

    writer.keyword("TO");
    granteeType.unparse(writer, 0, 0);
    grantee.unparse(writer, leftPrec, rightPrec);
  }

  public SqlNodeList getPrivilegeList() {
    return privilegeList;
  }

  public ReferenceType getRefType() {
    return refType;
  }

  public SqlIdentifier getRefValue() {
    return refValue;
  }

  public SqlIdentifier getCatalogName() {
    return catalogName;
  }

  public SqlLiteral getGranteeType() {
    return granteeType;
  }

  public SqlIdentifier getGrantee() {
    return grantee;
  }
}
