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

import com.dremio.common.utils.PathUtils;
import com.dremio.exec.planner.sql.handlers.SqlHandlerUtil;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.collect.ImmutableList;
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

public class SqlCreateView extends SqlCall {
  private static final SqlLiteral sqlLiteralNull = SqlLiteral.createNull(SqlParserPos.ZERO);
  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("CREATE_VIEW", SqlKind.CREATE_VIEW) {
        @Override
        public SqlCall createCall(
            SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
          return new SqlCreateView(
              pos,
              (SqlIdentifier) operands[0],
              (SqlNodeList) operands[1],
              operands[2],
              (SqlLiteral) operands[3],
              (SqlPolicy) operands[4],
              ((SqlLiteral) operands[5]).symbolValue(ReferenceType.class),
              (SqlIdentifier) operands[6]);
        }
      };

  private SqlIdentifier viewName;
  private SqlNodeList fieldList;
  private SqlNode query;
  private boolean replaceView;
  private SqlPolicy policy;
  private ReferenceType refType;
  private SqlIdentifier refValue;

  public SqlCreateView(
      SqlParserPos pos,
      SqlIdentifier viewName,
      SqlNodeList fieldList,
      SqlNode query,
      SqlLiteral replaceView,
      SqlPolicy policy,
      ReferenceType refType,
      SqlIdentifier refValue) {
    this(pos, viewName, fieldList, query, replaceView.booleanValue(), policy, refType, refValue);
  }

  public SqlCreateView(
      SqlParserPos pos,
      SqlIdentifier viewName,
      SqlNodeList fieldList,
      SqlNode query,
      boolean replaceView,
      SqlPolicy policy,
      ReferenceType refType,
      SqlIdentifier refValue) {
    // For "CREATE VIEW ... AS ..." statement, pos is set to be the position of the `AS` token in
    // the parser.
    // This would help us to get the row or query expression, i.e. the definition of the view.
    // Note for other SQL statements, pos is usually set to be the beginning of the statement.
    super(pos);
    this.viewName = viewName;
    this.query = query;
    this.replaceView = replaceView;
    this.fieldList = fieldList;
    this.policy = policy;
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
    ops.add(viewName);
    ops.add(fieldList);
    ops.add(query);
    ops.add(SqlLiteral.createBoolean(replaceView, SqlParserPos.ZERO));
    ops.add(policy);
    ops.add(
        getRefType() == null
            ? sqlLiteralNull
            : SqlLiteral.createSymbol(getRefType(), SqlParserPos.ZERO));
    ops.add(refValue);
    return ops;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("CREATE");
    if (replaceView) {
      writer.keyword("OR");
      writer.keyword("REPLACE");
    }
    writer.keyword("VIEW");
    viewName.unparse(writer, leftPrec, rightPrec);
    if (fieldList.size() > 0) {
      SqlHandlerUtil.unparseSqlNodeList(writer, leftPrec, rightPrec, fieldList);
    }
    if (policy != null) {
      writer.keyword("ADD");
      writer.keyword("ROW");
      writer.keyword("ACCESS");
      writer.keyword("POLICY");
      policy.unparse(writer, leftPrec, rightPrec);
    }
    if (refType != null && refValue != null) {
      writer.keyword("AT");
      writer.keyword(refType.toString());
      refValue.unparse(writer, leftPrec, rightPrec);
    }
    writer.keyword("AS");
    query.unparse(writer, leftPrec, rightPrec);
  }

  public List<String> getSchemaPath() {
    if (viewName.isSimple()) {
      return ImmutableList.of();
    }

    return viewName.names.subList(0, viewName.names.size() - 1);
  }

  public String getName() {
    if (viewName.isSimple()) {
      return viewName.getSimple();
    }

    return viewName.names.get(viewName.names.size() - 1);
  }

  public String getFullName() {
    if (viewName.isSimple()) {
      return viewName.getSimple();
    }
    return PathUtils.constructFullPath(viewName.names);
  }

  public NamespaceKey getPath() {
    return new NamespaceKey(viewName.names);
  }

  public List<String> getFieldNames() {
    List<String> fieldNames = Lists.newArrayList();
    for (SqlNode node : fieldList.getList()) {
      fieldNames.add(((SqlColumnPolicyPair) node).getName().toString());
    }
    return fieldNames;
  }

  public List<String> getFieldNamesWithoutColumnMasking() {
    List<String> fieldNames = Lists.newArrayList();
    for (SqlNode node : fieldList.getList()) {
      if (((SqlColumnPolicyPair) node).getPolicy() == null) {
        fieldNames.add(((SqlColumnPolicyPair) node).getName().toString());
      }
    }
    return fieldNames;
  }

  public SqlNodeList getFieldList() {
    return fieldList;
  }

  public SqlPolicy getPolicy() {
    return policy;
  }

  public SqlNode getQuery() {
    return query;
  }

  public boolean getReplace() {
    return replaceView;
  }

  public ReferenceType getRefType() {
    return refType;
  }

  public SqlIdentifier getRefValue() {
    return refValue;
  }
}
