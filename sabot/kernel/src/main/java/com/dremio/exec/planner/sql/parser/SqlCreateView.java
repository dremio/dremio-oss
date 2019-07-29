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

import com.dremio.exec.planner.sql.handlers.SqlHandlerUtil;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class SqlCreateView extends SqlCall {
  public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("CREATE_VIEW", SqlKind.CREATE_VIEW) {
    @Override
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
      return new SqlCreateView(pos, (SqlIdentifier) operands[0], (SqlNodeList) operands[1], operands[2], (SqlLiteral) operands[3]);
    }
  };

  private SqlIdentifier viewName;
  private SqlNodeList fieldList;
  private SqlNode query;
  private boolean replaceView;

  public SqlCreateView(SqlParserPos pos, SqlIdentifier viewName, SqlNodeList fieldList,
      SqlNode query, SqlLiteral replaceView) {
    this(pos, viewName, fieldList, query, replaceView.booleanValue());
  }

  public SqlCreateView(SqlParserPos pos, SqlIdentifier viewName, SqlNodeList fieldList,
                       SqlNode query, boolean replaceView) {
    super(pos);
    this.viewName = viewName;
    this.query = query;
    this.replaceView = replaceView;
    this.fieldList = fieldList;
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
    writer.keyword("AS");
    query.unparse(writer, leftPrec, rightPrec);
  }

  public List<String> getSchemaPath() {
    if (viewName.isSimple()) {
      return ImmutableList.of();
    }

    return viewName.names.subList(0, viewName.names.size()-1);
  }

  public String getName() {
    if (viewName.isSimple()) {
      return viewName.getSimple();
    }

    return viewName.names.get(viewName.names.size() - 1);
  }

  public NamespaceKey getPath() {
    return new NamespaceKey(viewName.names);
  }

  public List<String> getFieldNames() {
    List<String> fieldNames = Lists.newArrayList();
    for (SqlNode node : fieldList.getList()) {
      fieldNames.add(node.toString());
    }
    return fieldNames;
  }

  public SqlNode getQuery() { return query; }
  public boolean getReplace() { return replaceView; }

}
