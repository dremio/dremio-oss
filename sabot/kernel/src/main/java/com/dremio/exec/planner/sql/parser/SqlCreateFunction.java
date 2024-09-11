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

import com.dremio.catalog.model.VersionContext;
import com.dremio.exec.planner.sql.handlers.SqlHandlerUtil;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.stream.Collectors;
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

/**
 * CREATE [ OR REPLACE ] FUNCTION [ IF NOT EXISTS ] function_name ( [ function_parameter [, ...] ] )
 * [ AT BRANCH r ] RETURNS { data_type } RETURN { query }
 */
public class SqlCreateFunction extends SqlCall {
  private static final SqlLiteral sqlLiteralNull = SqlLiteral.createNull(SqlParserPos.ZERO);

  private final SqlIdentifier name;
  private final SqlNodeList fieldList;
  private final SqlNode expression;
  private final SqlFunctionReturnType returnType;
  private boolean shouldReplace;
  private boolean ifNotExists;
  private SqlTableVersionSpec sqlTableVersionSpec;

  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("CREATE_FUNCTION", SqlKind.OTHER) {
        @Override
        public SqlCall createCall(
            SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
          Preconditions.checkArgument(
              operands.length == 7, "SqlCreateFunction.createCall() has to get 7 operands!");
          return new SqlCreateFunction(
              pos,
              (SqlLiteral) operands[0],
              (SqlIdentifier) operands[1],
              (SqlNodeList) operands[2],
              operands[3],
              (SqlLiteral) operands[4],
              (SqlFunctionReturnType) operands[5],
              (SqlTableVersionSpec) operands[6]);
        }
      };

  public SqlCreateFunction(
      SqlParserPos pos,
      SqlLiteral shouldReplace,
      SqlIdentifier name,
      SqlNodeList fieldList,
      SqlNode expression,
      SqlLiteral ifNotExists,
      SqlFunctionReturnType returnType,
      SqlTableVersionSpec sqlTableVersionSpec) {
    super(pos);
    this.shouldReplace = shouldReplace.booleanValue();
    this.name = name;
    this.fieldList = fieldList;
    this.expression = expression;
    this.ifNotExists = ifNotExists.booleanValue();
    this.returnType = returnType;
    this.sqlTableVersionSpec = sqlTableVersionSpec;
  }

  public SqlIdentifier getName() {
    return name;
  }

  public NamespaceKey getPath() {
    return new NamespaceKey(name.names);
  }

  public String getFullName() {
    if (name.isSimple()) {
      return name.getSimple();
    }
    return name.names.stream().collect(Collectors.joining("."));
  }

  public SqlNodeList getFieldList() {
    return fieldList;
  }

  public SqlFunctionReturnType getReturnType() {
    return returnType;
  }

  public boolean isTabularFunction() {
    return returnType.isTabular();
  }

  public SqlNode getExpression() {
    return expression;
  }

  public boolean shouldReplace() {
    return shouldReplace;
  }

  public boolean isIfNotExists() {
    return ifNotExists;
  }

  public SqlTableVersionSpec getSqlTableVersionSpec() {
    return sqlTableVersionSpec;
  }

  public VersionContext getVersionContext() {
    if (sqlTableVersionSpec != null) {
      return sqlTableVersionSpec.getTableVersionSpec().getTableVersionContext().asVersionContext();
    }
    return null;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return Lists.newArrayList(
        SqlLiteral.createBoolean(shouldReplace, SqlParserPos.ZERO),
        name,
        fieldList,
        expression,
        SqlLiteral.createBoolean(ifNotExists, SqlParserPos.ZERO),
        returnType,
        sqlTableVersionSpec);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("CREATE");
    if (shouldReplace) {
      writer.keyword("OR");
      writer.keyword("REPLACE");
    }
    writer.keyword("FUNCTION");
    if (ifNotExists) {
      writer.keyword("IF");
      writer.keyword("NOT");
      writer.keyword("EXISTS");
    }
    name.unparse(writer, leftPrec, rightPrec);
    if (fieldList.size() > 0) {
      SqlHandlerUtil.unparseSqlNodeList(writer, leftPrec, rightPrec, fieldList);
    }
    sqlTableVersionSpec.unparse(writer, leftPrec, rightPrec);
    writer.keyword("RETURNS");
    returnType.unparse(writer, leftPrec, rightPrec);

    writer.keyword("RETURN");
    expression.unparse(writer, leftPrec, rightPrec);
  }
}
