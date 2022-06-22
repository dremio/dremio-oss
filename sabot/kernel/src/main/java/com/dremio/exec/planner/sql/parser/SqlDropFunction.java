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
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.dremio.service.namespace.NamespaceKey;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * DROP FUNCTION [ IF EXISTS ] function_name
 */
public class SqlDropFunction extends SqlCall {
  private final SqlIdentifier name;
  private boolean ifExists;

  public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("DROP_FUNCTION", SqlKind.OTHER) {
    @Override
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
      Preconditions.checkArgument(operands.length == 2, "SqlCreateFunction.createCall() has to get 5 operands!");
      return new SqlDropFunction(
        pos,
        (SqlLiteral) operands[0],
        (SqlIdentifier) operands[1]
      );
    }
  };

  public SqlDropFunction(SqlParserPos pos, SqlLiteral ifExists, SqlIdentifier name) {
    super(pos);
    this.ifExists = ifExists.booleanValue();
    this.name = name;
  }

  public SqlIdentifier getName() {
    return name;
  }

  public NamespaceKey getPath() {
    return new NamespaceKey(name.names);
  }

  public boolean isIfExists() {
    return ifExists;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return ImmutableList.of(
      SqlLiteral.createBoolean(ifExists, SqlParserPos.ZERO),
      name);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("DROP");
    if (ifExists) {
      writer.keyword("IF");
      writer.keyword("EXISTS");
    }
    writer.keyword("FUNCTION");
    name.unparse(writer, leftPrec, rightPrec);
  }
}
