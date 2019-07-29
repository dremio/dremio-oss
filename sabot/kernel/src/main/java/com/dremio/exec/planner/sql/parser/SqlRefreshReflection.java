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
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.NlsString;

import com.dremio.exec.planner.sql.handlers.query.SqlToPlanHandler;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

public class SqlRefreshReflection extends SqlCall implements SqlToPlanHandler.Creator {

  public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("REFRESH_REFLECTION", SqlKind.OTHER) {
    @Override
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
      Preconditions.checkArgument(operands.length == 2, "SqlRefreshReflection.createCall() has to get 3 operands!");
      return new SqlRefreshReflection(
          pos,
          (SqlLiteral) operands[0],
          (SqlLiteral) operands[1]
              );
    }
  };

  private final SqlLiteral reflectionId;
  private final SqlLiteral materializationId;

  public SqlRefreshReflection(
      SqlParserPos pos,
      SqlNode reflectionId,
      SqlNode materializationId) {
    super(pos);
    this.reflectionId = (SqlLiteral) reflectionId;
    this.materializationId = (SqlLiteral) materializationId;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    List<SqlNode> ops = Lists.newArrayList();
    ops.add(reflectionId);
    ops.add(materializationId);
    return ops;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("REFRESH");
    writer.keyword("REFLECTION");
    reflectionId.unparse(writer, leftPrec, rightPrec);
    writer.keyword("AS");
    materializationId.unparse(writer, leftPrec, rightPrec);
  }

  public SqlParserPos getReflectionIdPos() {
    return reflectionId.getParserPosition();
  }

  public SqlParserPos getMaterializationIdPos() {
    return materializationId.getParserPosition();
  }

  public String getReflectionId() {
    return ((NlsString) reflectionId.getValue()).getValue();
  }

  public String getMaterializationId() {
    return ((NlsString) materializationId.getValue()).getValue();
  }

  @Override
  public SqlToPlanHandler toPlanHandler() {
    try {
      return (SqlToPlanHandler) Class.forName("com.dremio.service.reflection.refresh.RefreshHandler").newInstance();
    } catch (ReflectiveOperationException e) {
      throw Throwables.propagate(e);
    }
  }
}
