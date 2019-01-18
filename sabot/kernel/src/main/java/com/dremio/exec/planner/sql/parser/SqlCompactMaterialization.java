/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import com.dremio.exec.planner.sql.handlers.query.SqlToPlanHandler;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

public class SqlCompactMaterialization extends SqlCall implements SqlToPlanHandler.Creator {

  public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("COMPACT_MATERIALIZATION", SqlKind.OTHER) {
    @Override
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
      Preconditions.checkArgument(operands.length == 2, "SqlCompactRefresh.createCall() has to get 2 operands!");
      return new SqlCompactMaterialization(pos, operands[0], operands[1]);
    }
  };

  private SqlIdentifier materializationPath;
  private SqlLiteral newMaterializationId;

  public SqlCompactMaterialization(SqlParserPos pos, SqlNode materializationPath, SqlNode newMaterializationPath) {
    super(pos);
    this.materializationPath = (SqlIdentifier) materializationPath;
    this.newMaterializationId = (SqlLiteral) newMaterializationPath;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return ImmutableList.of(materializationPath, newMaterializationId);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("COMPACT");
    writer.keyword("REFRESH");
    materializationPath.unparse(writer, leftPrec, rightPrec);
    writer.keyword("AS");
    newMaterializationId.unparse(writer, leftPrec, rightPrec);
  }

  public List<String> getMaterializationPath() {
    return materializationPath.names;
  }

  public String getNewMaterializationId() {
    return newMaterializationId.toValue();
  }

  @Override
  public SqlToPlanHandler toPlanHandler() {
    try {
      return (SqlToPlanHandler) Class.forName("com.dremio.service.reflection.compact.CompactRefreshHandler").newInstance();
    } catch (ReflectiveOperationException e) {
      throw Throwables.propagate(e);
    }
  }
}
