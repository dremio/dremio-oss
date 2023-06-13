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

import static com.dremio.exec.planner.VacuumOutputSchema.getRelDataType;

import java.util.List;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
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
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import com.dremio.exec.planner.sql.handlers.query.SqlToPlanHandler;
import com.dremio.exec.planner.sql.handlers.query.VacuumTableHandler;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;


public class SqlVacuumTable extends SqlVacuum implements SqlToPlanHandler.Creator {
  public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("VACUUM", SqlKind.OTHER) {
    @Override
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
      Preconditions.checkArgument(operands.length == 3, "SqlVacuumTable.createCall() " +
        "has 3 operands!");
      return new SqlVacuumTable(
        pos,
        (SqlIdentifier) operands[0],
        (SqlNodeList) operands[1],
        (SqlNodeList) operands[2]);
    }

    @Override
    public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
      final RelDataTypeFactory typeFactory = validator.getTypeFactory();
      return getRelDataType(typeFactory);
    }
  };

  private final SqlIdentifier table;

  /**
   * Creates a SqlVacuum.
   */
  public SqlVacuumTable(
    SqlParserPos pos,
    SqlIdentifier table,
    SqlNodeList optionsList,
    SqlNodeList optionsValueList) {
    super(pos, optionsList, optionsValueList);
    this.table = table;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  public SqlIdentifier getTable() {
    return table;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return ImmutableList.of(
        table,
        optionsList,
        optionsValueList);
  }

  @Override
  public NamespaceKey getPath() {
    return new NamespaceKey(table.names);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("VACUUM");
    writer.keyword("TABLE");
    table.unparse(writer, leftPrec, rightPrec);

    writer.keyword("EXPIRE");
    writer.keyword("SNAPSHOTS");
    if(optionsList != null) {
      for (int i = 0; i < optionsList.size(); i++) {
        optionsList.get(i).unparse(writer, leftPrec, rightPrec);
        optionsValueList.get(i).unparse(writer, leftPrec, rightPrec);
      }
    }
  }

  @Override
  public SqlToPlanHandler toPlanHandler() {
    return new VacuumTableHandler();
  }
}
