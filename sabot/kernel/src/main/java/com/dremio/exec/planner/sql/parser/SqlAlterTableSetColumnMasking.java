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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
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

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.sql.handlers.direct.SimpleDirectHandler;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * CREATE [ OR REPLACE] COLUMN POLICY <policy_name>
 * AS ( <field_name> <data_type> [, ... ] )
 * RETURN <data_type>
 * <policy_sql_expression>
 */
public class SqlAlterTableSetColumnMasking extends SqlAlterTable implements SimpleDirectHandler.Creator {
  private final SqlIdentifier columnName;
  private final SqlPolicy columnMasking;

  public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("SET_COLUMN_MASKING", SqlKind.OTHER) {
    @Override
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
      Preconditions.checkArgument(operands.length == 3, "SqlAlterTableSetColumnMasking.createCall() has to get 3 operands!");
      return new SqlAlterTableSetColumnMasking(
        pos,
        (SqlIdentifier) operands[0],
        (SqlIdentifier) operands[1],
        (SqlPolicy) operands[2]
      );
    }
  };

  public SqlAlterTableSetColumnMasking(SqlParserPos pos, SqlIdentifier tblName, SqlIdentifier columnName, SqlPolicy columnMasking) {
    super(pos, tblName);
    this.columnName = columnName;
    this.columnMasking = columnMasking;
  }

  public SqlIdentifier getColumnName() {
    return columnName;
  }

  public SqlPolicy getColumnMasking() {
    return columnMasking;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return ImmutableList.of(
      tblName,
      columnName,
      columnMasking);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    super.unparse(writer, leftPrec, rightPrec);
    writer.keyword("MODIFY");
    writer.keyword("COLUMN");
    columnName.unparse(writer, leftPrec, rightPrec);
    writer.keyword("SET");
    writer.keyword("MASKING");
    writer.keyword("POLICY");
    columnMasking.unparse(writer, leftPrec, rightPrec);
  }

  @Override
  public SimpleDirectHandler toDirectHandler(QueryContext context) {
    if (!context.getOptions().getOption(ExecConstants.ENABLE_NATIVE_ROW_COLUMN_POLICIES)) {
      throw new UnsupportedOperationException("Native row/column policies are not supported.");
    }

    try {
      final Class<?> cl = Class.forName("com.dremio.exec.planner.sql.handlers.EnterpriseAlterTableSetColumnMasking");
      final Constructor<?> ctor = cl.getConstructor(QueryContext.class);
      return (SimpleDirectHandler) ctor.newInstance(context);
    } catch (InstantiationException | IllegalAccessException | ClassNotFoundException | NoSuchMethodException | InvocationTargetException e2) {
      final UserException.Builder exceptionBuilder = UserException.unsupportedError()
        .message("This command is not supported in this edition of Dremio.");
      throw exceptionBuilder.buildSilently();
    }
  }
}
