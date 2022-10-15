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

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlUnresolvedFunction;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.dremio.exec.catalog.TableVersionContext;
import com.dremio.exec.catalog.TableVersionType;

/**
 * Implementation of SqlCall which wraps TableMacro calls that have time travel version specifications.
 * This call expects that it's operator is always either a {@link SqlUnresolvedFunction} (prior to TableMacro
 * resolution) or a {@link SqlVersionedTableMacro} operator.  It serves as a conduit for passing the parsed
 * version specification, kept in a {@link TableVersionSpec} instance.
 */
public class SqlVersionedTableMacroCall extends SqlBasicCall {

  private final TableVersionSpec tableVersionSpec;
  private final SqlIdentifier alias;

  public SqlVersionedTableMacroCall(SqlOperator operator, SqlNode[] operands, TableVersionType tableVersionType,
                                    SqlNode versionSpecifier, SqlIdentifier alias, SqlParserPos pos) {
    super(operator, operands, pos);
    this.tableVersionSpec = new TableVersionSpec(tableVersionType, versionSpecifier);
    this.alias = alias;
  }

  public TableVersionSpec getTableVersionSpec() {
    return tableVersionSpec;
  }

  public TableVersionContext getResolvedTableVersionContext() {
    return tableVersionSpec.getResolvedTableVersionContext();
  }

  public SqlIdentifier getAlias() {
    return alias;
  }

  @Override
  public void setOperator(SqlOperator operator) {
    if (!(operator instanceof SqlVersionedTableMacro || operator instanceof SqlUnresolvedFunction)) {
      throw new UnsupportedOperationException("Function does not support time travel version specifications");
    }
    super.setOperator(operator);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    SqlCharStringLiteral tableLiteral = (SqlCharStringLiteral)getOperands()[0] ;
    writer.print(tableLiteral.getNlsString().getValue() + " ");
    writer.keyword("AT");
    getTableVersionSpec().unparseVersionSpec(writer, leftPrec, rightPrec);
  }

}
