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

import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.catalog.model.dataset.TableVersionType;
import com.google.common.collect.Lists;

/**
 * Implementation of SqlCall which  serves as a conduit for passing the parsed
 * version specification, kept in a {@link TableVersionSpec} instance.
 */
public class SqlTableVersionSpec extends SqlCall {
  public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("AT_VERSION", SqlKind.OTHER);
  public static final SqlTableVersionSpec NOT_SPECIFIED = new SqlTableVersionSpec(SqlParserPos.ZERO, TableVersionType.NOT_SPECIFIED, SqlLiteral.createCharString("MAIN", SqlParserPos.ZERO));
  private final TableVersionSpec tableVersionSpec;

  public SqlTableVersionSpec(SqlParserPos pos, TableVersionType tableVersionType,
                             SqlNode versionSpecifier ) {
    super( pos);
    this.tableVersionSpec = new TableVersionSpec(tableVersionType, versionSpecifier, null);
  }

  public TableVersionSpec getTableVersionSpec() {
    return tableVersionSpec;
  }

  public TableVersionContext getResolvedTableVersionContext() {
    return tableVersionSpec.getResolvedTableVersionContext();
  }


  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    List<SqlNode> operandList = Lists.newArrayList();
    operandList.add(new SqlIdentifier(getTableVersionSpec().getTableVersionType().toSqlRepresentation(), SqlParserPos.ZERO));
    operandList.add(getTableVersionSpec().getVersionSpecifier());
    operandList.add(getTableVersionSpec().getTimestamp());
    return operandList;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("AT");
    getTableVersionSpec().unparseVersionSpec(writer, leftPrec, rightPrec);
  }

  @Override
  public String toString() {
    return tableVersionSpec.toString();
  }

}
