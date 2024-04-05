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

import com.dremio.service.namespace.NamespaceKey;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * Sql parse tree node for SQL command: SHOW CREATE (VIEW | TABLE) datasetName [ AT ( REF[ERENCE] |
 * BRANCH | TAG | COMMIT ) refValue ]
 */
public class SqlShowCreate extends SqlCall {

  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("SHOW_CREATE_VIEW", SqlKind.OTHER) {
        @Override
        public SqlCall createCall(
            SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
          return new SqlShowCreate(
              pos,
              ((SqlLiteral) operands[0]).booleanValue(),
              (SqlIdentifier) operands[1],
              operands[2] != null
                  ? ((SqlLiteral) operands[2]).symbolValue(ReferenceType.class)
                  : null,
              (SqlIdentifier) operands[3]);
        }
      };

  private boolean isView;
  private SqlIdentifier datasetName;
  private ReferenceType refType;
  private SqlIdentifier refValue;

  public SqlShowCreate(
      SqlParserPos pos,
      boolean isView,
      SqlIdentifier datasetName,
      ReferenceType refType,
      SqlIdentifier refValue) {
    super(pos);
    this.isView = isView;
    this.datasetName = datasetName;
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
    ops.add(SqlLiteral.createBoolean(isView, SqlParserPos.ZERO));
    ops.add(datasetName);
    ops.add(getRefType() == null ? null : SqlLiteral.createSymbol(getRefType(), SqlParserPos.ZERO));
    ops.add(refValue);
    return ops;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("SHOW");
    writer.keyword("CREATE");
    writer.keyword(isView ? "VIEW" : "TABLE");

    datasetName.unparse(writer, leftPrec, rightPrec);

    if (refType != null && refValue != null) {
      writer.keyword("AT");
      writer.keyword(refType.toString());
      refValue.unparse(writer, leftPrec, rightPrec);
    }
  }

  public NamespaceKey getPath() {
    return new NamespaceKey(datasetName.names);
  }

  public String getFullName() {
    if (datasetName.isSimple()) {
      return datasetName.getSimple();
    }
    return datasetName.names.stream().collect(Collectors.joining("."));
  }

  public ReferenceType getRefType() {
    return refType;
  }

  public SqlIdentifier getRefValue() {
    return refValue;
  }

  public boolean getIsView() {
    return isView;
  }
}
