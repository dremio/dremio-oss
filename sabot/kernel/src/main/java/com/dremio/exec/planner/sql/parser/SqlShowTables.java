/*
 * Copyright (C) 2017 Dremio Corporation
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

import com.google.common.collect.Lists;

/**
 * Sql parse tree node to represent statement:
 * SHOW TABLES [{FROM | IN} db_name] [LIKE 'pattern']
 */
public class SqlShowTables extends SqlCall {

  private final SqlIdentifier db;
  private final SqlNode likePattern;

  public static final SqlSpecialOperator OPERATOR =
    new SqlSpecialOperator("SHOW_TABLES", SqlKind.OTHER) {
    @Override
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
      return new SqlShowTables(pos, (SqlIdentifier) operands[0], operands[1]);
    }
  };

  public SqlShowTables(SqlParserPos pos, SqlIdentifier db, SqlNode likePattern) {
    super(pos);
    this.db = db;
    this.likePattern = likePattern;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    List<SqlNode> opList = Lists.newArrayList();
    opList.add(db);
    opList.add(likePattern);
    return opList;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("SHOW");
    writer.keyword("TABLES");
    if (db != null) {
      db.unparse(writer, leftPrec, rightPrec);
    }
    if (likePattern != null) {
      writer.keyword("LIKE");
      likePattern.unparse(writer, leftPrec, rightPrec);
    }
  }

  public SqlIdentifier getDb() { return db; }
  public SqlNode getLikePattern() { return likePattern; }


}
