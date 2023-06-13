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

import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlVisitor;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.TableVersionType;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.sql.ParserConfig;
import com.dremio.service.Pointer;
import com.google.common.collect.Lists;

/**
 * Helper methods or constants used in parsing a SQL query.
 */
public class ParserUtil {

  public static SqlNode createCondition(SqlNode left, SqlOperator op, SqlNode right) {

    // if one of the operands is null, return the other
    if (left == null || right == null) {
      return left != null ? left : right;
    }

    List<Object> listCondition = Lists.newArrayList();
    listCondition.add(left);
    listCondition.add(new SqlParserUtil.ToTreeListItem(op, SqlParserPos.ZERO));
    listCondition.add(right);

    return SqlParserUtil.toTree(listCondition);
  }

  public static String unparseIdentifier(SqlIdentifier identifier) {
    StringBuilder buf = new StringBuilder();
    SqlWriterConfig config = SqlPrettyWriter.config()
        .withDialect(CalciteSqlDialect.DEFAULT)
        .withQuoteAllIdentifiers(false);
    SqlPrettyWriter writer = new SqlPrettyWriter(config, buf);
    identifier.unparse(writer, 0, 0);
    return buf.toString();
  }

  public static void validateParsedViewQuery(SqlNode viewQuery) throws UserException {
    if (viewQuery.isA(SqlKind.DML)
      || viewQuery.isA(SqlKind.DDL)
    ) {
      throw UserException.unsupportedError().message("Cannot create view on statement of this type").buildSilently();
    }
  }

  public static void validateViewQuery(String viewQuery) throws UserException {
    ParserConfig PARSER_CONFIG = new ParserConfig(
      Quoting.DOUBLE_QUOTE,
      1000,
      true,
      PlannerSettings.FULL_NESTED_SCHEMA_SUPPORT.getDefault().getBoolVal());
    SqlParser parser = SqlParser.create(viewQuery, PARSER_CONFIG);
    SqlNode sqlNode = null;
    try {
      sqlNode = parser.parseStmt();
    } catch (SqlParseException parseException) {
      // Don't catch real parser exceptions here. The purpose for this methodis only for catching unsupported
      // query types from successful parsed statments. If there is areal parser error, the existing  flow
      // to run the query and get back job results will catch and handle the parse exception correctly with the
      // error handling that's already in place.
      return;
    }
    validateParsedViewQuery(sqlNode);
  }

  public static boolean checkTimeTravelOnView(String viewQuery) throws UserException {
    ParserConfig PARSER_CONFIG = new ParserConfig(
      Quoting.DOUBLE_QUOTE,
      1000,
      true,
      PlannerSettings.FULL_NESTED_SCHEMA_SUPPORT.getDefault().getBoolVal());
    SqlParser parser = SqlParser.create(viewQuery, PARSER_CONFIG);
    try {
      SqlNode sqlNode = parser.parseStmt();
      return isTimeTravelQuery(sqlNode);
    } catch (SqlParseException parseException) {
      // Don't catch real parser exceptions here. The purpose for this methodis only for catching unsupported
      // query types from successful parsed statments. If there is areal parser error, the existing  flow
      // to run the query and get back job results will catch and handle the parse exception correctly with the
      // error handling that's already in place.
      return false;
    }

  }

  public static boolean isTimeTravelQuery(SqlNode sqlNode) {
    Pointer<Boolean> timeTravel = new Pointer<>(false);
    SqlVisitor<Void> visitor = new SqlBasicVisitor<Void>() {
      @Override
      public Void visit(SqlCall call) {
        if ((call instanceof SqlVersionedTableMacroCall) &&
          ((((SqlVersionedTableMacroCall) call).getTableVersionSpec().getTableVersionType() == TableVersionType.TIMESTAMP) ||
            (((SqlVersionedTableMacroCall) call).getTableVersionSpec().getTableVersionType() == TableVersionType.SNAPSHOT_ID))) {
          timeTravel.value = true;
          return null;
        }

        return super.visit(call);
      }
    };

    sqlNode.accept(visitor);
    return timeTravel.value;
  }

}
