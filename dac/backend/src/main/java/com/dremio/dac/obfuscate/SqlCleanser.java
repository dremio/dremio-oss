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
package com.dremio.dac.obfuscate;

import com.dremio.exec.calcite.SqlNodes;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.sql.ParserConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlShuttle;

/**
 * Tool to redact the identifiers and string literals in a sql statement This is package-protected
 * and all usages should be via ObfuscationUtils because this does not check whether to obfuscate or
 * not.
 */
class SqlCleanser {
  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(SqlCleanser.class);
  private static final ParserConfig CONFIG =
      new ParserConfig(
          Quoting.DOUBLE_QUOTE,
          1000,
          true,
          PlannerSettings.FULL_NESTED_SCHEMA_SUPPORT.getDefault().getBoolVal());

  private final FromVisitor fromVisitor = new FromVisitor();
  private final IdentifierAndLiteralFinder finder = new IdentifierAndLiteralFinder();

  public static String cleanseSql(String sql) {
    String cleansedSql;
    SqlParser parser = SqlParser.create(sql, CONFIG);
    try {
      SqlNodeList sqlNodeList = parser.parseStmtList();

      SqlCleanser cleanser = new SqlCleanser();
      SqlNode cleansedTree = sqlNodeList.get(0).accept(cleanser.fromVisitor);
      cleansedSql = SqlNodes.toSQLString(cleansedTree);
    } catch (Exception e) {
      String errorMsg =
          "Exception while parsing sql, so obfuscating whole sql with a random hex string. ";
      LOGGER.error(errorMsg, e);
      cleansedSql = errorMsg + ObfuscationUtils.obfuscatePartial(sql);
    }
    return cleansedSql;
  }

  private final class FromVisitor extends SqlShuttle {
    @Override
    public SqlNode visit(SqlCall call) {
      switch (call.getKind()) {
        case SELECT:
          return processSelect((SqlSelect) call);
        case JOIN:
          return processJoin((SqlJoin) call);
        case ORDER_BY:
          return processOrderBy((SqlOrderBy) call);
        case WITH:
          return processWith((SqlWith) call);
        case WITH_ITEM:
          return processWithItem((SqlWithItem) call);
        default:
          return super.visit(call);
      }
    }

    @Override
    public SqlNode visit(SqlIdentifier id) {
      if (!ObfuscationUtils.shouldObfuscateFull()) {
        return id;
      }

      List<String> redactedNames =
          id.names.stream()
              .map(s -> Integer.toHexString(s.toLowerCase().hashCode()))
              .collect(Collectors.toList());
      return new SqlIdentifier(redactedNames, id.getCollation(), id.getParserPosition(), null);
    }

    private SqlCall processWithItem(SqlWithItem withItem) {
      List<SqlNode> ops = new ArrayList<>(withItem.getOperandList());
      if (withItem.name != null) {
        ops.set(0, withItem.name.accept(this));
      }
      if (withItem.columnList != null) {
        ops.set(1, withItem.columnList.accept(finder));
      }
      if (withItem.query != null) {
        ops.set(2, withItem.query.accept(this));
      }
      return withItem.getOperator().createCall(withItem.getParserPosition(), ops);
    }

    private SqlCall processWith(SqlWith with) {
      List<SqlNode> ops = new ArrayList<>(with.getOperandList());
      if (with.withList != null) {
        ops.set(0, with.withList.accept(this));
      }
      if (with.body != null) {
        ops.set(1, with.body.accept(this));
      }
      return with.getOperator().createCall(with.getParserPosition(), ops);
    }

    private SqlCall processOrderBy(SqlOrderBy orderBy) {
      List<SqlNode> ops = new ArrayList<>(orderBy.getOperandList());
      if (orderBy.query != null) {
        ops.set(0, orderBy.query.accept(this));
      }
      if (orderBy.orderList != null) {
        ops.set(1, orderBy.orderList.accept(finder));
      }
      return orderBy.getOperator().createCall(orderBy.getParserPosition(), ops);
    }

    private SqlCall processSelect(SqlSelect select) {
      List<SqlNode> ops = new ArrayList<>(select.getOperandList());
      if (select.getSelectList() != null) {
        ops.set(1, select.getSelectList().accept(finder));
      }
      if (select.getFrom() != null) {
        ops.set(2, select.getFrom().accept(this));
      }
      if (select.hasWhere()) {
        ops.set(3, select.getWhere().accept(finder));
      }
      if (select.getGroup() != null) {
        ops.set(4, select.getGroup().accept(finder));
      }
      if (select.getHaving() != null) {
        ops.set(5, select.getHaving().accept(finder));
      }
      if (select.hasOrderBy()) {
        ops.set(7, select.getOrderList().accept(finder));
      }
      return select.getOperator().createCall(select.getParserPosition(), ops);
    }

    private SqlCall processJoin(SqlJoin join) {
      List<SqlNode> ops = new ArrayList<>(join.getOperandList());
      if (join.getLeft() != null) {
        ops.set(0, join.getLeft().accept(this));
      }
      if (join.getRight() != null) {
        ops.set(3, join.getRight().accept(this));
      }
      if (join.getCondition() != null) {
        ops.set(5, join.getCondition().accept(finder));
      }
      return join.getOperator().createCall(join.getParserPosition(), ops);
    }
  }

  private final class IdentifierAndLiteralFinder extends SqlShuttle {

    @Override
    public SqlNode visit(SqlCall call) {
      switch (call.getKind()) {
        case SELECT:
        case JOIN:
          return call.accept(fromVisitor);
        default:
      }
      return super.visit(call);
    }

    @Override
    public SqlNode visit(SqlLiteral literal) {
      if (!ObfuscationUtils.shouldObfuscatePartial()) {
        return literal;
      }
      if (literal instanceof SqlCharStringLiteral) {
        SqlCharStringLiteral charStringLiteral = (SqlCharStringLiteral) literal;
        String value = charStringLiteral.toValue();
        String newValue = "literal_" + Integer.toHexString(value.hashCode());
        return SqlLiteral.createCharString(
            newValue,
            charStringLiteral.getNlsString().getCharsetName(),
            literal.getParserPosition());
      }
      return literal;
    }

    @Override
    public SqlNode visit(SqlIdentifier id) {
      if (!ObfuscationUtils.shouldObfuscateFull()) {
        return id;
      }
      List<String> names = new ArrayList<>(id.names);
      for (int i = 0; i < names.size() - 1; i++) {
        names.set(i, Integer.toHexString(names.get(i).toLowerCase().hashCode()));
      }
      names.set(
          names.size() - 1,
          "field_" + Integer.toHexString(names.get(names.size() - 1).toLowerCase().hashCode()));
      return new SqlIdentifier(names, id.getCollation(), id.getParserPosition(), null);
    }
  }
}
