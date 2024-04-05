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
package com.dremio.common.rel2sql.utilities;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.fun.SqlCase;

/**
 * An processor that finds alias references from the select list for the order by clause in the
 * query.
 */
public class OrderByAliasProcessor {
  private final SqlNodeList originalOrderBy;
  private final SqlNodeList updatedOrderBy;
  private final String tableAlias;
  private final List<SqlNode> selectList;
  private final Map<List<String>, List<String>> aliasCache = new HashMap<>();

  public OrderByAliasProcessor(
      SqlNodeList originalOrderBy, String tableAlias, List<SqlNode> selectList) {
    this.originalOrderBy = SqlNodeList.clone(originalOrderBy);
    this.updatedOrderBy = new SqlNodeList(originalOrderBy.getParserPosition());
    this.tableAlias = tableAlias;
    this.selectList = Collections.unmodifiableList(selectList);
  }

  /**
   * Given a SqlNodeList that consists of SqlNodes from an order by clause, evaluate whether each
   * SqlNode contains a column reference with an alias in the select list.
   *
   * @return a new SqlNodeList containing rewritten SqlNodes with correct table and column alias
   *     references.
   */
  public SqlNodeList processOrderBy() {
    for (SqlNode node : originalOrderBy) {
      updatedOrderBy.add(processSqlNodeInOrderBy(node));
    }
    return updatedOrderBy;
  }

  /**
   * Given a SqlNode, evaluate whether it contains a SqlIdentifier that contains a column reference
   * with an alias in the select list. If an alias of the column reference is found in the select
   * list, rewrite the node such that it references the table alias and the column alias set in the
   * select list.
   *
   * <p>If no alias corresponding to the SqlNode is found in the select list, simply return a
   * rewritten SqlNode that references the table alias and its current column name.
   *
   * <p>Only SqlNode of kind CASE, NULLS_FIRST, NULLS_LAST, IS_NULL, DESCENDING and IDENTIFIER needs
   * to be processed.
   *
   * @param orderNode the SqlNode to process.
   * @return If SqlNode is one of the supported kind to process, returns a new SqlNode containing
   *     updated table and column alias references. Otherwise, returns the given SqlNode as is.
   */
  private SqlNode processSqlNodeInOrderBy(SqlNode orderNode) {
    switch (orderNode.getKind()) {
      case CASE:
        return handleSqlCase((SqlCase) orderNode);
      case NULLS_FIRST:
      case NULLS_LAST:
      case IS_NULL:
      case DESCENDING:
        ((SqlBasicCall) orderNode)
            .setOperand(0, processSqlNodeInOrderBy(((SqlBasicCall) orderNode).operand(0)));
        return orderNode;
      case IDENTIFIER:
        return handleSqlIdentifier((SqlIdentifier) orderNode);
      default:
        return orderNode;
    }
  }

  /**
   * Creates a new SqlCase that wraps a list of processed when operands. Only the when list needs to
   * be processed because ArpDialect - emulateNullDirection produces a SqlCase node of the following
   * format: Case When "SqlNode" Then 1 Else 0
   *
   * @param orderNode the given SqlNode from the order by clause.
   * @return a new SqlCase object with an updated list of when operands.
   */
  private SqlNode handleSqlCase(SqlCase orderNode) {
    final SqlNodeList whenOperands = orderNode.getWhenOperands();
    final List<SqlNode> processedOperands = new ArrayList<>();
    for (SqlNode whenOperand : whenOperands) {
      processedOperands.add(processSqlNodeInOrderBy(whenOperand));
    }
    return new SqlCase(
        orderNode.getParserPosition(),
        orderNode.getValueOperand(),
        new SqlNodeList(processedOperands, whenOperands.getParserPosition()),
        orderNode.getThenOperands(),
        orderNode.getElseOperand());
  }

  /**
   * Creates a clone of the given SqlIdentifier node, with updated table and column alias names.
   * Given the names in the original SqlIdentifier node, it searches for its alias in the select
   * list. A cloned SqlIdentifier is create with the updated table alias. If an alias for the column
   * is found in the select list, the column alias is used. Otherwise, its current name is used.
   *
   * @param orderNode the given SqlNode from the order by clause.
   * @return a cloned SqlIdentifier node with a list of updated names.
   */
  private SqlNode handleSqlIdentifier(SqlIdentifier orderNode) {
    final List<String> names = new ArrayList<>();
    names.add(tableAlias);

    // When pushing out order by, we need to make sure that the order by clause uses the correct
    // alias
    // set in the select list of the sub-query it originated from
    if (!selectList.isEmpty()) {
      final List<String> orderNodeNames = orderNode.names;
      boolean isAliasInCache = aliasCache.containsKey(orderNodeNames);
      List<String> alias = aliasCache.get(orderNodeNames);

      // First, look for column alias from the cache
      if (isAliasInCache && (alias != null)) {
        names.addAll(aliasCache.get(orderNodeNames));
      } else if (!isAliasInCache && (alias == null)) {
        final List<String> columnAliasFromSelect = findColumnAliasFromSelectList(orderNodeNames);
        if (!columnAliasFromSelect.isEmpty()) {
          names.addAll(columnAliasFromSelect);
        }
      }
    }

    // Order by column does not have a column alias set in the select list of this query
    if (names.size() == 1) {
      // Only add the last element from the identifier names as it is the column name
      // The schema path is not needed as the pushed out order by clause will
      // be using a table alias to access the column.
      names.add(orderNode.names.get(orderNode.names.size() - 1));
    }

    final SqlNode sqlIdentifier = orderNode.clone(orderNode.getParserPosition());
    ((SqlIdentifier) sqlIdentifier).setNames(names, null);
    return sqlIdentifier;
  }

  /**
   * Given a list representing the table accessor and name of the column, find out whether the
   * column is given an alias in the select list.
   *
   * @param orderNodeNames the table accessor and name of the column
   * @return If an alias is found, return the alias name. Otherwise, return an empty list of String.
   */
  private List<String> findColumnAliasFromSelectList(List<String> orderNodeNames) {
    for (SqlNode selectItem : selectList) {
      if (selectItem.getKind() == SqlKind.AS) {

        final List<SqlNode> operands = ((SqlCall) selectItem).getOperandList();
        final SqlNode column = operands.get(0);
        final SqlNode columnAlias = operands.get(1);

        if (column instanceof SqlIdentifier
            && columnAlias instanceof SqlIdentifier
            && orderNodeNames.equals(((SqlIdentifier) column).names)) {
          List<String> columnAliasNames = ((SqlIdentifier) columnAlias).names;
          aliasCache.putIfAbsent(orderNodeNames, columnAliasNames);
          return columnAliasNames;
        }
      }
    }
    // No alias for this order by node, put null entry for this column's names
    aliasCache.putIfAbsent(orderNodeNames, null);
    return Collections.emptyList();
  }
}
