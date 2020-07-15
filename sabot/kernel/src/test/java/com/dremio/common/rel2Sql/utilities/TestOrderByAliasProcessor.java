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
package com.dremio.common.rel2Sql.utilities;

import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.List;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Litmus;
import org.junit.Test;

import com.dremio.common.rel2sql.utilities.OrderByAliasProcessor;
import com.google.common.collect.ImmutableList;

/**
 * Unit test class for {@code com.dremio.common.rel2sql.utilities.OrderByAliasProcessor}
 */
public class TestOrderByAliasProcessor {
  private static final String TABLE_ALIAS = "t0";
  private static final String TABLE_ACCESSOR = "dremio_integer";
  private static final String KEY_COL_NAME = "key";
  private static final String VAL_COL_NAME = "val";
  private static final String KEY_COL_ALIAS = "KEY$0";
  private static final String VAL_COL_ALIAS = "VAL$0";

  @Test
  public void testSimpleOrderByWithSelectStar() {
    // select *
    // order by dremio_integer.key

    testProcessSqlNodeInOrderBy(
      SqlNodeList.of(newSqlIdentifier(TABLE_ACCESSOR, KEY_COL_NAME)),
      Collections.emptyList(),
      SqlNodeList.of(newSqlIdentifier(TABLE_ALIAS, KEY_COL_NAME)));
  }

  @Test
  public void testSimpleOrderByNoAliasFromSelect() {
    // select dremio_integer.val
    // order by dremio_integer.key

    testProcessSqlNodeInOrderBy(
      SqlNodeList.of(newSqlIdentifier(TABLE_ACCESSOR, KEY_COL_NAME)),
      ImmutableList.of(newSqlIdentifier(TABLE_ACCESSOR, VAL_COL_NAME)),
      SqlNodeList.of(newSqlIdentifier(TABLE_ALIAS, KEY_COL_NAME)));
  }

  @Test
  public void testSimpleOrderbyWithAliasFromSelect() {
    // select dremio_integer.val, dremio_integer.key as KEY$0
    // order by dremio_integer.key

    testProcessSqlNodeInOrderBy(
      SqlNodeList.of(newSqlIdentifier(TABLE_ACCESSOR, KEY_COL_NAME)),
      ImmutableList.of(newSqlIdentifier(TABLE_ACCESSOR, VAL_COL_NAME),
        newSqlCall(SqlStdOperatorTable.AS,
          newSqlIdentifier(TABLE_ACCESSOR, KEY_COL_NAME),
          newSqlIdentifier(KEY_COL_ALIAS))),
      SqlNodeList.of(newSqlIdentifier(TABLE_ALIAS, KEY_COL_ALIAS)));
  }

  @Test
  public void testOrderByDescWrappingIdWithSelectAlias() {
    // select dremio_integer.val, dremio_integer.key as KEY$0
    // order by dremio_integer.key desc

    SqlOperator desc = SqlStdOperatorTable.DESC;

    testProcessSqlNodeInOrderBy(
      SqlNodeList.of(newSqlCall(desc, newSqlIdentifier(TABLE_ACCESSOR, KEY_COL_NAME))),
      ImmutableList.of(newSqlIdentifier(TABLE_ACCESSOR, VAL_COL_NAME),
        newSqlCall(SqlStdOperatorTable.AS,
          newSqlIdentifier(TABLE_ACCESSOR, KEY_COL_NAME),
          newSqlIdentifier(KEY_COL_ALIAS))),
      SqlNodeList.of(newSqlCall(desc, newSqlIdentifier(TABLE_ALIAS, KEY_COL_ALIAS))));
  }

  @Test
  public void testOrderByNullsFirstWrappingDescIDWithSelectAlias() {
    // select dremio_integer.val, dremio_integer.key as KEY$0
    // order by dremio_integer.key nulls first desc

    SqlOperator nullsFirst = SqlStdOperatorTable.NULLS_FIRST;
    SqlOperator desc = SqlStdOperatorTable.DESC;

    testProcessSqlNodeInOrderBy(
      SqlNodeList.of(newSqlCall(nullsFirst, newSqlCall(desc, newSqlIdentifier(TABLE_ACCESSOR, KEY_COL_NAME)))),
      ImmutableList.of(newSqlIdentifier(TABLE_ACCESSOR, VAL_COL_NAME),
        newSqlCall(SqlStdOperatorTable.AS,
          newSqlIdentifier(TABLE_ACCESSOR, KEY_COL_NAME),
          newSqlIdentifier(KEY_COL_ALIAS))),
      SqlNodeList.of(newSqlCall(nullsFirst, newSqlCall(desc, newSqlIdentifier(TABLE_ALIAS, KEY_COL_ALIAS)))));
  }

  @Test
  public void testOrderByNullsLastWrappingDescIDWithSelectAlias() {
    // select dremio_integer.val, dremio_integer.key as KEY$0
    // order by dremio_integer.key nulls last desc

    SqlOperator nullsLast = SqlStdOperatorTable.NULLS_LAST;
    SqlOperator desc = SqlStdOperatorTable.DESC;

    testProcessSqlNodeInOrderBy(
      SqlNodeList.of(newSqlCall(nullsLast, newSqlCall(desc, newSqlIdentifier(TABLE_ACCESSOR, KEY_COL_NAME)))),
      ImmutableList.of(newSqlIdentifier(TABLE_ACCESSOR, VAL_COL_NAME),
        newSqlCall(SqlStdOperatorTable.AS,
          newSqlIdentifier(TABLE_ACCESSOR, KEY_COL_NAME),
          newSqlIdentifier(KEY_COL_ALIAS))),
      SqlNodeList.of(newSqlCall(nullsLast, newSqlCall(desc, newSqlIdentifier(TABLE_ALIAS, KEY_COL_ALIAS)))));
  }

  @Test
  public void testOrderByWithCaseWrappingIsNullIdWithSelectAlias() {
    // select dremio_integer.val, dremio_integer.key as KEY$0
    // order by case when dremio_integer.key is null then 1 else 0

    SqlOperator isNull = SqlStdOperatorTable.IS_NULL;

    testProcessSqlNodeInOrderBy(
      SqlNodeList.of(newSqlCase(
        SqlNodeList.of(isNull.createCall(SqlParserPos.ZERO, newSqlIdentifier(TABLE_ACCESSOR, KEY_COL_NAME))),
        SqlNodeList.of(SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO)),
        SqlNodeList.of(SqlLiteral.createExactNumeric("0", SqlParserPos.ZERO)))),
      ImmutableList.of(newSqlIdentifier(TABLE_ACCESSOR, VAL_COL_NAME),
        newSqlCall(SqlStdOperatorTable.AS,
          newSqlIdentifier(TABLE_ACCESSOR, KEY_COL_NAME),
          newSqlIdentifier(KEY_COL_ALIAS))),
      SqlNodeList.of(newSqlCase(
        SqlNodeList.of(isNull.createCall(SqlParserPos.ZERO, newSqlIdentifier(TABLE_ALIAS, KEY_COL_ALIAS))),
        SqlNodeList.of(SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO)),
        SqlNodeList.of(SqlLiteral.createExactNumeric("0", SqlParserPos.ZERO))))
    );
  }

  @Test
  public void testSimpleOrderByWithMultipleColumns() {
    // select dremio_integer.key as KEY$0, dremio_integer.val as VAL$0
    // order by dremio_integer.key nulls first desc, dremio_integer.val

    SqlOperator desc = SqlStdOperatorTable.DESC;
    SqlOperator nullsFirst = SqlStdOperatorTable.NULLS_FIRST;

    testProcessSqlNodeInOrderBy(
      SqlNodeList.of(
        newSqlCall(nullsFirst,
          newSqlCall(desc, newSqlIdentifier(TABLE_ACCESSOR, KEY_COL_NAME))),
        newSqlIdentifier(TABLE_ACCESSOR, VAL_COL_NAME)),
      ImmutableList.of(
        newSqlCall(SqlStdOperatorTable.AS,
          newSqlIdentifier(TABLE_ACCESSOR, KEY_COL_NAME),
          newSqlIdentifier(KEY_COL_ALIAS)),
        newSqlCall(SqlStdOperatorTable.AS,
          newSqlIdentifier(TABLE_ACCESSOR, VAL_COL_NAME),
          newSqlIdentifier(VAL_COL_ALIAS))),
      SqlNodeList.of(
        newSqlCall(nullsFirst,
          newSqlCall(desc, newSqlIdentifier(TABLE_ALIAS, KEY_COL_ALIAS))),
        newSqlIdentifier(TABLE_ALIAS, VAL_COL_ALIAS))
    );
  }

  @Test
  public void testOrderByWithMultipleColumnsWithCase() {
    // select dremio_integer.key as KEY$0, dremio_integer.val as VAL$0
    // order by case when dremio_integer.key is null then 1 else 0 end desc,
    //          dremio_integer.val nulls last desc

    SqlOperator isNull = SqlStdOperatorTable.IS_NULL;
    SqlOperator desc = SqlStdOperatorTable.DESC;
    SqlOperator nullsLast = SqlStdOperatorTable.NULLS_LAST;

    testProcessSqlNodeInOrderBy(
      SqlNodeList.of(
        newSqlCase(
          SqlNodeList.of(isNull.createCall(SqlParserPos.ZERO, newSqlIdentifier(TABLE_ACCESSOR, KEY_COL_NAME))),
          SqlNodeList.of(SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO)),
          SqlNodeList.of(SqlLiteral.createExactNumeric("0", SqlParserPos.ZERO))),
        newSqlCall(nullsLast,
          newSqlCall(desc, newSqlIdentifier(TABLE_ACCESSOR, VAL_COL_NAME)))),
      ImmutableList.of(
        newSqlCall(SqlStdOperatorTable.AS,
          newSqlIdentifier(TABLE_ACCESSOR, KEY_COL_NAME),
          newSqlIdentifier(KEY_COL_ALIAS)),
        newSqlCall(SqlStdOperatorTable.AS,
          newSqlIdentifier(TABLE_ACCESSOR, VAL_COL_NAME),
          newSqlIdentifier(VAL_COL_ALIAS))),
      SqlNodeList.of(
        newSqlCase(
          SqlNodeList.of(isNull.createCall(SqlParserPos.ZERO, newSqlIdentifier(TABLE_ALIAS, KEY_COL_ALIAS))),
          SqlNodeList.of(SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO)),
          SqlNodeList.of(SqlLiteral.createExactNumeric("0", SqlParserPos.ZERO))),
        newSqlCall(nullsLast,
          newSqlCall(desc, newSqlIdentifier(TABLE_ALIAS, VAL_COL_ALIAS))))
    );
  }


  private SqlIdentifier newSqlIdentifier(String ... names) {
    return new SqlIdentifier(ImmutableList.copyOf(names), SqlParserPos.ZERO);
  }

  private SqlCall newSqlCall(SqlOperator operator, SqlNode ... operands) {
    return operator.createCall(SqlParserPos.ZERO, operands);
  }

  private SqlCase newSqlCase(SqlNodeList whenList, SqlNodeList thenList, SqlNode elseClause) {
    return new SqlCase(
      SqlParserPos.ZERO,
      null,
      whenList,
      thenList,
      elseClause);
  }

  private void testProcessSqlNodeInOrderBy(SqlNodeList orderBy, List<SqlNode> selectList, SqlNodeList expectedResult) {
    final OrderByAliasProcessor processor = new OrderByAliasProcessor(orderBy, TABLE_ALIAS, selectList);
    assertTrue(processor.processOrderBy().equalsDeep(expectedResult, Litmus.IGNORE));
  }
}
