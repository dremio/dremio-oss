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
package com.dremio.exec.planner.sql;

import static org.apache.calcite.avatica.util.Quoting.DOUBLE_QUOTE;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTableIdentifierCollector {

  private static final ParserConfig parserConfig = new ParserConfig(DOUBLE_QUOTE, 128, false);

  @Test
  public void testTempTable() throws Exception {
    final String sql = "with temp(c1) as (select c1 from T1) select distinct c1 from temp";

    final SqlParser parser = SqlParser.create(sql, parserConfig);
    final SqlNode sqlNode = parser.parseStmt();

    final List<SqlIdentifier> tableIdentifiers = TableIdentifierCollector.collect(sqlNode);
    Assertions.assertEquals(1, tableIdentifiers.size());
    Assertions.assertIterableEquals(ImmutableList.of("T1"), tableIdentifiers.get(0).names);
  }

  @Test
  public void testNamedIdentifiers() throws Exception {
    final String sql =
        "select w.c1 as col1, x.c2 as col2, y.c3 as col3, z.c4 as col4 "
            + "from T1 w, T2 x, T3 as y, T4 as z";

    final SqlParser parser = SqlParser.create(sql, parserConfig);
    final SqlNode sqlNode = parser.parseStmt();

    final List<SqlIdentifier> tableIdentifiers = TableIdentifierCollector.collect(sqlNode);
    Assertions.assertEquals(4, tableIdentifiers.size());
    Assertions.assertIterableEquals(ImmutableList.of("T1"), tableIdentifiers.get(0).names);
    Assertions.assertIterableEquals(ImmutableList.of("T2"), tableIdentifiers.get(1).names);
    Assertions.assertIterableEquals(ImmutableList.of("T3"), tableIdentifiers.get(2).names);
    Assertions.assertIterableEquals(ImmutableList.of("T4"), tableIdentifiers.get(3).names);
  }

  @Test
  public void testJoin() throws Exception {
    final String sql =
        "select x.c1, y.c2, z.c3 "
            + "from T1 as x "
            + "join T2 as y on x.c1 = y.c1 "
            + "join T3 as z on x.c1 = z.c1";

    final SqlParser parser = SqlParser.create(sql, parserConfig);
    final SqlNode sqlNode = parser.parseStmt();

    final List<SqlIdentifier> tableIdentifiers = TableIdentifierCollector.collect(sqlNode);
    Assertions.assertEquals(3, tableIdentifiers.size());
    Assertions.assertIterableEquals(ImmutableList.of("T1"), tableIdentifiers.get(0).names);
    Assertions.assertIterableEquals(ImmutableList.of("T2"), tableIdentifiers.get(1).names);
    Assertions.assertIterableEquals(ImmutableList.of("T3"), tableIdentifiers.get(2).names);
  }

  @Test
  public void testLateralJoin() throws Exception {
    final String sql =
        "select * from T1 x left join lateral ("
            + "select * from ("
            + "select * from T2 y where x.c1 = y.c1) sub2 "
            + "inner join lateral ("
            + "select * from T3 z where x.c1 = z.c1) sub3 on sub2.c3 = sub3.c3"
            + ") sub1 on x.c2 = sub1.c2";

    final SqlParser parser = SqlParser.create(sql, parserConfig);
    final SqlNode sqlNode = parser.parseStmt();

    final List<SqlIdentifier> tableIdentifiers = TableIdentifierCollector.collect(sqlNode);
    Assertions.assertEquals(3, tableIdentifiers.size());
    Assertions.assertIterableEquals(ImmutableList.of("T1"), tableIdentifiers.get(0).names);
    Assertions.assertIterableEquals(ImmutableList.of("T2"), tableIdentifiers.get(1).names);
    Assertions.assertIterableEquals(ImmutableList.of("T3"), tableIdentifiers.get(2).names);
  }

  @Test
  public void testSetOps() throws Exception {
    final String sql =
        "select * from T1 "
            + "union select * from T2 "
            + "union all select * from T3 "
            + "intersect select * from T4 "
            + "except select * from T5";

    final SqlParser parser = SqlParser.create(sql, parserConfig);
    final SqlNode sqlNode = parser.parseStmt();

    final List<SqlIdentifier> tableIdentifiers = TableIdentifierCollector.collect(sqlNode);
    Assertions.assertEquals(5, tableIdentifiers.size());
    Assertions.assertIterableEquals(ImmutableList.of("T1"), tableIdentifiers.get(0).names);
    Assertions.assertIterableEquals(ImmutableList.of("T2"), tableIdentifiers.get(1).names);
    Assertions.assertIterableEquals(ImmutableList.of("T3"), tableIdentifiers.get(2).names);
    Assertions.assertIterableEquals(ImmutableList.of("T4"), tableIdentifiers.get(3).names);
    Assertions.assertIterableEquals(ImmutableList.of("T5"), tableIdentifiers.get(4).names);
  }

  @Test
  public void testSubQueryInSelect() throws Exception {
    final String sql =
        "select c1, "
            + "(select avg(c2) from T1 x where x.c1 = z.c1), "
            + "(select avg(c3) from T2 y where y.c1 = z.c1) from T3 z";

    final SqlParser parser = SqlParser.create(sql, parserConfig);
    final SqlNode sqlNode = parser.parseStmt();

    final List<SqlIdentifier> tableIdentifiers = TableIdentifierCollector.collect(sqlNode);
    Assertions.assertEquals(3, tableIdentifiers.size());
    Assertions.assertIterableEquals(ImmutableList.of("T1"), tableIdentifiers.get(0).names);
    Assertions.assertIterableEquals(ImmutableList.of("T2"), tableIdentifiers.get(1).names);
    Assertions.assertIterableEquals(ImmutableList.of("T3"), tableIdentifiers.get(2).names);
  }

  @Test
  public void testNamedSubQueryInFrom() throws Exception {
    final String sql =
        "select L.c1, R.c2 "
            + "from (select * from T1 where T1.c3 = 0) as L, (select * from T2 where T2.c3 = 0) as R";

    final SqlParser parser = SqlParser.create(sql, parserConfig);
    final SqlNode sqlNode = parser.parseStmt();

    final List<SqlIdentifier> tableIdentifiers = TableIdentifierCollector.collect(sqlNode);
    Assertions.assertEquals(2, tableIdentifiers.size());
    Assertions.assertIterableEquals(ImmutableList.of("T1"), tableIdentifiers.get(0).names);
    Assertions.assertIterableEquals(ImmutableList.of("T2"), tableIdentifiers.get(1).names);
  }

  @Test
  public void testSubQueryInWhere() throws Exception {
    final String sql = "select c1, c2 from T1 where c2 in (select c2 from T2 where T2.c1 = 1)";

    final SqlParser parser = SqlParser.create(sql, parserConfig);
    final SqlNode sqlNode = parser.parseStmt();

    final List<SqlIdentifier> tableIdentifiers = TableIdentifierCollector.collect(sqlNode);
    Assertions.assertEquals(2, tableIdentifiers.size());
    Assertions.assertIterableEquals(ImmutableList.of("T1"), tableIdentifiers.get(0).names);
    Assertions.assertIterableEquals(ImmutableList.of("T2"), tableIdentifiers.get(1).names);
  }

  @Test
  public void testSubQueryInHaving() throws Exception {
    final String sql =
        "select c1, avg(c2) from T1 "
            + "group by c1 "
            + "having avg(c2) > (select avg(c2) from T2)";

    final SqlParser parser = SqlParser.create(sql, parserConfig);
    final SqlNode sqlNode = parser.parseStmt();

    final List<SqlIdentifier> tableIdentifiers = TableIdentifierCollector.collect(sqlNode);
    Assertions.assertEquals(2, tableIdentifiers.size());
    Assertions.assertIterableEquals(ImmutableList.of("T1"), tableIdentifiers.get(0).names);
    Assertions.assertIterableEquals(ImmutableList.of("T2"), tableIdentifiers.get(1).names);
  }

  @Test
  public void testTableWithVersion() throws Exception {
    // Extracting identifiers from table function not supported
    final String sql =
        "select T1.c1 as col1, T2.c2 as col2 "
            + "from T1, T2 at branch main "
            + "where T1.c3 = T2.c3";

    final SqlParser parser = SqlParser.create(sql, parserConfig);
    final SqlNode sqlNode = parser.parseStmt();

    final List<SqlIdentifier> tableIdentifiers = TableIdentifierCollector.collect(sqlNode);
    Assertions.assertEquals(1, tableIdentifiers.size());
    Assertions.assertIterableEquals(ImmutableList.of("T1"), tableIdentifiers.get(0).names);
  }
}
