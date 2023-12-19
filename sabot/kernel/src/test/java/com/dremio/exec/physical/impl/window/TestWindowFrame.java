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
package com.dremio.exec.physical.impl.window;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Ignore;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.common.exceptions.UserRemoteException;
import com.dremio.common.util.TestTools;
import com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType;
import com.dremio.sabot.op.windowframe.Partition;

public class TestWindowFrame extends BaseTestQuery {

  private static final String TEST_RES_PATH = TestTools.getWorkingPath() + "/src/test/resources";

  @Test
  public void testMultipleFramers() throws Exception {
    final String window = " OVER(PARTITION BY position_id ORDER by sub)";
    test("SELECT COUNT(*)"+window+", SUM(salary)"+window+", ROW_NUMBER()"+window+", RANK()"+window+" " +
      "FROM dfs.\""+TEST_RES_PATH+"/window/b1.p1\""
    );
  }

  @Test
  public void testUnboundedFollowing() throws Exception {
    testBuilder()
      .sqlQuery(getFile("window/q3.sql"), TEST_RES_PATH)
      .ordered()
      .sqlBaselineQuery(getFile("window/q4.sql"), TEST_RES_PATH)
      .build()
      .run();
  }

  @Test
  public void testAggregateRowsUnboundedAndCurrentRow() throws Exception {
    final String table = "dfs.\""+TEST_RES_PATH+"/window/b4.p4\"";
    testBuilder()
      .sqlQuery(getFile("window/aggregate_rows_unbounded_current.sql"), table)
      .ordered()
      .sqlBaselineQuery(getFile("window/aggregate_rows_unbounded_current_baseline.sql"), table)
      .build()
      .run();
  }

  @Test
  public void testLastValueRowsUnboundedAndCurrentRow() throws Exception {
    final String table = "dfs.\""+TEST_RES_PATH+"/window/b4.p4\"";
    testBuilder()
      .sqlQuery(getFile("window/last_value_rows_unbounded_current.sql"), table)
      .unOrdered()
      .sqlBaselineQuery(getFile("window/last_value_rows_unbounded_current_baseline.sql"), table)
      .build()
      .run();
  }

  @Test
  public void testAggregateRangeCurrentAndCurrent() throws Exception {
    final String table = "dfs.\""+TEST_RES_PATH+"/window/b4.p4\"";
    testBuilder()
      .sqlQuery(getFile("window/aggregate_range_current_current.sql"), table)
      .unOrdered()
      .sqlBaselineQuery(getFile("window/aggregate_range_current_current_baseline.sql"), table)
      .build()
      .run();
  }

  @Test
  public void testFirstValueRangeCurrentAndCurrent() throws Exception {
    final String table = "dfs.\""+TEST_RES_PATH+"/window/b4.p4\"";
    testBuilder()
      .sqlQuery(getFile("window/first_value_range_current_current.sql"), table)
      .unOrdered()
      .sqlBaselineQuery(getFile("window/first_value_range_current_current_baseline.sql"), table)
      .build()
      .run();
  }

  @Test // DRILL-1862
  public void testEmptyPartitionBy() throws Exception {
    test("SELECT employee_id, position_id, salary, SUM(salary) OVER(ORDER BY position_id) FROM cp.\"employee.json\" LIMIT 10");
  }

  @Test // DRILL-3172
  public void testEmptyOverClause() throws Exception {
    test("SELECT employee_id, position_id, salary, SUM(salary) OVER() FROM cp.\"employee.json\" LIMIT 10");
  }

  @Test // DRILL-3218
  public void testMaxVarChar() throws Exception {
    test(getFile("window/q3218.sql"), TEST_RES_PATH);
  }

  @Test // DRILL-3220
  public void testCountConst() throws Exception {
    test(getFile("window/q3220.sql"), TEST_RES_PATH);
  }

  @Test // DRILL-3604
  public void testFix3604() throws Exception {
    // make sure the query doesn't fail
    test(getFile("window/3604.sql"), TEST_RES_PATH);
  }

  @Test // DRILL-3605
  public void testFix3605() throws Exception {
    testBuilder()
      .sqlQuery(getFile("window/3605.sql"), TEST_RES_PATH)
      .ordered()
      .csvBaselineFile("window/3605.tsv")
      .baselineColumns("col2", "lead_col2")
      .build()
      .run();
  }

  @Test // DRILL-3606
  public void testFix3606() throws Exception {
    testBuilder()
      .sqlQuery(getFile("window/3606.sql"), TEST_RES_PATH)
      .ordered()
      .csvBaselineFile("window/3606.tsv")
      .baselineColumns("col2", "lead_col2")
      .build()
      .run();
  }

  @Test
  public void testLead() throws Exception {
    testBuilder()
      .sqlQuery(getFile("window/lead.oby.sql"), TEST_RES_PATH)
      .ordered()
      .csvBaselineFile("window/b4.p4.lead.oby.tsv")
      .baselineColumns("lead")
      .build()
      .run();
  }

  @Test
  public void testLagWithPby() throws Exception {
    testBuilder()
      .sqlQuery(getFile("window/lag.pby.oby.sql"), TEST_RES_PATH)
      .ordered()
      .csvBaselineFile("window/b4.p4.lag.pby.oby.tsv")
      .baselineColumns("lag")
      .build()
      .run();
  }


  @Test
  public void testLag() throws Exception {
    testBuilder()
      .sqlQuery(getFile("window/lag.oby.sql"), TEST_RES_PATH)
      .ordered()
      .csvBaselineFile("window/b4.p4.lag.oby.tsv")
      .baselineColumns("lag")
      .build()
      .run();
  }

  @Test
  public void testLeadWithPby() throws Exception {
    testBuilder()
      .sqlQuery(getFile("window/lead.pby.oby.sql"), TEST_RES_PATH)
      .ordered()
      .csvBaselineFile("window/b4.p4.lead.pby.oby.tsv")
      .baselineColumns("lead")
      .build()
      .run();
  }

  @Test
  public void testLeadUnderPrecedentOperation() throws Exception {
    test("select 1/(LEAD(n_nationKey, 2) over (partition by n_nationKey order by n_nationKey)) \n" +
      "from cp.\"tpch/nation.parquet\"");
  }

  @Test
  public void testLeadUnderNestedPrecedentOperation() throws Exception {
    test("select 1/(1/(LEAD(n_nationKey, 2) over (partition by n_nationKey order by n_nationKey))) \n" +
      "from cp.\"tpch/nation.parquet\"");
  }

  @Test
  public void testLagUnderPrecedentOperation() throws Exception {
   test("select 1/(LAG(n_nationKey, 2) over (partition by n_nationKey order by n_nationKey)) \n" +
      "from cp.\"tpch/nation.parquet\"");
  }

  @Test
  public void testLagUnderNestedPrecedentOperation() throws Exception {
    test("select 1/(1/(LAG(n_nationKey, 2) over (partition by n_nationKey order by n_nationKey))) \n" +
      "from cp.\"tpch/nation.parquet\"");
  }

  @Test
  public void testFirstValue() throws Exception {
    testBuilder()
      .sqlQuery(getFile("window/fval.pby.sql"), TEST_RES_PATH)
      .ordered()
      .csvBaselineFile("window/b4.p4.fval.pby.tsv")
      .baselineColumns("first_value")
      .build()
      .run();
  }

  @Test
  public void testLastValue() throws Exception {
    testBuilder()
      .sqlQuery(getFile("window/lval.pby.oby.sql"), TEST_RES_PATH)
      .ordered()
      .csvBaselineFile("window/b4.p4.lval.pby.oby.tsv")
      .baselineColumns("last_value")
      .build()
      .run();
  }

  @Test
  public void testFirstValueAllTypes() throws Exception {
    // make sure all types are handled properly
    test(getFile("window/fval.alltypes.sql"), TEST_RES_PATH);
  }

  @Test
  public void testLastValueAllTypes() throws Exception {
    // make sure all types are handled properly
    test(getFile("window/fval.alltypes.sql"), TEST_RES_PATH);
  }

  @Test
  @Ignore //DX-18534
  public void testNtile() throws Exception {
    testBuilder()
      .sqlQuery(getFile("window/ntile.sql"), TEST_RES_PATH)
      .ordered()
      .csvBaselineFile("window/b2.p4.ntile.tsv")
      .baselineColumns("ntile")
      .build()
      .run();
  }

  @Test
  public void test3648Fix() throws Exception {
    testBuilder()
      .sqlQuery(getFile("window/3648.sql"), TEST_RES_PATH)
      .ordered()
      .csvBaselineFile("window/3648.tsv")
      .baselineColumns("ntile")
      .build()
      .run();
  }

  @Test
  public void test3654Fix() throws Exception {
    test("SELECT FIRST_VALUE(col8) OVER(PARTITION BY col7 ORDER BY col8) FROM dfs.\"%s/window/3648.parquet\"", TEST_RES_PATH);
  }

  @Test
  public void test3643Fix() throws Exception {
    try {
      test("SELECT NTILE(0) OVER(PARTITION BY col7 ORDER BY col8) FROM dfs.\"%s/window/3648.parquet\"", TEST_RES_PATH);
      fail("Query should have failed");
    } catch (UserRemoteException e) {
      assertEquals(ErrorType.FUNCTION, e.getErrorType());
    }
  }

  @Test
  public void test3668Fix() throws Exception {
    //testNoResult("set \"store.parquet.vectorize\" = false");
    testBuilder()
      .sqlQuery(getFile("window/3668.sql"), TEST_RES_PATH)
      .ordered()
      .baselineColumns("cnt").baselineValues(2L)
      .build()
      .run();
  }

  @Test
  public void testLeadParams() throws Exception {
    // make sure we only support default arguments for LEAD/LAG functions
    final String query = "SELECT %s OVER(PARTITION BY col7 ORDER BY col8) FROM dfs.\"%s/window/3648.parquet\"";

    test(query, "LEAD(col8, 1)", TEST_RES_PATH);
    test(query, "LAG(col8, 1)", TEST_RES_PATH);
    test(query, "LEAD(col8, 2)", TEST_RES_PATH);
    test(query, "LAG(col8, 2)", TEST_RES_PATH);
  }

  @Test
  public void testPartitionNtile() {
    Partition partition = new Partition();
    partition.updateLength(12, false);

    assertEquals(1, partition.ntile(5));
    partition.rowAggregated();
    assertEquals(1, partition.ntile(5));
    partition.rowAggregated();
    assertEquals(1, partition.ntile(5));

    partition.rowAggregated();
    assertEquals(2, partition.ntile(5));
    partition.rowAggregated();
    assertEquals(2, partition.ntile(5));
    partition.rowAggregated();
    assertEquals(2, partition.ntile(5));

    partition.rowAggregated();
    assertEquals(3, partition.ntile(5));
    partition.rowAggregated();
    assertEquals(3, partition.ntile(5));

    partition.rowAggregated();
    assertEquals(4, partition.ntile(5));
    partition.rowAggregated();
    assertEquals(4, partition.ntile(5));

    partition.rowAggregated();
    assertEquals(5, partition.ntile(5));
    partition.rowAggregated();
    assertEquals(5, partition.ntile(5));
  }

  @Test
  public void test4457() throws Exception {
    runSQL("CREATE TABLE dfs_test.\"4457\" AS " +
      "SELECT columns[0] AS c0, NULLIF(columns[1], 'null') AS c1 " +
      "FROM cp.\"/window/4457.csv\"");

    testBuilder()
      .sqlQuery("SELECT COALESCE(FIRST_VALUE(c1) OVER(ORDER BY c0 RANGE BETWEEN CURRENT ROW AND CURRENT ROW), 'EMPTY') AS fv FROM dfs_test.\"4457\"")
      .ordered()
      .baselineColumns("fv")
      .baselineValues("a")
      .baselineValues("b")
      .baselineValues("EMPTY")
      .go();
  }
}
