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

package com.dremio.exec.physical.impl.join;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.List;

import org.apache.arrow.vector.ValueVector;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.common.util.FileUtils;
import com.dremio.exec.proto.UserBitShared.QueryType;
import com.dremio.exec.record.RecordBatchLoader;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.sabot.rpc.user.QueryDataBatch;
import com.google.common.base.Charsets;
import com.google.common.io.Files;

@Ignore("DX-3872")
public class TestHashJoinAdvanced extends BaseTestQuery {

  // Have to disable merge join, if this testcase is to test "HASH-JOIN".
  @BeforeClass
  public static void disableMergeJoin() throws Exception {
    test("alter session set \"planner.enable_mergejoin\" = false");
  }

  @AfterClass
  public static void enableMergeJoin() throws Exception {
    test("alter session set \"planner.enable_mergejoin\" = true");
  }

  @Test //DRILL-2197 Left Self Join with complex type in projection
  @Ignore
  public void testLeftSelfHashJoinWithMap() throws Exception {
    final String query = " select a.id, b.oooi.oa.oab.oabc oabc, b.ooof.oa.oab oab from cp.\"join/complex_1.json\" a left outer join cp.\"join/complex_1.json\" b on a.id=b.id order by a.id";

    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .jsonBaselineFile("join/DRILL-2197-result-1.json")
      .build()
      .run();
  }

  @Test //DRILL-2197 Left Join with complex type in projection
  @Ignore("enable union type")
  public void testLeftHashJoinWithMap() throws Exception {
    final String query = " select a.id, b.oooi.oa.oab.oabc oabc, b.ooof.oa.oab oab from cp.\"join/complex_1.json\" a left outer join cp.\"join/complex_2.json\" b on a.id=b.id order by a.id";

    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .jsonBaselineFile("join/DRILL-2197-result-2.json")
      .build()
      .run();
  }

  @Test
  @Ignore
  public void testFOJWithRequiredTypes() throws Exception {
    String query = "select t1.varchar_col from " +
        "cp.\"parquet/drill-2707_required_types.parquet\" t1 full outer join cp.\"parquet/alltypes.json\" t2 " +
        "on t1.int_col = t2.INT_col order by t1.varchar_col limit 1";

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("varchar_col")
        .baselineValues("doob")
        .go();
  }

  @Test  // DRILL-2771, similar problem as DRILL-2197 except problem reproduces with right outer join instead of left
  @Ignore
  public void testRightJoinWithMap() throws Exception {
    final String query = " select a.id, b.oooi.oa.oab.oabc oabc, b.ooof.oa.oab oab from " +
        "cp.\"join/complex_1.json\" b right outer join cp.\"join/complex_1.json\" a on a.id = b.id order by a.id";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .jsonBaselineFile("join/DRILL-2197-result-1.json")
        .build()
        .run();
  }
  @Test
  public void testJoinWithDifferentTypesInCondition() throws Exception {
    String query = "select t1.full_name from cp.\"employee.json\" t1, cp.\"department.json\" t2 " +
        "where cast(t1.department_id as double) = t2.department_id and t1.employee_id = 1";

    testBuilder()
        .sqlQuery(query)
        .optionSettingQueriesForTestQuery("alter session set \"planner.enable_hashjoin\" = true")
        .unOrdered()
        .baselineColumns("full_name")
        .baselineValues("Sheri Nowmer")
        .go();


    query = "select t1.bigint_col from cp.\"jsoninput/implicit_cast_join_1.json\" t1, cp.\"jsoninput/implicit_cast_join_1.json\" t2 " +
        " where t1.bigint_col = cast(t2.bigint_col as int) and" + // join condition with bigint and int
        " t1.double_col  = cast(t2.double_col as float) and" + // join condition with double and float
        " t1.bigint_col = cast(t2.bigint_col as double)"; // join condition with bigint and double

    testBuilder()
        .sqlQuery(query)
        .optionSettingQueriesForTestQuery("alter session set \"planner.enable_hashjoin\" = true")
        .unOrdered()
        .baselineColumns("bigint_col")
        .baselineValues(1L)
        .go();

    query = "select count(*) col1 from " +
        "(select t1.date_opt from cp.\"parquet/date_dictionary.parquet\" t1, cp.\"parquet/timestamp_table.parquet\" t2 " +
        "where t1.date_opt = cast(t2.timestamp_col as date))"; // join condition contains date and timestamp

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("col1")
        .baselineValues(4L)
        .go();
  }



  @Test
  public void simpleEqualityJoin() throws Throwable {
    // Function checks hash join with single equality condition

    final String plan = Files.toString(FileUtils.getResourceAsFile("/join/hash_join.json"), Charsets.UTF_8)
                    .replace("#{TEST_FILE_1}", FileUtils.getResourceAsFile("/build_side_input.json").toURI().toString())
                    .replace("#{TEST_FILE_2}", FileUtils.getResourceAsFile("/probe_side_input.json").toURI().toString());

    List<QueryDataBatch> results = testRunAndReturn(QueryType.PHYSICAL, plan);
    try(RecordBatchLoader batchLoader = new RecordBatchLoader(nodes[0].getContext().getAllocator())){

      QueryDataBatch batch = results.get(1);
      assertTrue(batchLoader.load(batch.getHeader().getDef(), batch.getData()));

      Iterator<VectorWrapper<?>> itr = batchLoader.iterator();

      // Just test the join key
      long[] colA = {1, 1, 2, 2, 1, 1};

      // Check the output of decimal9
      ValueVector intValueVector = itr.next().getValueVector();


      for (int i = 0; i < intValueVector.getValueCount(); i++) {
        assertEquals(intValueVector.getObject(i), colA[i]);
      }
      assertEquals(6, intValueVector.getValueCount());
    }

    for (QueryDataBatch result : results) {
      result.release();
    }

  }


  @Test
  public void multipleConditionJoin() throws Exception {

    // Function tests hash join with multiple join conditions
    final String plan = Files
        .toString(FileUtils.getResourceAsFile("/join/hj_multi_condition_join.json"), Charsets.UTF_8)
        .replace("#{TEST_FILE_1}", FileUtils.getResourceAsFile("/build_side_input.json").toURI().toString())
        .replace("#{TEST_FILE_2}", FileUtils.getResourceAsFile("/probe_side_input.json").toURI().toString());

    List<QueryDataBatch> results = testRunAndReturn(QueryType.PHYSICAL, plan);
    try (RecordBatchLoader batchLoader = new RecordBatchLoader(nodes[0].getContext().getAllocator())) {
      final QueryDataBatch batch = results.get(1);
      assertTrue(batchLoader.load(batch.getHeader().getDef(), batch.getData()));

      final Iterator<VectorWrapper<?>> itr = batchLoader.iterator();

      // Just test the join key
      final long[] colA = { 1, 2, 1 };
      final long[] colC = { 100, 200, 500 };

      // Check the output of decimal9
      final ValueVector intValueVector1 = itr.next().getValueVector();
      final ValueVector intValueVector2 = itr.next().getValueVector();

      for (int i = 0; i < intValueVector1.getValueCount(); i++) {
        assertEquals(intValueVector1.getObject(i), colA[i]);
        assertEquals(intValueVector2.getObject(i), colC[i]);
      }
      assertEquals(3, intValueVector1.getValueCount());
    }

    for (final QueryDataBatch result : results) {
      result.release();
    }

  }

  @Test
  public void hjWithExchange() throws Exception {
    assertEquals(25, testPhysicalFromFile("join/hj_exchanges.json"));
  }

  @Test
  public void hjWithExchange1() throws Exception {
    assertEquals(272, testPhysicalFromFile("join/hj_exchanges1.json"));
  }

  @Test
  public void testHashJoinExprInCondition() throws Exception {
    assertEquals(10, testPhysicalFromFile("join/hashJoinExpr.json"));

  }
}
