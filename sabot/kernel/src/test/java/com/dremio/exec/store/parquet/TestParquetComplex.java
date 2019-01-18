/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.exec.store.parquet;

import static java.util.Arrays.asList;

import java.nio.file.Paths;

import org.junit.Ignore;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.TestBuilder;
import com.dremio.exec.ExecConstants;

public class TestParquetComplex extends BaseTestQuery {

  private static final String DATAFILE = "cp.\"store/parquet/complex/complex.parquet\"";

  @Test
  public void sort() throws Exception {
    String query = String.format("select * from %s order by amount", DATAFILE);
    testBuilder()
            .sqlQuery(query)
            .ordered()
            .jsonBaselineFile("store/parquet/complex/baseline_sorted.json")
            .build()
            .run();
  }

  @Test
  public void topN() throws Exception {
    String query = String.format("select * from %s order by amount limit 5", DATAFILE);
    testBuilder()
            .sqlQuery(query)
            .ordered()
            .jsonBaselineFile("store/parquet/complex/baseline_sorted.json")
            .build()
            .run();
  }

  @Test
  public void hashJoin() throws Exception{
    String query = String.format("select t1.amount, t1.\"date\", t1.marketing_info, t1.\"time\", t1.trans_id, t1.trans_info, t1.user_info " +
            "from %s t1, %s t2 where t1.amount = t2.amount", DATAFILE, DATAFILE);
    testBuilder()
            .sqlQuery(query)
            .unOrdered()
            .jsonBaselineFile("store/parquet/complex/baseline.json")
            .build()
            .run();
  }

  @Test
  public void filterNonComplexColumn() throws Exception {
    // default batch size
    runFilterNonComplexColumn();
    // with different batch sizes
    for(Long batchSize : asList(2L, 1L, 3L, 5L)) {
      try (AutoCloseable op1 = withOption(ExecConstants.TARGET_BATCH_RECORDS_MIN, batchSize);
           AutoCloseable op2 = withOption(ExecConstants.TARGET_BATCH_RECORDS_MAX, batchSize)) {
        runFilterNonComplexColumn();
      }
    }
  }

  private void runFilterNonComplexColumn() throws Exception {
    String query = String.format("select t1.user_info.cust_id as cust_id from %s t1 where t1.trans_id > 0 and " +
        "t1.trans_id < 4",
      DATAFILE);
    TestBuilder builder = testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .jsonBaselineFile("store/parquet/complex/baseline.json")
      .baselineColumns("cust_id");
    builder.baselineValues(86623L)
      .baselineValues(11L)
      .baselineValues(666L).build().run();
  }

  @Test
  public void mergeJoin() throws Exception{
    test("alter session set \"planner.enable_hashjoin\" = false");
    test("alter session set \"planner.enable_mergejoin\" = true");
    String query = String.format("select t1.amount, t1.\"date\", t1.marketing_info, t1.\"time\", t1.trans_id, t1.trans_info, t1.user_info " +
            "from %s t1, %s t2 where t1.amount = t2.amount", DATAFILE, DATAFILE);
    testBuilder()
            .sqlQuery(query)
            .unOrdered()
            .jsonBaselineFile("store/parquet/complex/baseline.json")
            .build()
            .run();
    test("alter session set \"planner.enable_mergejoin\" = false");
  }

  @Test
  public void selectAllColumns() throws Exception {
    String query = String.format("select amount, \"date\", marketing_info, \"time\", trans_id, trans_info, user_info from %s", DATAFILE);
    testBuilder()
            .sqlQuery(query)
            .ordered()
            .jsonBaselineFile("store/parquet/complex/baseline.json")
            .build()
            .run();
  }

  @Test
  public void q() throws Exception {
    test("select p.marketing_info from cp.\"store/parquet/complex/complex.parquet\" p");
  }

  @Test
  public void selectMap() throws Exception {
    String query = "select marketing_info from cp.\"store/parquet/complex/complex.parquet\"";
    testBuilder()
            .sqlQuery(query)
            .ordered()
            .jsonBaselineFile("store/parquet/complex/baseline5.json")
            .build()
            .run();
  }

  @Test
  public void selectMapAndElements() throws Exception {
    String query = "select marketing_info, t.marketing_info.camp_id as camp_id, t.marketing_info.keywords[2] as keyword2 from cp.\"store/parquet/complex/complex.parquet\" t";
    testBuilder()
            .sqlQuery(query)
            .ordered()
            .jsonBaselineFile("store/parquet/complex/baseline6.json")
            .build()
            .run();
  }

  @Test
  public void selectMultiElements() throws Exception {
    String query = "select t.marketing_info.camp_id as camp_id, t.marketing_info.keywords as keywords from cp.\"store/parquet/complex/complex.parquet\" t";
    testBuilder()
            .sqlQuery(query)
            .ordered()
            .jsonBaselineFile("store/parquet/complex/baseline7.json")
            .build()
            .run();
  }

  @Test
  public void testStar() throws Exception {
    testBuilder()
            .sqlQuery("select * from cp.\"store/parquet/complex/complex.parquet\"")
            .ordered()
            .jsonBaselineFile("store/parquet/complex/baseline.json")
            .build()
            .run();
  }

  @Test
  @Ignore
  public void missingColumnInMap() throws Exception {
    String query = "select t.trans_info.keywords as keywords from cp.\"store/parquet/complex/complex.parquet\" t";
    String[] columns = {"keywords"};
    testBuilder()
            .sqlQuery(query)
            .ordered()
            .jsonBaselineFile("store/parquet/complex/baseline2.json")
            .baselineColumns(columns)
            .build()
            .run();
  }

  @Test
  public void secondElementInMap() throws Exception {
    String query = String.format("select t.\"marketing_info\".keywords as keywords from %s t", DATAFILE);
    String[] columns = {"keywords"};
    testBuilder()
            .sqlQuery(query)
            .ordered()
            .jsonBaselineFile("store/parquet/complex/baseline3.json")
            .baselineColumns(columns)
            .build()
            .run();
  }

  @Test
  public void elementsOfArray() throws Exception {
    String query = String.format("select t.\"marketing_info\".keywords[0] as keyword0, t.\"marketing_info\".keywords[2] as keyword2 from %s t", DATAFILE);
    String[] columns = {"keyword0", "keyword2"};
    testBuilder()
            .sqlQuery(query)
            .unOrdered()
            .jsonBaselineFile("store/parquet/complex/baseline4.json")
            .baselineColumns(columns)
            .build()
            .run();
  }

  @Test
  public void elementsOfArrayCaseInsensitive() throws Exception {
    String query = String.format("select t.\"MARKETING_INFO\".keywords[0] as keyword0, t.\"Marketing_Info\".Keywords[2] as keyword2 from %s t", DATAFILE);
    String[] columns = {"keyword0", "keyword2"};
    testBuilder()
            .sqlQuery(query)
            .unOrdered()
            .jsonBaselineFile("store/parquet/complex/baseline4.json")
            .baselineColumns(columns)
            .build()
            .run();
  }

  @Test //DRILL-3533
  @Ignore("json null type")
  public void notxistsField() throws Exception {
    String query = String.format("select t.\"marketing_info\".notexists as notexists, t.\"marketing_info\".camp_id as id from %s t", DATAFILE);
    String[] columns = {"notexists", "id"};
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .jsonBaselineFile("store/parquet/complex/baseline8.json")
        .baselineColumns(columns)
        .build()
        .run();
  }

  /**
   * Test to confirm we can read repeated maps. The parquet file has the following schema:
   * <pre>
   *   message root {
   *      repeated group rep_map {
   *         optional binary str1 (UTF8);
   *         optional binary str2 (UTF8);
   *         ...
   *      }
   *   }
   * </pre>
   */
  @Test
  public void testLegacyRepeatedMap() throws Exception {
    final String query = "SELECT sub.fmap.str1 as str FROM (" +
            "SELECT flatten(t.rep_map) fmap FROM cp.\"/parquet/alltypes-repeated.parquet\" t) sub";
    testBuilder()
            .unOrdered()
            .sqlQuery(query)
            .baselineColumns("str")
            .baselineValues("1xJmosVH6baysHAmuhGitWQwX")
            .baselineValues("OOOkN1rz5K0HmTdlpFLVXoJwr")
            .go();
  }

  @Test
  public void testZeroRowParquet() throws Exception {
    String currentPath = Paths.get(".").toAbsolutePath().normalize().toString();
    test("select * from dfs.\"" + currentPath + "/src/test/resources/store/parquet/zero-rows\"");
  }
}
