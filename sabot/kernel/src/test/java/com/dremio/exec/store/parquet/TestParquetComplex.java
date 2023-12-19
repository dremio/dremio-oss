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
package com.dremio.exec.store.parquet;

import static java.util.Arrays.asList;

import java.math.BigDecimal;
import java.nio.file.Paths;

import org.apache.arrow.vector.util.JsonStringArrayList;
import org.apache.arrow.vector.util.JsonStringHashMap;
import org.apache.arrow.vector.util.Text;
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
  public void testZeroRowParquetDir() throws Exception {
    String currentPath = Paths.get(".").toAbsolutePath().normalize().toString();
    test("select * from dfs.\"" + currentPath + "/src/test/resources/store/parquet/zero-rows\"");
  }

  @Test
  public void testZeroRowParquetFile() throws Exception {
    test("select * from cp.\"store/parquet/complex/zero-rows.parquet\" p");
  }

  @Test
  public void nestedComplexProjection() throws Exception {
    String query = "SELECT col2, " +
      "col1[0][0][0].\"f1\"[0][0][0] f1, " +
      "col1[0][0][0].\"f2\".\"sub_f1\"[0][0][0] sub_f1, " +
      "col1[0][0][0].\"f2\".\"sub_f2\"[0][0][0].\"sub_sub_f1\" sub_sub_f1, " +
      "col1[0][0][0].\"f2\".\"sub_f2\"[0][0][0].\"sub_sub_f2\" sub_sub_f2 " +
      "FROM cp.\"/parquet/very_complex.parquet\"";
    testBuilder()
      .sqlQuery(query)
      .ordered()
      .baselineColumns("col2", "f1", "sub_f1", "sub_sub_f1", "sub_sub_f2")
      .baselineValues(3, 1, 2, 1, "abc")
      .build()
      .run();
  }

  private void filterListsWithNulls(String columnName) throws Exception {
    String query = "SELECT " +
      "intcol[2] as intcol, bigintcol[2] as bigintcol, floatcol[2] as floatcol," +
      "doublecol[2] as doublecol, decimalcol[2] as decimalcol," +
      "charcol[2] as charcol, varcharcol[2] as varcharcol," +
      "stringcol[2] as stringcol, datecol[2] as datecol, timestampcol[2] as timestampcol " +
      "FROM cp.\"/parquet/list_null_test.parquet\" where " + columnName + "[2] is null";
    testBuilder()
      .sqlQuery(query)
      .ordered()
      .baselineColumns("intcol", "bigintcol", "floatcol", "doublecol", "decimalcol",
        "charcol", "varcharcol", "stringcol", "datecol", "timestampcol")
      .baselineValues(null, null, null, null, null, null, null, null, null, null)
      .build()
      .run();
  }

  @Test
  public void testListsWithNulls() throws Exception {
    String query = "SELECT " +
      "intcol[2] as intcol, bigintcol[2] as bigintcol, floatcol[2] as floatcol," +
      "doublecol[2] as doublecol, decimalcol[2] as decimalcol," +
      "charcol[2] as charcol, varcharcol[2] as varcharcol," +
      "stringcol[2] as stringcol, datecol[2] as datecol, timestampcol[2] as timestampcol " +
      "FROM cp.\"/parquet/list_null_test.parquet\"";
    testBuilder()
      .sqlQuery(query)
      .ordered()
      .baselineColumns("intcol", "bigintcol", "floatcol", "doublecol", "decimalcol",
        "charcol", "varcharcol", "stringcol", "datecol", "timestampcol")
      .baselineValues(null, null, null, null, null, null, null, null, null, null)
      .build()
      .run();
    filterListsWithNulls("intcol");
    filterListsWithNulls("bigintcol");
    filterListsWithNulls("floatcol");
    filterListsWithNulls("doublecol");
    filterListsWithNulls("decimalcol");
    filterListsWithNulls("charcol");
    filterListsWithNulls("varcharcol");
    filterListsWithNulls("stringcol");
    filterListsWithNulls("datecol");
    filterListsWithNulls("timestampcol");

    query = "SELECT " +
      "intcol[3] as intcol, bigintcol[3] as bigintcol, floatcol[3] as floatcol," +
      "doublecol[3] as doublecol, decimalcol[3] as decimalcol," +
      "charcol[3] as charcol, varcharcol[3] as varcharcol," +
      "stringcol[3] as stringcol, datecol[3] as datecol, timestampcol[3] as timestampcol " +
      "FROM cp.\"/parquet/list_null_test.parquet\"";
    testBuilder()
      .sqlQuery(query)
      .ordered()
      .baselineColumns("intcol", "bigintcol", "floatcol", "doublecol", "decimalcol",
        "charcol", "varcharcol", "stringcol", "datecol", "timestampcol")
      .baselineValues(4, 4L, 4.4F,
        4.4D, new BigDecimal("4"), "d", "d", "d", null, null)
      .build()
      .run();
  }

  @Test
  public void testListofListWithNulls() throws Exception {
    JsonStringArrayList<Text> thirdLevelList = new JsonStringArrayList<>();
    thirdLevelList.add(new Text("a"));
    thirdLevelList.add(null);

    JsonStringArrayList<JsonStringArrayList> secondLevelList = new JsonStringArrayList<>();
    secondLevelList.add(thirdLevelList);
    secondLevelList.add(null);

    JsonStringArrayList<JsonStringArrayList> topLevelList = new JsonStringArrayList<>();
    topLevelList.add(secondLevelList);
    topLevelList.add(null);

    String query = "SELECT " +
      "col1 " +
      "FROM cp.\"/parquet/list_list_null_test.parquet\"";
    testBuilder()
      .sqlQuery(query)
      .ordered()
      .baselineColumns("col1")
      .baselineValues(topLevelList)
      .build()
      .run();

    query = "SELECT " +
      "col1[0] as c1, col1[1] as c2 " +
      "FROM cp.\"/parquet/list_list_null_test.parquet\"";
    testBuilder()
      .sqlQuery(query)
      .ordered()
      .baselineColumns("c1", "c2")
      .baselineValues(secondLevelList, null)
      .build()
      .run();

    query = "SELECT " +
      "col1[0] as c1, col1[1] as c2 " +
      "FROM cp.\"/parquet/list_list_null_test.parquet\" where col1[1] is null";
    testBuilder()
      .sqlQuery(query)
      .ordered()
      .baselineColumns("c1", "c2")
      .baselineValues(secondLevelList, null)
      .build()
      .run();

    query = "SELECT " +
      "col1[0][0] as c1, col1[0][1] as c2 " +
      "FROM cp.\"/parquet/list_list_null_test.parquet\"";
    testBuilder()
      .sqlQuery(query)
      .ordered()
      .baselineColumns("c1", "c2")
      .baselineValues(thirdLevelList, null)
      .build()
      .run();

    query = "SELECT " +
      "col1[0][0] as c1, col1[0][1] as c2 " +
      "FROM cp.\"/parquet/list_list_null_test.parquet\" where col1[0][1] is null";
    testBuilder()
      .sqlQuery(query)
      .ordered()
      .baselineColumns("c1", "c2")
      .baselineValues(thirdLevelList, null)
      .build()
      .run();
  }

  @Test
  public void testListofStructWithNulls() throws Exception {
    JsonStringArrayList<Text> thirdLevelList = new JsonStringArrayList<>();
    thirdLevelList.add(new Text("a"));
    thirdLevelList.add(null);

    JsonStringHashMap<String, Object> secondLevelStruct = new JsonStringHashMap<String, Object>();
    secondLevelStruct.put("f1", thirdLevelList);

    JsonStringArrayList<JsonStringHashMap> topLevelList = new JsonStringArrayList<>();
    topLevelList.add(secondLevelStruct);
    topLevelList.add(null);

    String query = "SELECT " +
      "col1 " +
      "FROM cp.\"/parquet/list_struct_null_test.parquet\"";
    testBuilder()
      .sqlQuery(query)
      .ordered()
      .baselineColumns("col1")
      .baselineValues(topLevelList)
      .build()
      .run();

    query = "SELECT " +
      "col1[0] as c1, col1[1] as c2 " +
      "FROM cp.\"/parquet/list_struct_null_test.parquet\"";
    testBuilder()
      .sqlQuery(query)
      .ordered()
      .baselineColumns("c1", "c2")
      .baselineValues(secondLevelStruct, null)
      .build()
      .run();

    query = "SELECT " +
      "col1[0] as c1, col1[1] as c2 " +
      "FROM cp.\"/parquet/list_struct_null_test.parquet\" where col1[1] is null";
    testBuilder()
      .sqlQuery(query)
      .ordered()
      .baselineColumns("c1", "c2")
      .baselineValues(secondLevelStruct, null)
      .build()
      .run();

    query = "SELECT " +
      "col1[0].\"f1\" as c1 " +
      "FROM cp.\"/parquet/list_struct_null_test.parquet\"";
    testBuilder()
      .sqlQuery(query)
      .ordered()
      .baselineColumns("c1")
      .baselineValues(thirdLevelList)
      .build()
      .run();

    query = "SELECT " +
      "col1[0].\"f1\"[0] as c1, col1[0].\"f1\"[1] as c2 " +
      "FROM cp.\"/parquet/list_struct_null_test.parquet\"";
    testBuilder()
      .sqlQuery(query)
      .ordered()
      .baselineColumns("c1", "c2")
      .baselineValues("a", null)
      .build()
      .run();

    query = "SELECT " +
      "col1[0].\"f1\"[0] as c1, col1[0].\"f1\"[1] as c2 " +
      "FROM cp.\"/parquet/list_struct_null_test.parquet\" where col1[0].\"f1\"[1] is null";
    testBuilder()
      .sqlQuery(query)
      .ordered()
      .baselineColumns("c1", "c2")
      .baselineValues("a", null)
      .build()
      .run();
  }
}
