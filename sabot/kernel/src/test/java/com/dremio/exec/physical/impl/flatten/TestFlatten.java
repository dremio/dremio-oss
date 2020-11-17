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
package com.dremio.exec.physical.impl.flatten;


import static com.dremio.TestBuilder.listOf;
import static com.dremio.TestBuilder.mapOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.regex.Pattern;

import org.apache.arrow.vector.util.JsonStringHashMap;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.BaseTestQuery;
import com.dremio.PlanTestBase;
import com.dremio.QueryTestUtil;
import com.dremio.TestBuilder;
import com.dremio.common.util.FileUtils;
import com.dremio.common.util.TestTools;
import com.dremio.exec.fn.interp.TestConstantFolding;
import com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType;
import com.dremio.exec.proto.UserBitShared.QueryType;
import com.google.common.collect.Lists;

public class TestFlatten extends PlanTestBase {

  /**
   *  enable this if you have the following files:
   *    - /tmp/yelp_academic_dataset_business.json
   *    - /tmp/mapkv.json
   *    - /tmp/drill1665.json
   *    - /tmp/bigfile.json
   */
  public static boolean RUN_ADVANCED_TESTS = false;

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private static final String WORKING_PATH = TestTools.getWorkingPath();
  private static final String TEST_RES_PATH = WORKING_PATH + "/src/test/resources";

  @BeforeClass
  public static void setupTest() throws Exception {
    String query = "create table dfs_test.parquetTable as select * from cp.\"/jsoninput/input_for_parquet.json\"";
    test(query);
  }

  @Before
  public void resetSession() throws Exception {
    test("ALTER SESSION RESET ALL");
    test("ALTER SESSION SET \"planner.experimental.tpf_logical\"= true");
  }

  @Test
  public void testConvertFromWithFlatten() throws Exception {
    final String queryConvert = String.format("select convert_from(columns[1], 'JSON') from dfs.\"%s/store/text/convertFrom.tbl\"", TEST_RES_PATH);
    errorMsgWithTypeTestHelper(queryConvert, ErrorType.VALIDATION, "Using CONVERT_FROM(*, 'JSON') is only supported against string literals and direct table references of types VARCHAR and VARBINARY.");
    //testPlanSubstrPatterns(queryConvert, new String[] { "CONVERT_FROMJSON(ITEM($0, 1))" }, null);
    final String queryFlattenConvert = String.format("select flatten(convert_from(columns[1], 'JSON')) from dfs.\"%s/store/text/convertFrom.tbl\"", TEST_RES_PATH);
    errorMsgWithTypeTestHelper(queryConvert, ErrorType.VALIDATION, "Using CONVERT_FROM(*, 'JSON') is only supported against string literals and direct table references of types VARCHAR and VARBINARY.");
    //testPlanSubstrPatterns(queryFlattenConvert, new String[] { "CONVERT_FROMJSON(ITEM($0, 1))" }, null);
  }

  @Test
  public void testFlatten() throws Exception {
    String query = "select flatten(complex), rownum from cp.\"/store/json/test_flatten_mappify2.json\"";
    testPlanSubstrPatterns(query, new String[] {"columns=[`rownum`, `complex`]"}, null);
    test(query);
    query = "select complex, rownum from cp.\"/store/json/test_flatten_mappify2.json\"";
    testPlanSubstrPatterns(query, new String[] {"columns=[`rownum`, `complex`]"}, null);
    test(query);
  }

  @Test
  public void testFlattenDistributed() throws Exception {
    test("set planner.slice_target = 1");
    test("explain plan for select a, flatten(b) from cp.\"/store/json/test_flatten_sort.json\" order by a");
    test("select a, flatten(b) from cp.\"/store/json/test_flatten_sort.json\" order by a");
  }

  @Test
  public void testFlatten_Drill2162_complex() throws Exception {
    String path = folder.getRoot().toPath().toString();

    String jsonRecords = BaseTestQuery.getFile("flatten/complex_transaction_example_data.json");
    int numCopies = 700;
    new TestConstantFolding.SmallFileCreator(folder)
        .setRecord(jsonRecords)
        .createFiles(1, numCopies, "json");

    List<JsonStringHashMap<String,Object>> data = Lists.newArrayList(
        mapOf("uid", 1l,
            "lst_lst_0", listOf(1l, 2l, 3l, 4l, 5l),
            "lst_lst_1", listOf(2l, 3l, 4l, 5l, 6l),
            "lst_lst", listOf(
            listOf(1l, 2l, 3l, 4l, 5l),
            listOf(2l, 3l, 4l, 5l, 6l))
        ),
        mapOf("uid", 2l,
            "lst_lst_0", listOf(1l, 2l, 3l, 4l, 5l),
            "lst_lst_1", listOf(2l, 3l, 4l, 5l, 6l),
            "lst_lst", listOf(
            listOf(1l, 2l, 3l, 4l, 5l),
            listOf(2l, 3l, 4l, 5l, 6l))
        )
    );

    List<JsonStringHashMap<String, Object>> result = flatten(flatten(flatten(data, "lst_lst_1"), "lst_lst_0"), "lst_lst");

    String query = "select uid, flatten(d.lst_lst[1]) lst1, flatten(d.lst_lst[0]) lst0, flatten(d.lst_lst) lst from dfs.\"" + path + "/bigfile/bigfile.json\" d";
    testPlanSubstrPatterns(query, new String[] {"columns=[`uid`, `lst_lst`, `lst_lst`[0], `lst_lst`[1]]"}, null);
    TestBuilder builder = testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("uid", "lst1", "lst0", "lst");
    for (int i = 0; i < numCopies; i++) {
      for (JsonStringHashMap<String, Object> record : result) {
        builder.baselineValues(record.get("uid"), record.get("lst_lst_1"), record.get("lst_lst_0"), record.get("lst_lst"));
      }
    }
    builder.go();
  };

  @Test
  public void testFlattenReferenceImpl() throws Exception {
    List<JsonStringHashMap<String,Object>> data = Lists.newArrayList(
        mapOf("a",1,
              "b",2,
              "list_col", listOf(10,9),
              "nested_list_col",listOf(
                  listOf(100,99),
                  listOf(1000,999)
            )));
    List<JsonStringHashMap<String, Object>> result = flatten(flatten(flatten(data, "list_col"), "nested_list_col"), "nested_list_col");
     List<JsonStringHashMap<String, Object>> expectedResult = Lists.newArrayList(
        mapOf("nested_list_col", 100,  "list_col", 10,"a", 1, "b",2),
        mapOf("nested_list_col", 99,   "list_col", 10,"a", 1, "b",2),
        mapOf("nested_list_col", 1000, "list_col", 10,"a", 1, "b",2),
        mapOf("nested_list_col", 999,  "list_col", 10,"a", 1, "b",2),
        mapOf("nested_list_col", 100,  "list_col", 9, "a", 1, "b",2),
        mapOf("nested_list_col", 99,   "list_col", 9, "a", 1, "b",2),
        mapOf("nested_list_col", 1000, "list_col", 9, "a", 1, "b",2),
        mapOf("nested_list_col", 999,  "list_col", 9, "a", 1, "b",2)
    );
    int i = 0;
    for (JsonStringHashMap<String, Object> record : result) {
      assertEquals(record, expectedResult.get(i));
      i++;
    }
  }

  private List<JsonStringHashMap<String, Object>> flatten(
      List<JsonStringHashMap<String,Object>> incomingRecords,
      String colToFlatten) {
    return flatten(incomingRecords, colToFlatten, colToFlatten);
  }

  private List<JsonStringHashMap<String, Object>> flatten(
      List<JsonStringHashMap<String,Object>> incomingRecords,
      String colToFlatten,
      String flattenedDataColName) {
    List<JsonStringHashMap<String,Object>> output = Lists.newArrayList();
    for (JsonStringHashMap<String, Object> incomingRecord : incomingRecords) {
      List<?> dataToFlatten = (List<?>) incomingRecord.get(colToFlatten);
      for (int i = 0; i < dataToFlatten.size(); i++) {
        final JsonStringHashMap<String, Object> newRecord = new JsonStringHashMap<>();
        newRecord.put(flattenedDataColName, dataToFlatten.get(i));
        for (String s : incomingRecord.keySet()) {
          if (s.equals(colToFlatten)) {
            continue;
          }
          newRecord.put(s, incomingRecord.get(s));
        }
        output.add(newRecord);
      }
    }
    return output;
  }

  @Test
  public void testFlatten_Drill2162_simple() throws Exception {
    String path = folder.getRoot().toPath().toString();

    List<Long> inputList = Lists.newArrayList();
    String jsonRecord = "{ \"int_list\" : [";
    final int listSize = 30;
    for (int i = 1; i < listSize; i++ ) {
      jsonRecord += i + ", ";
      inputList.add((long) i);
    }
    jsonRecord += listSize + "] }";
    inputList.add((long) listSize);
    int numRecords = 3000;
    new TestConstantFolding.SmallFileCreator(folder)
        .setRecord(jsonRecord)
        .createFiles(1, numRecords, "json");

    List<JsonStringHashMap<String,Object>> data = Lists.newArrayList(
        mapOf("int_list", inputList)
    );

    List<JsonStringHashMap<String, Object>> result = flatten(data, "int_list");

    String query = "select flatten(int_list) as int_list from dfs.\"" + path + "/bigfile/bigfile.json\"";
    String plan = getPlanInString("explain plan for " + query, OPTIQ_FORMAT);
    // int_list is the only column
    assertTrue(plan.contains("columns=[`int_list`]") || plan.contains("columns=[`*`]"));
    TestBuilder builder = testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("int_list");

    for (int i = 0; i < numRecords; i++) {
      for (JsonStringHashMap<String, Object> record : result) {
        builder.baselineValues(record.get("int_list"));
      }
    }
    builder.go();
  };

  @Test
  public void drill1671() throws Exception{
    String query = "select * from (select count(*) as cnt from (select id, flatten(evnts1), flatten(evnts2), flatten(evnts3), flatten(evnts4), flatten(evnts5), flatten(evnts6), flatten(evnts7), flatten(evnts8), flatten(evnts9), flatten(evnts10), flatten(evnts11) from cp.\"/flatten/many-arrays-50.json\")x )y where cnt = 2048";
    testPlanSubstrPatterns(query, new String[] {"columns=[`id`, `evnts1`, `evnts2`, `evnts3`, `evnts4`, `evnts5`, `evnts6`, `evnts7`, `evnts8`, `evnts9`, `evnts10`, `evnts11`]"}, null);
    int rowCount = testSql(query);
    assertEquals(1, rowCount);
  }

  @Test
  public void drill3370() throws Exception {
    String query = "select a from (select flatten(arr) as a from cp.\"/flatten/drill-3370.json\") where a > 100";
    testPlanSubstrPatterns(query, new String[] {"columns=[`arr`]"}, null);
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("a")
        .baselineValues(131l)
        .baselineValues(106l)
        .go();
  }

  @Test
  @Ignore("not yet fixed")
  public void drill1660() throws Exception {
    test("select * from cp.\"/flatten/empty-rm.json\"");
  }

  @Test // repeated list within a repeated map
  public void drill1673() throws Exception {
    String path = folder.getRoot().toPath().toString();

    String jsonRecords = BaseTestQuery.getFile("store/json/1673.json");
    int numCopies = 25000;
    new TestConstantFolding.SmallFileCreator(folder)
        .setRecord(jsonRecords)
        .createFiles(1, numCopies, "json");

    String query = "select t.fixed_column as fixed_column, flatten(t.list_column) as list_col from dfs.\"" + path + "/bigfile/bigfile.json\" as t";
    String plan = getPlanInString("explain plan for " + query, OPTIQ_FORMAT);
    // There are only two columns fixed_column and list_column, so * is also possible
    assertTrue(plan, plan.contains("columns=[`fixed_column`, `list_column`]") || plan.contains("columns=[`*`]"));
    TestBuilder builder = testBuilder()
        .sqlQuery(query)
        .baselineColumns("fixed_column", "list_col")
        .unOrdered();
    Object map1 = mapOf("id1", "1",
                        "name", "zhu",
                        "num", listOf(listOf(1l, 2l, 3l)));
    Object map2 = mapOf("id1", "2",
                      "name", "hao",
                      "num", listOf(listOf(4l, 5l, 6l)));
    for (int i = 0; i < numCopies; i++) {
      builder.baselineValues("abc", map1);
      builder.baselineValues("abc", map2);
    }

    builder.go();
  }

  @Test
  @Ignore("DX-10548")
  public void drill1653() throws Exception{
    String query = "select * from (select sum(t.flat.\"value\") as sm from (select id, flatten(kvgen(m)) as flat from cp.\"/flatten/missing-map.json\")t) where sm = 10 ";
    testPlanSubstrPatterns(query, new String[] {"columns=[`m`]"}, null);
    int rowCount = testSql(query);
    assertEquals(1, rowCount);
  }

  @Test
  public void drill1652() throws Exception {
    if(RUN_ADVANCED_TESTS){
      test("select uid, flatten(transactions) from dfs.\"/tmp/bigfile.json\"");
    }
  }

  @Test
  @Ignore("Still not working.")
  public void drill1649() throws Exception {
    test("select event_info.uid, transaction_info.trans_id, event_info.event.evnt_id\n" +
        "from (\n" +
        " select userinfo.transaction.trans_id trans_id, max(userinfo.event.event_time) max_event_time\n" +
        " from (\n" +
        "     select uid, flatten(events) event, flatten(transactions) transaction from cp.\"/flatten/single-user-transactions.json\"\n" +
        " ) userinfo\n" +
        " where userinfo.transaction.trans_time >= userinfo.event.event_time\n" +
        " group by userinfo.transaction.trans_id\n" +
        ") transaction_info\n" +
        "inner join\n" +
        "(\n" +
        " select uid, flatten(events) event\n" +
        " from cp.\"/flatten/single-user-transactions.json\"\n" +
        ") event_info\n" +
        "on transaction_info.max_event_time = event_info.event.event_time;");
  }

  @Test
  public void testKVGenFlatten1() throws Exception {
    String query = "select flatten(kvgen(f1)) as monkey, x from cp.\"/store/json/test_flatten_mapify.json\"";
    String plan = getPlanInString("explain plan for " + query, OPTIQ_FORMAT);
    // columns f1 and x are the only columns
    assertTrue(plan.contains("columns=[`f1`, `x`]") || plan.contains("columns=[`*`]"));
    // works - TODO and verify results
    test(query);
  }

  @Test
  public void testTwoFlattens() throws Exception {
    // second re-write rule has been added to test the fixes together, this now runs
    String query = "select \"integer\", \"float\", x, flatten(z), flatten(l) from cp.\"/jsoninput/input2_modified.json\"";
    testPlanSubstrPatterns(query, new String[] {"columns=[`integer`, `float`, `x`, `z`, `l`]"}, null);
    test(query);
  }

  @Test
  public void testFlattenRepeatedMap() throws Exception {
    String query = "select \"integer\", \"float\", x, flatten(z) from cp.\"/jsoninput/input2.json\"";
    testPlanSubstrPatterns(query, new String[] {"columns=[`integer`, `float`, `x`, `z`]"}, null);
    test(query);
  }

  @Test
  public void testKVGen() throws Exception {
    test("select kvgen(x) from cp.\"/jsoninput/input5.json\"");
  }

  @Test
  public void testFlattenKVGenFlatten() throws Exception {
    String query = "select \"integer\", \"float\", x, flatten(kvgen(flatten(z))) from cp.\"/jsoninput/input2.json\"";
    testPlanSubstrPatterns(query, new String[] {"columns=[`integer`, `float`, `x`, `z`]"}, null);
    // currently does not fail, but produces incorrect results, requires second re-write rule to split up expressions
    // with complex outputs
    test(query);
  }

  @Test
  public void testKVGenFlatten2() throws Exception {
    // currently runs
    // TODO - re-verify results by hand
    if(RUN_ADVANCED_TESTS){
      test("select flatten(kvgen(visited_cellid_counts)) as mytb from dfs.\"/tmp/mapkv.json\"") ;
    }
  }

  @Test
  public void testFilterFlattenedRecords() throws Exception {
    String query = "select t2.key from (select t.monkey.\"value\" as val, t.monkey.key as key from (select flatten(kvgen(f1)) as monkey, x " +
        "from cp.\"/store/json/test_flatten_mapify.json\") as t) as t2 where t2.val > 1";
    testPlanSubstrPatterns(query, new String[] {"columns=[`f1`]"}, null);
    // TODO - hand verify results
    test(query);
  }

  @Test
  public void testFilterFlattenedRecords2() throws Exception {
    // previously failed in generated code
    //  "value" is neither a method, a field, nor a member class of "org.apache.arrow.vector.holders.RepeatedVarCharHolder" [ 42eb1fa1-0742-4e4f-8723-609215c18900 on 10.250.0.86:31010 ]
    // appears to be resolving the data coming out of flatten as repeated, check fast schema stuff

    // FIXED BY RETURNING PROPER SCHEMA DURING FAST SCHEMA STEP
    // these types of problems are being solved more generally as we develp better support for chaning schema
    if(RUN_ADVANCED_TESTS){
      test("select celltbl.catl from (\n" +
          "        select flatten(categories) catl from dfs.\"/tmp/yelp_academic_dataset_business.json\" b limit 100\n" +
          "    )  celltbl where celltbl.catl = 'Doctors'");
    }
  }

  @Test
  public void countAggFlattened() throws Exception {
    if(RUN_ADVANCED_TESTS){
      test("select celltbl.catl, count(celltbl.catl) from ( " +
          "select business_id, flatten(categories) catl from dfs.\"/tmp/yelp_academic_dataset_business.json\" b limit 100 " +
          ")  celltbl group by celltbl.catl limit 10 ");
    }
  }


  @Test
  public void flattenAndAdditionalColumn() throws Exception {
    if(RUN_ADVANCED_TESTS){
      test("select business_id, flatten(categories) from dfs.\"/tmp/yelp_academic_dataset_business.json\" b");
    }
  }

  @Test
  public void testFailingFlattenAlone() throws Exception {
    if(RUN_ADVANCED_TESTS){
      test("select flatten(categories) from dfs.\"/tmp/yelp_academic_dataset_business.json\" b  ");
    }
  }

  @Test
  public void testDistinctAggrFlattened() throws Exception {
    if(RUN_ADVANCED_TESTS){
      test(" select distinct(celltbl.catl) from (\n" +
          "        select flatten(categories) catl from dfs.\"/tmp/yelp_academic_dataset_business.json\" b\n" +
          "    )  celltbl");
    }

  }

  @Test
  public void testDrill1665() throws Exception {
    if(RUN_ADVANCED_TESTS){
      test("select id, flatten(evnts) as rpt from dfs.\"/tmp/drill1665.json\"");
    }
  }

  @Test
  public void testFlattenComplexRepeatedMap() throws Exception {
    String query = "select a, flatten(r_map_1), flatten(r_map_2) from cp.\"/store/json/complex_repeated_map.json\"";
    testPlanSubstrPatterns(query, new String[] {"columns=[`r_map_1`, `r_map_2`, `a`]"}, null);
    test(query);
  }

  @Test
  public void testFlatten2_levelRepeatedMap() throws Exception {
    String query = "select flatten(rm) from cp.\"/store/json/2_level_repeated_map.json\"";
    String plan = getPlanInString("explain plan for " + query, OPTIQ_FORMAT);
    // rm is the only column
    assertTrue(plan.contains("columns=[`rm`]") || plan.contains("columns=[`*`]"));
    test(query);
  }

  @Test
  public void testDrill_1770() throws Exception {
    String query = "select flatten(sub.fk.\"value\") from (select flatten(kvgen(map)) fk from cp.\"/store/json/nested_repeated_map.json\") sub";
    String plan = getPlanInString("explain plan for " + query, OPTIQ_FORMAT);
    // Map is the only column in the doc
    assertTrue(plan.contains("columns=[`map`]") || plan.contains("columns=[`*`]"));
    test(query);
  }


  @Test //DRILL-2254
  public void testSingleFlattenFromNestedRepeatedList() throws Exception {
    final String query = "select t.uid, flatten(t.odd) odd from cp.\"project/complex/a.json\" t";
    testPlanSubstrPatterns(query, new String[] {"columns=[`uid`, `odd`]"}, null);
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .jsonBaselineFile("flatten/drill-2254-result-single.json")
        .build()
        .run();
  }

  @Test //DRILL-2254 supplementary
  public void testMultiFlattenFromNestedRepeatedList() throws Exception {
    final String query = "select t.uid, flatten(flatten(t.odd)) odd from cp.\"project/complex/a.json\" t";
    testPlanSubstrPatterns(query, new String[] {"columns=[`uid`, `odd`]"}, null);
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .jsonBaselineFile("flatten/drill-2254-result-multi.json")
        .build()
        .run();

    printResult(testRunAndReturn(QueryType.SQL, query));
  }

  @Test //DRILL-2254 supplementary
  public void testSingleMultiFlattenFromNestedRepeatedList() throws Exception {
    final String query = "select t.uid, flatten(t.odd) once, flatten(flatten(t.odd)) twice from cp.\"project/complex/a.json\" t";
    testPlanSubstrPatterns(query, new String[] {"columns=[`uid`, `odd`]"}, null);
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .jsonBaselineFile("flatten/drill-2254-result-mix.json")
        .build()
        .run();
  }


  @Test
  public void testDrill_2013() throws Exception {
    String query = "select flatten(complex), rownum from cp.\"/store/json/test_flatten_mappify2.json\" where rownum > 5";
    testPlanSubstrPatterns(query, new String[] {"columns=[`rownum`, `complex`]"}, null);
    testBuilder()
            .sqlQuery(query)
            .expectsEmptyResultSet()
            .build().run();
  }

  @Test
  public void testDRILL_2106() throws Exception {
    String query = "select rl, flatten(rl) frl from (select \"integer\", flatten(rl) as rl from cp.\"jsoninput/input2.json\")";
    testPlanSubstrPatterns(query, new String[] {"columns=[`rl`]"}, null);
    testBuilder()
            .sqlQuery(query)
            .unOrdered()
            .jsonBaselineFile("flatten/drill-2106-result.json")
            .go();
  }

  @Test
  public void testDRILL_2106_2() throws Exception {
    String query = "select rl, flatten(rl) frl from (select flatten(rl) as rl, \"integer\" from cp.\"jsoninput/input2.json\")";
    testPlanSubstrPatterns(query, new String[] {"columns=[`rl`]"}, null);
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .jsonBaselineFile("flatten/drill-2106-result.json")
        .go();

  }


  @Test // see DRILL-2146
  @Ignore("DX-17846")
  public void testFlattenWithStar() throws Exception {
    // Push down of filter does not work if pclean is not enabled
    // May be solved later by fixing DX-11163
    test("SET planner.experimental.pclean_logical=true");
    String root = FileUtils.getResourceAsFile("/store/text/sample.json").toURI().toString();
    String q1 = String.format("select *, flatten(j.topping) tt, flatten(j.batters.batter) bb, j.id " +
        "from dfs_test.\"%s\" j " +
        "where j.type = 'donut'", root);
    String q2 = String.format("select *, flatten(j.topping) tt, flatten(j.batters.batter) bb, j.id, j.type " +
        "from dfs_test.\"%s\" j " +
        "where j.type = 'donut'", root);

    // The plan could expand (if costing is changed in scan)
    String plan1 = getPlanInString("explain plan for " + q1, OPTIQ_FORMAT);
    assertTrue("Plan missing values: " + plan1, plan1.contains("columns=[`id`, `type`, `name`, `ppu`, `batters`, `topping`, `batters`.`batter`]")
        || plan1.contains("columns=[`*`]"));
    String plan2 = getPlanInString("explain plan for " + q2, OPTIQ_FORMAT);
    assertTrue("Plan missing values: " + plan2, plan2.contains("columns=[`id`, `type`, `name`, `ppu`, `batters`, `topping`, `batters`.`batter`]")
        || plan2.contains("columns=[`*`]"));

    test(q1);
    test(q2);
  }

  @Test // see DRILL-2012
  public void testMultipleFalttenWithWhereClause() throws Exception {
    String root = FileUtils.getResourceAsFile("/store/text/sample.json").toURI().toString();
    String q1 = String.format("select flatten(j.topping) tt " +
        "from dfs_test.\"%s\" j " +
        "where j.type = 'donut'", root);
    String q2 = String.format("select j.type, flatten(j.topping) tt " +
         "from dfs_test.\"%s\" j " +
         "where j.type = 'donut'", root);

    testPlanSubstrPatterns(q1, new String[] {"columns=[`type`, `topping`]"}, null);
    testPlanSubstrPatterns(q2, new String[] {"columns=[`type`, `topping`]"}, null);
    test(q1);
    test(q2);
  }

  @Test //DRILL-2099
  public void testFlattenAfterSort() throws Exception {
    String query = "select flatten(s1.rms.rptd) rptds from " +
        "(select d.uid uid, flatten(d.map.rm) rms from cp.\"jsoninput/flatten_post_sort.json\" d order by d.uid) s1";
    testPlanSubstrPatterns(query, new String[] {"columns=[`uid`, `map`.`rm`]"}, null);
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .jsonBaselineFile("flatten/drill-2099-result.json")
        .go();
  }

  @Test //DRILL-2268
  public void testFlattenAfterJoin1() throws Exception {
    String query = "select flatten(sub1.events) flat_events  from "+
      "(select t1.events events from cp.\"complex/json/flatten_join.json\" t1 "+
      "inner join cp.\"complex/json/flatten_join.json\" t2 on t1.id=t2.id) sub1";
    testPlanSubstrPatterns(query, new String[] {"columns=[`id`, `events`]", "columns=[`id`]"}, null);
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .jsonBaselineFile("complex/drill-2268-1-result.json")
      .go();
  }

  @Test //DRILL-2268
  public void testFlattenAfterJoin2() throws Exception {
    String query = "select flatten(t1.events) flat_events from cp.\"complex/json/flatten_join.json\" t1 " +
      "inner join cp.\"complex/json/flatten_join.json\" t2 on t1.id=t2.id";
    testPlanSubstrPatterns(query, new String[] {"columns=[`id`, `events`]", "columns=[`id`]"}, null);
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .jsonBaselineFile("complex/drill-2268-2-result.json")
      .go();
  }

  @Test //DRILL-2268
  public void testFlattenAfterJoin3() throws Exception {
    String query = "select flatten(sub1.lst_lst) flat_lst_lst from "+
      "(select t1.lst_lst lst_lst from cp.\"complex/json/flatten_join.json\" t1 "+
      "inner join cp.\"complex/json/flatten_join.json\" t2 on t1.id=t2.id) sub1";

    testPlanSubstrPatterns(query, new String[] {"columns=[`id`, `lst_lst`]", "columns=[`id`]"}, null);
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .jsonBaselineFile("complex/drill-2268-3-result.json")
      .go();
  }

  @Test
  public void testFlattenWithScalarFunc() throws Exception {
    final String query = "select flatten(t.l) + 1  as c1 from cp.\"/jsoninput/input2.json\" t";
    testPlanSubstrPatterns(query, new String[] {"columns=[`l`]"}, null);
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("c1")
        .baselineValues(5L)
        .baselineValues(3L)
        .baselineValues(5L)
        .baselineValues(3L)
        .baselineValues(5L)
        .baselineValues(3L)
        .go();
  }

  @Test
  public void testFlattenWithProjIntoScan_0() throws Exception {
    String query = "select sub.myinteger, sub.zflat.orange from (select flatten(t.z) as zflat, t.\"integer\" as myinteger from cp.\"/jsoninput/input2.json\" t) sub where sub.zflat.orange is not null";
    testPlanSubstrPatterns(query, new String[] {"columns=[`integer`, `z`.`orange`]"}, null);
    String col = isComplexTypeSupport() ? "orange" : "EXPR$1";
    testBuilder().sqlQuery(query).unOrdered().baselineColumns("myinteger", col)
            .baselineValues(2010L, "yellow")
            .baselineValues(6005L, "stucco")
            .go();
  }

  @Test
  public void testFlattenWithProjIntoScan_1() throws Exception {
    String query = "select sub.myinteger, sub.zflat.orange, sub.zflat.pink, sub.lflat from (select flatten(t.z) as zflat, flatten(t.l) as lflat, t.\"integer\" as myinteger from cp.\"/jsoninput/input6.json\" t) sub where sub.zflat.orange is not null";
    testPlanSubstrPatterns(query, new String[] {"columns=[`integer`, `z`.`orange`, `z`.`pink`, `l`]"}, null);
    String col2 = isComplexTypeSupport() ? "orange" : "EXPR$1";
    String col3 = isComplexTypeSupport() ? "pink" : "EXPR$2";
    testBuilder().sqlQuery(query).unOrdered().baselineColumns("myinteger", col2, col3, "lflat")
            .baselineValues(2010L, "yellow", "red", 4L)
            .baselineValues(2010L, "yellow", "red", 2L)
            .baselineValues(6005L, "stucco", null, 4L)
            .baselineValues(6005L, "stucco", null, 2L)
            .go();
  }

  @Test
  public void testFlattenWithProjIntoScan_2() throws Exception {
    String query = "select flatten(t.z) as zflat, flatten(t.l) as lflat, t.\"integer\" as myinteger from cp.\"/jsoninput/input2.json\" t";
    testPlanSubstrPatterns(query, new String[] {"columns=[`integer`, `z`, `l`]"}, null);
    testBuilder().sqlQuery(query).unOrdered().baselineColumns("zflat", "lflat", "myinteger")
            .baselineValues(mapOf("orange", "yellow", "pink", "red"), 4L, 2010L)
            .baselineValues(mapOf("orange", "yellow", "pink", "red"), 2L, 2010L)
            .baselineValues(mapOf("pink", "purple"), 4L, 2010L)
            .baselineValues(mapOf("pink", "purple"), 2L, 2010L)
            .baselineValues(mapOf("pink", "lilac"), 4L, 2001L)
            .baselineValues(mapOf("pink", "lilac"), 2L, 2001L)
            .baselineValues(mapOf("orange", "stucco"), 4L, 6005L)
            .baselineValues(mapOf("orange", "stucco"), 2L, 6005L)
            .go();
  }

  @Test
  public void testFlattenWithProjIntoScan_3() throws Exception {
    String query = "select sub.myinteger, sub.zflat.orange, sub.zflat.pink, sub.lflat from (select flatten(t.z) as zflat, flatten(t.l) as lflat, t.\"integer\" as myinteger from cp.\"/jsoninput/input6.json\" t) sub";
    testPlanSubstrPatterns(query, new String[] {"columns=[`integer`, `z`.`orange`, `z`.`pink`, `l`]"}, null);
    String col2 = isComplexTypeSupport() ? "orange" : "EXPR$1";
    String col3 = isComplexTypeSupport() ? "pink" : "EXPR$2";
    testBuilder().sqlQuery(query).unOrdered().baselineColumns("myinteger", col2, col3, "lflat")
            .baselineValues(2010L, "yellow", "red", 4L)
            .baselineValues(2010L, "yellow", "red", 2L)
            .baselineValues(2010L, null, "purple", 4L)
            .baselineValues(2010L, null, "purple", 2L)
            .baselineValues(2001L, null, "lilac", 4L)
            .baselineValues(2001L, null, "lilac", 2L)
            .baselineValues(6005L, "stucco", null, 4L)
            .baselineValues(6005L, "stucco", null, 2L)
            .go();
  }

  @Test
  public void testFlattenWithProjIntoScan_4() throws Exception {
    String query = "select flatten(t.z) as zflat, flatten(t.l) as lflat from cp.\"/jsoninput/input2.json\" t";
    testPlanSubstrPatterns(query, new String[] {"columns=[`z`, `l`]"}, null);
    testBuilder().sqlQuery(query).unOrdered().baselineColumns("zflat", "lflat")
            .baselineValues(mapOf("orange", "yellow", "pink", "red"), 4L)
            .baselineValues(mapOf("orange", "yellow", "pink", "red"), 2L)
            .baselineValues(mapOf("pink", "purple"), 4L)
            .baselineValues(mapOf("pink", "purple"), 2L)
            .baselineValues(mapOf("pink", "lilac"), 4L)
            .baselineValues(mapOf("pink", "lilac"), 2L)
            .baselineValues(mapOf("orange", "stucco"), 4L)
            .baselineValues(mapOf("orange", "stucco"), 2L)
            .go();
  }

  @Test
  public void testFlattenWithProjIntoScan_5() throws Exception {
    String query = "select flatten(t.l) as lflat from cp.\"/jsoninput/input2.json\" t";
    testPlanSubstrPatterns(query, new String[] {"columns=[`l`]"}, null);
    testBuilder().sqlQuery(query).unOrdered().baselineColumns("lflat")
            .baselineValues(4L).baselineValues(2L)
            .baselineValues(4L).baselineValues(2L)
            .baselineValues(4L).baselineValues(2L)
            .go();
  }

  @Test
  public void testFlattenWithProjIntoScan_6() throws Exception {
    String query = "select flatten(t.z) as zflat from cp.\"/jsoninput/input2.json\" t";
    testPlanSubstrPatterns(query, new String[] {"columns=[`z`]"}, null);
    testBuilder().sqlQuery(query).unOrdered().baselineColumns("zflat")
            .baselineValues(mapOf("orange", "yellow", "pink", "red"))
            .baselineValues(mapOf("pink", "purple"))
            .baselineValues(mapOf("pink", "lilac"))
            .baselineValues(mapOf("orange", "stucco"))
            .go();
  }

  @Test
  public void testFlattenWithParquet_0() throws Exception {
    String query = "select flatten(t.z) as zflat, flatten(t.l) as lflat from dfs_test.parquetTable t";
    testPlanSubstrPatterns(query, new String[] {"columns=[`z`, `l`]"}, null);
    testBuilder().sqlQuery(query).unOrdered().baselineColumns("zflat", "lflat")
            .baselineValues(mapOf("orange", "yellow", "pink", "red", "white", "polar"), 4L)
            .baselineValues(mapOf("orange", "yellow", "pink", "red", "white", "polar"), 2L)
            .baselineValues(mapOf("pink", "purple"), 4L)
            .baselineValues(mapOf("pink", "purple"), 2L)
            .baselineValues(mapOf("pink", "lilac"), 4L)
            .baselineValues(mapOf("pink", "lilac"), 2L)
            .baselineValues(mapOf("orange", "stucco"), 4L)
            .baselineValues(mapOf("orange", "stucco"), 2L)
            .go();
  }

  @Test
  public void testFlattenWithParquet_mapOfListOfMap() throws Exception {
    final String outputFile = "dfs_test.map_list_map_parquet";
    runSQL("CREATE TABLE " + outputFile + " AS SELECT * FROM cp.\"/json/map_list_map.json\"");
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT sub.flat.field as field FROM (SELECT flatten(t.mymap.mylist) as flat FROM %s t) sub", outputFile)
      .baselineColumns("field")
      .baselineValues(1L)
      .baselineValues(2L)
      .baselineValues(3L)
      .baselineValues(1L)
      .baselineValues((Object) null)
      .baselineValues((Object) null)
      .go();
  }

  @Test
  public void testFlattenWithParquet_1() throws Exception {
    String query = "select sub.myinteger, sub.zflat.orange, sub.zflat.pink, sub.lflat from (select flatten(t.z) as zflat, flatten(t.l) as lflat, t.\"integer\" as myinteger from dfs_test.parquetTable t) sub";
    testPlanSubstrPatterns(query, new String[] {"columns=[`integer`, `z`.`orange`, `z`.`pink`, `l`]"}, null);
    String col2 = isComplexTypeSupport() ? "orange" : "EXPR$1";
    String col3 = isComplexTypeSupport() ? "pink" : "EXPR$2";
    testBuilder().sqlQuery(query).unOrdered().baselineColumns("myinteger", col2, col3, "lflat")
            .baselineValues(2010L, "yellow", "red", 4L)
            .baselineValues(2010L, "yellow", "red", 2L)
            .baselineValues(2010L, null, "purple", 4L)
            .baselineValues(2010L, null, "purple", 2L)
            .baselineValues(2001L, null, "lilac", 4L)
            .baselineValues(2001L, null, "lilac", 2L)
            .baselineValues(6005L, "stucco", null, 4L)
            .baselineValues(6005L, "stucco", null, 2L)
            .go();
  }

  @Test
  public void ensureFieldNotDependentOnFlattenIsPushedBelowFlatten() throws Exception {

    final String query = "SELECT errorCode, errorMessage, nested_1.data.q AS q, nested_1.data.v AS v, TO_TIMESTAMP(nested_1.data.ts, 'YYYY-MM-DD\"T\"HH24:MI:SS.FFFTZO') AS ts, tagId\n" +
        "FROM (\n" +
        "  SELECT errorCode, errorMessage, flatten(nested_0.tagList.data) AS data, nested_0.tagList.tagId AS tagId\n" +
        "  FROM (\n" +
        "    SELECT errorCode, errorMessage, flatten(tagList) AS tagList\n" +
        "    FROM cp.\"flatten/multiflatten.json\"\n" +
        "  ) nested_0\n" +
        ") nested_1";

    final String plan = getPlanInString("EXPLAIN PLAN for " + QueryTestUtil.normalizeQuery(query), OPTIQ_FORMAT);
    assertTrue(Pattern.compile(".*Flatten.*(ITEM[^\\)]*tagId|\\$0\\.tagId).*", Pattern.MULTILINE + Pattern.DOTALL).matcher(plan).matches());
  }

  @Test
  public void testFlattenWithSubQuery() throws Exception {
    String query = "select \"integer\" from cp.\"/jsoninput/input2.json\" where 2 in (select flatten(t.l) from cp.\"/jsoninput/input2.json\" t)";
    testPlanSubstrPatterns(query, new String[] {"columns=[`integer`]", "columns=[`l`]"}, null);
    testBuilder().sqlQuery(query).unOrdered().baselineColumns("integer")
      .baselineValues(2010L)
      .baselineValues(-2002L)
      .baselineValues(2001L)
      .baselineValues(6005L)
      .go();
  }

  @Test //DX-9579
  public void testFlattenInJoinCondition1() throws Exception {
    try {
      String query = "select t1.id id from cp.\"complex/json/flatten_join.json\" t1 "+
        "inner join cp.\"complex/json/flatten_join.json\" t2 on flatten(t1.lst_lst) = t2.id";
      testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .jsonBaselineFile("complex/drill-2268-3-result.json")
        .go();
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Flatten is not supported as part of join condition"));
    }
  }

  @Test //DX-9579
  public void testFlattenInJoinCondition2() throws Exception {
    try {
      String query = "select t1.id id from cp.\"complex/json/flatten_join.json\" t1 "+
        "inner join cp.\"complex/json/flatten_join.json\" t2 on flatten(t1.lst_lst) + 1 = t2.id";
      testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .jsonBaselineFile("complex/drill-2268-3-result.json")
        .go();
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Flatten is not supported as part of join condition"));
    }
  }
}

