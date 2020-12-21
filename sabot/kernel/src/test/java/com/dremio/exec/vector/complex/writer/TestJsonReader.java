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
package com.dremio.exec.vector.complex.writer;

import static com.dremio.TestBuilder.listOf;
import static com.dremio.TestBuilder.mapOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.util.JsonStringArrayList;
import org.apache.arrow.vector.util.JsonStringHashMap;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.PlanTestBase;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.util.FileUtils;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.CatalogOptions;
import com.dremio.exec.catalog.ColumnCountTooLargeException;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.record.RecordBatchLoader;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.sabot.rpc.user.QueryDataBatch;
import com.google.common.base.Charsets;
import com.google.common.io.Files;


public class TestJsonReader extends PlanTestBase {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestJsonReader.class);

  private static final boolean VERBOSE_DEBUG = false;

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Ignore("DX-9357. Test is file order dependent.")
  @Test
  public void testEmptyList() throws Exception {
    String root = FileUtils.getResourceAsFile("/store/json/emptyLists").toURI().toString();
    String query = String.format("select count(a[0]) as ct from dfs_test.\"%s\"", root);

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("ct")
        .baselineValues(6l)
        .build()
        .run();
  }

  @Test
  @Ignore
  public void schemaChange() throws Exception {
    test("select b from dfs.\"${WORKING_PATH}/src/test/resources/vector/complex/writer/schemaChange/\"");
  }

  @Test
  @Ignore("all text mode")
  public void testFieldSelectionBug() throws Exception {
    try {
      testBuilder()
          .sqlQuery("select t.field_4.inner_3 as col_1, t.field_4 as col_2 from cp.\"store/json/schema_change_int_to_string.json\" t")
          .unOrdered()
          .optionSettingQueriesForTestQuery("alter session set \"store.json.all_text_mode\" = true")
          .baselineColumns("col_1", "col_2")
          .baselineValues(
              mapOf(),
              mapOf(
                  "inner_1", listOf(),
                  "inner_3", mapOf()))
          .baselineValues(
              mapOf("inner_object_field_1", "2"),
              mapOf(
                  "inner_1", listOf("1", "2", "3"),
                  "inner_2", "3",
                  "inner_3", mapOf("inner_object_field_1", "2")))
          .baselineValues(
              mapOf(),
              mapOf(
                  "inner_1", listOf("4", "5", "6"),
                  "inner_2", "3",
                  "inner_3", mapOf()))
          .go();
    } finally {
      test("alter session set \"store.json.all_text_mode\" = false");
    }
  }

  @Test
  public void testSplitAndTransferFailure() throws Exception {
    final String testVal = "a string";
    testBuilder()
        .sqlQuery("select flatten(config) as flat from cp.\"/store/json/null_list.json\"")
        .ordered()
        .baselineColumns("flat")
        .baselineValues(listOf())
        .baselineValues(listOf(testVal))
        .go();

    testBuilder()
        .sqlQuery("select flatten(config) as flat from cp.\"/store/json/null_list_v2.json\"")
        .ordered()
        .baselineColumns("flat")
        .baselineValues(mapOf())
        .baselineValues(mapOf("repeated_varchar", listOf(testVal)))
        .go();

    testBuilder()
        .sqlQuery("select flatten(config) as flat from cp.\"/store/json/null_list_v3.json\"")
        .ordered()
        .baselineColumns("flat")
        .baselineValues(mapOf("repeated_map", listOf(mapOf())))
        .baselineValues(mapOf("repeated_map", listOf(mapOf("repeated_varchar", listOf(testVal)))))
        .go();
  }

  @Test
  @Ignore("DRILL-1824")
  public void schemaChangeValidate() throws Exception {
    testBuilder() //
      .sqlQuery("select b from dfs.\"${WORKING_PATH}/src/test/resources/vector/complex/writer/schemaChange/\"") //
      .unOrdered() //
      .jsonBaselineFile("/vector/complex/writer/expected.json") //
      .build()
      .run();
  }

  public void runTestsOnFile(String filename, UserBitShared.QueryType queryType, String[] queries, long[] rowCounts) throws Exception {
    if (VERBOSE_DEBUG) {
      System.out.println("===================");
      System.out.println("source data in json");
      System.out.println("===================");
      System.out.println(Files.toString(FileUtils.getResourceAsFile(filename), Charsets.UTF_8));
    }

    int i = 0;
    for (String query : queries) {
      if (VERBOSE_DEBUG) {
        System.out.println("=====");
        System.out.println("query");
        System.out.println("=====");
        System.out.println(query);
        System.out.println("======");
        System.out.println("result");
        System.out.println("======");
      }
      int rowCount = testRunAndPrint(queryType, query);
      assertEquals(rowCounts[i], rowCount);
      System.out.println();
      i++;
    }
  }

  @Test
  public void testReadCompressed() throws Exception {
    String filepath = "compressed_json.json";
    File f = folder.newFile(filepath);
    PrintWriter out = new PrintWriter(f);
    out.println("{\"a\" :5}");
    out.close();

    gzipIt(f);
    testBuilder()
        .sqlQuery("select * from dfs.\"" + f.getPath() + ".gz" + "\"")
        .unOrdered()
        .baselineColumns("a")
        .baselineValues(5l)
        .build().run();

    // test reading the uncompressed version as well
    testBuilder()
        .sqlQuery("select * from dfs.\"" + f.getPath() + "\"")
        .unOrdered()
        .baselineColumns("a")
        .baselineValues(5l)
        .build().run();
  }

  public static void gzipIt(File sourceFile) throws IOException {

    // modified from: http://www.mkyong.com/java/how-to-compress-a-file-in-gzip-format/
    byte[] buffer = new byte[1024];
    GZIPOutputStream gzos =
        new GZIPOutputStream(new FileOutputStream(sourceFile.getPath() + ".gz"));

    FileInputStream in =
        new FileInputStream(sourceFile);

    int len;
    while ((len = in.read(buffer)) > 0) {
      gzos.write(buffer, 0, len);
    }
    in.close();
    gzos.finish();
    gzos.close();
  }

  @Test
  public void testDrill_1419() throws Exception {
    String[] queries = {"select t.trans_id, t.trans_info.prod_id[0],t.trans_info.prod_id[1] from cp.\"/store/json/clicks.json\" t limit 5"};
    long[] rowCounts = {5};
    String filename = "/store/json/clicks.json";
    runTestsOnFile(filename, UserBitShared.QueryType.SQL, queries, rowCounts);
  }

  @Test
  public void testLeafLimitInnerDoc() throws Exception {
    testLeafLimit("/store/json/nestedDoc.json");
  }

  @Test
  public void testLeafMixed() throws Exception {
    testLeafLimit("/store/json/mixedTypes.json");
  }

  @Test
  public void testTextLeafLimitInnerDoc() throws Exception {
    try (AutoCloseable op = withOption(ExecConstants.JSON_READER_ALL_TEXT_MODE_VALIDATOR, true)) {
      testLeafLimit("/store/json/nestedDoc.json");
    }
  }

  @Test
  public void testTextLeafMixed() throws Exception {
    try (AutoCloseable op = withOption(ExecConstants.JSON_READER_ALL_TEXT_MODE_VALIDATOR, true)) {
      testLeafLimit("/store/json/mixedTypes.json");
    }
  }

  private void testLeafLimit(String file) throws Exception {
    try (AutoCloseable op1 = withOption(CatalogOptions.METADATA_LEAF_COLUMN_MAX, 4)) {
      String[] queries = {"select * from cp.\"" + file + "\" t"};
      long[] rowCounts = {1};
      runTestsOnFile(file, UserBitShared.QueryType.SQL, queries, rowCounts);
      Assert.fail("Expected ColumnCountTooLargeException from exceeding metadata max leaf limit");
    } catch (UserException e) {
      Assert.assertEquals(ColumnCountTooLargeException.class.getCanonicalName(), e.getOrCreatePBError(false).getException().getExceptionClass());
    }
  }

  @Test
  public void testSingleColumnRead_vector_fill_bug() throws Exception {
    String[] queries = {"select * from cp.\"/store/json/single_column_long_file.json\""};
    long[] rowCounts = {13512};
    String filename = "/store/json/single_column_long_file.json";
    runTestsOnFile(filename, UserBitShared.QueryType.SQL, queries, rowCounts);
  }

  @Test
  @Ignore
  public void testNonExistentColumnReadAlone() throws Exception {
    String[] queries = {"select non_existent_column from cp.\"/store/json/single_column_long_file.json\""};
    long[] rowCounts = {13512};
    String filename = "/store/json/single_column_long_file.json";
    runTestsOnFile(filename, UserBitShared.QueryType.SQL, queries, rowCounts);
  }

  @Test
  public void readComplexWithStar() throws Exception {
    test("select * from cp.\"/store/json/test_complex_read_with_star.json\"");
    List<QueryDataBatch> results = testSqlWithResults("select * from cp.\"/store/json/test_complex_read_with_star.json\"");
    assertEquals(1, results.size());

    RecordBatchLoader batchLoader = new RecordBatchLoader(getAllocator());
    QueryDataBatch batch = results.get(0);

    assertTrue(batchLoader.load(batch.getHeader().getDef(), batch.getData()));
    assertEquals(3, batchLoader.getSchema().getFieldCount());
    testExistentColumns(batchLoader);

    batch.release();
    batchLoader.clear();
  }

  @Test
  public void testNullWhereListExpected() throws Exception {
    String[] queries = {"select * from cp.\"/store/json/null_where_list_expected.json\""};
    long[] rowCounts = {3};
    String filename = "/store/json/null_where_list_expected.json";
    runTestsOnFile(filename, UserBitShared.QueryType.SQL, queries, rowCounts);
  }

  @Test
  public void testNullWhereMapExpected() throws Exception {
    String[] queries = {"select * from cp.\"/store/json/null_where_map_expected.json\""};
    long[] rowCounts = {3};
    String filename = "/store/json/null_where_map_expected.json";
    runTestsOnFile(filename, UserBitShared.QueryType.SQL, queries, rowCounts);
  }

  @Test
  public void testNullAndEmptyMaps() throws Exception {
    testBuilder()
            .ordered()
            .sqlQuery("SELECT map FROM cp.\"/json/null_map.json\" LIMIT 10")
            .baselineColumns("map")
            .baselineValues(mapOf("a", 1L, "b", 2L, "c", 3L))
            .baselineValues(((JsonStringHashMap<String, Object>) null))
            .baselineValues(mapOf())
            .baselineValues(((JsonStringHashMap<String, Object>) null))
            .baselineValues(mapOf("a", 1L, "b", 2L, "c", 3L))
            .build()
            .run();
  }

  @Test
  public void testNullAndEmptyLists() throws Exception {
    testBuilder()
            .ordered()
            .sqlQuery("SELECT mylist FROM cp.\"/json/null_list.json\" LIMIT 10")
            .baselineColumns("mylist")
            .baselineValues(listOf("a", "b", "c"))
            .baselineValues(((JsonStringArrayList<Object>) null))
            .baselineValues(listOf())
            .baselineValues(((JsonStringArrayList<Object>) null))
            .baselineValues(listOf("a", "b", "c"))
            .build()
            .run();
  }

  @Test
  @Ignore("schema change in raw data")
  public void ensureProjectionPushdown() throws Exception {
    // Tests to make sure that we are correctly eliminating schema changing columns.  If completes, means that the projection pushdown was successful.
    test("alter system set \"store.json.all_text_mode\" = false; "
        + "select  t.field_1, t.field_3.inner_1, t.field_3.inner_2, t.field_4.inner_1 "
        + "from cp.\"store/json/schema_change_int_to_string.json\" t");
  }

  // The project pushdown rule is correctly adding the projected columns to the scan, however it is not removing
  // the redundant project operator after the scan, this tests runs a physical plan generated from one of the tests to
  // ensure that the project is filtering out the correct data in the scan alone
  @Test
  @Ignore
  public void testProjectPushdown() throws Exception {
    String[] queries = {Files.toString(FileUtils.getResourceAsFile("/store/json/project_pushdown_json_physical_plan.json"), Charsets.UTF_8)};
    long[] rowCounts = {3};
    String filename = "/store/json/schema_change_int_to_string.json";
    runTestsOnFile(filename, UserBitShared.QueryType.PHYSICAL, queries, rowCounts);

    List<QueryDataBatch> results = testPhysicalWithResults(queries[0]);
    assertEquals(1, results.size());
    // "`field_1`", "`field_3`.`inner_1`", "`field_3`.`inner_2`", "`field_4`.`inner_1`"

    RecordBatchLoader batchLoader = new RecordBatchLoader(getAllocator());
    QueryDataBatch batch = results.get(0);
    assertTrue(batchLoader.load(batch.getHeader().getDef(), batch.getData()));

    // this used to be five.  It is now three.  This is because the plan doesn't have a project.
    // Scanners are not responsible for projecting non-existent columns (as long as they project one column)
    assertEquals(3, batchLoader.getSchema().getFieldCount());
    testExistentColumns(batchLoader);

    batch.release();
    batchLoader.clear();
  }

  @Test
  @Ignore("ignored until DX-3863 is fixed. looks like sampling is not working as expected?")
    public void testJsonDirectoryWithEmptyFile() throws Exception {
    String root = FileUtils.getResourceAsFile("/store/json/jsonDirectoryWithEmpyFile").toURI().toString();

    String queryRightEmpty = String.format(
        "select * from dfs_test.\"%s\"", root);
    try {
      test(queryRightEmpty);
    } catch (Exception e) {

    }

    testBuilder()
        .sqlQuery(queryRightEmpty)
        .unOrdered()
        .baselineColumns("a")
        .baselineValues(1l)
        .build()
        .run();
  }

  private void testExistentColumns(RecordBatchLoader batchLoader) throws SchemaChangeException {
    VectorWrapper<?> vw = batchLoader.getValueAccessorById(
        ListVector.class, //
        batchLoader.getValueVectorId(SchemaPath.getCompoundPath("field_1")).getFieldIds() //
    );
    assertEquals("[1]", vw.getValueVector().getObject(0).toString());
    assertEquals("[5]", vw.getValueVector().getObject(1).toString());
    assertEquals("[5,10,15]", vw.getValueVector().getObject(2).toString());

    vw = batchLoader.getValueAccessorById(
        IntVector.class, //
        batchLoader.getValueVectorId(SchemaPath.getCompoundPath("field_3", "inner_1")).getFieldIds() //
    );
    assertNull(vw.getValueVector().getObject(0));
    assertEquals(2l, vw.getValueVector().getObject(1));
    assertEquals(5l, vw.getValueVector().getObject(2));

    vw = batchLoader.getValueAccessorById(
        IntVector.class, //
        batchLoader.getValueVectorId(SchemaPath.getCompoundPath("field_3", "inner_2")).getFieldIds() //
    );
    assertNull(vw.getValueVector().getObject(0));
    assertNull(vw.getValueVector().getObject(1));
    assertEquals(3l, vw.getValueVector().getObject(2));

    vw = batchLoader.getValueAccessorById(
        ListVector.class, //
        batchLoader.getValueVectorId(SchemaPath.getCompoundPath("field_4", "inner_1")).getFieldIds() //
    );
    assertEquals("[1,2,3]", vw.getValueVector().getObject(1).toString());
    assertEquals("[4,5,6]", vw.getValueVector().getObject(2).toString());
  }

  @Test
  @Ignore("schema learning")
  public void drill_4032() throws Exception {
    String dfs_temp = getDfsTestTmpSchemaLocation();
    File table_dir = new File(dfs_temp, "drill_4032");
    table_dir.mkdir();
    BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream(new File(table_dir, "a.json")));
    os.write("{\"col1\": \"val1\",\"col2\": null}".getBytes());
    os.write("{\"col1\": \"val1\",\"col2\": {\"col3\":\"abc\", \"col4\":\"xyz\"}}".getBytes());
    os.flush();
    os.close();
    os = new BufferedOutputStream(new FileOutputStream(new File(table_dir, "b.json")));
    os.write("{\"col1\": \"val1\",\"col2\": null}".getBytes());
    os.write("{\"col1\": \"val1\",\"col2\": null}".getBytes());
    os.flush();
    os.close();
    testNoResult("select t.col2.col3 from dfs_test.tmp.drill_4032 t");
  }

  @Test
  @Ignore("all text mode")
  public void drill_4479() throws Exception {
    int numRowsPerBatch = 4096;
    try {
      String dfs_temp = getDfsTestTmpSchemaLocation();
      File table_dir = new File(dfs_temp, "drill_4479");
      table_dir.mkdir();
      BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream(new File(table_dir, "mostlynulls.json")));
      // Create an entire batch of null values for 3 columns
      for (int i = 0; i < numRowsPerBatch; i++) {
        os.write("{\"a\": null, \"b\": null, \"c\": null}".getBytes());
      }
      // Add a row with {bigint,  float, string} values
      os.write("{\"a\": 123456789123, \"b\": 99.999, \"c\": \"Hello World\"}".getBytes());
      os.flush();
      os.close();

      String query1 = "select c, count(*) as cnt from dfs_test.tmp.drill_4479 t group by c";
      String query2 = "select a, b, c, count(*) as cnt from dfs_test.tmp.drill_4479 t group by a, b, c";
      String query3 = "select max(a) as x, max(b) as y, max(c) as z from dfs_test.tmp.drill_4479 t";

      testBuilder()
        .sqlQuery(query1)
        .ordered()
        .optionSettingQueriesForTestQuery("alter session set \"store.json.all_text_mode\" = true")
        .baselineColumns("c", "cnt")
        .baselineValues(null, 4096L)
        .baselineValues("Hello World", 1L)
        .go();

      testBuilder()
        .sqlQuery(query2)
        .ordered()
        .optionSettingQueriesForTestQuery("alter session set \"store.json.all_text_mode\" = true")
        .baselineColumns("a", "b", "c", "cnt")
        .baselineValues(null, null, null, 4096L)
        .baselineValues("123456789123", "99.999", "Hello World", 1L)
        .go();

      testBuilder()
        .sqlQuery(query3)
        .ordered()
        .optionSettingQueriesForTestQuery("alter session set \"store.json.all_text_mode\" = true")
        .baselineColumns("x", "y", "z")
        .baselineValues("123456789123", "99.999", "Hello World")
        .go();

    } finally {
      testNoResult("alter session set \"store.json.all_text_mode\" = false");
    }
  }

  @Test
  public void testSkipAll() throws Exception {
    final String query = "SELECT count(*) FROM cp.\"json/map_list_map.json\"";
    testPhysicalPlan(query, "columns=[]");
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .sqlBaselineQuery("SELECT count(id) FROM cp.\"json/map_list_map.json\"")
        .go();
  }

  @Test
  public void caseStruct1() throws Exception {
    final String query = "SELECT CASE WHEN t.a.b = 1 THEN t.a ELSE null END AS a FROM cp.\"jsoninput/input4.json\" t";

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("a")
        .baselineValues(mapOf("b", 1L, "c", mapOf("d", 2L)))
        .build()
        .run();
  }

  @Test
  public void caseList1() throws Exception {
    final String query = "SELECT FLATTEN(CASE WHEN t.z[0].orange = 'yellow' THEN t.rl[0] ELSE t.l END) AS a" +
        " FROM cp.\"jsoninput/input2.json\" t";

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("a")
        .baselineValues(2L)
        .baselineValues(1L)
        .baselineValues(4L)
        .baselineValues(2L)
        .baselineValues(4L)
        .baselineValues(2L)
        .build()
        .run();
  }


  @Test
  public void caseList2() throws Exception {
    final String query = "SELECT FLATTEN(CASE WHEN t.z[0].orange = 'yellow' THEN null ELSE t.l END) AS a" +
        " FROM cp.\"jsoninput/input2.json\" t";

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("a")
        .baselineValues(4L)
        .baselineValues(2L)
        .baselineValues(4L)
        .baselineValues(2L)
        .build()
        .run();
  }
}
