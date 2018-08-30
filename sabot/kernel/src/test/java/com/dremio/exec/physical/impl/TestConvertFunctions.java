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
package com.dremio.exec.physical.impl;

import static com.dremio.TestBuilder.listOf;
import static com.dremio.TestBuilder.mapOf;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.util.DateUtility;
import org.apache.arrow.vector.util.JsonStringArrayList;
import org.apache.arrow.vector.util.JsonStringHashMap;
import org.joda.time.LocalDateTime;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.expr.fn.FunctionErrorContext;
import com.dremio.exec.expr.fn.FunctionErrorContextBuilder;
import com.dremio.exec.proto.UserBitShared.QueryType;
import com.dremio.exec.record.RecordBatchLoader;
import com.dremio.exec.util.ByteBufUtil.HadoopWritables;
import com.dremio.sabot.rpc.user.QueryDataBatch;

import io.netty.buffer.ArrowBuf;

public class TestConvertFunctions extends BaseTestQuery {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestConvertFunctions.class);

  private static final FunctionErrorContext context = FunctionErrorContextBuilder.builder().build();

  private static final float DELTA = (float) 0.0001;

  // "1980-01-01 01:23:45.678"
  private static final String DATE_TIME_BE = "\\x00\\x00\\x00\\x49\\x77\\x85\\x1f\\x8e";
  private static final String DATE_TIME_LE = "\\x8e\\x1f\\x85\\x77\\x49\\x00\\x00\\x00";

  private static LocalDateTime time = LocalDateTime.parse("01:23:45.678", DateUtility.getTimeFormatter());
  private static LocalDateTime date = LocalDateTime.parse("1980-01-01", DateUtility.getDateTimeFormatter());

  String textFileContent;

  @Test // DRILL-3854
  public void testConvertFromConvertToInt() throws Exception {
    try {
      final String newTblName = "testConvertFromConvertToInt_tbl";
      final String ctasQuery = String.format("CREATE TABLE %s.%s as \n" +
          "SELECT convert_to(r_regionkey, 'INT') as ct \n" +
          "FROM cp.\"tpch/region.parquet\"",
          TEMP_SCHEMA, newTblName);
      final String query = String.format("SELECT convert_from(ct, 'INT') as cf \n" +
          "FROM %s.%s \n" +
          "ORDER BY ct",
          TEMP_SCHEMA, newTblName);

      test("alter session set \"planner.slice_target\" = 1");
      test(ctasQuery);
      testBuilder()
          .sqlQuery(query)
          .ordered()
          .baselineColumns("cf")
          .baselineValues(0)
          .baselineValues(1)
          .baselineValues(2)
          .baselineValues(3)
          .baselineValues(4)
          .build()
          .run();
    } finally {
      test("alter session set \"planner.slice_target\" = " + ExecConstants.SLICE_TARGET_DEFAULT);
    }
  }

  @Test
  public void testExplainConvertFromJSON() throws Exception {
    final String subQuery = "SELECT CONVERT_FROM(list, 'JSON') AS L, CONVERT_FROM(map, 'JSON') AS M FROM cp.\"functions/conv/list_map.json\"";
    final String query = "SELECT q.L[1] AS L1, q.M.f AS Mf FROM (" + subQuery + ") q";
    test("EXPLAIN PLAN FOR " + query);
  }

  @Test
  public void testQueryListFromMultipleConvertFromJSON() throws Exception {
    final String subQuery = "SELECT CONVERT_FROM(list, 'JSON') AS L, CONVERT_FROM(map, 'JSON') AS M FROM cp.\"functions/conv/list_map.json\"";
    final String query = "SELECT q.L[1] AS L1 FROM (" + subQuery + ") q";

    setEnableReAttempts(true);
    try {
      testRunAndPrint(QueryType.SQL, subQuery);
      testBuilder()
        .ordered()
        .sqlQuery(query)
        .baselineColumns("L1")
        .baselineValues("b")
        .go();
    } finally {
      setEnableReAttempts(false);
    }
  }

  @Test
  public void testQueryMapFromMultipleConvertFromJSON() throws Exception {
    final String subQuery = "SELECT CONVERT_FROM(list, 'JSON') AS L, CONVERT_FROM(map, 'JSON') AS M FROM cp.\"functions/conv/list_map.json\"";
    final String query = "SELECT q.M.f AS Mf FROM (" + subQuery + ") q";

    setEnableReAttempts(true);
    try {
      testRunAndPrint(QueryType.SQL, subQuery);
      testBuilder()
        .ordered()
        .sqlQuery(query)
        .baselineColumns("Mf")
        .baselineValues(10L)
        .go();
    } finally {
      setEnableReAttempts(false);
    }
  }

  @Test
  public void testQueryListMapFromMultipleConvertFromJSON() throws Exception {
    final String subQuery = "SELECT CONVERT_FROM(list, 'JSON') AS L, CONVERT_FROM(map, 'JSON') AS M FROM cp.\"functions/conv/list_map.json\"";
    final String query = "SELECT q.L[1] AS L1, q.M.f AS Mf FROM (" + subQuery + ") q";

    setEnableReAttempts(true);
    try {
      testRunAndPrint(QueryType.SQL, subQuery);
      testBuilder()
        .ordered()
        .sqlQuery(query)
        .baselineColumns("L1", "Mf")
        .baselineValues("b", 10L)
        .go();
    } finally {
      setEnableReAttempts(false);
    }
  }

  private String prepareConvertTestQuery(String inputFile, String inputField, String outputField, String ctasTable) throws Exception {
    final String ctas = String.format("CREATE TABLE dfs_test.%s AS SELECT CONVERT_TO(%s, 'JSON') AS %s FROM %s",
      ctasTable, inputField, outputField, inputFile);
    runSQL(ctas);
    return String.format("SELECT CONVERT_FROM(%s, 'JSON') AS %s FROM dfs_test.%s", outputField, outputField, ctasTable);
  }

  @Test
  public void test_JSON_convertTo_empty_null_lists() throws Exception {
    final String query = prepareConvertTestQuery("cp.\"/json/null_list.json\"", "mylist", "list","null_list_json");
    setEnableReAttempts(true);
    try {
      testBuilder()
        .ordered()
        .sqlQuery(query)
        .baselineColumns("list")
        .baselineValues(listOf("a", "b", "c"))
        .baselineValues(((JsonStringArrayList<Object>) null))
        .baselineValues(listOf())
        .baselineValues(((JsonStringArrayList<Object>) null))
        .baselineValues(listOf("a", "b", "c"))
        .build()
        .run();
    } finally {
      setEnableReAttempts(false);
    }
  }

  @Test
  public void test_JSON_convertTo_empty_null_maps() throws Exception {
    final String query = prepareConvertTestQuery("cp.\"/json/null_map.json\"", "map", "map", "null_map_json");
    setEnableReAttempts(true);
    try {
      testBuilder()
        .ordered()
        .sqlQuery(query)
        .baselineColumns("map")
        .baselineValues(mapOf("a", 1L, "b", 2L, "c", 3L))
        .baselineValues(((JsonStringHashMap<String, Object>) null))
        .baselineValues(mapOf())
        .baselineValues(((JsonStringHashMap<String, Object>) null))
        .baselineValues(mapOf("a", 1L, "b", 2L, "c", 3L))
        .build()
        .run();
    } finally {
      setEnableReAttempts(false);
    }
  }

  @Test
  public void test_castConvertToEmptyListErrorDrill1416Part1() throws Exception {
    final String query = prepareConvertTestQuery("cp.\"/store/json/input2.json\"", "rl[1]", "list_col", "input2_json");

    errorMsgTestHelper("SELECT CAST(list_col AS VARCHAR(100)) FROM dfs_test.input2_json",
      "Cast function cannot convert value of type VARBINARY(65536) to type VARCHAR(100)");

    setEnableReAttempts(true);
    try {
      Object listVal = listOf(4L, 6L);
      testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("list_col")
        .baselineValues(listVal)
        .baselineValues((JsonStringArrayList<Object>) null)
        .baselineValues(listVal)
        .baselineValues(listVal)
        .go();
    } finally {
      setEnableReAttempts(false);
    }
  }

  @Test
  public void test_castConvertToEmptyListErrorDrill1416Part2() throws Exception {
    final String query = prepareConvertTestQuery("cp.\"/store/json/json_project_null_object_from_list.json\"", "rl[1]", "map_col", "json_project_null_json");

    Object mapVal1 = mapOf("f1", 4L, "f2", 6L);
    Object mapVal2 = mapOf("f1", 11L);
    setEnableReAttempts(true);
    try {
      testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("map_col")
        .baselineValues(mapVal1)
        .baselineValues((JsonStringHashMap<String, Object>) null)
        .baselineValues(mapVal2)
        .baselineValues(mapVal1)
        .go();
    } finally {
      setEnableReAttempts(false);
    }
  }

  @Test
  public void testConvertToComplexJSON() throws Exception {
    errorMsgTestHelper("select cast(convert_to(rl[1], 'EXTENDEDJSON') as varchar(100)) as json_str from cp.\"/store/json/input2.json\"",
        "Cast function cannot convert value of type VARBINARY(65536) to type VARCHAR(100)");

    String result1 =
        "[ {\n" +
            "  \"$numberLong\" : 4\n" +
            "}, {\n" +
            "  \"$numberLong\" : 6\n" +
            "} ]";
    String[] result2 = new String[] { null };

    testBuilder()
        .sqlQuery("select cast(convert_from(convert_to(rl[1], 'EXTENDEDJSON'), 'UTF8') as varchar(100)) as json_str from cp.\"/store/json/input2.json\"")
        .unOrdered()
        .baselineColumns("json_str")
        .baselineValues(result1)
        .baselineValues(result2)
        .baselineValues(result1)
        .baselineValues(result1)
        .go();

  }

  @Test
  public void testFixedInts4SQL_from() throws Throwable {
    verifySQL("select"
           + "   convert_from(binary_string('\\xBE\\xBA\\xFE\\xCA'), 'INT')"
           + " from"
           + "   cp.\"employee.json\" LIMIT 1",
            0xCAFEBABE);
  }

  @Test
  public void testFixedInts4SQL_to() throws Throwable {
    verifySQL("select"
           + "   convert_to(-889275714, 'INT')"
           + " from"
           + "   cp.\"employee.json\" LIMIT 1",
           new byte[] {(byte) 0xBE, (byte) 0xBA, (byte) 0xFE, (byte) 0xCA});
  }

  @Test
  public void testHadooopVInt() throws Exception {
    final int _0 = 0;
    final int _9 = 9;
    final ArrowBuf buffer = getAllocator().buffer(_9);

    long longVal = 0;
    buffer.clear();
    HadoopWritables.writeVLong(context, buffer, _0, _9, 0);
    longVal = HadoopWritables.readVLong(context, buffer, _0, _9);
    assertEquals(longVal, 0);

    buffer.clear();
    HadoopWritables.writeVLong(context, buffer, _0, _9, Long.MAX_VALUE);
    longVal = HadoopWritables.readVLong(context, buffer, _0, _9);
    assertEquals(longVal, Long.MAX_VALUE);

    buffer.clear();
    HadoopWritables.writeVLong(context, buffer, _0, _9, Long.MIN_VALUE);
    longVal = HadoopWritables.readVLong(context, buffer, _0, _9);
    assertEquals(longVal, Long.MIN_VALUE);

    int intVal = 0;
    buffer.clear();
    HadoopWritables.writeVInt(context, buffer, _0, _9, 0);
    intVal = HadoopWritables.readVInt(context, buffer, _0, _9);
    assertEquals(intVal, 0);

    buffer.clear();
    HadoopWritables.writeVInt(context, buffer, _0, _9, Integer.MAX_VALUE);
    intVal = HadoopWritables.readVInt(context, buffer, _0, _9);
    assertEquals(intVal, Integer.MAX_VALUE);

    buffer.clear();
    HadoopWritables.writeVInt(context, buffer, _0, _9, Integer.MIN_VALUE);
    intVal = HadoopWritables.readVInt(context, buffer, _0, _9);
    assertEquals(intVal, Integer.MIN_VALUE);
    buffer.release();
  }

  @Test // DRILL-4862
  public void testBinaryString() throws Exception {
    final String[] queries = {
        "SELECT convert_from(binary_string(key), 'INT_BE') as intkey \n" +
            "FROM cp.\"functions/conv/conv.json\""
    };

    for (String query: queries) {
      testBuilder()
          .sqlQuery(query)
          .ordered()
          .baselineColumns("intkey")
          .baselineValues(1244739896)
          .baselineValues(new Object[] { null })
          .baselineValues(1313814865)
          .baselineValues(1852782897)
          .build()
          .run();
    }
  }

  @Test
  public void testConvertIllegalUtf8() throws Exception {
    final String[] queries = {
      "SELECT convert_from(binary_string('\\x41\\x20\\x42\\x20\\x43'), 'UTF8') as val FROM (values(1))",
      "SELECT convert_from(binary_string('\\xF9\\x41\\x20\\x42\\x20\\x43'), 'UTF8', '') as val FROM (values(1))",
      "SELECT convert_from(binary_string('\\x41\\x20\\xFA\\x42\\x20\\x43'), 'UTF8', '') as val FROM (values(1))",
      "SELECT convert_from(binary_string('\\x41\\x20\\x42\\x20\\x43\\xFC'), 'UTF8', '') as val FROM (values(1))",
      "SELECT convert_from(binary_string('\\xFB\\x20\\x42\\x20\\x43'), 'UTF8', 'A') as val FROM (values(1))",
      "SELECT convert_from(binary_string('\\x41\\x20\\xFE\\x20\\x43'), 'UTF8', 'B') as val FROM (values(1))",
      "SELECT convert_from(binary_string('\\x41\\x20\\x42\\x20\\xFF'), 'UTF8', 'C') as val FROM (values(1))",
      "SELECT convert_from(binary_string('\\x41\\x20\\x42\\x20\\x43'), 'UTF8', '') as val FROM (values(1))",
      "SELECT convert_from(binary_string('\\x41\\x20\\x42\\x20\\x43'), 'UTF8', 'X') as val FROM (values(1))",
    };

    for (String query: queries) {
      testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("val")
        .baselineValues("A B C")
        .build()
        .run();
    }

    // Now test without a constant expression -- repeating a previous test (testConvertToComplexJSON), only with
    // an extra argument to convert_from
    String result1 =
      "[ {\n" +
        "  \"$numberLong\" : 4\n" +
        "}, {\n" +
        "  \"$numberLong\" : 6\n" +
        "} ]";
    String[] result2 = new String[] { null };

    testBuilder()
      .sqlQuery("select cast(convert_from(convert_to(rl[1], 'EXTENDEDJSON'), 'UTF8', '') as varchar(100)) as json_str from cp.\"/store/json/input2.json\"")
      .unOrdered()
      .baselineColumns("json_str")
      .baselineValues(result1)
      .baselineValues(result2)
      .baselineValues(result1)
      .baselineValues(result1)
      .go();
  }

  @Test
  public void testIsUtf8() throws Exception {
    final String[] queries = {
      "SELECT is_utf8(convert_from(binary_string('\\x41\\x20\\x42\\x20\\x43'), 'UTF8')) as val FROM (values(1))",
      "SELECT NOT is_utf8(convert_from(binary_string('\\xF9\\x41\\x20\\x42\\x20\\x43'), 'UTF8')) as val FROM (values(1))",
      "SELECT NOT is_utf8(convert_from(binary_string('\\x41\\x20\\xFA\\x42\\x20\\x43'), 'UTF8')) as val FROM (values(1))",
      "SELECT NOT is_utf8(convert_from(binary_string('\\x41\\x20\\x42\\x20\\x43\\xFC'), 'UTF8')) as val FROM (values(1))"
    };

    for (String query: queries) {
      testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("val")
        .baselineValues(true)
        .build()
        .run();
    }

    testBuilder()
      .sqlQuery("SELECT is_utf8(l_comment) as A FROM cp.\"tpch/lineitem.parquet\" LIMIT 1")
      .ordered()
      .baselineColumns("A")
      .baselineValues(true)
      .build()
      .run();

  }

  protected <T> void verifySQL(String sql, T expectedResults) throws Throwable {
    verifyResults(sql, expectedResults, getRunResult(QueryType.SQL, sql));
  }

  protected Object[] getRunResult(QueryType queryType, String planString) throws Exception {
    List<QueryDataBatch> resultList = testRunAndReturn(queryType, planString);

    List<Object> res = new ArrayList<>();
    RecordBatchLoader loader = new RecordBatchLoader(getAllocator());
    for(QueryDataBatch result : resultList) {
      if (result.getData() != null) {
        loader.load(result.getHeader().getDef(), result.getData());
        ValueVector v = loader.iterator().next().getValueVector();
        for (int j = 0; j < v.getValueCount(); j++) {
          if  (v instanceof VarCharVector) {
            res.add(new String(((VarCharVector) v).get(j)));
          } else {
            res.add(v.getObject(j));
          }
        }
        loader.clear();
        result.release();
      }
    }

    return res.toArray();
  }

  protected <T> void verifyResults(String expression, T expectedResults, Object[] actualResults) throws Exception {
    String testName = String.format("Expression: %s.", expression);
    assertEquals(testName, 1, actualResults.length);
    assertNotNull(testName, actualResults[0]);
    if (expectedResults.getClass().isArray()) {
      assertArraysEquals(testName, expectedResults, actualResults[0]);
    } else {
      assertEquals(testName, expectedResults, actualResults[0]);
    }
  }

  protected void assertArraysEquals(Object expected, Object actual) {
    assertArraysEquals(null, expected, actual);
  }

  protected void assertArraysEquals(String message, Object expected, Object actual) {
    if (expected instanceof byte[] && actual instanceof byte[]) {
      assertArrayEquals(message, (byte[]) expected, (byte[]) actual);
    } else if (expected instanceof Object[] && actual instanceof Object[]) {
      assertArrayEquals(message, (Object[]) expected, (Object[]) actual);
    } else if (expected instanceof char[] && actual instanceof char[]) {
      assertArrayEquals(message, (char[]) expected, (char[]) actual);
    } else if (expected instanceof short[] && actual instanceof short[]) {
      assertArrayEquals(message, (short[]) expected, (short[]) actual);
    } else if (expected instanceof int[] && actual instanceof int[]) {
      assertArrayEquals(message, (int[]) expected, (int[]) actual);
    } else if (expected instanceof long[] && actual instanceof long[]) {
      assertArrayEquals(message, (long[]) expected, (long[]) actual);
    } else if (expected instanceof float[] && actual instanceof float[]) {
      assertArrayEquals(message, (float[]) expected, (float[]) actual, DELTA);
    } else if (expected instanceof double[] && actual instanceof double[]) {
      assertArrayEquals(message, (double[]) expected, (double[]) actual, DELTA);
    } else {
      fail(String.format("%s: Error comparing arrays of type '%s' and '%s'",
          expected.getClass().getName(), (actual == null ? "null" : actual.getClass().getName())));
    }
  }

}
