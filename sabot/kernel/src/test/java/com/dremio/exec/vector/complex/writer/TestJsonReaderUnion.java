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
import static org.junit.Assert.assertTrue;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.BaseTestQuery;
import com.dremio.exec.planner.physical.ScreenPrel;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.test.UserExceptionMatcher;

public class TestJsonReaderUnion extends BaseTestQuery {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();


  @BeforeClass
  public static void setup() throws Exception {
    test("alter system set \"exec.enable_union_type\" = true");
  }

  @AfterClass
  public static void shutdown() throws Exception {
    test("alter system set \"exec.enable_union_type\" = false");
  }

  @Test
  public void testDistribution() throws Exception {
    test("set planner.slice_target = 1");
    test("select cast(a as float), cast(b as varchar), c from cp.\"jsoninput/union/b.json\" where a <> 1 order by a desc");
  }

  @Ignore("DX-3988")
  @Test
  public void testSelectFromListWithCase() throws Exception {
    String query = "select a, typeOf(a) \"type\" from " +
            "(select case when is_list(field2) then field2[4][1].inner7 end a " +
            "from cp.\"jsoninput/union/a.json\") where a is not null";

    testBuilder()
            .sqlQuery(query)
            .ordered()
            .baselineColumns("a", "type")
            .baselineValues(13L, "BIGINT")
            .go();
  }

  @Test
  public void testTypeCase() throws Exception {
    String query = "select case when is_bigint(field1) " +
            "then cast(field1 as int) when is_list(field1) then cast(field1[0] as int) " +
            "when is_struct(field1) then cast(t.field1.inner1 as int) end f1 from cp.\"jsoninput/union/a.json\" t";

    testBuilder()
            .sqlQuery(query)
            .ordered()
            .baselineColumns("f1")
            .baselineValues(1)
            .baselineValues(2)
            .baselineValues(3)
            .baselineValues(3)
            .go();
  }

  @Test
  public void testSumWithTypeCase() throws Exception {
    String query = "select sum(cast(f1 as bigint)) sum_f1 from " +
            "(select case when is_bigint(field1) then assert_bigint(field1) " +
            "when is_list(field1) then field1[0] when is_struct(field1) then t.field1.inner1 end f1 " +
            "from cp.\"jsoninput/union/a.json\" t)";

    testBuilder()
            .sqlQuery(query)
            .ordered()
            .optionSettingQueriesForTestQuery("alter session set \"exec.enable_union_type\" = true")
            .baselineColumns("sum_f1")
            .baselineValues(9L)
            .go();
  }

  @Test
  public void testUnionExpressionMaterialization() throws Exception {
    String query = "select a + b c from cp.\"jsoninput/union/b.json\"";

    testBuilder()
            .sqlQuery(query)
            .ordered()
            .baselineColumns("c")
            .baselineValues(3.0)
            .baselineValues(7.0)
            .baselineValues(11.0)
            .go();
  }

  @Test
  public void testNestedUnionExpression() throws Exception {
    String query = "select typeof(t.a.b) c from cp.\"jsoninput/union/nestedUnion.json\" t";

    testBuilder()
            .sqlQuery(query)
            .ordered()
            .baselineColumns("c")
            .baselineValues("BIGINT")
            .baselineValues("VARCHAR")
            .baselineValues("STRUCT")
            .go();
  }

  @Test
  public void testSumMultipleBatches() throws Exception {
    String dfs_temp = getDfsTestTmpSchemaLocation();
    File table_dir = new File(dfs_temp, "multi_batch");
    table_dir.mkdir();
    BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream(new File(table_dir, "a.json")));
    for (int i = 0; i < 10000; i++) {
      os.write("{ type : \"map\", data : { a : 1 } }\n".getBytes());
      os.write("{ type : \"bigint\", data : 1 }\n".getBytes());
    }
    os.flush();
    os.close();
    String query = "select sum(cast(case when \"type\" = 'map' then t.data.a else assert_bigint(data) end as bigint)) \"sum\" from dfs_test.multi_batch t";

    testBuilder()
            .sqlQuery(query)
            .ordered()
            .baselineColumns("sum")
            .baselineValues(20000L)
            .go();
  }

  @Test
  @Ignore("query fails in sql validation if the bigint value is found first")
  public void testSumFilesWithDifferentSchema() throws Exception {
    String dfs_temp = getDfsTestTmpSchemaLocation();
    File table_dir = new File(dfs_temp, "multi_file");
    table_dir.mkdir();
    BufferedOutputStream os;
    os = new BufferedOutputStream(new FileOutputStream(new File(table_dir, "a.json")));
    for (int i = 0; i < 10000; i++) {
      os.write("{ type : \"bigint\", data : 1 }\n".getBytes());
    }
    os.flush();
    os.close();
    os = new BufferedOutputStream(new FileOutputStream(new File(table_dir, "b.json")));
    for (int i = 0; i < 10000; i++) {
      os.write("{ type : \"map\", data : { a : 1 } }\n".getBytes());
    }
    os.flush();
    os.close();
    String query = "select sum(cast(case when \"type\" = 'map' then t.data.a else data end as bigint)) \"sum\" from dfs_test.multi_file t";

    try {
      test(query);
    } catch (Exception e) {
      assertTrue(e.getMessage(), e.getMessage().contains("SCHEMA_CHANGE"));
    }

    testBuilder()
            .sqlQuery(query)
            .ordered()
            .baselineColumns("sum")
            .baselineValues(20000L)
            .go();
  }

  @Test
  public void flattenAssertList() throws Exception {
    final String query = "SELECT flatten(assert_list(field2)) AS l FROM " +
        "(SELECT * FROM cp.\"jsoninput/union/c.json\" t WHERE is_list(t.field2))";

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("l")
        .baselineValues(3L)
        .baselineValues(4L)
        .baselineValues(5L)
        .baselineValues(6L)
        .baselineValues(7L)
        .baselineValues(8L)
        .go();
  }

  @Test
  public void mappifyAssertStruct() throws Exception {
    final String query = "SELECT mappify(assert_struct(field1)) AS l FROM " +
        "(SELECT * FROM cp.\"jsoninput/union/d.json\" t WHERE is_struct(t.field1))";

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("l")
        .baselineValues(listOf(
            mapOf("key", "inner1", "value", 3L),
            mapOf("key", "inner2", "value", 4L)
        ))
        .baselineValues(listOf(
          mapOf("key", "inner1", "value", 5L),
          mapOf("key", "inner2", "value", 6L)
        ))
        .go();
  }

  @Test
  public void assertFailure() throws Exception {
    thrownException.expect(new UserExceptionMatcher(UserBitShared.DremioPBError.ErrorType.VALIDATION,
        "The field must be of struct type or a mixed type that contains a struct type"));

    test("SELECT mappify(assert_struct(field2)) AS l FROM " +
        "(SELECT * FROM cp.\"jsoninput/union/a.json\" t WHERE is_struct(t.field2))");
  }

  @Test
  public void unionAssertFailure() throws Exception {
    thrownException.expect(new UserExceptionMatcher(UserBitShared.DremioPBError.ErrorType.UNSUPPORTED_OPERATION,
      ScreenPrel.MIXED_TYPES_ERROR));

    test("SELECT * FROM cp.\"jsoninput/mixed_number_types.json\"");
  }
}
