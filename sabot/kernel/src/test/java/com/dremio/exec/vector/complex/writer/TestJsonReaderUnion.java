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
package com.dremio.exec.vector.complex.writer;

import static com.dremio.TestBuilder.listOf;
import static com.dremio.TestBuilder.mapOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.BaseTestQuery;
import com.dremio.exec.proto.UserBitShared.QueryType;
import com.dremio.exec.proto.UserBitShared.RecordBatchDef;
import com.dremio.sabot.rpc.user.QueryDataBatch;

public class TestJsonReaderUnion extends BaseTestQuery {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();


  @BeforeClass
  public static void setup() throws Exception {
    test("alter system set `exec.enable_union_type` = true");
  }

  @AfterClass
  public static void shutdown() throws Exception {
    test("alter system set `exec.enable_union_type` = false");
  }

  @Test
  public void testDistribution() throws Exception {
    test("set planner.slice_target = 1");
    test("select * from cp.`jsoninput/union/b.json` where a <> 1 order by a desc");
  }

  @Test
  public void testSelectStarWithUnionType() throws Exception {
    String query = "select * from cp.`jsoninput/union/a.json`";
    testBuilder()
            .sqlQuery(query)
            .ordered()
            .baselineColumns("field1", "field2")
            .baselineValues(
                    1L, 1.2
            )
            .baselineValues(
                    listOf(2L), 1.2
            )
            .baselineValues(
                    mapOf("inner1", 3L, "inner2", 4L), listOf(3L, 4.0, "5")
            )
            .baselineValues(
                    mapOf("inner1", 3L,
                            "inner2", listOf(
                                    mapOf(
                                            "innerInner1", 1L,
                                            "innerInner2",
                                            listOf(
                                                    3L,
                                                    "a"
                                            )
                                    )
                            )
                    ),
                    listOf(
                            mapOf("inner3", 7L),
                            4.0,
                            "5",
                            mapOf("inner4", 9L),
                            listOf(
                                    mapOf(
                                            "inner5", 10L,
                                            "inner6", 11L
                                    ),
                                    mapOf(
                                            "inner5", 12L,
                                            "inner7", 13L
                                    )
                            )
                    )
            ).go();
  }

  @Ignore("DX-3988")
  @Test
  public void testSelectFromListWithCase() throws Exception {
    String query = "select a, typeOf(a) `type` from " +
            "(select case when is_list(field2) then field2[4][1].inner7 end a " +
            "from cp.`jsoninput/union/a.json`) where a is not null";

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
            "then field1 when is_list(field1) then field1[0] " +
            "when is_map(field1) then t.field1.inner1 end f1 from cp.`jsoninput/union/a.json` t";

    testBuilder()
            .sqlQuery(query)
            .ordered()
            .baselineColumns("f1")
            .baselineValues(1L)
            .baselineValues(2L)
            .baselineValues(3L)
            .baselineValues(3L)
            .go();
  }

  @Test
  public void testSumWithTypeCase() throws Exception {
    String query = "select sum(cast(f1 as bigint)) sum_f1 from " +
            "(select case when is_bigint(field1) then assert_bigint(field1) " +
            "when is_list(field1) then field1[0] when is_map(field1) then t.field1.inner1 end f1 " +
            "from cp.`jsoninput/union/a.json` t)";

    testBuilder()
            .sqlQuery(query)
            .ordered()
            .optionSettingQueriesForTestQuery("alter session set `exec.enable_union_type` = true")
            .baselineColumns("sum_f1")
            .baselineValues(9L)
            .go();
  }

  @Test
  public void testUnionExpressionMaterialization() throws Exception {
    String query = "select a + b c from cp.`jsoninput/union/b.json`";

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
    String query = "select typeof(t.a.b) c from cp.`jsoninput/union/nestedUnion.json` t";

    testBuilder()
            .sqlQuery(query)
            .ordered()
            .baselineColumns("c")
            .baselineValues("BIGINT")
            .baselineValues("VARCHAR")
            .baselineValues("MAP")
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
    String query = "select sum(cast(case when `type` = 'map' then t.data.a else assert_bigint(data) end as bigint)) `sum` from dfs_test.multi_batch t";

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
    String query = "select sum(cast(case when `type` = 'map' then t.data.a else data end as bigint)) `sum` from dfs_test.multi_file t";

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
  public void testColumnOrderingWithUnionVector() throws Exception {
    List<QueryDataBatch> results = null;
    try {
      results = testRunAndReturn(QueryType.SQL, "SELECT * FROM cp.`type_changes.json`");
      final RecordBatchDef def = results.get(0).getHeader().getDef();
      assertEquals(2, def.getFieldCount());
      assertEquals("a", def.getField(0).getNamePart().getName());
      assertEquals("b", def.getField(1).getNamePart().getName());
    } finally {
      if (results != null) {
        for(QueryDataBatch r : results) {
          r.release();
        }
      }
    }
  }
}
