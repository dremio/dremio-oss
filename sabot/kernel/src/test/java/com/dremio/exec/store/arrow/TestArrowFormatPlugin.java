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
package com.dremio.exec.store.arrow;

import static com.dremio.TestBuilder.mapOf;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.PlanTestBase;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.store.easy.arrow.ArrowFormatPlugin;

/**
 * Tests for {@link ArrowFormatPlugin}
 */
public class TestArrowFormatPlugin extends PlanTestBase {

  @Rule
  public final TemporaryFolder tmp = new TemporaryFolder();

  @BeforeClass
  public static void generateTestData() throws Exception {
    test("CREATE TABLE dfs_test.arrowRegion " +
        "STORE AS (type => 'arrow') AS SELECT * FROM cp.\"region.json\"");

    test("CREATE TABLE dfs_test.lineitem " +
        "STORE AS (type => 'arrow') AS SELECT * FROM cp.\"tpch/lineitem.parquet\"");

    test("CREATE TABLE dfs_test.orders " +
        "STORE AS (type => 'arrow') AS SELECT * FROM cp.\"tpch/orders.parquet\"");

    // Currently DROP TABLE doesn't support table with options, so the above tables are not deleted as part of
    // @AfterClass method. They will be deleted in @AfterClass of superclass as the base temporary directory is deleted
    // at the end of tests.
  }

  @Test
  public void simple() throws Exception {
    final String query = "SELECT * FROM TABLE(dfs_test.arrowRegion(type => 'arrow'))";
    test(query);

    testBuilder()
        .unOrdered()
        .sqlQuery(query)
        .sqlBaselineQuery("SELECT * FROM cp.\"region.json\"")
        .go();
  }

  @Test
  public void join() throws Exception {

    final String joinArrow = "SELECT * FROM TABLE(dfs_test.lineitem(type => 'arrow')) l JOIN " +
        "TABLE(dfs_test.orders(type => 'arrow')) o ON l.l_orderkey = o.o_orderkey";

    final String joinParquet = "SELECT * FROM cp.\"tpch/lineitem.parquet\" l JOIN " +
        "cp.\"tpch/orders.parquet\" o ON l.l_orderkey = o.o_orderkey";

    testBuilder()
        .unOrdered()
        .sqlQuery(joinArrow)
        .sqlBaselineQuery(joinParquet)
        .go();
  }

  @Test
  public void projectPushDown() throws Exception {
    final String query = "SELECT o_orderkey, o_orderpriority FROM TABLE(dfs_test.orders(type => 'arrow')) " +
        "ORDER BY o_orderkey LIMIT 2";

    testPlanSubstrPatterns(query, new String[] { "columns=[`o_orderkey`, `o_orderpriority`]" }, null);

    testBuilder()
        .ordered()
        .sqlQuery(query)
        .baselineColumns("o_orderkey", "o_orderpriority")
        .baselineValues(1, "5-LOW")
        .baselineValues(2, "1-URGENT")
        .go();
  }

  @Test
  public void mapDX3266() throws Exception {
    File tmpFile = tmp.newFile("mapDX3266.json");
    try (BufferedWriter bw = new BufferedWriter(new FileWriter(tmpFile))) {
      // exact input is important to trigger buffer mismatch size issue
      bw.write("{\"a\": {\"Tuesday\": {\"close\": \"19:00\",\"open\": \"10:00\"},\"Friday\":  {\"close\": \"19:00\",\"open\": \"10:00\"}}}\n");
      bw.write("{\"a\": {\"Monday\": {\"close\": \"02:00\",\"open\": \"10:00\"},\"Tuesday\":  {\"close\": \"19:00\",\"open\": \"10:00\"}}}\n");
      bw.write("{\"a\": {\"Monday\": {\"close\": \"14:30\",\"open\": \"10:00\"},\"Tuesday\":  {\"close\": \"19:00\",\"open\": \"10:00\"}}}\n");
    }
    test("CREATE TABLE dfs_test.test STORE AS (type => 'arrow') AS SELECT * FROM dfs.\"" + tmpFile.getAbsolutePath() + "\"");
    test("SELECT * FROM TABLE(dfs_test.test(type => 'arrow'))");
  }

  @Test
  public void arrowReaderTest() throws Exception {
    try (AutoCloseable ac = withSystemOption(ExecConstants.PARQUET_AUTO_CORRECT_DATES_VALIDATOR, true)) {
      // the arrow files used below are as generated in this::generateTestData()
      // before arrow format change. see DX-18576
      String query = "SELECT * FROM cp.\"/store/arrow/region/0_0_0.dremarrow1\"";
      testBuilder()
        .unOrdered()
        .sqlQuery(query)
        .sqlBaselineQuery("SELECT * FROM cp.\"region.json\"")
        .go();

      query = "SELECT * FROM cp.\"/store/arrow/orders/0_0_0.dremarrow1\"";
      testBuilder()
        .unOrdered()
        .sqlQuery(query)
        .sqlBaselineQuery("SELECT * FROM cp.\"tpch/orders.parquet\"")
        .go();
    }
  }

  @Test
  public void complexProjectPushdown() throws Exception {
    File tmpFile = tmp.newFile("complexPpd.json");
    try (BufferedWriter bw = new BufferedWriter(new FileWriter(tmpFile))) {
      bw.write("{\"a\": {\"Tuesday\": {\"close\": 19,\"open\": 10},\"Friday\":  {\"close\": 19,\"open\": 10}}}");
    }
    test("CREATE TABLE dfs_test.complexPpd STORE AS (type => 'arrow') AS SELECT * FROM dfs.\"" + tmpFile.getAbsolutePath() + "\"");

    String query = "SELECT t.a.\"Tuesday\" as col FROM TABLE(dfs_test.complexPpd(type => 'arrow')) as t";

    testPlanMatchingPatterns(query, new String[]{"columns=\\[`a`.`Tuesday`\\]"}, null);

    testBuilder()
        .unOrdered()
        .sqlQuery(query)
        .baselineColumns("col")
        .baselineValues(mapOf("close", 19L, "open", 10L))
        .go();
  }

  @Test
  public void complexProjectPushdown2() throws Exception {
    File tmpFile = tmp.newFile("complexPpd2.json");
    try (BufferedWriter bw = new BufferedWriter(new FileWriter(tmpFile))) {
      bw.write("{ \"a\" : [ { \"b\" : 1, \"c\" : 2}, { \"b\" : 2, \"c\" : 3 }] }");
    }
    test("CREATE TABLE dfs_test.complexPpd2 STORE AS (type => 'arrow') AS SELECT * FROM dfs.\"" + tmpFile.getAbsolutePath() + "\"");

    String query = "SELECT t.a[0].b as col FROM TABLE(dfs_test.complexPpd2(type => 'arrow')) as t";

    testPlanMatchingPatterns(query, new String[]{"columns=\\[`a`\\[0\\].`b`\\]"}, null);

    testBuilder()
        .unOrdered()
        .sqlQuery(query)
        .baselineColumns("col")
        .baselineValues(1L)
        .go();
  }
}
