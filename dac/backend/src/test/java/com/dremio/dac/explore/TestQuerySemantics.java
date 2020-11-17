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
package com.dremio.dac.explore;

import static com.dremio.dac.proto.model.dataset.OrderDirection.ASC;
import static com.dremio.dac.proto.model.dataset.OrderDirection.DESC;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.dremio.dac.proto.model.dataset.Column;
import com.dremio.dac.proto.model.dataset.ExpCalculatedField;
import com.dremio.dac.proto.model.dataset.ExpColumnReference;
import com.dremio.dac.proto.model.dataset.From;
import com.dremio.dac.proto.model.dataset.FromSQL;
import com.dremio.dac.proto.model.dataset.FromTable;
import com.dremio.dac.proto.model.dataset.Order;
import com.dremio.dac.proto.model.dataset.VirtualDatasetState;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.util.JSONUtil;
import com.dremio.exec.server.SabotContext;
import com.dremio.service.jobs.JobsProtoUtil;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.jobs.metadata.QueryMetadata;

/**
 * verify we understand queries
 */
public class TestQuerySemantics extends BaseTestServer {

  private static String table = "\"cp\".\"tpch/supplier.parquet\"";
  private static final From from = new FromTable(table).setAlias("tpch/supplier.parquet").wrap();

  private void assertEquals(VirtualDatasetState expected, VirtualDatasetState actual) {
    String expectedJSON = JSONUtil.toString(expected);
    String actualJSON = JSONUtil.toString(actual);
    Assert.assertEquals(
        "expected: " + expectedJSON + "\nactual: " + actualJSON,
        expectedJSON, actualJSON);
  }

  private VirtualDatasetState extract(SqlQuery sql){
    QueryMetadata metadata = QueryParser.extract(sql, getCurrentDremioDaemon().getBindingProvider().lookup(SabotContext.class));
    return QuerySemantics.extract(JobsProtoUtil.toBuf(metadata));
  }

  @Test
  public void testStar() {
    VirtualDatasetState ds = extract(
            getQueryFromSQL("select * from " + table));
    assertEquals(
            new VirtualDatasetState()
                .setFrom(from)
              .setContextList(Collections.<String>emptyList())
              .setReferredTablesList(Arrays.asList("tpch/supplier.parquet"))
              .setColumnsList(cols("s_suppkey", "s_name", "s_address", "s_nationkey", "s_phone", "s_acctbal", "s_comment")),
            ds);
  }

  @Test
  public void selectConstant() {
    final String query = "SELECT 1729 AS special";
    final VirtualDatasetState ds = extract(getQueryFromSQL(query));
    assertEquals(ds, new VirtualDatasetState()
        .setFrom(new FromSQL(query).setAlias("nested_0").wrap())
        .setContextList(Collections.<String>emptyList())
        .setReferredTablesList(Collections.<String>emptyList())
        .setColumnsList(cols("special")));
  }

  @Test
  public void selectConstantNested() {
    final String query = "SELECT * FROM (SELECT 87539319 AS special ORDER BY 1 LIMIT 1)";
    final VirtualDatasetState ds = extract(getQueryFromSQL(query));
    assertEquals(new VirtualDatasetState()
        .setFrom(new FromSQL(query).setAlias("nested_0").wrap())
        .setContextList(Collections.<String>emptyList())
        .setReferredTablesList(Collections.<String>emptyList())
        .setColumnsList(cols("special")), ds);
  }

  @Test
  public void testTimestampCloseToZero() throws Exception {
    // to test that times that < 100 millis are processed by Calcite correctly
    // otherwise java.lang.StringIndexOutOfBoundsException would be thrown by Calcite
    // String index out of range: -2 or (-1)
    VirtualDatasetState ds = extract(
      getQueryFromSQL(
        "select TIMESTAMP '1970-01-01 00:00:00.100' from sys.memory"));
    assertNotNull(ds);

    ds = extract(
      getQueryFromSQL(
      "select TIMESTAMP '1970-01-01 00:00:00.099' from sys.memory"));
    assertNotNull(ds);

    ds = extract(
      getQueryFromSQL(
        "select TIMESTAMP '1970-01-01 00:00:00.000' from sys.memory"));
    assertNotNull(ds);

    ds = extract(
      getQueryFromSQL(
        "select TIMESTAMP '1970-01-01 00:00:00.001' from sys.memory"));
    assertNotNull(ds);

    ds = extract(
      getQueryFromSQL(
        "select TIME '00:00:00.100' from sys.memory"));
    assertNotNull(ds);

    ds = extract(
      getQueryFromSQL(
        "select TIME '00:00:00.099' from sys.memory"));
    assertNotNull(ds);

    ds = extract(
      getQueryFromSQL(
        "select TIME '00:00:00.000' from sys.memory"));
    assertNotNull(ds);

    ds = extract(
      getQueryFromSQL(
        "select TIME '00:00:00.001' from sys.memory"));
    assertNotNull(ds);
  }

  @Test
  public void testStarAs() {
    VirtualDatasetState ds = extract(
            getQueryFromSQL("select * from " + table + " as foo"));
    assertEquals(
            new VirtualDatasetState()
                .setFrom(new FromTable(table).setAlias("foo").wrap()).setContextList(Collections.<String>emptyList())
                .setReferredTablesList(Arrays.asList("tpch/supplier.parquet"))
                .setColumnsList(cols("s_suppkey", "s_name", "s_address", "s_nationkey", "s_phone", "s_acctbal", "s_comment")),
            ds);
  }

  @Test
  public void testFields() {
    VirtualDatasetState ds = extract(
            getQueryFromSQL("select s_suppkey, s_name, s_address from " + table));
    assertEquals(
        new VirtualDatasetState()
            .setFrom(from)
            .setColumnsList(cols("s_suppkey", "s_name", "s_address"))
            .setContextList(Collections.<String>emptyList())
            .setReferredTablesList(Arrays.asList("tpch/supplier.parquet")),
        ds);
  }

  @Test
  public void testFieldsAs() {
    VirtualDatasetState ds = extract(
            getQueryFromSQL("select s_suppkey, s_address as mycol from " + table));
    assertEquals(
        new VirtualDatasetState()
            .setFrom(from)
            .setColumnsList(asList(
                new Column("s_suppkey", new ExpColumnReference("s_suppkey").wrap()),
                new Column("mycol", new ExpColumnReference("s_address").wrap())))
            .setContextList(Collections.<String>emptyList())
            .setReferredTablesList(Arrays.asList("tpch/supplier.parquet")),

        ds);
  }

  @Test
  public void testFlatten() {
    VirtualDatasetState ds =
        extract(getQueryFromSQL("select flatten(b), a as mycol from cp.\"json/nested.json\""));
    assertEquals(
        new VirtualDatasetState()
            .setFrom(new FromTable("\"cp\".\"json/nested.json\"").setAlias("json/nested.json").wrap())
            .setColumnsList(asList(
                new Column("EXPR$0", new ExpCalculatedField("FLATTEN(\"json/nested.json\".\"b\")").wrap()),
                new Column("mycol", new ExpColumnReference("a").wrap())))
            .setContextList(Collections.<String>emptyList())
            .setReferredTablesList(Arrays.asList("json/nested.json")),
        ds);
  }

  @Test
  public void testCount() {
    VirtualDatasetState ds =
        extract(getQueryFromSQL("select count(*) from cp.\"json/nested.json\""));
    assertEquals(
        new VirtualDatasetState()
            .setFrom(new FromTable("\"cp\".\"json/nested.json\"").setAlias("json/nested.json").wrap())
            .setColumnsList(asList(
                new Column("EXPR$0", new ExpCalculatedField("COUNT(*)").wrap())
            )).setContextList(Collections.<String>emptyList())
            .setReferredTablesList(Arrays.asList("json/nested.json")),
        ds);
  }

  @Test
  public void testFunctionUpper() {
    VirtualDatasetState ds =
        extract(getQueryFromSQL("select upper(a) from cp.\"json/convert_case.json\""));
    assertEquals(
        new VirtualDatasetState()
            .setFrom(new FromTable("\"cp\".\"json/convert_case.json\"").setAlias("json/convert_case.json").wrap())
            .setColumnsList(asList(
                new Column("EXPR$0", new ExpCalculatedField("UPPER(\"json/convert_case.json\".\"a\")").wrap())))
            .setContextList(Collections.<String>emptyList())
            .setReferredTablesList(Arrays.asList("json/convert_case.json")),
        ds);
  }

  @Test
  public void testOrder() {
    VirtualDatasetState ds =
        extract(getQueryFromSQL("select a from cp.\"json/nested.json\" order by a"));
    assertEquals(
        new VirtualDatasetState()
            .setFrom(new FromTable("\"cp\".\"json/nested.json\"").setAlias("json/nested.json").wrap())
            .setColumnsList(cols("a")).setOrdersList(asList(new Order("a", ASC)))
            .setContextList(Collections.<String>emptyList())
            .setReferredTablesList(Arrays.asList("json/nested.json")),
        ds);
  }

  @Test
  public void testMultipleOrder() {
    VirtualDatasetState ds =
        extract(getQueryFromSQL("select a, b from cp.\"json/nested.json\" order by b desc, a asc"));
    assertEquals(
        new VirtualDatasetState()
            .setFrom(new FromTable("\"cp\".\"json/nested.json\"").setAlias("json/nested.json").wrap())
            .setColumnsList(cols("a", "b"))
            .setOrdersList(asList(
                new Order("b", DESC),
                new Order("a", ASC)))
            .setContextList(Collections.<String>emptyList())
            .setReferredTablesList(Arrays.asList("json/nested.json")),
        ds);
  }

  @Test
  public void testOrderAlias() {
    VirtualDatasetState ds =
        extract(getQueryFromSQL("select a as b from cp.\"json/nested.json\" order by b"));
    assertEquals(
        new VirtualDatasetState()
            .setFrom(new FromTable("\"cp\".\"json/nested.json\"").setAlias("json/nested.json").wrap())
            .setColumnsList(asList(new Column("b", new ExpColumnReference("a").wrap())))
            .setOrdersList(asList(new Order("b", ASC)))
            .setContextList(Collections.<String>emptyList())
            .setReferredTablesList(Arrays.asList("json/nested.json")),
        ds);
  }

  @Test
  public void testNestedColRef() {
    VirtualDatasetState ds =
        extract(getQueryFromSQL("select t.a.Tuesday as tuesday from cp.\"json/extract_map.json\" as t"));
    if(!isComplexTypeSupport()) {
      assertEquals(
        new VirtualDatasetState()
          .setFrom(new FromTable("\"cp\".\"json/extract_map.json\"").setAlias("t").wrap())
          .setColumnsList(asList(new Column("tuesday", new ExpCalculatedField("\"t\".\"a\"['Tuesday']").wrap())))
          .setContextList(Collections.<String>emptyList())
          .setReferredTablesList(Arrays.asList("json/extract_map.json")),
        ds);
    } else {
      assertEquals(
        new VirtualDatasetState()
          .setFrom(new FromSQL("select t.a.Tuesday as tuesday from cp.\"json/extract_map.json\" as t").setAlias("nested_0").wrap())
          .setColumnsList(asList(new Column("tuesday", new ExpColumnReference("tuesday").wrap())))
          .setContextList(Collections.<String>emptyList())
          .setReferredTablesList(Arrays.asList("json/extract_map.json")),
        ds);
    }
  }

  private static List<Column> cols(String... columns){
    List<Column> cols = new ArrayList<>(columns.length);
    for(String col : columns){
      cols.add(new Column(col, new ExpColumnReference(col).wrap()));
    }
    return cols;
  }
}
