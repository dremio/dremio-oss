/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.plugins.elastic;

import static com.dremio.TestBuilder.listOf;
import static com.dremio.TestBuilder.mapOf;
import static com.dremio.plugins.elastic.ElasticsearchType.OBJECT;
import static com.dremio.plugins.elastic.ElasticsearchType.STRING;

import org.junit.Test;

import com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * Test for elasticsearch contains
 */
public class TestElasticsearchContains extends ElasticBaseTestQuery {

  @Test
  public void testProject() throws Exception {
    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
        new ElasticsearchCluster.ColumnData("location", STRING, new Object[][]{
            {"San Francisco"},
            {"Oakland"},
            {"San Jose"}
        })
    };

    elastic.load(schema, table, data);
    String sql = String.format("select  contains(location:Oakland) from elasticsearch.%s.%s", schema, table);
    errorTypeTestHelper(sql, ErrorType.PLAN);
  }

  @Test
  public void testFilterNested() throws Exception {

    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
        new ElasticsearchCluster.ColumnData("location", STRING, new Object[][]{
            {"San Francisco"},
            {"Oakland"},
            {"San Jose"}
        })
    };

    elastic.load(schema, table, data);
    String sql = String.format("select location from elasticsearch.%s.%s where contains(location:Oakland) = false", schema, table);
    errorTypeTestHelper(sql, ErrorType.PLAN);
  }

  @Test
  public void testFilterNot() throws Exception {

    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
        new ElasticsearchCluster.ColumnData("location", STRING, new Object[][]{
            {"San Francisco"},
            {"Oakland"},
            {"San Jose"}
        })
    };

    elastic.load(schema, table, data);
    String sql = String.format("select location from elasticsearch.%s.%s where NOT contains(location:Oakland)", schema, table);
    errorTypeTestHelper(sql, ErrorType.PLAN);
  }

  @Test
  public void testFilter() throws Exception {

    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
        new ElasticsearchCluster.ColumnData("location", STRING, new Object[][]{
            {"San Francisco"},
            {"Oakland"},
            {"San Jose"}
        })
    };

    elastic.load(schema, table, data);
    String sql = String.format("select location from elasticsearch.%s.%s where contains(location:Oakland)", schema, table);
    testPlanMatchingPatterns(sql, new String[] {
      "\\\"query\\\" : \\{\n" +
      "    \\\"query_string\\\" : \\{\n" +
      "      \\\"query\" : \\\"location : Oakland\\\"\n" +
      "    \\}"}, null);
    testBuilder().sqlQuery(sql).unOrdered()
        .baselineColumns("location")
        .baselineValues("Oakland")
        .go();
  }

  @Test
  public void testSimple() throws Exception {

    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
            new ElasticsearchCluster.ColumnData("location", STRING, new Object[][]{
                    {"San Francisco"},
                    {"Oakland"},
                    {"San Jose"}
            })
    };

    elastic.load(schema, table, data);
    String sql = String.format("select * from (select location x, location y from elasticsearch.%s.%s) where contains(x:Oakland y:\"San Francisco\"~1)", schema, table);
    testBuilder().sqlQuery(sql).unOrdered().baselineColumns("x", "y")
        .baselineValues("San Francisco", "San Francisco")
        .baselineValues("Oakland", "Oakland")
        .go();
  }

  @Test
  public void testSimpleOR() throws Exception {

    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
        new ElasticsearchCluster.ColumnData("location", STRING, new Object[][]{
            {"San Francisco"},
            {"Oakland"},
            {"San Jose"}
        })
    };

    elastic.load(schema, table, data);
    String sql = String.format("select * from (select location x, location y from elasticsearch.%s.%s) where contains(x:Oakland OR y:\"San Francisco\"~1)", schema, table);
    testBuilder().sqlQuery(sql).unOrdered().baselineColumns("x", "y")
        .baselineValues("San Francisco", "San Francisco")
        .baselineValues("Oakland", "Oakland")
        .go();
  }

  @Test
  public void testSimpleAnd() throws Exception {

    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
        new ElasticsearchCluster.ColumnData("location", STRING, new Object[][]{
            {"San Francisco"},
            {"Oakland"},
            {"San Jose"}
        })
    };

    elastic.load(schema, table, data);
    String sql = String.format("select * from (select location x, location y from elasticsearch.%s.%s) where contains(x:San AND y:Francisco)", schema, table);
    testBuilder().sqlQuery(sql).unOrdered().baselineColumns("x", "y")
        .baselineValues("San Francisco", "San Francisco")
        .go();
  }

  @Test
  public void testSimpleAnd2() throws Exception {

    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
        new ElasticsearchCluster.ColumnData("location", STRING, new Object[][]{
            {"San Francisco"},
            {"Oakland"},
            {"San Jose"}
        })
    };

    elastic.load(schema, table, data);
    String sql = String.format("select * from (select location x, location y from elasticsearch.%s.%s) where contains(x:San AND y:SAN)", schema, table);
    testBuilder().sqlQuery(sql).unOrdered().baselineColumns("x", "y")
        .baselineValues("San Francisco", "San Francisco")
        .baselineValues("San Jose", "San Jose")
        .go();
  }

  @Test
  public void testNested() throws Exception {

    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
            new ElasticsearchCluster.ColumnData("location", OBJECT, new Object[][]{
                    {ImmutableMap.of("city", "San Francisco")},
                    {ImmutableMap.of("city", "Oakland")}
            })
    };

    elastic.load(schema, table, data);
    String sql = String.format("select location from elasticsearch.%s.%s l where contains(l.location.city:Oakland)", schema, table);
    testPlanMatchingPatterns(sql, new String[] {
      "\\\"query_string\\\" : \\{\n" +
      "      \\\"query\\\" : \\\"location.city : Oakland\\\"\n" +
      "    \\}"},
      null);
    testBuilder().sqlQuery(sql).unOrdered()
        .baselineColumns("location")
        .baselineValues(mapOf("city", "Oakland"))
        .go();
  }

  @Test
  public void testNestedArray() throws Exception {

    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
            new ElasticsearchCluster.ColumnData("location", OBJECT, new Object[][]{
                    {ImmutableMap.of("city", ImmutableList.of("San Francisco"))},
                    {ImmutableMap.of("city", ImmutableList.of("Oakland"))}
            })
    };

    elastic.load(schema, table, data);
    String sql = String.format("select location from elasticsearch.%s.%s l where contains(l.location.city:Oakland)", schema, table);
    testBuilder().sqlQuery(sql).unOrdered()
        .baselineColumns("location")
        .baselineValues(mapOf("city", listOf("Oakland")))
        .go();
  }

  @Test
  public void testNestedArrayWithLeafLimit() throws Exception {

    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
        new ElasticsearchCluster.ColumnData("location", OBJECT, new Object[][]{
            {ImmutableMap.of("city", ImmutableList.of("San Francisco"))},
            {ImmutableMap.of("city", ImmutableList.of("Oakland"))}
        })
    };

    elastic.load(schema, table, data);
    try {
      test("set planner.leaf_limit_enable = true");
      String sql = String.format("select location from elasticsearch.%s.%s l where contains(l.location.city:Oakland)", schema, table);
      testBuilder().sqlQuery(sql).unOrdered()
          .baselineColumns("location")
          .baselineValues(mapOf("city", listOf("Oakland")))
          .go();
    } finally {
      test("set planner.leaf_limit_enable = false");
    }
  }

  @Test
  public void testNestedQueryWithSampleAndWithoutSample() throws Exception {

    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
        new ElasticsearchCluster.ColumnData("ID", STRING, new Object[][]{  {"one"}, {"two"} , {"three"}, {"four"}, {"two"} }),
        new ElasticsearchCluster.ColumnData("location", STRING, new Object[][]{
            {"San Francisco"},
            {"Oakland"},
            {"San Jose"},
            {"ABC Oakland"},
            {"Oakland"}
        })
    };
    elastic.load(schema, table, data);

    String sql = String.format("select sum(1) as `sumok`, `customSqlQuery`.`ID` as `idid` from "
        + "(select * from elasticsearch.%s.%s l where contains(location:Oakland) ) `customSqlQuery` group by `customSqlQuery`.`ID`", schema, table);
    try {
      test("explain plan for " + sql);
      testBuilder().sqlQuery(sql).unOrdered().baselineColumns("sumok", "idid")
          .baselineValues(2L, "two")
          .baselineValues(1L, "four")
          .go();

      test("set planner.leaf_limit_enable = true");
      test("explain plan for " + sql);
      testBuilder().sqlQuery(sql).unOrdered().baselineColumns("sumok", "idid")
          .baselineValues(2L, "two")
          .baselineValues(1L, "four")
          .go();
    } finally {
      test("set planner.leaf_limit_enable = true");
    }
  }
}
