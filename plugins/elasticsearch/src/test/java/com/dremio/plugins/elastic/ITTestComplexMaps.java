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
package com.dremio.plugins.elastic;

import static com.dremio.TestBuilder.listOf;
import static com.dremio.TestBuilder.mapOf;
import static com.dremio.plugins.elastic.ElasticsearchType.INTEGER;
import static com.dremio.plugins.elastic.ElasticsearchType.NESTED;
import static com.dremio.plugins.elastic.ElasticsearchType.TEXT;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class ITTestComplexMaps extends ElasticBaseTestQuery {

  private String ELASTIC_TABLE = null;

  @Override
  @Before
  public void before() throws Exception {
    super.before();
    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
      new ElasticsearchCluster.ColumnData("simple", INTEGER, new Object[][]{
        {123},
        {987}
      }),
      new ElasticsearchCluster.ColumnData("@full address", NESTED, new Object[][]{
        {ImmutableMap.of("street number", 12345, "@street name", "A Street", "city", "Cambridge", "state", "MA")},
        {ImmutableMap.of("street number", 987, "@street name", "B Street", "city", "San Francisco", "state", "CA")}
      }),
      new ElasticsearchCluster.ColumnData("@complex map!", NESTED, new Object[][]{
        {ImmutableMap.of(
          "boolean field", false,
          "list of numbers", ImmutableList.of(1, 2, 3),
          "map inside complex?", ImmutableMap.of("inner1", true, "inner 2", false, "inner3!", "too deep"),
          "two layers deep", ImmutableMap.of("oneInner 1", "one inner", "oneInner 2", ImmutableMap.of("twoInner 1", 123, "twoInner 2", "way too deep")),
          "state", "MA")},
        {ImmutableMap.of(
          "boolean field", true,
          "list of numbers", ImmutableList.of(7, 8, 9, 10),
          "map inside complex?", ImmutableMap.of("inner1", true, "inner 2", false, "inner3!", "also too deep"),
          "two layers deep", ImmutableMap.of("oneInner 1", "repeat one inner", "oneInner 2", ImmutableMap.of("twoInner 1", 789, "twoInner 2", "also way too deep")),
          "state", "CA")}
      }),
      new ElasticsearchCluster.ColumnData("@ a list!", TEXT, ImmutableMap.of("index", "false"), new Object[][] {
        {ImmutableList.of("ABC", "DEF", "GHI")},
        {null}
      }),
      new ElasticsearchCluster.ColumnData("secondList", TEXT, ImmutableMap.of("index", "false"), new Object[][] {
        {ImmutableList.of("ABC", "DEF", "GHI")},
        {ImmutableList.of("singleElement")}
      }),
      new ElasticsearchCluster.ColumnData("a nested list", TEXT, new Object[][] {
        {ImmutableList.of(ImmutableList.of("abc", "def"), ImmutableList.of("ghi"))},
        {ImmutableList.of(ImmutableList.of("single"))}
      }),
      new ElasticsearchCluster.ColumnData("list of complex map!", NESTED, new Object[][]{
        {ImmutableList.of(ImmutableMap.of(
          "boolean field", false,
          "list of numbers", ImmutableList.of(1, 2, 3),
          "map inside complex?", ImmutableMap.of("inner1", true, "inner 2", false, "inner3!", "too deep"),
          "two layers deep", ImmutableMap.of("oneInner 1", "one inner", "oneInner 2", ImmutableMap.of("twoInner 1", 123, "twoInner 2", "way too deep")),
          "state", "MA"),
          ImmutableMap.of(
            "boolean field", true,
            "list of numbers", ImmutableList.of(7, 8, 9, 10),
            "map inside complex?", ImmutableMap.of("inner1", true, "inner 2", false, "inner3!", "also too deep"),
            "two layers deep", ImmutableMap.of("oneInner 1", "repeat one inner", "oneInner 2", ImmutableMap.of("twoInner 1", 789, "twoInner 2", "also way too deep")),
            "state", "CA"))},
        {ImmutableList.of(ImmutableMap.of(
          "boolean field", true,
          "list of numbers", ImmutableList.of(7, 8, 9, 10),
          "map inside complex?", ImmutableMap.of("inner1", true, "inner 2", false, "inner3!", "also too deep"),
          "two layers deep", ImmutableMap.of("oneInner 1", "repeat one inner", "oneInner 2", ImmutableMap.of("twoInner 1", 789, "twoInner 2", "also way too deep")),
          "state", "CA"))}
      }),
      new ElasticsearchCluster.ColumnData("messedUpList", TEXT, new Object[][] {
        {ImmutableList.of(ImmutableList.of("abc", "def"), ImmutableList.of("ghi"))},
        {"single"}
      }),
      new ElasticsearchCluster.ColumnData("messedUpList2", TEXT, new Object[][] {
        {ImmutableList.of(ImmutableList.of("abc", "def"), ImmutableList.of("ghi"))},
        {ImmutableList.of("single")}
      }),
    };
    load(schema, table, data);
    ELASTIC_TABLE = String.format("elasticsearch.%s.%s", schema, table);
  }


  @Test
  public void testGroupByWithComplexMapsBoolean() throws Exception {
    final String query = "SELECT count(*), t.\"@complex map!\".\"map inside complex?\".\"inner1\" as myBool, "
      + "t.secondList[1], t.\"@ a list!\"[0], t.\"list of complex map!\"[0].\"list of numbers\"[1] as listInt FROM " + ELASTIC_TABLE + " as t "
      + "group by t.\"@complex map!\".\"map inside complex?\".\"inner1\", "
      + "t.secondList[1], t.\"@ a list!\"[0], t.\"list of complex map!\"[0].\"list of numbers\"[1]";
    verifyJsonInPlan(query, new String[] {
      "[{\n" +
        "  \"query\" : {\n" +
        "    \"match_all\" : { }\n" +
        "  },\n" +
        "  \"aggregations\" : {\n" +
        "    \"myBool\" : {\n" +
        "      \"terms\" : {\n" +
        "        \"script\" : {\n" +
        "          \"inline\" : \"(_source[\\\"@complex map!\\\"] == null || _source[\\\"@complex map!\\\"][\\\"map inside complex?\\\"] == null || _source[\\\"@complex map!\\\"][\\\"map inside complex?\\\"][\\\"inner1\\\"] == null) ? false : _source[\\\"@complex map!\\\"][\\\"map inside complex?\\\"][\\\"inner1\\\"]\"\n" +
        "        },\n" +
        "        \"missing\" : \"NULL_BOOLEAN_TAG\",\n" +
        "        \"size\" : 2147483647\n" +
        "      },\n" +
        "      \"aggregations\" : {\n" +
        "        \"EXPR$2\" : {\n" +
        "          \"terms\" : {\n" +
        "            \"script\" : {\n" +
        "              \"inline\" : \"(_source.secondList == null || _source.secondList[1] == null) ? false : _source.secondList[1]\"\n" +
        "            },\n" +
        "            \"missing\" : \"NULL_STRING_TAG\",\n" +
        "            \"size\" : 2147483647\n" +
        "          },\n" +
        "          \"aggregations\" : {\n" +
        "            \"EXPR$3\" : {\n" +
        "              \"terms\" : {\n" +
        "                \"script\" : {\n" +
        "                  \"inline\" : \"(_source[\\\"@ a list!\\\"] == null || _source[\\\"@ a list!\\\"][0] == null) ? false : _source[\\\"@ a list!\\\"][0]\"\n" +
        "                },\n" +
        "                \"missing\" : \"NULL_STRING_TAG\",\n" +
        "                \"size\" : 2147483647\n" +
        "              },\n" +
        "              \"aggregations\" : {\n" +
        "                \"listInt\" : {\n" +
        "                  \"terms\" : {\n" +
        "                    \"script\" : {\n" +
        "                      \"inline\" : \"(_source[\\\"list of complex map!\\\"] == null || _source[\\\"list of complex map!\\\"][0] == null || _source[\\\"list of complex map!\\\"][0][\\\"list of numbers\\\"] == null || _source[\\\"list of complex map!\\\"][0][\\\"list of numbers\\\"][1] == null) ? false : _source[\\\"list of complex map!\\\"][0][\\\"list of numbers\\\"][1]\"\n" +
        "                    },\n" +
        "                    \"missing\" : -9223372036854775808,\n" +
        "                    \"size\" : 2147483647\n" +
        "                  }\n" +
        "                }\n" +
        "              }\n" +
        "            }\n" +
        "          }\n" +
        "        }\n" +
        "      }\n" +
        "    }\n" +
        "  }\n" +
        "}]"});
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("EXPR$0", "myBool", "EXPR$2", "EXPR$3", "listInt")
      .baselineValues(1L, true, "DEF", "ABC", 2L)
      .baselineValues(1L, true, null, null, 8L)
      .go();
  }

  @Test
  public void testGroupByWithComplexMaps() throws Exception {
    final String query = "SELECT t.\"@complex map!\".\"two layers deep\".\"oneInner 2\".\"twoInner 1\" as deepInt, "
      + "t.secondList[1], t.\"@ a list!\"[0], t.\"list of complex map!\"[0].\"list of numbers\"[1] as listInt FROM " + ELASTIC_TABLE + " as t "
      + "group by t.\"@complex map!\".\"two layers deep\".\"oneInner 2\".\"twoInner 1\", "
      + "t.secondList[1], t.\"@ a list!\"[0], t.\"list of complex map!\"[0].\"list of numbers\"[1]";
    verifyJsonInPlan(query, new String[] {
      "[{\n" +
        "  \"query\" : {\n" +
        "    \"match_all\" : { }\n" +
        "  },\n" +
        "  \"aggregations\" : {\n" +
        "    \"deepInt\" : {\n" +
        "      \"terms\" : {\n" +
        "        \"script\" : {\n" +
        "          \"inline\" : \"(_source[\\\"@complex map!\\\"] == null || _source[\\\"@complex map!\\\"][\\\"two layers deep\\\"] == null || _source[\\\"@complex map!\\\"][\\\"two layers deep\\\"][\\\"oneInner 2\\\"] == null || _source[\\\"@complex map!\\\"][\\\"two layers deep\\\"][\\\"oneInner 2\\\"][\\\"twoInner 1\\\"] == null) ? false : _source[\\\"@complex map!\\\"][\\\"two layers deep\\\"][\\\"oneInner 2\\\"][\\\"twoInner 1\\\"]\"\n" +
        "        },\n" +
        "        \"missing\" : -9223372036854775808,\n" +
        "        \"size\" : 2147483647\n" +
        "      },\n" +
        "      \"aggregations\" : {\n" +
        "        \"EXPR$1\" : {\n" +
        "          \"terms\" : {\n" +
        "            \"script\" : {\n" +
        "              \"inline\" : \"(_source.secondList == null || _source.secondList[1] == null) ? false : _source.secondList[1]\"\n" +
        "            },\n" +
        "            \"missing\" : \"NULL_STRING_TAG\",\n" +
        "            \"size\" : 2147483647\n" +
        "          },\n" +
        "          \"aggregations\" : {\n" +
        "            \"EXPR$2\" : {\n" +
        "              \"terms\" : {\n" +
        "                \"script\" : {\n" +
        "                  \"inline\" : \"(_source[\\\"@ a list!\\\"] == null || _source[\\\"@ a list!\\\"][0] == null) ? false : _source[\\\"@ a list!\\\"][0]\"\n" +
        "                },\n" +
        "                \"missing\" : \"NULL_STRING_TAG\",\n" +
        "                \"size\" : 2147483647\n" +
        "              },\n" +
        "              \"aggregations\" : {\n" +
        "                \"listInt\" : {\n" +
        "                  \"terms\" : {\n" +
        "                    \"script\" : {\n" +
        "                      \"inline\" : \"(_source[\\\"list of complex map!\\\"] == null || _source[\\\"list of complex map!\\\"][0] == null || _source[\\\"list of complex map!\\\"][0][\\\"list of numbers\\\"] == null || _source[\\\"list of complex map!\\\"][0][\\\"list of numbers\\\"][1] == null) ? false : _source[\\\"list of complex map!\\\"][0][\\\"list of numbers\\\"][1]\"\n" +
        "                    },\n" +
        "                    \"missing\" : -9223372036854775808,\n" +
        "                    \"size\" : 2147483647\n" +
        "                  }\n" +
        "                }\n" +
        "              }\n" +
        "            }\n" +
        "          }\n" +
        "        }\n" +
        "      }\n" +
        "    }\n" +
        "  }\n" +
        "}]"});
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("deepInt", "EXPR$1", "EXPR$2", "listInt")
      .baselineValues(123L, "DEF", "ABC", 2L)
      .baselineValues(789L, null, null, 8L)
      .go();
  }

  @Test
  public void testAggCallWithComplexMaps() throws Exception {
    final String query = "SELECT SUM(t.\"@complex map!\".\"two layers deep\".\"oneInner 2\".\"twoInner 1\") as deepInt, "
      + "COUNT(t.secondList[1]), COUNT(t.\"@ a list!\"[0]), SUM(t.\"list of complex map!\"[0].\"list of numbers\"[1]) as listInt FROM " + ELASTIC_TABLE + " as t";
    verifyJsonInPlan(query, new String[] {
      "[{\n" +
        "  \"query\" : {\n" +
        "    \"match_all\" : { }\n" +
        "  },\n" +
        "  \"aggregations\" : {\n" +
        "    \"deepInt\" : {\n" +
        "      \"sum\" : {\n" +
        "        \"script\" : {\n" +
        "          \"inline\" : \"(_source[\\\"@complex map!\\\"] == null || _source[\\\"@complex map!\\\"][\\\"two layers deep\\\"] == null || _source[\\\"@complex map!\\\"][\\\"two layers deep\\\"][\\\"oneInner 2\\\"] == null || _source[\\\"@complex map!\\\"][\\\"two layers deep\\\"][\\\"oneInner 2\\\"][\\\"twoInner 1\\\"] == null) ? false : _source[\\\"@complex map!\\\"][\\\"two layers deep\\\"][\\\"oneInner 2\\\"][\\\"twoInner 1\\\"]\"\n" +
        "        }\n" +
        "      }\n" +
        "    },\n" +
        "    \"EXPR$1\" : {\n" +
        "      \"value_count\" : {\n" +
        "        \"script\" : {\n" +
        "          \"inline\" : \"(_source.secondList == null || _source.secondList[1] == null) ? false : _source.secondList[1]\"\n" +
        "        }\n" +
        "      }\n" +
        "    },\n" +
        "    \"EXPR$2\" : {\n" +
        "      \"value_count\" : {\n" +
        "        \"script\" : {\n" +
        "          \"inline\" : \"(_source[\\\"@ a list!\\\"] == null || _source[\\\"@ a list!\\\"][0] == null) ? false : _source[\\\"@ a list!\\\"][0]\"\n" +
        "        }\n" +
        "      }\n" +
        "    },\n" +
        "    \"listInt\" : {\n" +
        "      \"sum\" : {\n" +
        "        \"script\" : {\n" +
        "          \"inline\" : \"(_source[\\\"list of complex map!\\\"] == null || _source[\\\"list of complex map!\\\"][0] == null || _source[\\\"list of complex map!\\\"][0][\\\"list of numbers\\\"] == null || _source[\\\"list of complex map!\\\"][0][\\\"list of numbers\\\"][1] == null) ? false : _source[\\\"list of complex map!\\\"][0][\\\"list of numbers\\\"][1]\"\n" +
        "        }\n" +
        "      }\n" +
        "    }\n" +
        "  }\n" +
        "}]"});
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("deepInt", "EXPR$1", "EXPR$2", "listInt")
      .baselineValues(912L, 1L, 1L, 10L)
      .go();
  }

  @Test
  public void testGroupByAndAggCallWithComplexMaps() throws Exception {
    final String query = "SELECT t.\"@complex map!\".\"two layers deep\".\"oneInner 2\".\"twoInner 1\" as deepInt, "
      + "t.secondList[1], COUNT(t.\"@ a list!\"[0]), SUM(t.\"list of complex map!\"[0].\"list of numbers\"[1]) as listInt FROM " + ELASTIC_TABLE + " as t "
      + "group by t.secondList[1], t.\"@complex map!\".\"two layers deep\".\"oneInner 2\".\"twoInner 1\"";
    verifyJsonInPlan(query, new String[] {
      "[{\n" +
        "  \"query\" : {\n" +
        "    \"match_all\" : { }\n" +
        "  },\n" +
        "  \"aggregations\" : {\n" +
        "    \"EXPR$1\" : {\n" +
        "      \"terms\" : {\n" +
        "        \"script\" : {\n" +
        "          \"inline\" : \"(_source.secondList == null || _source.secondList[1] == null) ? false : _source.secondList[1]\"\n" +
        "        },\n" +
        "        \"missing\" : \"NULL_STRING_TAG\",\n" +
        "        \"size\" : 2147483647\n" +
        "      },\n" +
        "      \"aggregations\" : {\n" +
        "        \"deepInt\" : {\n" +
        "          \"terms\" : {\n" +
        "            \"script\" : {\n" +
        "              \"inline\" : \"(_source[\\\"@complex map!\\\"] == null || _source[\\\"@complex map!\\\"][\\\"two layers deep\\\"] == null || _source[\\\"@complex map!\\\"][\\\"two layers deep\\\"][\\\"oneInner 2\\\"] == null || _source[\\\"@complex map!\\\"][\\\"two layers deep\\\"][\\\"oneInner 2\\\"][\\\"twoInner 1\\\"] == null) ? false : _source[\\\"@complex map!\\\"][\\\"two layers deep\\\"][\\\"oneInner 2\\\"][\\\"twoInner 1\\\"]\"\n" +
        "            },\n" +
        "            \"missing\" : -9223372036854775808,\n" +
        "            \"size\" : 2147483647\n" +
        "          },\n" +
        "          \"aggregations\" : {\n" +
        "            \"EXPR$2\" : {\n" +
        "              \"value_count\" : {\n" +
        "                \"script\" : {\n" +
        "                  \"inline\" : \"(_source[\\\"@ a list!\\\"] == null || _source[\\\"@ a list!\\\"][0] == null) ? false : _source[\\\"@ a list!\\\"][0]\"\n" +
        "                }\n" +
        "              }\n" +
        "            },\n" +
        "            \"listInt\" : {\n" +
        "              \"sum\" : {\n" +
        "                \"script\" : {\n" +
        "                  \"inline\" : \"(_source[\\\"list of complex map!\\\"] == null || _source[\\\"list of complex map!\\\"][0] == null || _source[\\\"list of complex map!\\\"][0][\\\"list of numbers\\\"] == null || _source[\\\"list of complex map!\\\"][0][\\\"list of numbers\\\"][1] == null) ? false : _source[\\\"list of complex map!\\\"][0][\\\"list of numbers\\\"][1]\"\n" +
        "                }\n" +
        "              }\n" +
        "            }\n" +
        "          }\n" +
        "        }\n" +
        "      }\n" +
        "    }\n" +
        "  }\n" +
        "}]"});
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("deepInt", "EXPR$1", "EXPR$2", "listInt")
      .baselineValues(123L, "DEF", 1L, 2L)
      .baselineValues(789L, null, 0L, 8L)
      .go();
  }

  @Test
  public void testSelectComplexMapWithMessedUpList() throws Exception {
    final String query = "SELECT t.\"messedUpList\" as \"origList\", t.\"@complex map!\".\"two layers deep\".\"oneInner 2\".\"twoInner 2\" AS \"@complex map!\" FROM " + ELASTIC_TABLE + " as t";
    verifyJsonInPlan(query, new String[] {
      "[{\n" +
        "  \"query\" : {\n" +
        "    \"match_all\" : { }\n" +
        "  },\n" +
        "  \"_source\" : {\n" +
        "    \"includes\" : [ \"messedUpList\", \"@complex map!\" ],\n" +
        "    \"excludes\" : [ ]\n" +
        "  }\n" +
        "}]"});
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("`origList`", "`@complex map!`")
      .baselineValues(listOf("abc", "def", "ghi"), "way too deep")
      .baselineValues(listOf("single"), "also way too deep")
      .go();
  }

  @Test
  public void testSelectComplexMapWithMessedUpList2() throws Exception {
    final String query = "SELECT t.\"messedUpList2\" as \"origList\", t.\"@complex map!\".\"two layers deep\".\"oneInner 2\".\"twoInner 2\" AS \"@complex map!\" FROM " + ELASTIC_TABLE + " as t";
    verifyJsonInPlan(query, new String[] {
      "[{\n" +
        "  \"query\" : {\n" +
        "    \"match_all\" : { }\n" +
        "  },\n" +
        "  \"script_fields\" : {\n" +
        "    \"origList\" : {\n" +
        "      \"script\" : {\n" +
        "        \"inline\" : \"(_source.messedUpList2 == null) ? false : _source.messedUpList2\"\n" +
        "      }\n" +
        "    },\n" +
        "    \"@complex map!\" : {\n" +
        "      \"script\" : {\n" +
        "        \"inline\" : \"(_source[\\\"@complex map!\\\"] == null || _source[\\\"@complex map!\\\"][\\\"two layers deep\\\"] == null || _source[\\\"@complex map!\\\"][\\\"two layers deep\\\"][\\\"oneInner 2\\\"] == null || _source[\\\"@complex map!\\\"][\\\"two layers deep\\\"][\\\"oneInner 2\\\"][\\\"twoInner 2\\\"] == null) ? false : _source[\\\"@complex map!\\\"][\\\"two layers deep\\\"][\\\"oneInner 2\\\"][\\\"twoInner 2\\\"]\"\n" +
        "      }\n" +
        "    }\n" +
        "  }\n" +
        "}]"});
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("`origList`", "`@complex map!`")
      .baselineValues(listOf("abc", "def", "ghi"), "way too deep")
      .baselineValues(listOf("single"), "also way too deep")
      .go();
  }

  @Test
  public void testSelectComplexMapWithMessedUpList2Item() throws Exception {
    final String query = "SELECT t.\"messedUpList2\" as \"origList\", t.\"@complex map!\".\"two layers deep\".\"oneInner 2\".\"twoInner 2\" AS \"@complex map!\" FROM " + ELASTIC_TABLE + " as t";
    verifyJsonInPlan(query, new String[] {
      "[{\n" +
        "  \"query\" : {\n" +
        "    \"match_all\" : { }\n" +
        "  },\n" +
        "  \"_source\" : {\n" +
        "    \"includes\" : [ \"messedUpList2\", \"@complex map!\" ],\n" +
        "    \"excludes\" : [ ]\n" +
        "  }\n" +
        "}]"});
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("`origList`", "`@complex map!`")
      .baselineValues(listOf("abc", "def", "ghi"), "way too deep")
      .baselineValues(listOf("single"), "also way too deep")
      .go();
  }

  @Test
  public void testSelectComplexMapWithListOfComplexMap() throws Exception {
    final String query = "SELECT t.\"list of complex map!\" as \"origList\", t.\"@complex map!\".\"two layers deep\".\"oneInner 2\".\"twoInner 2\" AS \"@complex map!\" FROM " + ELASTIC_TABLE + " as t";
    verifyJsonInPlan(query, new String[] {
      "[{\n" +
        "  \"query\" : {\n" +
        "    \"match_all\" : { }\n" +
        "  },\n" +
        "  \"script_fields\" : {\n" +
        "    \"origList\" : {\n" +
        "      \"script\" : {\n" +
        "        \"inline\" : \"(_source[\\\"list of complex map!\\\"] == null) ? false : _source[\\\"list of complex map!\\\"]\"\n" +
        "      }\n" +
        "    },\n" +
        "    \"@complex map!\" : {\n" +
        "      \"script\" : {\n" +
        "        \"inline\" : \"(_source[\\\"@complex map!\\\"] == null || _source[\\\"@complex map!\\\"][\\\"two layers deep\\\"] == null || _source[\\\"@complex map!\\\"][\\\"two layers deep\\\"][\\\"oneInner 2\\\"] == null || _source[\\\"@complex map!\\\"][\\\"two layers deep\\\"][\\\"oneInner 2\\\"][\\\"twoInner 2\\\"] == null) ? false : _source[\\\"@complex map!\\\"][\\\"two layers deep\\\"][\\\"oneInner 2\\\"][\\\"twoInner 2\\\"]\"\n" +
        "      }\n" +
        "    }\n" +
        "  }\n" +
        "}]"});
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("`origList`", "`@complex map!`")
      .baselineValues(
        listOf(
          mapOf(
            "boolean field", false,
            "list of numbers", listOf(1L, 2L, 3L),
            "map inside complex?", mapOf("inner1", true, "inner 2", false, "inner3!", "too deep"),
            "two layers deep", mapOf("oneInner 1", "one inner", "oneInner 2", mapOf("twoInner 1", 123L, "twoInner 2", "way too deep")),
            "state", "MA"),
          mapOf(
            "boolean field", true,
            "list of numbers", listOf(7L, 8L, 9L, 10L),
            "map inside complex?", mapOf("inner1", true, "inner 2", false, "inner3!", "also too deep"),
            "two layers deep", mapOf("oneInner 1", "repeat one inner", "oneInner 2", mapOf("twoInner 1", 789L, "twoInner 2", "also way too deep")),
            "state", "CA")),
        "way too deep")
      .baselineValues(
        listOf(
          mapOf(
            "boolean field", true,
            "list of numbers", listOf(7L, 8L, 9L, 10L),
            "map inside complex?", mapOf("inner1", true, "inner 2", false, "inner3!", "also too deep"),
            "two layers deep", mapOf("oneInner 1", "repeat one inner", "oneInner 2", mapOf("twoInner 1", 789L, "twoInner 2", "also way too deep")),
            "state", "CA")),
        "also way too deep")
      .go();
  }


  @Test
  public void testSelectComplexMapWithListOfComplexMap2() throws Exception {
    final String query = "SELECT t.\"list of complex map!\"[0] as \"origList\", t.\"@complex map!\".\"two layers deep\".\"oneInner 2\".\"twoInner 2\" AS \"@complex map!\" FROM " + ELASTIC_TABLE + " as t";
    verifyJsonInPlan(query, new String[] {
      "[{\n" +
        "  \"query\" : {\n" +
        "    \"match_all\" : { }\n" +
        "  },\n" +
        "  \"script_fields\" : {\n" +
        "    \"origList\" : {\n" +
        "      \"script\" : {\n" +
        "        \"inline\" : \"(_source[\\\"list of complex map!\\\"] == null || _source[\\\"list of complex map!\\\"][0] == null) ? false : _source[\\\"list of complex map!\\\"][0]\"\n" +
        "      }\n" +
        "    },\n" +
        "    \"@complex map!\" : {\n" +
        "      \"script\" : {\n" +
        "        \"inline\" : \"(_source[\\\"@complex map!\\\"] == null || _source[\\\"@complex map!\\\"][\\\"two layers deep\\\"] == null || _source[\\\"@complex map!\\\"][\\\"two layers deep\\\"][\\\"oneInner 2\\\"] == null || _source[\\\"@complex map!\\\"][\\\"two layers deep\\\"][\\\"oneInner 2\\\"][\\\"twoInner 2\\\"] == null) ? false : _source[\\\"@complex map!\\\"][\\\"two layers deep\\\"][\\\"oneInner 2\\\"][\\\"twoInner 2\\\"]\"\n" +
        "      }\n" +
        "    }\n" +
        "  }\n" +
        "}]"});
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("`origList`", "`@complex map!`")
      .baselineValues(
        mapOf(
          "boolean field", false,
          "list of numbers", listOf(1L, 2L, 3L),
          "map inside complex?", mapOf("inner1", true, "inner 2", false, "inner3!", "too deep"),
          "two layers deep", mapOf("oneInner 1", "one inner", "oneInner 2", mapOf("twoInner 1", 123L, "twoInner 2", "way too deep")),
          "state", "MA"),
        "way too deep")
      .baselineValues(
        mapOf(
          "boolean field", true,
          "list of numbers", listOf(7L, 8L, 9L, 10L),
          "map inside complex?", mapOf("inner1", true, "inner 2", false, "inner3!", "also too deep"),
          "two layers deep", mapOf("oneInner 1", "repeat one inner", "oneInner 2", mapOf("twoInner 1", 789L, "twoInner 2", "also way too deep")),
          "state", "CA"),
        "also way too deep")
      .go();
  }

  @Test
  public void testSelectComplexMapWithComplexMap() throws Exception {
   final String query = "SELECT t.\"@complex map!\".\"list of numbers\" as \"origMap\", t.\"@complex map!\".\"two layers deep\".\"oneInner 2\".\"twoInner 2\" AS \"@complex map!\" FROM " + ELASTIC_TABLE + " as t";
    verifyJsonInPlan(query, new String[] {
      "[{\n" +
        "  \"query\" : {\n" +
        "    \"match_all\" : { }\n" +
        "  },\n" +
        "  \"script_fields\" : {\n" +
        "    \"origMap\" : {\n" +
        "      \"script\" : {\n" +
        "        \"inline\" : \"(_source[\\\"@complex map!\\\"] == null || _source[\\\"@complex map!\\\"][\\\"list of numbers\\\"] == null) ? false : _source[\\\"@complex map!\\\"][\\\"list of numbers\\\"]\"\n" +
        "      }\n" +
        "    },\n" +
        "    \"@complex map!\" : {\n" +
        "      \"script\" : {\n" +
        "        \"inline\" : \"(_source[\\\"@complex map!\\\"] == null || _source[\\\"@complex map!\\\"][\\\"two layers deep\\\"] == null || _source[\\\"@complex map!\\\"][\\\"two layers deep\\\"][\\\"oneInner 2\\\"] == null || _source[\\\"@complex map!\\\"][\\\"two layers deep\\\"][\\\"oneInner 2\\\"][\\\"twoInner 2\\\"] == null) ? false : _source[\\\"@complex map!\\\"][\\\"two layers deep\\\"][\\\"oneInner 2\\\"][\\\"twoInner 2\\\"]\"\n" +
        "      }\n" +
        "    }\n" +
        "  }\n" +
        "}]"});
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("`origMap`", "`@complex map!`")
      .baselineValues(listOf(1L, 2L, 3L), "way too deep")
      .baselineValues(listOf(7L, 8L, 9L, 10L), "also way too deep")
      .go();
  }

  @Test
  public void testSelectComplexMapWithComplexMap2() throws Exception {
    final String query = "SELECT t.\"@complex map!\" as \"origMap\", t.\"@complex map!\".\"two layers deep\".\"oneInner 2\".\"twoInner 2\" AS \"@complex map!\" FROM " + ELASTIC_TABLE + " as t";
    verifyJsonInPlan(query, new String[] {
      "[{\n" +
        "  \"query\" : {\n" +
        "    \"match_all\" : { }\n" +
        "  },\n" +
        "  \"script_fields\" : {\n" +
        "    \"origMap\" : {\n" +
        "      \"script\" : {\n" +
        "        \"inline\" : \"(_source[\\\"@complex map!\\\"] == null) ? false : _source[\\\"@complex map!\\\"]\"\n" +
        "      }\n" +
        "    },\n" +
        "    \"@complex map!\" : {\n" +
        "      \"script\" : {\n" +
        "        \"inline\" : \"(_source[\\\"@complex map!\\\"] == null || _source[\\\"@complex map!\\\"][\\\"two layers deep\\\"] == null || _source[\\\"@complex map!\\\"][\\\"two layers deep\\\"][\\\"oneInner 2\\\"] == null || _source[\\\"@complex map!\\\"][\\\"two layers deep\\\"][\\\"oneInner 2\\\"][\\\"twoInner 2\\\"] == null) ? false : _source[\\\"@complex map!\\\"][\\\"two layers deep\\\"][\\\"oneInner 2\\\"][\\\"twoInner 2\\\"]\"\n" +
        "      }\n" +
        "    }\n" +
        "  }\n" +
        "}]"});
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("`origMap`", "`@complex map!`")
      .baselineValues(
        mapOf(
          "boolean field", false,
          "list of numbers", listOf(1L, 2L, 3L),
          "map inside complex?", mapOf("inner1", true, "inner 2", false, "inner3!", "too deep"),
          "two layers deep", mapOf("oneInner 1", "one inner", "oneInner 2", mapOf("twoInner 1", 123L, "twoInner 2", "way too deep")),
          "state", "MA"),
        "way too deep")
      .baselineValues(
        mapOf(
          "boolean field", true,
          "list of numbers", listOf(7L, 8L, 9L, 10L),
          "map inside complex?", mapOf("inner1", true, "inner 2", false, "inner3!", "also too deep"),
          "two layers deep", mapOf("oneInner 1", "repeat one inner", "oneInner 2", mapOf("twoInner 1", 789L, "twoInner 2", "also way too deep")),
          "state", "CA"),
        "also way too deep")
      .go();
  }

  @Test
  public void testSelectComplexMapWithList() throws Exception {
    final String query = "SELECT t.\"@ a list!\" as \"@ a list!\", t.\"@complex map!\".\"two layers deep\".\"oneInner 2\".\"twoInner 2\" AS \"@complex map!\" FROM " + ELASTIC_TABLE + " as t";
    verifyJsonInPlan(query, new String[] {
      "[{\n" +
        "  \"query\" : {\n" +
        "    \"match_all\" : { }\n" +
        "  },\n" +
        "  \"script_fields\" : {\n" +
        "    \"@ a list!\" : {\n" +
        "      \"script\" : {\n" +
        "        \"inline\" : \"(_source[\\\"@ a list!\\\"] == null) ? false : _source[\\\"@ a list!\\\"]\"\n" +
        "      }\n" +
        "    },\n" +
        "    \"@complex map!\" : {\n" +
        "      \"script\" : {\n" +
        "        \"inline\" : \"(_source[\\\"@complex map!\\\"] == null || _source[\\\"@complex map!\\\"][\\\"two layers deep\\\"] == null || _source[\\\"@complex map!\\\"][\\\"two layers deep\\\"][\\\"oneInner 2\\\"] == null || _source[\\\"@complex map!\\\"][\\\"two layers deep\\\"][\\\"oneInner 2\\\"][\\\"twoInner 2\\\"] == null) ? false : _source[\\\"@complex map!\\\"][\\\"two layers deep\\\"][\\\"oneInner 2\\\"][\\\"twoInner 2\\\"]\"\n" +
        "      }\n" +
        "    }\n" +
        "  }\n" +
        "}]"});
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("`@ a list!`", "`@complex map!`")
      .baselineValues(listOf("ABC", "DEF", "GHI"), "way too deep")
      .baselineValues(null, "also way too deep")
      .go();
  }

  @Test
  public void testSelectComplexMapWithNestedList() throws Exception {
    final String query = "SELECT t.\"secondList\" as \"secondList\", t.\"a nested list\" as \"a nested list\", t.\"@complex map!\".\"two layers deep\".\"oneInner 2\".\"twoInner 2\" AS \"@complex map!\" FROM " + ELASTIC_TABLE + " as t";
    verifyJsonInPlan(query, new String[] {
      "[{\n" +
        "  \"query\" : {\n" +
        "    \"match_all\" : { }\n" +
        "  },\n" +
        "  \"script_fields\" : {\n" +
        "    \"secondList\" : {\n" +
        "      \"script\" : {\n" +
        "        \"inline\" : \"(_source.secondList == null) ? false : _source.secondList\"\n" +
        "      }\n" +
        "    },\n" +
        "    \"a nested list\" : {\n" +
        "      \"script\" : {\n" +
        "        \"inline\" : \"(_source[\\\"a nested list\\\"] == null) ? false : _source[\\\"a nested list\\\"]\"\n" +
        "      }\n" +
        "    },\n" +
        "    \"@complex map!\" : {\n" +
        "      \"script\" : {\n" +
        "        \"inline\" : \"(_source[\\\"@complex map!\\\"] == null || _source[\\\"@complex map!\\\"][\\\"two layers deep\\\"] == null || _source[\\\"@complex map!\\\"][\\\"two layers deep\\\"][\\\"oneInner 2\\\"] == null || _source[\\\"@complex map!\\\"][\\\"two layers deep\\\"][\\\"oneInner 2\\\"][\\\"twoInner 2\\\"] == null) ? false : _source[\\\"@complex map!\\\"][\\\"two layers deep\\\"][\\\"oneInner 2\\\"][\\\"twoInner 2\\\"]\"\n" +
        "      }\n" +
        "    }\n" +
        "  }\n" +
        "}]"});
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("`secondList`", "`a nested list`", "`@complex map!`")
      .baselineValues(listOf("ABC", "DEF", "GHI"), listOf("abc", "def", "ghi"), "way too deep")
      .baselineValues(listOf("singleElement"), listOf("single"), "also way too deep")
      .go();
  }

  public static void verifyJsonInPlan(String s, String[] s2){

  }
  @Test
  public void testSelectComplexMapWithNestedList2() throws Exception {
    final String query = "SELECT t.\"@ a list!\" as \"@ a list!\", t.\"a nested list\" as \"a nested list\", t.\"@complex map!\".\"two layers deep\".\"oneInner 2\".\"twoInner 2\" AS \"@complex map!\" FROM " + ELASTIC_TABLE + " as t";
    verifyJsonInPlan(query, new String[] {
      "[{\n" +
        "  \"query\" : {\n" +
        "    \"match_all\" : { }\n" +
        "  },\n" +
        "  \"script_fields\" : {\n" +
        "    \"@ a list!\" : {\n" +
        "      \"script\" : {\n" +
        "        \"inline\" : \"(_source[\\\"@ a list!\\\"] == null) ? false : _source[\\\"@ a list!\\\"]\"\n" +
        "      }\n" +
        "    },\n" +
        "    \"a nested list\" : {\n" +
        "      \"script\" : {\n" +
        "        \"inline\" : \"(_source[\\\"a nested list\\\"] == null) ? false : _source[\\\"a nested list\\\"]\"\n" +
        "      }\n" +
        "    },\n" +
        "    \"@complex map!\" : {\n" +
        "      \"script\" : {\n" +
        "        \"inline\" : \"(_source[\\\"@complex map!\\\"] == null || _source[\\\"@complex map!\\\"][\\\"two layers deep\\\"] == null || _source[\\\"@complex map!\\\"][\\\"two layers deep\\\"][\\\"oneInner 2\\\"] == null || _source[\\\"@complex map!\\\"][\\\"two layers deep\\\"][\\\"oneInner 2\\\"][\\\"twoInner 2\\\"] == null) ? false : _source[\\\"@complex map!\\\"][\\\"two layers deep\\\"][\\\"oneInner 2\\\"][\\\"twoInner 2\\\"]\"\n" +
        "      }\n" +
        "    }\n" +
        "  }\n" +
        "}]"});
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("`@ a list!`", "`a nested list`", "`@complex map!`")
      .baselineValues(listOf("ABC", "DEF", "GHI"), listOf("abc", "def", "ghi"), "way too deep")
      .baselineValues(null, listOf("single"), "also way too deep")
      .go();
  }


  @Test
  public void testSelectComplexMapWithNestedListIndex() throws Exception {
    final String query = "SELECT t.\"secondList\" as \"secondList\", t.\"a nested list\" as \"a nested list\", t.\"@complex map!\".\"two layers deep\".\"oneInner 2\".\"twoInner 2\" AS \"@complex map!\" FROM " + ELASTIC_TABLE + " as t";
    verifyJsonInPlan(query, new String[] {
      "[{\n" +
        "  \"query\" : {\n" +
        "    \"match_all\" : { }\n" +
        "  },\n" +
        "  \"script_fields\" : {\n" +
        "    \"secondList\" : {\n" +
        "      \"script\" : {\n" +
        "        \"inline\" : \"(_source.secondList == null) ? false : _source.secondList\"\n" +
        "      }\n" +
        "    },\n" +
        "    \"a nested list\" : {\n" +
        "      \"script\" : {\n" +
        "        \"inline\" : \"(_source[\\\"a nested list\\\"] == null || _source[\\\"a nested list\\\"][0] == null) ? false : _source[\\\"a nested list\\\"][0]\"\n" +
        "      }\n" +
        "    },\n" +
        "    \"@complex map!\" : {\n" +
        "      \"script\" : {\n" +
        "        \"inline\" : \"(_source[\\\"@complex map!\\\"] == null || _source[\\\"@complex map!\\\"][\\\"two layers deep\\\"] == null || _source[\\\"@complex map!\\\"][\\\"two layers deep\\\"][\\\"oneInner 2\\\"] == null || _source[\\\"@complex map!\\\"][\\\"two layers deep\\\"][\\\"oneInner 2\\\"][\\\"twoInner 2\\\"] == null) ? false : _source[\\\"@complex map!\\\"][\\\"two layers deep\\\"][\\\"oneInner 2\\\"][\\\"twoInner 2\\\"]\"\n" +
        "      }\n" +
        "    }\n" +
        "  }\n" +
        "}]"});
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("`secondList`", "`a nested list`", "`@complex map!`")
      .baselineValues(listOf("ABC", "DEF", "GHI"), listOf("abc", "def", "ghi"), "way too deep")
      .baselineValues(listOf("singleElement"), listOf("single"), "also way too deep")
      .go();
  }

  @Test
  public void testSelectTwoLayersInnerMapElement() throws Exception {
    final String query = "SELECT t.simple, t.\"@complex map!\".\"two layers deep\".\"oneInner 2\".\"twoInner 2\" AS \"@complex map!\" FROM " + ELASTIC_TABLE + " as t";
    verifyJsonInPlan(query, new String[]{
      "[{\n" +
        "  \"query\" : {\n" +
        "    \"match_all\" : { }\n" +
        "  },\n" +
        "  \"script_fields\" : {\n" +
        "    \"simple\" : {\n" +
        "      \"script\" : {\n" +
        "        \"inline\" : \"(_source.simple == null) ? false : (_source.simple).intValue()\"\n" +
        "      }\n" +
        "    },\n" +
        "    \"@complex map!\" : {\n" +
        "      \"script\" : {\n" +
        "        \"inline\" : \"(_source[\\\"@complex map!\\\"] == null || _source[\\\"@complex map!\\\"][\\\"two layers deep\\\"] == null || _source[\\\"@complex map!\\\"][\\\"two layers deep\\\"][\\\"oneInner 2\\\"] == null || _source[\\\"@complex map!\\\"][\\\"two layers deep\\\"][\\\"oneInner 2\\\"][\\\"twoInner 2\\\"] == null) ? false : _source[\\\"@complex map!\\\"][\\\"two layers deep\\\"][\\\"oneInner 2\\\"][\\\"twoInner 2\\\"]\"\n" +
        "      }\n" +
        "    }\n" +
        "  }\n" +
        "}]\n"
    });
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("simple", "`@complex map!`")
      .baselineValues(123, "way too deep")
      .baselineValues(987, "also way too deep")
      .go();
  }

  @Test
  public void testSelectTwoLayersInnerMapElement2() throws Exception {
    final String query = "SELECT t.simple, t.\"@complex map!\".\"two layers deep\".\"oneInner 2\".\"twoInner 1\" AS \"@complex map!\" FROM " + ELASTIC_TABLE + " as t";
    verifyJsonInPlan(query, new String[]{
      "[{\n" +
        "  \"query\" : {\n" +
        "    \"match_all\" : { }\n" +
        "  },\n" +
        "  \"script_fields\" : {\n" +
        "    \"simple\" : {\n" +
        "      \"script\" : {\n" +
        "        \"inline\" : \"(_source.simple == null) ? false : (_source.simple).intValue()\"\n" +
        "      }\n" +
        "    },\n" +
        "    \"@complex map!\" : {\n" +
        "      \"script\" : {\n" +
        "        \"inline\" : \"(_source[\\\"@complex map!\\\"] == null || _source[\\\"@complex map!\\\"][\\\"two layers deep\\\"] == null || _source[\\\"@complex map!\\\"][\\\"two layers deep\\\"][\\\"oneInner 2\\\"] == null || _source[\\\"@complex map!\\\"][\\\"two layers deep\\\"][\\\"oneInner 2\\\"][\\\"twoInner 1\\\"] == null) ? false : _source[\\\"@complex map!\\\"][\\\"two layers deep\\\"][\\\"oneInner 2\\\"][\\\"twoInner 1\\\"]\"\n" +
        "      }\n" +
        "    }\n" +
        "  }\n" +
        "}]\n"
    });
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("simple", "`@complex map!`")
      .baselineValues(123, 123L)
      .baselineValues(987, 789L)
      .go();
  }

  @Test
  public void testSelectInnerMapElement() throws Exception {
    final String query = "SELECT t.simple, t.\"@complex map!\".\"boolean field\" AS \"@complex map!\" FROM " + ELASTIC_TABLE + " as t";
    verifyJsonInPlan(query, new String[]{
      "[{\n" +
        "  \"query\" : {\n" +
        "    \"match_all\" : { }\n" +
        "  },\n" +
        "  \"script_fields\" : {\n" +
        "    \"simple\" : {\n" +
        "      \"script\" : {\n" +
        "        \"inline\" : \"(_source.simple == null) ? false : (_source.simple).intValue()\"\n" +
        "      }\n" +
        "    },\n" +
        "    \"@complex map!\" : {\n" +
        "      \"script\" : {\n" +
        "        \"inline\" : \"(_source[\\\"@complex map!\\\"] == null || _source[\\\"@complex map!\\\"][\\\"boolean field\\\"] == null) ? false : _source[\\\"@complex map!\\\"][\\\"boolean field\\\"]\"\n" +
        "      }\n" +
        "    }\n" +
        "  }\n" +
        "}]\n"
    });
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("simple", "`@complex map!`")
      .baselineValues(123, false)
      .baselineValues(987, true)
      .go();
  }

  @Test
  public void testMapElement() throws Exception {
    final String sqlQuery = "select t.\"@full address\".\"street number\", t.\"@full address\".\"@street name\" from " + ELASTIC_TABLE + " as t";
    verifyJsonInPlan(sqlQuery, new String[]{
      "[{\n" +
        "  \"query\" : {\n" +
        "    \"match_all\" : { }\n" +
        "  },\n" +
        "  \"script_fields\" : {\n" +
        "    \"EXPR$0\" : {\n" +
        "      \"script\" : {\n" +
        "        \"inline\" : \"(_source[\\\"@full address\\\"] == null || _source[\\\"@full address\\\"][\\\"street number\\\"] == null) ? false : _source[\\\"@full address\\\"][\\\"street number\\\"]\"\n" +
        "      }\n" +
        "    },\n" +
        "    \"EXPR$1\" : {\n" +
        "      \"script\" : {\n" +
        "        \"inline\" : \"(_source[\\\"@full address\\\"] == null || _source[\\\"@full address\\\"][\\\"@street name\\\"] == null) ? false : _source[\\\"@full address\\\"][\\\"@street name\\\"]\"\n" +
        "      }\n" +
        "    }\n" +
        "  }\n" +
        "}]"
    });
    String col1 = isComplexTypeSupport() ? "`street number`": "EXPR$0";
    String col2 = isComplexTypeSupport() ? "`@street name`" : "EXPR$1";
    testBuilder()
      .sqlQuery(sqlQuery)
      .unOrdered()
      .baselineColumns(col1, col2)
      .baselineValues(12345L, "A Street")
      .baselineValues(987L, "B Street")
      .go();
  }
}
