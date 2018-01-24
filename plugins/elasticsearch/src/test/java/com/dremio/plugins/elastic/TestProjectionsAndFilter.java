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
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.sql.Timestamp;

import org.joda.time.LocalDateTime;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.exec.proto.UserBitShared.QueryType;
import com.dremio.plugins.elastic.ElasticsearchCluster.ColumnData;

public class TestProjectionsAndFilter extends ElasticBaseTestQuery {

  @Before
  public void loadTable() throws Exception {
    ColumnData[] data = getBusinessData();
    load(schema, table, data);
  }

  // DX-6681
  @Test
  public final void countLiteralGroupBy() throws Exception {
    final String sqlQuery = "select stars, count(5) as a, count(1) as b, count(*) as c from elasticsearch." + schema + "." + table + " where review_count < 10 group by stars";
    testPlanMatchingPatterns(sqlQuery, new String[0], new String[]{"Aggregate"});
    testBuilder().sqlQuery(sqlQuery).unOrdered().baselineColumns("stars", "a", "b", "c")
      .baselineValues(1.0f,1l,1l,1l)
      .go();
  }

  //DX-6681
  @Test
  public final void countLiteral() throws Exception {
    final String sqlQuery = "select count(5) as a, count(1) as b, count(*) as c from elasticsearch." + schema + "." + table + " where review_count < 10";
    testPlanMatchingPatterns(sqlQuery, new String[0], new String[]{"Aggregate"});
    testBuilder().sqlQuery(sqlQuery).unOrdered().baselineColumns("a", "b", "c")
      .baselineValues(1l,1l,1l)
      .go();
  }

  @Test
  public final void runTestFilterSwapLessThan() throws Exception {
    final String sqlQuery = "select stars + 1 as plusOne from elasticsearch." + schema + "." + table + " where review_count < 10";
    final String sqlQuery2 = "select stars + 1 as plusOne from elasticsearch." + schema + "." + table + " where 10 > review_count";
    final String plan = "="
        + "[{\n" +
        "  \"from\" : 0,\n" +
        "  \"size\" : 4000,\n" +
        "  \"query\" : {\n" +
        "    \"range\" : {\n" +
        "      \"review_count\" : {\n" +
        "        \"from\" : null,\n" +
        "        \"to\" : 10,\n" +
        "        \"include_lower\" : true,\n" +
        "        \"include_upper\" : false\n" +
        "      }\n" +
        "    }\n" +
        "  },\n" +
        "  \"_source\" : {\n" +
        "    \"includes\" : [ \"review_count\", \"stars\" ],\n" +
        "    \"excludes\" : [ ]\n" +
        "  }\n" +
        "}]";
    verifyJsonInPlan(sqlQuery, new String[]{ plan });
    testBuilder().sqlQuery(sqlQuery).unOrdered().baselineColumns("plusOne")
      .baselineValues(2.0f)
      .go();
    verifyJsonInPlan(sqlQuery2, new String[]{ plan });
    testBuilder().sqlQuery(sqlQuery2).unOrdered().baselineColumns("plusOne")
      .baselineValues(2.0f)
      .go();
  }

  @Test
  public final void runTestFilterSwapLessThanEqual() throws Exception {
    final String sqlQuery = "select stars + 1 as plusOne from elasticsearch." + schema + "." + table + " where review_count <= 11";
    final String sqlQuery2 = "select stars + 1 as plusOne from elasticsearch." + schema + "." + table + " where 11 >= review_count";
    final String plan = "[{\n" +
        "  \"from\" : 0,\n" +
        "  \"size\" : 4000,\n" +
        "  \"query\" : {\n" +
        "    \"range\" : {\n" +
        "      \"review_count\" : {\n" +
        "        \"from\" : null,\n" +
        "        \"to\" : 11,\n" +
        "        \"include_lower\" : true,\n" +
        "        \"include_upper\" : true\n" +
        "      }\n" +
        "    }\n" +
        "  },\n" +
        "  \"_source\" : {\n" +
        "    \"includes\" : [ \"review_count\", \"stars\" ],\n" +
        "    \"excludes\" : [ ]\n" +
        "  }\n" +
        "}]";
    verifyJsonInPlan(sqlQuery, new String[]{ plan });
    testBuilder().sqlQuery(sqlQuery).unOrdered().baselineColumns("plusOne")
      .baselineValues(2.0f)
      .baselineValues(5.5f)
      .baselineValues(5.5f)
      .go();
    verifyJsonInPlan(sqlQuery2, new String[]{ plan });
    testBuilder().sqlQuery(sqlQuery2).unOrdered().baselineColumns("plusOne")
      .baselineValues(2.0f)
      .baselineValues(5.5f)
      .baselineValues(5.5f)
      .go();
  }

  @Test
  public final void runTestFilterSwapGreaterThan() throws Exception {
    final String sqlQuery = "select stars + 1 as plusOne from elasticsearch." + schema + "." + table + " where review_count > 11";
    final String sqlQuery2 = "select stars + 1 as plusOne from elasticsearch." + schema + "." + table + " where 11 < review_count";
    final String plan = "[{\n" +
        "  \"from\" : 0,\n" +
        "  \"size\" : 4000,\n" +
        "  \"query\" : {\n" +
        "    \"range\" : {\n" +
        "      \"review_count\" : {\n" +
        "        \"from\" : 11,\n" +
        "        \"to\" : null,\n" +
        "        \"include_lower\" : false,\n" +
        "        \"include_upper\" : true\n" +
        "      }\n" +
        "    }\n" +
        "  },\n" +
        "  \"_source\" : {\n" +
        "    \"includes\" : [ \"review_count\", \"stars\" ],\n" +
        "    \"excludes\" : [ ]\n" +
        "  }\n" +
        "}]";
    verifyJsonInPlan(sqlQuery, new String[]{ plan });
    testBuilder().sqlQuery(sqlQuery).unOrdered().baselineColumns("plusOne")
      .baselineValues(4.5f)
      .baselineValues(6.0f)
      .go();
    verifyJsonInPlan(sqlQuery2, new String[]{ plan });
    testBuilder().sqlQuery(sqlQuery2).unOrdered().baselineColumns("plusOne")
      .baselineValues(4.5f)
      .baselineValues(6.0f)
      .go();
  }

  @Test
  public final void runTestFilterSwapGreaterThanEqual() throws Exception {
    final String sqlQuery = "select stars + 1 as plusOne from elasticsearch." + schema + "." + table + " where review_count >= 11";
    final String sqlQuery2 = "select stars + 1 as plusOne from elasticsearch." + schema + "." + table + " where 11 <= review_count";
    final String plan = "[{\n" +
        "  \"from\" : 0,\n" +
        "  \"size\" : 4000,\n" +
        "  \"query\" : {\n" +
        "    \"range\" : {\n" +
        "      \"review_count\" : {\n" +
        "        \"from\" : 11,\n" +
        "        \"to\" : null,\n" +
        "        \"include_lower\" : true,\n" +
        "        \"include_upper\" : true\n" +
        "      }\n" +
        "    }\n" +
        "  },\n" +
        "  \"_source\" : {\n" +
        "    \"includes\" : [ \"review_count\", \"stars\" ],\n" +
        "    \"excludes\" : [ ]\n" +
        "  }\n" +
        "}]";
    verifyJsonInPlan(sqlQuery, new String[]{ plan });
    testBuilder().sqlQuery(sqlQuery).unOrdered().baselineColumns("plusOne")
      .baselineValues(5.5f)
      .baselineValues(4.5f)
      .baselineValues(6.0f)
      .baselineValues(5.5f)
      .go();
    verifyJsonInPlan(sqlQuery2, new String[]{ plan });
    testBuilder().sqlQuery(sqlQuery2).unOrdered().baselineColumns("plusOne")
      .baselineValues(5.5f)
      .baselineValues(4.5f)
      .baselineValues(6.0f)
      .baselineValues(5.5f)
      .go();
  }

  @Test
  public final void runTestFilterWithUID() throws Exception {
    String sqlQuery = "select _uid from elasticsearch." + schema + "." + table + " where _uid is null";
    verifyJsonInPlan(sqlQuery, new String[] {
      "[{\n" +
      "  \"from\" : 0,\n" +
      "  \"size\" : 4000,\n" +
      "  \"query\" : {\n" +
      "    \"bool\" : {\n" +
      "      \"must_not\" : {\n" +
      "        \"exists\" : {\n" +
      "          \"field\" : \"_uid\"\n" +
      "        }\n" +
      "      }\n" +
      "    }\n" +
      "  },\n" +
      "  \"_source\" : {\n" +
      "    \"includes\" : [ \"_uid\" ],\n" +
      "    \"excludes\" : [ ]\n" +
      "  }\n" +
      "}]"
    });
    assertEquals(0, BaseTestQuery.getRecordCount(testRunAndReturn(QueryType.SQL, sqlQuery)));
  }

  @Test
  public final void runTestFilterWithUIDExists() throws Exception {
    String sqlQuery = "select _uid from elasticsearch." + schema + "." + table + " where _uid is not null and _uid <> 'ABC'";
    verifyJsonInPlan(sqlQuery, new String[] {
      "[{\n" +
      "  \"from\" : 0,\n" +
      "  \"size\" : 4000,\n" +
      "  \"query\" : {\n" +
      "    \"bool\" : {\n" +
      "      \"must_not\" : {\n" +
      "        \"match\" : {\n" +
      "          \"_uid\" : {\n" +
      "            \"query\" : \"ABC\",\n" +
      "            \"type\" : \"boolean\"\n" +
      "          }\n" +
      "        }\n" +
      "      }\n" +
      "    }\n" +
      "  },\n" +
      "  \"_source\" : {\n" +
      "    \"includes\" : [ \"_uid\" ],\n" +
      "    \"excludes\" : [ ]\n" +
      "  }\n" +
      "}]"
    });
    assertEquals(5, BaseTestQuery.getRecordCount(testRunAndReturn(QueryType.SQL, sqlQuery)));
  }

  @Test
  public final void runTestLike() throws Exception {
    String sqlQuery = "select city from elasticsearch." + schema + "." + table + " where city LIKE 'San%'";
    verifyJsonInPlan(sqlQuery, new String[] {
        "[{\n" +
        "  \"from\" : 0,\n" +
        "  \"size\" : 4000,\n" +
        "  \"query\" : {\n" +
        "    \"regexp\" : {\n" +
        "      \"city\" : {\n" +
        "        \"value\" : \"San.*\",\n" +
        "        \"flags_value\" : 65535\n" +
        "      }\n" +
        "    }\n" +
        "  },\n" +
        "  \"_source\" : {\n" +
        "    \"includes\" : [ \"city\" ],\n" +
        "    \"excludes\" : [ ]\n" +
        "  }\n" +
        "}]"
    });
    testBuilder()
        .sqlQuery(sqlQuery)
        .unOrdered()
        .baselineColumns("city")
        .baselineValues("San Francisco")
        .baselineValues("San Diego")
        .baselineValues("San Francisco")
        .go();
  }

  @Test
  public final void runTestNotLike() throws Exception {
    String sqlQuery = "select city from elasticsearch." + schema + "." + table + " where city NOT LIKE 'San%'";
    verifyJsonInPlan(sqlQuery, new String[] {
      "[{\n" +
        "  \"from\" : 0,\n" +
        "  \"size\" : 4000,\n" +
        "  \"query\" : {\n" +
        "    \"bool\" : {\n" +
        "      \"must\" : {\n" +
        "        \"exists\" : {\n" +
        "          \"field\" : \"city\"\n" +
        "        }\n" +
        "      },\n" +
        "      \"must_not\" : {\n" +
        "        \"regexp\" : {\n" +
        "          \"city\" : {\n" +
        "            \"value\" : \"San.*\",\n" +
        "            \"flags_value\" : 65535\n" +
        "          }\n" +
        "        }\n" +
        "      }\n" +
        "    }\n" +
        "  },\n" +
        "  \"_source\" : {\n" +
        "    \"includes\" : [ \"city\" ],\n" +
        "    \"excludes\" : [ ]\n" +
        "  }\n" +
        "}]"

    });
    testBuilder()
        .sqlQuery(sqlQuery)
        .unOrdered()
        .baselineColumns("city")
        .baselineValues("Cambridge")
        .baselineValues("Cambridge")
        .go();
  }

  @Test
  public final void runTestFilter() throws Exception {
    String sqlQuery = "select state, city_analyzed, review_count from elasticsearch." + schema + "." + table + " where stars >= 4";
    verifyJsonInPlan(sqlQuery, new String[] {
            "=[{\n" +
            "  \"from\" : 0,\n" +
            "  \"size\" : 4000,\n" +
            "  \"query\" : {\n" +
            "    \"range\" : {\n" +
            "      \"stars\" : {\n" +
            "        \"from\" : 4,\n" +
            "        \"to\" : null,\n" +
            "        \"include_lower\" : true,\n" +
            "        \"include_upper\" : true\n" +
            "      }\n" +
            "    }\n" +
            "  },\n" +
            "  \"_source\" : {\n" +
            "    \"includes\" : [ \"city_analyzed\", \"review_count\", \"stars\", \"state\" ],\n" +
            "    \"excludes\" : [ ]\n" +
            "  }\n" +
            "}])"
    });
    testBuilder()
            .sqlQuery(sqlQuery)
            .unOrdered()
            .baselineColumns("state", "city_analyzed", "review_count")
            .baselineValues("MA", "Cambridge", 11)
            .baselineValues("CA", "San Diego", 33)
            .baselineValues("MA", "Cambridge", 11)
            .go();
  }

  @Test
  public final void runTestFilterInClause() throws Exception {
    String sqlQuery = "select review_count from elasticsearch." + schema + "." + table +
      " where review_count in (1,11,22)";
    verifyJsonInPlan(sqlQuery, new String[]{
      "[{\n" +
      "  \"from\" : 0,\n" +
      "  \"size\" : 4000,\n" +
      "  \"query\" : {\n" +
      "    \"bool\" : {\n" +
      "      \"should\" : [ {\n" +
      "        \"match\" : {\n" +
      "          \"review_count\" : {\n" +
      "            \"query\" : 1,\n" +
      "            \"type\" : \"boolean\"\n" +
      "          }\n" +
      "        }\n" +
      "      }, {\n" +
      "        \"match\" : {\n" +
      "          \"review_count\" : {\n" +
      "            \"query\" : 11,\n" +
      "            \"type\" : \"boolean\"\n" +
      "          }\n" +
      "        }\n" +
      "      }, {\n" +
      "        \"match\" : {\n" +
      "          \"review_count\" : {\n" +
      "            \"query\" : 22,\n" +
      "            \"type\" : \"boolean\"\n" +
      "          }\n" +
      "        }\n" +
      "      } ]\n" +
      "    }\n" +
      "  },\n" +
      "  \"_source\" : {\n" +
      "    \"includes\" : [ \"review_count\" ],\n" +
      "    \"excludes\" : [ ]\n" +
      "  }\n" +
      "}]"
    });

    testBuilder()
      .sqlQuery(sqlQuery)
      .unOrdered()
      .baselineColumns("review_count")
      .baselineValues(11)
      .baselineValues(22)
      .baselineValues(11)
      .baselineValues(1)
      .go();
  }

  @Test
  public final void runTestFieldsEquality() throws Exception {
    String sqlQuery1 = "select state, city, review_count, stars from elasticsearch." + schema + "." + table + " where cast(review_count as double) = stars";
    verifyJsonInPlan(sqlQuery1,
        new String[] {
          "[{\n" +
          "  \"from\" : 0,\n" +
          "  \"size\" : 4000,\n" +
          "  \"query\" : {\n" +
          "    \"script\" : {\n" +
          "      \"script\" : {\n" +
          "        \"inline\" : \"(doc[\\\"review_count\\\"].empty || doc[\\\"stars\\\"].empty) ? null : ( (double)(doc[\\\"review_count\\\"].value) == (doc[\\\"stars\\\"].value).doubleValue() )\",\n" +
          "        \"lang\" : \"groovy\"\n" +
          "      }\n" +
          "    }\n" +
          "  },\n" +
          "  \"_source\" : {\n" +
          "    \"includes\" : [ \"city\", \"review_count\", \"stars\", \"state\" ],\n" +
          "    \"excludes\" : [ ]\n" +
          "  }\n" +
          "}]"
        });
    testBuilder()
      .sqlQuery(sqlQuery1)
      .unOrdered()
      .baselineColumns("state", "city", "review_count", "stars")
      .baselineValues("CA", "San Francisco", 1, 1.0F)
      .go();
  }

  @Test
  public final void runTestFieldsEquality2() throws Exception {
    String sqlQuery = "select state, city, review_count, stars from elasticsearch." + schema + "." + table + " where review_count = stars";
    verifyJsonInPlan(sqlQuery,
      new String[] {
        "[{\n" +
        "  \"from\" : 0,\n" +
        "  \"size\" : 4000,\n" +
        "  \"query\" : {\n" +
        "    \"script\" : {\n" +
        "      \"script\" : {\n" +
        "        \"inline\" : \"(doc[\\\"review_count\\\"].empty || doc[\\\"stars\\\"].empty) ? null : ( ((float)(doc[\\\"review_count\\\"].value)) == doc[\\\"stars\\\"].value )\",\n" +
        "        \"lang\" : \"groovy\"\n" +
        "      }\n" +
        "    }\n" +
        "  },\n" +
        "  \"_source\" : {\n" +
        "    \"includes\" : [ \"city\", \"review_count\", \"stars\", \"state\" ],\n" +
        "    \"excludes\" : [ ]\n" +
        "  }\n" +
        "}]"
    });

    testBuilder()
        .sqlQuery(sqlQuery)
        .unOrdered()
        .baselineColumns("state", "city", "review_count", "stars")
        .baselineValues("CA", "San Francisco", 1, 1.0F)
        .go();
  }

  @Test
  public final void runTestArrayAccessWithoutAggregate() throws Exception {
    // We cannot push down complex fields
    String sqlQuery = "select t.location_field[1].lat as lat_1 from elasticsearch." + schema + "." + table + " t";
    verifyJsonInPlan(sqlQuery, new String[] {
            "=[{\n" +
            "  \"from\" : 0,\n" +
            "  \"size\" : 4000,\n" +
            "  \"query\" : {\n" +
            "    \"match_all\" : { }\n" +
            "  },\n" +
            "  \"_source\" : {\n" +
            "    \"includes\" : [ \"location_field\" ],\n" +
            "    \"excludes\" : [ ]\n" +
            "  }\n" +
            "}])\n"
    });
    testBuilder()
            .sqlQuery(sqlQuery)
            .unOrdered()
            .baselineColumns("lat_1")
            .baselineValues(-11D)
            .baselineValues(-22D)
            .baselineValues(-33D)
            .baselineValues(-44D)
            .baselineValues(-55D)
            .go();
  }

  @Test
  public final void avoidComplexPushdownOnGeoField() throws Exception {
    // We cannot push down complex fields
    String sqlQuery = "select t.location_field[1] as location_1 from elasticsearch." + schema + "." + table + " t";
    verifyJsonInPlan(sqlQuery, new String[] {
            "=[{\n" +
            "  \"from\" : 0,\n" +
            "  \"size\" : 4000,\n" +
            "  \"query\" : {\n" +
            "    \"match_all\" : { }\n" +
            "  },\n" +
            "  \"_source\" : {\n" +
            "    \"includes\" : [ \"location_field\" ],\n" +
            "    \"excludes\" : [ ]\n" +
            "  }\n" +
            "}])"
    });
    testBuilder()
            .sqlQuery(sqlQuery)
            .unOrdered()
            .baselineColumns("location_1")
            .baselineValues(mapOf("lat", -11D, "lon", -11D))
            .baselineValues(mapOf("lat", -22D, "lon", -22D))
            .baselineValues(mapOf("lat", -33D, "lon", -33D))
            .baselineValues(mapOf("lat", -44D, "lon", -44D))
            .baselineValues(mapOf("lat", -55D, "lon", -55D))
            .go();
  }




  @Test
  public final void avoidPushingDownArbitraryProjection() throws Exception {
    String sqlQuery = "select stars + 1 as plusOne from elasticsearch." + schema + "." + table;
    verifyJsonInPlan(sqlQuery, new String[] {
        "=[{\n" +
        "  \"from\" : 0,\n" +
        "  \"size\" : 4000,\n" +
        "  \"query\" : {\n" +
        "    \"match_all\" : { }\n" +
        "  },\n" +
        "  \"_source\" : {\n" +
        "    \"includes\" : [ \"stars\" ],\n" +
        "    \"excludes\" : [ ]\n" +
        "  }\n" +
        "}])"
    });
    testBuilder().sqlQuery(sqlQuery).unOrdered().baselineColumns("plusOne")
            .baselineValues(5.5f)
            .baselineValues(4.5f)
            .baselineValues(6.0f)
            .baselineValues(5.5f)
            .baselineValues(2.0f)
            .go();
  }

  @Test
  public final void runTestComplexProjectWithFilter() throws Exception {
    String sqlQuery = "select stars + 1 as plusOne from elasticsearch." + schema + "." + table + " where review_count < 10";
    verifyJsonInPlan(sqlQuery, new String[]{
      "[{\n" +
      "  \"from\" : 0,\n" +
      "  \"size\" : 4000,\n" +
      "  \"query\" : {\n" +
      "    \"range\" : {\n" +
      "      \"review_count\" : {\n" +
      "        \"from\" : null,\n" +
      "        \"to\" : 10,\n" +
      "        \"include_lower\" : true,\n" +
      "        \"include_upper\" : false\n" +
      "      }\n" +
      "    }\n" +
      "  },\n" +
      "  \"_source\" : {\n" +
      "    \"includes\" : [ \"review_count\", \"stars\" ],\n" +
      "    \"excludes\" : [ ]\n" +
      "  }\n" +
      "}]"
    });
    testBuilder().sqlQuery(sqlQuery).unOrdered().baselineColumns("plusOne")
            .baselineValues(2.0f)
            .go();
  }

  @Test
  public final void runTestComplexProjectWithFilterProject() throws Exception {
    String sqlQuery = "select stars + 1 as plusOne from elasticsearch." + schema + "." + table + " where stars < 3";
    verifyJsonInPlan(sqlQuery, new String[] {
        "=[{\n" +
        "  \"from\" : 0,\n" +
        "  \"size\" : 4000,\n" +
        "  \"query\" : {\n" +
        "    \"range\" : {\n" +
        "      \"stars\" : {\n" +
        "        \"from\" : null,\n" +
        "        \"to\" : 3,\n" +
        "        \"include_lower\" : true,\n" +
        "        \"include_upper\" : false\n" +
        "      }\n" +
        "    }\n" +
        "  },\n" +
        "  \"_source\" : {\n" +
        "    \"includes\" : [ \"stars\" ],\n" +
        "    \"excludes\" : [ ]\n" +
        "  }\n" +
        "}])"
    });
    testBuilder().sqlQuery(sqlQuery).ordered().baselineColumns("plusOne")
            .baselineValues(2.0F)
            .go();
  }

  @Test
  public final void tesFilterRequiringScripts() throws Exception {
    String sqlQuery = "select state, stars from elasticsearch." + schema + "." + table +
        " where sqrt(stars) < 2";

    verifyJsonInPlan(sqlQuery, new String[]{
      "[{\n" +
      "  \"from\" : 0,\n" +
      "  \"size\" : 4000,\n" +
      "  \"query\" : {\n" +
      "    \"script\" : {\n" +
      "      \"script\" : {\n" +
      "        \"inline\" : \"(doc[\\\"stars\\\"].empty) ? null : ( Math.pow(doc[\\\"stars\\\"].value, 0.5) < 2 )\",\n" +
      "        \"lang\" : \"groovy\"\n" +
      "      }\n" +
      "    }\n" +
      "  },\n" +
      "  \"_source\" : {\n" +
      "    \"includes\" : [ \"stars\", \"state\" ],\n" +
      "    \"excludes\" : [ ]\n" +
      "  }\n" +
      "}]"
      });
    testBuilder().sqlQuery(sqlQuery).unOrdered().baselineColumns("state", "stars")
        .baselineValues("CA", 3.5f)
        .baselineValues("CA", 1.0f)
        .go();
  }

  @Test
  public final void testFilterRequiringScriptsOnVarchar() throws Exception {

    String sqlQuery = "select state, stars from elasticsearch." + schema + "." + table +
        " where concat(city, state) = 'San DiegoCA'";

    verifyJsonInPlan(sqlQuery, new String[]{
      "[{\n" +
      "  \"from\" : 0,\n" +
      "  \"size\" : 4000,\n" +
      "  \"query\" : {\n" +
      "    \"script\" : {\n" +
      "      \"script\" : {\n" +
      "        \"inline\" : \"(doc[\\\"city\\\"].empty || doc[\\\"state\\\"].empty) ? null : ( ( doc[\\\"city\\\"].value + doc[\\\"state\\\"].value ) == 'San DiegoCA' )\",\n" +
      "        \"lang\" : \"groovy\"\n" +
      "      }\n" +
      "    }\n" +
      "  },\n" +
      "  \"_source\" : {\n" +
      "    \"includes\" : [ \"city\", \"stars\", \"state\" ],\n" +
      "    \"excludes\" : [ ]\n" +
      "  }\n" +
      "}]"
    });

    testBuilder().sqlQuery(sqlQuery).unOrdered().baselineColumns("state", "stars")
        .baselineValues("CA", 5.0f)
        .go();
  }

  @Test
  public final void testFilterWithTrimFunc() throws Exception {

    String sqlQuery = "select city from elasticsearch." + schema + "." + table +
        " where char_length(trim(concat(city, '  '))) = 9";
    verifyJsonInPlan(sqlQuery, new String[]{
      "[{\n" +
      "  \"from\" : 0,\n" +
      "  \"size\" : 4000,\n" +
      "  \"query\" : {\n" +
      "    \"script\" : {\n" +
      "      \"script\" : {\n" +
      "        \"inline\" : \"(doc[\\\"city\\\"].empty) ? null : ( ( doc[\\\"city\\\"].value + '  ' ).trim().length() == 9 )\",\n" +
      "        \"lang\" : \"groovy\"\n" +
      "      }\n" +
      "    }\n" +
      "  },\n" +
      "  \"_source\" : {\n" +
      "    \"includes\" : [ \"city\" ],\n" +
      "    \"excludes\" : [ ]\n" +
      "  }\n" +
      "}]"
    });

    testBuilder().sqlQuery(sqlQuery).unOrdered().baselineColumns("city")
        .baselineValues("Cambridge")
        .baselineValues("San Diego")
        .baselineValues("Cambridge")
        .go();
    }

    @Test
    public final void testFilterWithTrimFunc2() throws Exception {

    String sqlQuery2 = "select city from elasticsearch." + schema + "." + table +
        " where char_length(rtrim(concat(city, '  '))) = 9";
    verifyJsonInPlan(sqlQuery2, new String[]{
      "[{\n" +
      "  \"from\" : 0,\n" +
      "  \"size\" : 4000,\n" +
      "  \"query\" : {\n" +
      "    \"match_all\" : { }\n" +
      "  },\n" +
      "  \"_source\" : {\n" +
      "    \"includes\" : [ \"city\" ],\n" +
      "    \"excludes\" : [ ]\n" +
      "  }\n" +
      "}]"
    });
    testBuilder().sqlQuery(sqlQuery2).unOrdered().baselineColumns("city")
        .baselineValues("Cambridge")
        .baselineValues("San Diego")
        .baselineValues("Cambridge")
        .go();
    }

    @Test
    public final void testFilterWithTrimFunc3() throws Exception {

    String sqlQuery3 = "select city from elasticsearch." + schema + "." + table +
        " where char_length(ltrim(concat('  ', city))) = 9";
    verifyJsonInPlan(sqlQuery3, new String[]{
      "[{\n" +
      "  \"from\" : 0,\n" +
      "  \"size\" : 4000,\n" +
      "  \"query\" : {\n" +
      "    \"match_all\" : { }\n" +
      "  },\n" +
      "  \"_source\" : {\n" +
      "    \"includes\" : [ \"city\" ],\n" +
      "    \"excludes\" : [ ]\n" +
      "  }\n" +
      "}]"
    });
    testBuilder().sqlQuery(sqlQuery3).unOrdered().baselineColumns("city")
        .baselineValues("Cambridge")
        .baselineValues("San Diego")
        .baselineValues("Cambridge")
        .go();
  }

  @Test
  public final void testFilterWithVarcharCaseChange() throws Exception {
    String sqlQuery3 = "select city from elasticsearch." + schema + "." + table +
        " where upper(lower(city)) = 'CAMBRIDGE'";
    verifyJsonInPlan(sqlQuery3, new String[]{
      "[{\n" +
      "  \"from\" : 0,\n" +
      "  \"size\" : 4000,\n" +
      "  \"query\" : {\n" +
      "    \"script\" : {\n" +
      "      \"script\" : {\n" +
      "        \"inline\" : \"(doc[\\\"city\\\"].empty) ? null : ( doc[\\\"city\\\"].value.toLowerCase().toUpperCase() == 'CAMBRIDGE' )\",\n" +
      "        \"lang\" : \"groovy\"\n" +
      "      }\n" +
      "    }\n" +
      "  },\n" +
      "  \"_source\" : {\n" +
      "    \"includes\" : [ \"city\" ],\n" +
      "    \"excludes\" : [ ]\n" +
      "  }\n" +
      "}]"
    });
    testBuilder().sqlQuery(sqlQuery3).unOrdered().baselineColumns("city")
        .baselineValues("Cambridge")
        .baselineValues("Cambridge")
        .go();

  }

  @Test
  public final void simpleInequalityNoPush() throws Exception {

    String sqlQuery = "select stars > 2 as out_col from elasticsearch." + schema + "." + table;
    verifyJsonInPlan(sqlQuery, new String[] {
      "[{\n" +
      "  \"from\" : 0,\n" +
      "  \"size\" : 4000,\n" +
      "  \"query\" : {\n" +
      "    \"match_all\" : { }\n" +
      "  },\n" +
      "  \"_source\" : {\n" +
      "    \"includes\" : [ \"stars\" ],\n" +
      "    \"excludes\" : [ ]\n" +
      "  }\n" +
      "}]"
    });
    testBuilder().sqlQuery(sqlQuery).unOrdered().baselineColumns("out_col")
        .baselineValues(true)
        .baselineValues(true)
        .baselineValues(true)
        .baselineValues(true)
        .baselineValues(false)
        .go();
  }

  @Test
  public final void runComplexConditionalNoPush() throws Exception {

    String sqlQuery = "select case when stars > 2 then 1 else 0 end as out_col from elasticsearch." + schema + "." + table;
    verifyJsonInPlan(sqlQuery, new String[]{
      "[{\n" +
      "  \"from\" : 0,\n" +
      "  \"size\" : 4000,\n" +
      "  \"query\" : {\n" +
      "    \"match_all\" : { }\n" +
      "  },\n" +
      "  \"_source\" : {\n" +
      "    \"includes\" : [ \"stars\" ],\n" +
      "    \"excludes\" : [ ]\n" +
      "  }\n" +
      "}]"
    });
    // TODO - Jason review, this baseline changed from double type to long when executed in Dremio instead of elastic
    // scripts, can numeric types other than double be returned from scripts?
    //    - actually I think I know why this is, Minji made the reader always read numbers as double because
    //      elastic would not provide type information for scripted fields and we didn't want schema changes
    //      relying on the JSON tokens which omitted decimal points on integer values
    testBuilder().sqlQuery(sqlQuery).unOrdered().baselineColumns("out_col")
        .baselineValues(1)
        .baselineValues(1)
        .baselineValues(1)
        .baselineValues(1)
        .baselineValues(0)
        .go();
  }

  @Test
  public final void tesFilterWithCase() throws Exception {

    String sqlQuery = "select city from elasticsearch." + schema + "." + table + " where case when stars < 2 then city else state end = 'San Francisco'";
    verifyJsonInPlan(sqlQuery, new String[]{
      "[{\n" +
      "  \"from\" : 0,\n" +
      "  \"size\" : 4000,\n" +
      "  \"query\" : {\n" +
      "    \"script\" : {\n" +
      "      \"script\" : {\n" +
      "        \"inline\" : \"(doc[\\\"stars\\\"].empty) ? null : ( ( ( ( doc[\\\"stars\\\"].value < 2 ) ) ? ( (doc[\\\"city\\\"].empty) ? null : doc[\\\"city\\\"].value ) : ( (doc[\\\"state\\\"].empty) ? null : doc[\\\"state\\\"].value ) ) == 'San Francisco' )\",\n" +
      "        \"lang\" : \"groovy\"\n" +
      "      }\n" +
      "    }\n" +
      "  },\n" +
      "  \"_source\" : {\n" +
      "    \"includes\" : [ \"city\", \"stars\", \"state\" ],\n" +
      "    \"excludes\" : [ ]\n" +
      "  }\n" +
      "}]"
    });
    testBuilder().sqlQuery(sqlQuery).unOrdered().baselineColumns("city")
        .baselineValues("San Francisco")
        .go();
  }

  @Test
  public final void runTestMathOperators() throws Exception {
    // this complex expression just produces the number 3, all of the terms after the first just cancel each other out
    // the expression is actually being constant folded, so this isn't the best test for the elastic expression
    // pushdown, but keeping it here just to track how planning changes if we start failing to fold constants
    String sqlQuery = "select stars + 2 - (2 * floor(power(2.1,2)))/4 + sqrt(4)/exp(0) - mod(5, 3) as complexIdentity " +
        "from elasticsearch." + schema + "." + table;

      verifyJsonInPlan(sqlQuery, new String[] {
          "=[{\n" +
          "  \"from\" : 0,\n" +
          "  \"size\" : 4000,\n" +
          "  \"query\" : {\n" +
          "    \"match_all\" : { }\n" +
          "  },\n" +
          "  \"_source\" : {\n" +
          "    \"includes\" : [ \"stars\" ],\n" +
          "    \"excludes\" : [ ]\n" +
          "  }\n" +
          "}]"
      });
    testBuilder()
        .sqlQuery(sqlQuery)
        .ordered()
        .baselineColumns("complexIdentity")
        .baselineValues(4.5)
        .baselineValues(3.5)
        .baselineValues(5.0)
        .baselineValues(4.5)
        .baselineValues(1.0)
        .go();
  }

  @Test
  public final void testFilterWithComplexMath() throws Exception {
    String sqlQuery = "select city " +
        "from elasticsearch." + schema + "." + table + " where " +
        "(stars + 2 - (2 * floor(power(stars,2)))/4 + sqrt(stars)/exp(0) - mod(review_count, 3)) = 0.37082869338697066";

    verifyJsonInPlan(sqlQuery, new String[]{
      "[{\n" +
      "  \"from\" : 0,\n" +
      "  \"size\" : 4000,\n" +
      "  \"query\" : {\n" +
      "    \"script\" : {\n" +
      "      \"script\" : {\n" +
      "        \"inline\" : \"(doc[\\\"stars\\\"].empty || doc[\\\"review_count\\\"].empty) ? null : ( ( ( ( ( doc[\\\"stars\\\"].value + 2 ) - ( ( 2 * Math.floor(Math.pow(doc[\\\"stars\\\"].value, 2)) ) / 4 ) ) + ( Math.pow(doc[\\\"stars\\\"].value, 0.5) / 1E0D ) ) - ( doc[\\\"review_count\\\"].value % 3 ) ) == 0.37082869338697066D )\",\n" +
      "        \"lang\" : \"groovy\"\n" +
      "      }\n" +
      "    }\n" +
      "  },\n" +
      "  \"_source\" : {\n" +
      "    \"includes\" : [ \"city\", \"review_count\", \"stars\" ],\n" +
      "    \"excludes\" : [ ]\n" +
      "  }\n" +
      "}]"
    });

    testBuilder()
        .sqlQuery(sqlQuery)
        .ordered()
        .baselineColumns("city")
        .baselineValues("San Francisco")
        .go();
  }


  @Test
  public final void runTestProjectBooleanNoPush() throws Exception {
    String sqlQuery = "select not `open` from elasticsearch." + schema + "." + table;
    verifyJsonInPlan(sqlQuery, new String[]{
            "=[{\n" +
            "  \"from\" : 0,\n" +
            "  \"size\" : 4000,\n" +
            "  \"query\" : {\n" +
            "    \"match_all\" : { }\n" +
            "  },\n" +
            "  \"_source\" : {\n" +
            "    \"includes\" : [ \"open\" ],\n" +
            "    \"excludes\" : [ ]\n" +
            "  }\n" +
            "}]"
    });
    testBuilder().sqlQuery(sqlQuery).unOrdered().baselineColumns("EXPR$0")
            .baselineValues(false)
            .baselineValues(false)
            .baselineValues(true)
            .baselineValues(false)
            .baselineValues(true)
            .go();
  }

  @Test
  public final void dateLessThan() throws Exception{
    String query = "select datefield from elasticsearch." + schema + "." + table + " where datefield < datefield";
    testBuilder().sqlQuery(query).expectsEmptyResultSet()
    .go();
  }

  @Test
  public final void dateLessThanOrEqual() throws Exception{
    String query = "select datefield from elasticsearch." + schema + "." + table + " where datefield <= datefield";
    testBuilder().sqlQuery(query).unOrdered().baselineColumns("datefield")
    .baselineValues(new LocalDateTime(Timestamp.valueOf("2014-02-10 10:50:42")))
    .baselineValues(new LocalDateTime(Timestamp.valueOf("2014-02-11 10:50:42")))
    .baselineValues(new LocalDateTime(Timestamp.valueOf("2014-02-12 10:50:42")))
    .baselineValues(new LocalDateTime(Timestamp.valueOf("2014-02-11 10:50:42")))
    .baselineValues(new LocalDateTime(Timestamp.valueOf("2014-02-10 10:50:42")))
    .go();
  }

  @Test
  public final void dateGreaterThan() throws Exception{
    String query = "select datefield from elasticsearch." + schema + "." + table + " where datefield > datefield";
    testBuilder().sqlQuery(query).expectsEmptyResultSet()
    .go();
  }

  @Test
  public final void dateGreaterThanOrEqual() throws Exception{
    String query = "select datefield from elasticsearch." + schema + "." + table + " where datefield >= datefield";
    testBuilder().sqlQuery(query).unOrdered().baselineColumns("datefield")
    .baselineValues(new LocalDateTime(Timestamp.valueOf("2014-02-10 10:50:42")))
    .baselineValues(new LocalDateTime(Timestamp.valueOf("2014-02-11 10:50:42")))
    .baselineValues(new LocalDateTime(Timestamp.valueOf("2014-02-12 10:50:42")))
    .baselineValues(new LocalDateTime(Timestamp.valueOf("2014-02-11 10:50:42")))
    .baselineValues(new LocalDateTime(Timestamp.valueOf("2014-02-10 10:50:42")))
    .go();
  }


  @Test
  public final void runTestProjectTimestamp() throws Exception {
    String sqlQuery = "select `datefield` from elasticsearch." + schema + "." + table;
    verifyJsonInPlan(sqlQuery, new String[] {
            "=[{\n" +
            "  \"from\" : 0,\n" +
            "  \"size\" : 4000,\n" +
            "  \"query\" : {\n" +
            "    \"match_all\" : { }\n" +
            "  },\n" +
            "  \"_source\" : {\n" +
            "    \"includes\" : [ \"datefield\" ],\n" +
            "    \"excludes\" : [ ]\n" +
            "  }\n" +
            "}])"});
    testBuilder().sqlQuery(sqlQuery).unOrdered().baselineColumns("datefield")
            .baselineValues(new LocalDateTime(Timestamp.valueOf("2014-02-10 10:50:42")))
            .baselineValues(new LocalDateTime(Timestamp.valueOf("2014-02-11 10:50:42")))
            .baselineValues(new LocalDateTime(Timestamp.valueOf("2014-02-12 10:50:42")))
            .baselineValues(new LocalDateTime(Timestamp.valueOf("2014-02-11 10:50:42")))
            .baselineValues(new LocalDateTime(Timestamp.valueOf("2014-02-10 10:50:42")))
            .go();
  }

  @Test
  public final void runTestProjectExtractYearMonth() throws Exception {
    String sqlQuery = "select extract(year from `datefield`), extract(month from `datefield`), "
        + "extract(day from `datefield`), extract(hour from `datefield`), extract(minute from `datefield`),"
        + "extract(second from `datefield`) from elasticsearch." + schema + "." + table;
    verifyJsonInPlan(sqlQuery, new String[] {
        "[{\n" +
        "  \"from\" : 0,\n" +
        "  \"size\" : 4000,\n" +
        "  \"query\" : {\n" +
        "    \"match_all\" : { }\n" +
        "  },\n" +
        "  \"_source\" : {\n" +
        "    \"includes\" : [ \"datefield\" ],\n" +
        "    \"excludes\" : [ ]\n" +
        "  }\n" +
        "}]"});

    testBuilder().sqlQuery(sqlQuery).unOrdered().baselineColumns("EXPR$0", "EXPR$1", "EXPR$2", "EXPR$3", "EXPR$4", "EXPR$5")
        .baselineValues(2014L, 2L, 10L, 10L, 50L, 42L)
        .baselineValues(2014L, 2L, 11L, 10L, 50L, 42L)
        .baselineValues(2014L, 2L, 12L, 10L, 50L, 42L)
        .baselineValues(2014L, 2L, 11L, 10L, 50L, 42L)
        .baselineValues(2014L, 2L, 10L, 10L, 50L, 42L)
        .go();
  }

  @Test
  public final void runTestProjectExtractDayFilter() throws Exception {
    String sqlQuery = "select datefield from elasticsearch." + schema + "." + table + " where  extract(day from `datefield`) = 10";
    verifyJsonInPlan(sqlQuery, new String[] {
      "[{\n" +
      "  \"from\" : 0,\n" +
      "  \"size\" : 4000,\n" +
      "  \"query\" : {\n" +
      "    \"script\" : {\n" +
      "      \"script\" : {\n" +
      "        \"inline\" : \"( (doc[\\\"datefield\\\"].empty) ? null : doc[\\\"datefield\\\"].date.dayOfMonth == 10L )\",\n" +
      "        \"lang\" : \"groovy\"\n" +
      "      }\n" +
      "    }\n" +
      "  },\n" +
      "  \"_source\" : {\n" +
      "    \"includes\" : [ \"datefield\" ],\n" +
      "    \"excludes\" : [ ]\n" +
      "  }\n" +
      "}]"
    });

    testBuilder().sqlQuery(sqlQuery).unOrdered().baselineColumns("datefield")
        .baselineValues(new LocalDateTime(Timestamp.valueOf("2014-02-10 10:50:42")))
        .baselineValues(new LocalDateTime(Timestamp.valueOf("2014-02-10 10:50:42")))
        .go();
  }

  @Test
  public final void runTestMetaFields() throws Exception {
    String sqlQuery = "select _index from elasticsearch." + schema + "." + table;
    verifyJsonInPlan(sqlQuery, new String[] {
      "=[{\n" +
      "  \"from\" : 0,\n" +
      "  \"size\" : 4000,\n" +
      "  \"query\" : {\n" +
      "    \"match_all\" : { }\n" +
      "  },\n" +
      "  \"_source\" : {\n" +
      "    \"includes\" : [ \"_index\" ],\n" +
      "    \"excludes\" : [ ]\n" +
      "  }\n" +
      "}]"});
    testBuilder().sqlQuery(sqlQuery).unOrdered().baselineColumns("_index")
      .baselineValues(schema).baselineValues(schema).baselineValues(schema).baselineValues(schema).baselineValues(schema).go();
  }

  @Test
  public final void runTestMetaFieldsUID() throws Exception {
    String sqlQuery = "select _uid from elasticsearch." + schema + "." + table;
    verifyJsonInPlan(sqlQuery, new String[] {
            "=[{\n" +
            "  \"from\" : 0,\n" +
            "  \"size\" : 4000,\n" +
            "  \"query\" : {\n" +
            "    \"match_all\" : { }\n" +
            "  },\n" +
            "  \"_source\" : {\n" +
            "    \"includes\" : [ \"_uid\" ],\n" +
            "    \"excludes\" : [ ]\n" +
            "  }\n" +
            "}]"});
    assertEquals(5, BaseTestQuery.getRecordCount(testRunAndReturn(QueryType.SQL, sqlQuery)));
  }


  @Test
  public final void runTestProjectWithNestedField() throws Exception {
    String sqlQuery = "select location_field, review_count from elasticsearch." + schema + "." + table;
    verifyJsonInPlan(sqlQuery, new String[] {
            "=[{\n" +
            "  \"from\" : 0,\n" +
            "  \"size\" : 4000,\n" +
            "  \"query\" : {\n" +
            "    \"match_all\" : { }\n" +
            "  },\n" +
            "  \"_source\" : {\n" +
            "    \"includes\" : [ \"location_field\", \"review_count\" ],\n" +
            "    \"excludes\" : [ ]\n" +
            "  }\n" +
            "}]"
    });
    testBuilder().sqlQuery(sqlQuery).unOrdered().baselineColumns("location_field", "review_count")
            .baselineValues(listOf(mapOf("lat", 11D, "lon", 11D), mapOf("lat", -11D, "lon", -11D)), 11)
            .baselineValues(listOf(mapOf("lat", 22D, "lon", 22D), mapOf("lat", -22D, "lon", -22D)), 22)
            .baselineValues(listOf(mapOf("lat", 33D, "lon", 33D), mapOf("lat", -33D, "lon", -33D)), 33)
            .baselineValues(listOf(mapOf("lat", 44D, "lon", 44D), mapOf("lat", -44D, "lon", -44D)), 11)
            .baselineValues(listOf(mapOf("lat", 55D, "lon", 55D), mapOf("lat", -55D, "lon", -55D)), 1)
            .go();
  }

  @Test
  public final void runTestProjectString() throws Exception {
    String sqlQuery = "select city, location_field, review_count from elasticsearch." + schema + "." + table;
    //testPlanMatchingPatterns(sqlQuery, null, null);
    test(sqlQuery);
  }

  @Test
  public final void runTestWhereClauseWithNestedNoExpressionPushdown() throws Exception {
    String sqlQuery = "select t.`open`, t.location_field[1]['lat'] as lat_1 from elasticsearch." + schema + "." + table + " t where t.location_field[1]['lat'] < -30";
    verifyJsonInPlan(sqlQuery, new String[] {
            "=[{\n" +
            "  \"from\" : 0,\n" +
            "  \"size\" : 4000,\n" +
            "  \"query\" : {\n" +
            "    \"match_all\" : { }\n" +
            "  },\n" +
            "  \"_source\" : {\n" +
            "    \"includes\" : [ \"location_field\", \"open\" ],\n" +
            "    \"excludes\" : [ ]\n" +
            "  }\n" +
            "}]"
    });
    testBuilder()
            .sqlQuery(sqlQuery)
            .unOrdered()
            .baselineColumns("open", "lat_1")
            .baselineValues(false, -33D)
            .baselineValues(true, -44D)
            .baselineValues(false, -55D)
            .go();
  }


  @Test
  public final void testCastToShortVarcharTruncates() throws Exception {
    String sqlQuery = "select city from elasticsearch." + schema + "." + table + " where concat(cast(review_count as varchar(1)), '') = '3'";
    verifyJsonInPlan(sqlQuery, new String[] {
      "[{\n" +
      "  \"from\" : 0,\n" +
      "  \"size\" : 4000,\n" +
      "  \"query\" : {\n" +
      "    \"script\" : {\n" +
      "      \"script\" : {\n" +
      "        \"inline\" : \"(doc[\\\"review_count\\\"].empty) ? null : ( ( Long.toString(doc[\\\"review_count\\\"].value).substring(0, Math.min(1, Double.toString(doc[\\\"review_count\\\"].value).length())) + '' ) == '3' )\",\n" +
      "        \"lang\" : \"groovy\"\n" +
      "      }\n" +
      "    }\n" +
      "  },\n" +
      "  \"_source\" : {\n" +
      "    \"includes\" : [ \"city\", \"review_count\" ],\n" +
      "    \"excludes\" : [ ]\n" +
      "  }\n" +
      "}]"
    });
    testBuilder()
            .sqlQuery(sqlQuery)
            .unOrdered()
            .baselineColumns("city")
            .baselineValues("San Diego")
            .go();
  }

  @Test
  public final void testMultipleFilters() throws Exception {

    String sqlQuery = "select * from "
            + "(select state, city, review_count from elasticsearch." + schema + "." + table
            + " where char_length(trim(concat(city, '  '))) = 9) t"
            + " where t.review_count <> 11";
    verifyJsonInPlan(sqlQuery, new String[]{
      "[{\n" +
      "  \"from\" : 0,\n" +
      "  \"size\" : 4000,\n" +
      "  \"query\" : {\n" +
      "    \"bool\" : {\n" +
      "      \"must\" : [ {\n" +
      "        \"bool\" : {\n" +
      "          \"must\" : {\n" +
      "            \"bool\" : {\n" +
      "              \"must_not\" : {\n" +
      "                \"match\" : {\n" +
      "                  \"review_count\" : {\n" +
      "                    \"query\" : 11,\n" +
      "                    \"type\" : \"boolean\"\n" +
      "                  }\n" +
      "                }\n" +
      "              }\n" +
      "            }\n" +
      "          }\n" +
      "        }\n" +
      "      }, {\n" +
      "        \"script\" : {\n" +
      "          \"script\" : {\n" +
      "            \"inline\" : \"(doc[\\\"city\\\"].empty || doc[\\\"review_count\\\"].empty) ? null : ( ( ( doc[\\\"city\\\"].value + '  ' ).trim().length() == 9 ) && ( doc[\\\"review_count\\\"].value != 11 ) )\",\n" +
      "            \"lang\" : \"groovy\"\n" +
      "          }\n" +
      "        }\n" +
      "      } ]\n" +
      "    }\n" +
      "  },\n" +
      "  \"_source\" : {\n" +
      "    \"includes\" : [ \"city\", \"review_count\", \"state\" ],\n" +
      "    \"excludes\" : [ ]\n" +
      "  }\n" +
      "}]"
    });
    testBuilder().sqlQuery(sqlQuery).unOrdered().baselineColumns("state", "city", "review_count")
            .baselineValues("CA", "San Diego", 33)
            .go();
  }

  @Test
  public final void testJsonComparison() throws Exception {
    compareJson("{\"a\":5}", "{ \"a\" : 5}");
    compareJson("{\"a\":[5]}", "{ \"a\" : [5]}");
    compareJson("{\"a\":[[5]]}", "{ \"a\" : [[5]]}");
    compareJson("{\"a\":[{\"key\" : [5]}]}", "{ \"a\" : [{\"key\" : [5]}]}");

  }

  @Test
  public final void testJsonComparisonFailure() throws Exception {
    Assume.assumeFalse(ElasticsearchCluster.USE_EXTERNAL_ES5);
    compareJsonExpectFailure("{\"a\":15}", "{ \"a\" : 5}");
    compareJsonExpectFailure("{\"a\":[5]}", "{ \"a\" : [5, 7]}");
    compareJsonExpectFailure("{\"a\":[[5]]}", "{ \"a\" : [[5], 7]}");
    compareJsonExpectFailure("{\"a\":[{\"key\" : [5]}]}", "{ \"a\" : [{\"key\" : [5, {\"sub_key\" : 12345}]}]}");
  }

  // helper for negative json comparison tests
  protected static void compareJsonExpectFailure(String expected, String actual) throws IOException {
    boolean unexpectedSuccess = false;
    try {
      compareJson(expected, actual);
      unexpectedSuccess = true;
    } catch (Throwable ex) {
      if (!ex.getMessage().contains("Comparison between JSON values failed")) {
        throw new RuntimeException("Negative test for json comparison failed with unexpected exception.");
      }
    }
    if (unexpectedSuccess) {
      throw new RuntimeException("Negative test for json comparison, did not throw expected exception.");
    }
  }
}
