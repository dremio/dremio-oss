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
package com.dremio.plugins.elastic;

import static com.dremio.TestBuilder.listOf;
import static com.dremio.TestBuilder.mapOf;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.ParseException;

import org.apache.arrow.vector.util.JsonStringHashMap;
import org.joda.time.LocalDateTime;
import org.junit.Before;
import org.junit.Test;

import com.dremio.plugins.elastic.ElasticsearchCluster.ColumnData;

public class TestNullValues extends ElasticBaseTestQuery {

  @Before
  public void loadTable() throws IOException, ParseException {
    ColumnData[] data = getNullBusinessData();
    elastic.load(schema, table, data);
  }

  @Test
  public final void runIsNullProjectFilter() throws Exception {
    String sqlQuery = "select stars, state, stars IS NULL from elasticsearch." + schema + "." + table + " where state IS NOT NULL";
    verifyJsonInPlan(sqlQuery, new String[] {
        "[{\n" +
        "  \"from\" : 0,\n" +
        "  \"size\" : 4000,\n" +
        "  \"query\" : {\n" +
        "    \"exists\" : {\n" +
        "      \"field\" : \"state\"\n" +
        "    }\n" +
        "  },\n" +
        "  \"_source\" : {\n" +
        "    \"includes\" : [ \"stars\", \"state\" ],\n" +
        "    \"excludes\" : [ ]\n" +
        "  }\n" +
        "}]"});
    testBuilder().sqlQuery(sqlQuery).unOrdered().baselineColumns("stars", "state", "EXPR$2")
        .baselineValues(null, "MA", true)
        .baselineValues(3.5f, "CA", false)
        .baselineValues(5.0f, "CA", false)
        .baselineValues(1f, "CA", false)
        .go();
  }

  @Test
  public final void runTestArrayWithNulls() throws Exception {
    // We cannot push down complex fields
    String sqlQuery = "select t.location_field[1].lat IS NULL from elasticsearch." + schema + "." + table + " t";
    verifyJsonInPlan(sqlQuery, new String[]{
        "[{\n" +
        "  \"from\" : 0,\n" +
        "  \"size\" : 4000,\n" +
        "  \"query\" : {\n" +
        "    \"match_all\" : { }\n" +
        "  },\n" +
        "  \"_source\" : {\n" +
        "    \"includes\" : [ \"location_field\" ],\n" +
        "    \"excludes\" : [ ]\n" +
        "  }\n" +
        "}]"
    });
    testBuilder()
        .sqlQuery(sqlQuery)
        .unOrdered()
        .baselineColumns("EXPR$0")
        .baselineValues(false)
        .baselineValues(false)
        .baselineValues(true)
        .baselineValues(false)
        .baselineValues(false)
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
        .baselineValues(null, null, null, null, null, null)
        .baselineValues(2014L, 2L, 12L, 10L, 50L, 42L)
        .baselineValues(2014L, 2L, 11L, 10L, 50L, 42L)
        .baselineValues(2014L, 2L, 10L, 10L, 50L, 42L)
        .go();
  }

  @Test
  public final void runTestFilter() throws Exception {
    // TODO: this could be improved. Since we don't need stars, we could remove that from the includes list. Since Edge projects are typically, off, probably not worth the effort.
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
            .baselineValues("CA", null, 33)
            .baselineValues(null, "Cambridge", 11)
            .go();
  }

  @Test
  public final void runTestFieldsEquality() throws Exception {
    String sqlQuery = "select state, city, review_count, stars from elasticsearch." + schema + "." + table + " where review_count = cast((stars * 33 / stars) as int)";
    verifyJsonInPlan(sqlQuery,
            new String[] {
              "[{\n" +
              "  \"from\" : 0,\n" +
              "  \"size\" : 4000,\n" +
              "  \"query\" : {\n" +
              "    \"script\" : {\n" +
              "      \"script\" : {\n" +
              "        \"inline\" : \"(doc[\\\"review_count\\\"].empty || doc[\\\"stars\\\"].empty) ? null : ( doc[\\\"review_count\\\"].value == (int)(( ( doc[\\\"stars\\\"].value * 33 ) / doc[\\\"stars\\\"].value )).doubleValue() )\",\n" +
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
            .baselineValues("CA", null, 33, 5.0F)
            .go();
  }

  @Test
  public final void runTestArrayAccess() throws Exception {
    // DX-3820: Should be able to pushdown MIN() as well, but for now just column selection.
    // We cannot push down complex fields right now.
    String sqlQuery = "select min(t.location_field[1].lat) as lat_1 from elasticsearch." + schema + "." + table + " t";
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
            "}]"
    });
    testBuilder()
            .sqlQuery(sqlQuery)
            .unOrdered()
            .baselineColumns("lat_1")
            .baselineValues(-55D)
            .go();
  }

  @Test
  public final void runTestArrayAccessWithoutAggregate() throws Exception {
    // We cannot push down complex fields
    String sqlQuery = "select t.location_field[1].lat as lat_1 from elasticsearch." + schema + "." + table + " t";
    verifyJsonInPlan(sqlQuery, new String[]{
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
            "}]"
    });
    testBuilder()
            .sqlQuery(sqlQuery)
            .unOrdered()
            .baselineColumns("lat_1")
            .baselineValues(-11D)
            .baselineValues(-22D)
            .baselineValues(new Object[] { null })
            .baselineValues(-44D)
            .baselineValues(-55D)
            .go();
  }

  @Test
  public final void runTestComplexArrayAccessWithoutAggregate() throws Exception {
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
            "}]"
    });
    testBuilder()
            .sqlQuery(sqlQuery)
            .unOrdered()
            .baselineColumns("location_1")
            .baselineValues(mapOf("lat", -11D, "lon", -11D))
            .baselineValues(mapOf("lat", -22D, "lon", -22D))
            .baselineValues((JsonStringHashMap<String, Object>)null)
            .baselineValues(mapOf("lat", -44D, "lon", -44D))
            .baselineValues(mapOf("lat", -55D, "lon", -55D))
            .go();
  }

  @Test
  public final void runTestComplexProject() throws Exception {
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
            "}]"
    });
    testBuilder().sqlQuery(sqlQuery).unOrdered().baselineColumns("plusOne")
            .baselineValues(new Object[] { null })
            .baselineValues(4.5f)
            .baselineValues(6.0f)
            .baselineValues(5.5f)
            .baselineValues(2.0f)
            .go();
  }

  @Test
  public final void runComplexConditional() throws Exception {

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

    // TODO DX-4901 - this pushdown is currently incorrect, the null handling with case (and other functions that are not
    // NULL_IF_NULL) needs to be fixed. For now this is avoiding running the second phase of the test with project pushdown
    // disabled. To re-enable, just use the testBuilder() helper method rather than calling this constructor directly
    testBuilder()
      .sqlQuery(sqlQuery)
      .unOrdered()
      .baselineColumns("out_col")
      .baselineValues(0)
      .baselineValues(1)
      .baselineValues(1)
      .baselineValues(1)
      .baselineValues(0)
      .go();
  }

  @Test
  public final void testFilterWithCase() throws Exception {

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
            .baselineValues(new Object[] {null})
            .baselineValues(3.5)
            .baselineValues(5.0)
            .baselineValues(4.5)
            .baselineValues(1.0)
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
            .baselineValues(new Object[] {null})
            .baselineValues(new LocalDateTime(Timestamp.valueOf("2014-02-12 10:50:42")))
            .baselineValues(new LocalDateTime(Timestamp.valueOf("2014-02-11 10:50:42")))
            .baselineValues(new LocalDateTime(Timestamp.valueOf("2014-02-10 10:50:42")))
            .go();
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
            "}])"
    });
    testBuilder().sqlQuery(sqlQuery).ordered().baselineColumns("location_field", "review_count")
            .baselineValues(listOf(mapOf("lat", 11D, "lon", 11D), mapOf("lat", -11D, "lon", -11D)), 11)
            .baselineValues(listOf(mapOf("lat", 22D, "lon", 22D), mapOf("lat", -22D, "lon", -22D)), 22)
            .baselineValues(null, 33)
            .baselineValues(listOf(mapOf("lat", 44D, "lon", 44D), mapOf("lat", -44D, "lon", -44D)), 11)
            .baselineValues(listOf(mapOf("lat", 55D, "lon", 55D), mapOf("lat", -55D, "lon", -55D)), null)
            .go();
  }

  @Test
  public final void runTestWhereClauseWithNested() throws Exception {
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
            "}])"
    });
    testBuilder()
            .sqlQuery(sqlQuery)
            .unOrdered()
            .baselineColumns("open", "lat_1")
            .baselineValues(true, -44D)
            .baselineValues(null, -55D)
            .go();
  }

}
