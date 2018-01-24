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

import java.util.Random;

import org.junit.Ignore;
import org.junit.Test;

/**
 * Tests for validating that pushdown rules are fired correctly.
 */

public class TestPredicatePushdown extends ElasticPredicatePushdownBase {
  @Test
  public void testPredicate_StringEquality() throws Exception {
    validate("string_field = 'abc'");
  }

  @Test
  public void testPredicate_StringNotEquals() throws Exception {
    validate("string_field <> 'abc'");
  }

  @Test
  public void testPredicate_IntegerEquality() throws Exception {
    validate("integer_field = 5");
  }

  @Test
  public void testPredicate_IntegerNotEquals() throws Exception {
    validate("integer_field <> 5");
  }

  @Test
  public void testPredicate_IntegerGreaterThan() throws Exception {
    validate("integer_field > 1000");
  }

  @Test
  public void testPredicate_IntegerLessThan() throws Exception {
    validate("integer_field < 543");
  }

  @Test
  public void testPredicate_RandomComplex() throws Exception {
    for (int i = 0; i < 10; i++) {
      Random random = new Random();
      int n = 1 + random.nextInt(7 - 1);
      validate(predicate(n));
    }
  }

  @Test
  public void testPartiallyPushedDownComplexConjunction() throws Exception {
    // AND can be partially pushed to index
    // only the first part of the expression is pushed down to the index since the last part contains a non pushable operator (is not null)
    // the entire expression is pushed in a script for completeness
    String predicate =
      "string_field = 'string_value_0' and (0 = integer_field" // pushed
        + " and "
        + "(7701633953967831279 <> long_field or " + NOT_PUSHABLE_TO_QUERY_ONLY_SCRIPTS + " ))";
    String sql = "select integer_field from elasticsearch." + schema + "." + table + " where " + predicate;

    assertPushDownContains(
      sql,
      "{\n" +
      "  \"from\" : 0,\n" +
      "  \"size\" : 4000,\n" +
      "  \"query\" : {\n" +
      "    \"bool\" : {\n" +
      "      \"must\" : [ {\n" +
      "        \"bool\" : {\n" +
      "          \"must\" : [ {\n" +
      "            \"match\" : {\n" +
      "              \"string_field\" : {\n" +
      "                \"query\" : \"string_value_0\",\n" +
      "                \"type\" : \"boolean\"\n" +
      "              }\n" +
      "            }\n" +
      "          }, {\n" +
      "            \"match\" : {\n" +
      "              \"integer_field\" : {\n" +
      "                \"query\" : 0,\n" +
      "                \"type\" : \"boolean\"\n" +
      "              }\n" +
      "            }\n" +
      "          } ]\n" +
      "        }\n" +
      "      }, {\n" +
      "        \"script\" : {\n" +
      "          \"script\" : {\n" +
      "            \"inline\" : \"(_source.string_field == null || doc[\\\"integer_field\\\"].empty) ? null : ( ( _source.string_field == 'string_value_0' ) && ( 0 == doc[\\\"integer_field\\\"].value ) && ((!doc[\\\"long_field\\\"].empty) && ( 7701633953967831279L != doc[\\\"long_field\\\"].value )) || ((!doc[\\\"float_field\\\"].empty) && (!doc[\\\"integer_field\\\"].empty) && ( doc[\\\"float_field\\\"].value == ((float)(doc[\\\"integer_field\\\"].value)) )) )\",\n" +
      "            \"lang\" : \"groovy\"\n" +
      "          }\n" +
      "        }\n" +
      "      } ]\n" +
      "    }\n" +
      "  }\n" +
      "}"
    );

    testBuilder().sqlQuery(sql).unOrdered()
      .baselineColumns("integer_field")
      .baselineValues(0)
      .go();
  }

  @Ignore("DX-5289")
  @Test
  public void testPartiallyPushedDownComplexConjunctionShouldNotUseScript() throws Exception {
    // AND can be partially pushed to index
    // only the first part of the expression is pushed down to the index since the last part contains a non pushable operator (is not null)
    // the entire expression is pushed in a script for completeness
    String predicate =
        "string_field = 'string_value_0' and (0 = integer_field" // pushed
        + " and "
        + "(7701633953967831279 <> long_field or " + NOT_PUSHABLE_TO_QUERY_ONLY_SCRIPTS + " ))";
    String sql = "select string_field, integer_field from elasticsearch." + schema + "." + table + " where " + predicate;

    assertPushDownContains(
        sql,
        "{\n" +
        "  \"query\" : {\n" +
        "    \"bool\" : {\n" +
        "      \"must\" : [ {\n" +
        "        \"bool\" : {\n" +
        "          \"must\" : [ {\n" +
        "            \"match\" : {\n" +
        "              \"string_field\" : {\n" +
        "                \"query\" : \"string_value_0\",\n" +
        "                \"type\" : \"boolean\"\n" +
        "              }\n" +
        "            }\n" +
        "          }, {\n" +
        "            \"match\" : {\n" +
        "              \"integer_field\" : {\n" +
        "                \"query\" : 0,\n" +
        "                \"type\" : \"boolean\"\n" +
        "              }\n" +
        "            }\n" +
        "          } ]\n" +
        "        }\n" +
        "      }, {\n" +
        "        \"script\" : {\n" +
        "          \"script\" : {\n" +
        "             \"inline\" : \"(_source.string_field == null || _source.integer_field == null || _source.long_field == null || _source.float_field == null) ? null : ( ( _source.string_field == 'string_value_0' ) && ( 0 == ((Integer)_source.integer_field) ) && ( ( 7701633953967831279 != ((Long)_source.long_field) ) || ( ((Float)_source.float_field) == ((Float) (((Integer)_source.integer_field))) ) ) )\"\n" +
        "          }\n" +
        "        }\n" +
        "      } ]\n" +
        "    }\n" +
        "  },\n" +
        "  \"_source\" : {\n" +
        "    \"includes\" : [ \"string_field\", \"integer_field\" ],\n" +
        "    \"excludes\" : [ ]\n" +
        "  }\n" +
        "}\n"
        );

    testBuilder().sqlQuery(sql).unOrdered()
    .baselineColumns(
        "string_field", "integer_field")
    .baselineValues(
        "string_value_0", 0)
    .go();
  }

  @Test
  public void testPartiallyPushedDownComplexConjunctionDifferentOrder() throws Exception {
    // same as previous test but changing the order to test for corner cases
    String predicate = "(7701633953967831279 <> long_field or " + NOT_PUSHABLE_TO_QUERY_ONLY_SCRIPTS + " ) and (string_field = 'string_value_0' and 0 = integer_field )";
    String sql = "select integer_field from elasticsearch." + schema + "." + table + " where " + predicate;

    assertPushDownContains(
      sql,
      "{\n" +
      "  \"from\" : 0,\n" +
      "  \"size\" : 4000,\n" +
      "  \"query\" : {\n" +
      "    \"bool\" : {\n" +
      "      \"must\" : [ {\n" +
      "        \"bool\" : {\n" +
      "          \"must\" : [ {\n" +
      "            \"match\" : {\n" +
      "              \"string_field\" : {\n" +
      "                \"query\" : \"string_value_0\",\n" +
      "                \"type\" : \"boolean\"\n" +
      "              }\n" +
      "            }\n" +
      "          }, {\n" +
      "            \"match\" : {\n" +
      "              \"integer_field\" : {\n" +
      "                \"query\" : 0,\n" +
      "                \"type\" : \"boolean\"\n" +
      "              }\n" +
      "            }\n" +
      "          } ]\n" +
      "        }\n" +
      "      }, {\n" +
      "        \"script\" : {\n" +
      "          \"script\" : {\n" +
      "            \"inline\" : \"(_source.string_field == null || doc[\\\"integer_field\\\"].empty) ? null : ( ((!doc[\\\"long_field\\\"].empty) && ( 7701633953967831279L != doc[\\\"long_field\\\"].value )) || ((!doc[\\\"float_field\\\"].empty) && (!doc[\\\"integer_field\\\"].empty) && ( doc[\\\"float_field\\\"].value == ((float)(doc[\\\"integer_field\\\"].value)) )) && ( _source.string_field == 'string_value_0' ) && ( 0 == doc[\\\"integer_field\\\"].value ) )\",\n" +
      "            \"lang\" : \"groovy\"\n" +
      "          }\n" +
      "        }\n" +
      "      } ]\n" +
      "    }\n" +
      "  }\n" +
      "}"
    );

    testBuilder().sqlQuery(sql).unOrdered()
      .baselineColumns("integer_field")
      .baselineValues(0)
      .go();

  }

  @Ignore("DX-5289")
  @Test
  public void testPartiallyPushedDownComplexConjunctionDifferentOrderShouldNotUseScripts() throws Exception {
    // same as previous test but changing the order to test for corner cases
    String predicate = "(7701633953967831279 <> long_field or " + NOT_PUSHABLE_TO_QUERY_ONLY_SCRIPTS + " ) and (string_field = 'string_value_0' and 0 = integer_field )";
    String sql = "select string_field, integer_field from elasticsearch." + schema + "." + table + " where " + predicate;

    assertPushDownContains(
        sql,
        "{\n" +
        "  \"from\" : 0,\n" +
        "  \"size\" : 4000,\n" +
        "  \"query\" : {\n" +
        "    \"bool\" : {\n" +
        "      \"must\" : [ {\n" +
        "        \"bool\" : {\n" +
        "          \"must\" : [ {\n" +
        "            \"match\" : {\n" +
        "              \"string_field\" : {\n" +
        "                \"query\" : \"string_value_0\",\n" +
        "                \"type\" : \"boolean\"\n" +
        "              }\n" +
        "            }\n" +
        "          }, {\n" +
        "            \"match\" : {\n" +
        "              \"integer_field\" : {\n" +
        "                \"query\" : 0,\n" +
        "                \"type\" : \"boolean\"\n" +
        "              }\n" +
        "            }\n" +
        "          } ]\n" +
        "        }\n" +
        "      }, {\n" +
        "        \"script\" : {\n" +
        "          \"script\" : {\n" +
        "            \"inline\" : \"(doc[\\\"string_field\\\"] == null || doc[\\\"integer_field\\\"] == null || doc[\\\"long_field\\\"] == null || doc[\\\"float_field\\\"] == null) ? null : ( ( doc[\\\"string_field\\\"].value == 'string_value_0' ) && ( 0 == doc[\\\"integer_field\\\"].value ) && ( ( 7701633953967831279 != doc[\\\"long_field\\\"].value ) || ( doc[\\\"float_field\\\"].value == ((float)(doc[\\\"integer_field\\\"].value)) ) ) )\"\n" +
        "          }\n" +
        "        }\n" +
        "      } ]\n" +
        "    }\n" +
        "  }\n" +
        "}\n"
        );

    testBuilder().sqlQuery(sql).unOrdered()
    .baselineColumns(
        "string_field", "integer_field")
    .baselineValues(
        "string_value_0", 0)
    .go();

  }

  /**
   * The part of the filter that is not pushed down to the index [1] will filter out the matching row
   * testing correctness of the logic
   *
   * [1] (7701633953967831279 = long_field or float_field is null)
   *
   * @throws Exception
   */
  @Test
  public void testPartiallyPushedDownComplexConjunctionNoResult() throws Exception {
    // checking the correctness of the push down.
    // the script filter filters more than the index
    String predicate = "(7701633953967831279 = long_field or " + NOT_PUSHABLE_TO_QUERY_ONLY_SCRIPTS + " ) and (string_field = 'string_value_0' and 0 = integer_field )";
    String sql = "select integer_field from elasticsearch." + schema + "." + table + " where " + predicate;

    assertPushDownContains(
      sql,
      "{\n" +
      "  \"from\" : 0,\n" +
      "  \"size\" : 4000,\n" +
      "  \"query\" : {\n" +
      "    \"bool\" : {\n" +
      "      \"must\" : [ {\n" +
      "        \"bool\" : {\n" +
      "          \"must\" : [ {\n" +
      "            \"match\" : {\n" +
      "              \"string_field\" : {\n" +
      "                \"query\" : \"string_value_0\",\n" +
      "                \"type\" : \"boolean\"\n" +
      "              }\n" +
      "            }\n" +
      "          }, {\n" +
      "            \"match\" : {\n" +
      "              \"integer_field\" : {\n" +
      "                \"query\" : 0,\n" +
      "                \"type\" : \"boolean\"\n" +
      "              }\n" +
      "            }\n" +
      "          } ]\n" +
      "        }\n" +
      "      }, {\n" +
      "        \"script\" : {\n" +
      "          \"script\" : {\n" +
      "            \"inline\" : \"(_source.string_field == null || doc[\\\"integer_field\\\"].empty) ? null : ( ((!doc[\\\"long_field\\\"].empty) && ( 7701633953967831279L == doc[\\\"long_field\\\"].value )) || ((!doc[\\\"float_field\\\"].empty) && (!doc[\\\"integer_field\\\"].empty) && ( doc[\\\"float_field\\\"].value == ((float)(doc[\\\"integer_field\\\"].value)) )) && ( _source.string_field == 'string_value_0' ) && ( 0 == doc[\\\"integer_field\\\"].value ) )\",\n" +
      "            \"lang\" : \"groovy\"\n" +
      "          }\n" +
      "        }\n" +
      "      } ]\n" +
      "    }\n" +
      "  }\n" +
      "}"
    );
    testNoResult(sql);
  }

  @Ignore("DX-5289")
  @Test
  public void testPartiallyPushedDownComplexConjunctionNoResultShouldNotUseScripts() throws Exception {
    // checking the correctness of the push down.
    // the script filter filters more than the index
    String predicate = "(7701633953967831279 = long_field or " + NOT_PUSHABLE_TO_QUERY_ONLY_SCRIPTS + " ) and (string_field = 'string_value_0' and 0 = integer_field )";
    String sql = "select string_field, integer_field from elasticsearch." + schema + "." + table + " where " + predicate;

    assertPushDownContains(
        sql,
        "{\n" +
        "  \"query\" : {\n" +
        "    \"bool\" : {\n" +
        "      \"must\" : [ {\n" +
        "        \"bool\" : {\n" +
        "          \"must\" : [ {\n" +
        "            \"match\" : {\n" +
        "              \"string_field\" : {\n" +
        "                \"query\" : \"string_value_0\",\n" +
        "                \"type\" : \"boolean\"\n" +
        "              }\n" +
        "            }\n" +
        "          }, {\n" +
        "            \"match\" : {\n" +
        "              \"integer_field\" : {\n" +
        "                \"query\" : 0,\n" +
        "                \"type\" : \"boolean\"\n" +
        "              }\n" +
        "            }\n" +
        "          } ]\n" +
        "        }\n" +
        "      }, {\n" +
        "        \"script\" : {\n" +
        "          \"script\" : {\n" +
        "            \"inline\" : \"(_source.long_field == null || _source.float_field == null || _source.integer_field == null || _source.string_field == null) ? null : ( ( ( 7701633953967831279 == ((Long)_source.long_field) ) || ( ((Float)_source.float_field) == ((Float) (((Integer)_source.integer_field))) ) ) && ( _source.string_field == 'string_value_0' ) && ( 0 == ((Integer)_source.integer_field) ) )\"\n" +
        "          }\n" +
        "        }\n" +
        "      } ]\n" +
        "    }\n" +
        "  },\n" +
        "  \"_source\" : {\n" +
        "    \"includes\" : [ \"integer_field\" ],\n" +
        "    \"excludes\" : [ ]\n" +
        "  }\n" +
        "}\n"
        );
    testNoResult(sql);
  }

  @Test
  public void testPartiallyPushedDisjunction() throws Exception {
    // checking OR is treated differently from AND
    // OR cannot be partially pushed
    String predicate = "string_field = 'string_value_0' or " + NOT_PUSHABLE_TO_QUERY_ONLY_SCRIPTS;
    String sql = "select string_field, float_field from elasticsearch." + schema + "." + table + " where " + predicate;

    assertPushDownContains(
        sql,
        "{\n" +
        "  \"from\" : 0,\n" +
        "  \"size\" : 4000,\n" +
        "  \"query\" : {\n" +
        "    \"script\" : {\n" +
        "      \"script\" : {\n" +
        "        \"inline\" : \"((_source.string_field != null) && ( _source.string_field == 'string_value_0' )) || ((!doc[\\\"float_field\\\"].empty) && (!doc[\\\"integer_field\\\"].empty) && ( doc[\\\"float_field\\\"].value == ((float)(doc[\\\"integer_field\\\"].value)) ))\",\n" +
        "        \"lang\" : \"groovy\"\n" +
        "      }\n" +
        "    }\n" +
        "  }\n" +
        "}"
        );

    testBuilder().sqlQuery(sql).unOrdered()
    .baselineColumns(
        "float_field",
        "string_field")
    .baselineValues(
        0.0f,
        "string_value_0")
    .go();

  }

  @Test
  public void testPartiallyPushedConjunction() throws Exception {
    // simple case of partial push down
    String predicate = "string_field = 'string_value_0' and " + NOT_PUSHABLE_TO_QUERY_ONLY_SCRIPTS;
    String sql = "select float_field from elasticsearch." + schema + "." + table + " where " + predicate;

    assertPushDownContains(
      sql,
      "{\n" +
      "  \"from\" : 0,\n" +
      "  \"size\" : 4000,\n" +
      "  \"query\" : {\n" +
      "    \"bool\" : {\n" +
      "      \"must\" : [ {\n" +
      "        \"bool\" : {\n" +
      "          \"must\" : {\n" +
      "            \"match\" : {\n" +
      "              \"string_field\" : {\n" +
      "                \"query\" : \"string_value_0\",\n" +
      "                \"type\" : \"boolean\"\n" +
      "              }\n" +
      "            }\n" +
      "          }\n" +
      "        }\n" +
      "      }, {\n" +
      "        \"script\" : {\n" +
      "          \"script\" : {\n" +
      "            \"inline\" : \"(_source.string_field == null || doc[\\\"float_field\\\"].empty || doc[\\\"integer_field\\\"].empty) ? null : ( ( _source.string_field == 'string_value_0' ) && ( doc[\\\"float_field\\\"].value == ((float)(doc[\\\"integer_field\\\"].value)) ) )\",\n" +
      "            \"lang\" : \"groovy\"\n" +
      "          }\n" +
      "        }\n" +
      "      } ]\n" +
      "    }\n" +
      "  }\n" +
      "}");

    testBuilder().sqlQuery(sql).unOrdered()
      .baselineColumns("float_field")
      .baselineValues(0.0f)
      .go();

  }

  @Ignore("DX-5289")
  @Test
  public void testPartiallyPushedConjunctionShouldNotScript() throws Exception {
    // simple case of partial push down
    String predicate = "string_field = 'string_value_0' and " + NOT_PUSHABLE_TO_QUERY_ONLY_SCRIPTS;
    String sql = "select string_field, float_field from elasticsearch." + schema + "." + table + " where " + predicate;

    assertPushDownContains(
        sql,
        "{\n" +
        "  \"query\" : {\n" +
        "    \"bool\" : {\n" +
        "      \"must\" : [ {\n" +
        "        \"bool\" : {\n" +
        "          \"must\" : {\n" + // this is
        "            \"match\" : {\n" +
        "              \"string_field\" : {\n" +
        "                \"query\" : \"string_value_0\",\n" +
        "                \"type\" : \"boolean\"\n" +
        "              }\n" +
        "            }\n" +
        "          }\n" +
        "        }\n" +
        "      }, {\n" +
        "        \"script\" : {\n" +
        "          \"script\" : {\n" +
        "            \"inline\" : \"(_source.string_field == null || _source.float_field == null || _source.integer_field == null) ? null : ( ( _source.string_field == 'string_value_0' ) && ( ((Float)_source.float_field) == ((Float) (((Integer)_source.integer_field))) ) )\"\n" +
        "          }\n" +
        "        }\n" +
        "      } ]\n" +
        "    }\n" +
        "  },\n" +
        "  \"_source\" : {\n" +
        "    \"includes\" : [ \"string_field\", \"float_field\" ],\n" +
        "    \"excludes\" : [ ]\n" +
        "  }\n" +
        "}\n");

    testBuilder().sqlQuery(sql).unOrdered()
    .baselineColumns(
        "float_field",
        "string_field")
    .baselineValues(
            0.0f,
            "string_value_0")
    .go();

  }

  @Test
  public void testPredicate_Exists() throws Exception {
    // simple case of partial push down
    String predicate = "string_field is not null" ;
    String sql = "select string_field, float_field from elasticsearch." + schema + "." + table + " where " + predicate;

    assertPushDownContains(
        sql,
        "{\n" +
        "  \"from\" : 0,\n" +
        "  \"size\" : 4000,\n" +
        "  \"query\" : {\n" +
        "    \"exists\" : {\n" +
        "      \"field\" : \"string_field\"\n" +
        "    }\n" +
        "  }\n" +
        "}");

    testBuilder().sqlQuery(sql).unOrdered()
    .baselineColumns(
        "float_field",
        "string_field")
    .baselineValues(
            0.0f,
            "string_value_0")
    .go();
  }

  @Test
  public void testPredicate_NotExists() throws Exception {
    // simple case of partial push down
    String predicate = "string_field is null" ;
    String sql = "select float_field from elasticsearch." + schema + "." + table + " where " + predicate;

    assertPushDownContains(
        sql,
        "{\n" +
        "  \"from\" : 0,\n" +
        "  \"size\" : 4000,\n" +
        "  \"query\" : {\n" +
        "    \"bool\" : {\n" +
        "      \"must_not\" : {\n" +
        "        \"exists\" : {\n" +
        "          \"field\" : \"string_field\"\n" +
        "        }\n" +
        "      }\n" +
        "    }\n" +
        "  }\n" +
        "}");

    testNoResult(sql);
  }

  @Test
  public void testNotLike() throws Exception {
    ElasticsearchCluster.ColumnData[] data = getBusinessData();

    elastic.load(schema, table, data);

    String sql = String.format("select city from elasticsearch.%s.%s where city not like '%%Cambridge%%' ", schema, table);

    test("explain plan for " + sql);

    assertPushDownContains(sql,
      "{\n" +
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
        "            \"value\" : \".*Cambridge.*\",\n" +
        "            \"flags_value\" : 65535\n" +
        "          }\n" +
        "        }\n" +
        "      }\n" +
        "    }\n" +
        "  }\n" +
        "}"
    );

    testBuilder()
      .sqlQuery(sql)
      .unOrdered()
      .baselineColumns("city")
      .baselineValues("San Francisco")
      .baselineValues("San Francisco")
      .baselineValues("San Diego")
      .go();
  }

  @Test
  public void testBooleanTypeInCompoundExpression() throws Exception {
    ElasticsearchCluster.ColumnData[] data = getBusinessData();

    elastic.load(schema, table, data);

    String sql = String.format("select * from elasticsearch.%s.%s where `open` = 1 and city like '%%Oakland%%' ", schema, table);

    assertPushDownContains(sql, "{\n" +
      "  \"from\" : 0,\n" +
      "  \"size\" : 4000,\n" +
      "  \"query\" : {\n" +
      "    \"bool\" : {\n" +
      "      \"must\" : [ {\n" +
      "        \"match\" : {\n" +
      "          \"open\" : {\n" +
      "            \"query\" : true,\n" +
      "            \"type\" : \"boolean\"\n" +
      "          }\n" +
      "        }\n" +
      "      }, {\n" +
      "        \"regexp\" : {\n" +
      "          \"city\" : {\n" +
      "            \"value\" : \".*Oakland.*\",\n" +
      "            \"flags_value\" : 65535\n" +
      "          }\n" +
      "        }\n" +
      "      } ]\n" +
      "    }\n" +
      "  }\n" +
      "}");
  }

  @Ignore("DX-5289")
  @Test
  public void testPredicate_NotExistsShouldNotUseScript() throws Exception {
    // simple case of partial push down
    String predicate = "string_field is null" ;
    String sql = "select string_field, float_field from elasticsearch." + schema + "." + table + " where " + predicate;

    assertPushDownContains(
      sql,
      "{\n" +
        "  \"query\" : {\n" +
        "    \"bool\" : {\n" +
        "      \"must_not\" : {\n" +
        "        \"exists\" : {\n" +
        "          \"field\" : \"string_field\"\n" +
        "        }\n" +
        "      }\n" +
        "    }\n" +
        "  },\n" +
        "  \"_source\" : {\n" +
        "    \"includes\" : [ \"string_field\", \"float_field\" ],\n" +
        "    \"excludes\" : [ ]\n" +
        "  }\n" +
        "}\n");

    testNoResult(sql);
  }
  @Test
  public void testComplexConjunctions() throws Exception {
    String s = "((0.7008325824013935 > double_field AND false < boolean_field) AND 0.9144399 = float_field)";
    validate(s);
  }

  @Test
  public void testComplexConjunctionAndDisjunction() throws Exception {
    String s = "((0.7008325824013935 > double_field AND false < boolean_field) OR 0.9144399 = float_field)";
    validate(s);
  }

  @Test
  public void testPredicate_NestedField_StringEquality() throws Exception {
    validate("person['first_name'] = 'mathilda'");
  }

  @Test
  public void testPredicate_NestedField_StringNotEquals() throws Exception {
    validate("person['first_name'] <> 'mathilda'");
  }

  @Test
  public void testPredicate_NestedAndTopLevelFields() throws Exception {
    validate("person['ssn'] > 1234 AND integer_field = 5");
  }

  @Test
  public void testPredicate_NestedGreaterThan() throws Exception {
    validate("person['ssn'] > 1234");
  }

  @Test
  public void testPredicate_NestedLessThan() throws Exception {
    validate("person['ssn'] > 1234");
  }

}
