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

import static com.dremio.plugins.elastic.ElasticsearchCluster.PRIMITIVE_TYPES;
import static java.lang.String.format;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;
import java.util.Random;

import org.apache.calcite.sql.SqlNode;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.exec.ExecTest;
import com.dremio.exec.PassthroughQueryObserver;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.physical.PhysicalPlan;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.planner.sql.SqlConverter;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.query.NormalHandler;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.proto.UserProtos;
import com.dremio.plugins.elastic.planning.ElasticsearchGroupScan;
import com.dremio.sabot.rpc.user.UserSession;

/**
 * Tests for validating that pushdown rules are fired correctly.
 */

public class TestPredicatePushdown extends ElasticBaseTestQuery {

  private static final Logger logger = LoggerFactory.getLogger(TestPredicatePushdown.class);
  private final Random random = new Random();

  private static QueryContext context;

  private static final String NOT_PUSHABLE_TO_QUERY_ONLY_SCRIPTS = "float_field = integer_field";

  @BeforeClass
  public static void beforeClass() {
    context = new QueryContext(session(), getSabotContext(), QueryId.getDefaultInstance());
  }

  @Override
  @Before
  public void before() throws Exception {
    super.before();
    elastic.populate(schema, table, 1);
  }

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

  /**
   * @param sql the SQL to plan for
   * @param fragment ES query to look for in push down
   * @throws Exception
   */
  private void assertPushDownContains(String sql, String fragment) throws Exception {
    ElasticsearchGroupScan scan = generate(sql);
    compareJson(fragment, scan.getScanSpec().getQuery());
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

  // can not produce self-contradicting expression as they would be optimized out
  // WHERE a = 0 and a = 1 is turned into LIMIT 0
  // WHERE a = 0 and a = 0 is turned into WHERE a = 0
  private String predicate(int n) {
    // to simplify we use each column only once
    n = Math.min(n, PRIMITIVE_TYPES.size() - 1);
    String[] predicates = new String[n];
    for (int i = 0; i < n; i++) {
      predicates[i] = primitiveTypePredicate(i);
    }

    String buffer = predicates[0];
    for (int i = 1; i < predicates.length; i++) {
      buffer = combine(buffer, predicates[i]);
    }

    return buffer;
  }

  private String combine(String predicate1, String predicate2) {
    return "(" + predicate1 + " " + binary[random.nextInt(1)] + " " + predicate2 + ")";
  }

  private String primitiveTypePredicate(int typeID) {
    ElasticsearchType[] array = new ElasticsearchType[PRIMITIVE_TYPES.size()];
    PRIMITIVE_TYPES.toArray(array);
    ElasticsearchType type = array[typeID];

    String value = randomValue(type);
    String field = fieldName(type);

    if (type == ElasticsearchType.STRING) {
      value = "'" + value + "'";
    }

    return value + " " + operators[random.nextInt(operators.length)] + " " + field;
  }

  private String[] binary = {"AND", "OR"};
  private String[] operators = {"=", "<>", ">", "<"};

  private String fieldName(ElasticsearchType type) {
    switch (type) {
      case STRING:
        return "string_field";
      case INTEGER:
        return "integer_field";
      case LONG:
        return "long_field";
      case FLOAT:
        return "float_field";
      case DOUBLE:
        return "double_field";
      case BOOLEAN:
        return "boolean_field";
      default:
        fail(format("Unexpected type in predicate test: %s", type));
        return null;
    }
  }

  private String randomValue(ElasticsearchType type) {
    switch (type) {
      case STRING:
        StringBuilder randomAsciiBuilder = new StringBuilder();
        for (int i = 0; i < 11; i++) {
          randomAsciiBuilder.append((char)(random.nextInt(26) + 'a'));
        }
        return randomAsciiBuilder.toString();
      case INTEGER:
        return String.valueOf(random.nextInt());
      case LONG:
        return String.valueOf(random.nextLong());
      case FLOAT:
        return String.valueOf(random.nextFloat());
      case DOUBLE:
        return String.valueOf(random.nextDouble());
      case BOOLEAN:
        return String.valueOf(random.nextBoolean());
      default:
        fail(format("Unexpected type in predicate test: %s", type));
        return null;
    }
  }

  private void validate(String predicate) throws Exception {

    ElasticsearchGroupScan scan = generateScanFromPredicate(predicate);
    assertTrue(scan.getScanSpec().isPushdown());

    String query = scan.getScanSpec().getQuery();
    assertNotNull(query);

    logger.debug("--> Generated query:\n{}", query);
  }

  private ElasticsearchGroupScan generateScanFromPredicate(String predicate) throws Exception {
    logger.debug("--> Testing predicate:\n{}", predicate);
    String sql = "select * from elasticsearch." + schema + "." + table + " where " + predicate;
    return generate(sql);
  }

  private ElasticsearchGroupScan generate(String sql) throws Exception {
    AttemptObserver observer = new PassthroughQueryObserver(ExecTest.mockUserClientConnection(null));
    SqlConverter converter = new SqlConverter(context.getPlannerSettings(), context.getNewDefaultSchema(),
        context.getOperatorTable(), context, context.getMaterializationProvider(), context.getFunctionRegistry(),
        context.getSession(), observer, context.getStorage(), context.getSubstitutionProviderFactory());
    SqlNode node = converter.parse(sql);
    SqlHandlerConfig config = new SqlHandlerConfig(context, converter, observer, null);
    NormalHandler handler = new NormalHandler();
    PhysicalPlan plan = handler.getPlan(config, sql, node);
    List<PhysicalOperator> operators = plan.getSortedOperators();
    ElasticsearchGroupScan scan = find(operators);
    assertNotNull("Physical plan does not contain an elasticsearch scan for query: " + sql, scan);
    return scan;
  }

  public static UserSession session() {
    return UserSession.Builder.newBuilder()
        .withUserProperties(UserProtos.UserProperties.getDefaultInstance())
        .withCredentials(UserBitShared.UserCredentials.newBuilder().setUserName("foo").build())
        .withOptionManager(getSabotContext().getOptionManager())
        .setSupportComplexTypes(true)
        .build();
  }

  private ElasticsearchGroupScan find(List<PhysicalOperator> operators) {
    for (PhysicalOperator operator : operators) {
      if (operator instanceof ElasticsearchGroupScan) {
        return (ElasticsearchGroupScan) operator;
      }
    }
    return null;
  }
}
