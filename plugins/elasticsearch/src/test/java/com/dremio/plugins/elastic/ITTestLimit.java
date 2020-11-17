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

import static com.dremio.TestBuilder.mapOf;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;

import org.junit.Before;
import org.junit.Test;

import com.dremio.common.AutoCloseables;
import com.dremio.common.CloseableByteBuf;
import com.dremio.common.DeferredException;
import com.dremio.common.utils.protos.AttemptId;
import com.dremio.common.utils.protos.ExternalIdHelper;
import com.dremio.common.utils.protos.QueryWritableBatch;
import com.dremio.exec.planner.observer.AbstractAttemptObserver;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.planner.observer.QueryObserver;
import com.dremio.exec.proto.GeneralRPCProtos.Ack;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.proto.UserBitShared.OperatorProfile;
import com.dremio.exec.proto.UserBitShared.QueryProfile;
import com.dremio.exec.proto.UserProtos.RunQuery;
import com.dremio.exec.proto.UserProtos.SubmissionSource;
import com.dremio.exec.rpc.Acks;
import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.exec.work.protector.UserResult;
import com.dremio.exec.work.user.LocalExecutionConfig;
import com.dremio.exec.work.user.SubstitutionSettings;
import com.dremio.plugins.elastic.ElasticsearchCluster.ColumnData;
import com.dremio.proto.model.attempts.AttemptReason;
import com.google.common.base.StandardSystemProperty;
import com.google.common.collect.ImmutableList;

public class ITTestLimit extends ElasticBaseTestQuery {

  @Before
  public void loadTable() throws IOException, ParseException {
    ColumnData[] data = getBusinessData();
    load(schema, table, data);
  }

  String AGG_LIMIT = "="
      + "[{\n" +
      "  \"size\" : 0,\n" +
      "  \"query\" : {\n" +
      "    \"match_all\" : { }\n" +
      "  },\n" +
      "  \"aggregations\" : {\n" +
      "    \"city\" : {\n" +
      "      \"terms\" : {\n" +
      "        \"field\" : \"city\",\n" +
      "        \"missing\" : \"NULL_STRING_TAG\",\n" +
      "        \"size\" : 2147483647\n" +
      "      }\n" +
      "    }\n" +
      "  }\n" +
      "}])";

  @Test
  public final void runTestFilterLimit1() throws Exception {
    String sqlQueryLimit1 = "select state, city_analyzed, review_count from elasticsearch." + schema + "." + table + " where stars >= 4 limit 1";
    verifyJsonInPlanHelper(sqlQueryLimit1, new String[] {
        "[{\n" +
        "  \"from\" : 0,\n" +
        "  \"size\" : 1,\n" +
        "  \"query\" : {\n" +
        "    \"range\" : {\n" +
        "      \"stars\" : {\n" +
        "        \"from\" : 4,\n" +
        "        \"to\" : null,\n" +
        "        \"include_lower\" : true,\n" +
        "        \"include_upper\" : true,\n" +
        "        \"boost\" : 1.0\n" +
        "      }\n" +
        "    }\n" +
        "  },\n" +
        "  \"_source\" : {\n" +
        "    \"includes\" : [\n" +
        "      \"city_analyzed\",\n" +
        "      \"review_count\",\n" +
        "      \"stars\",\n" +
        "      \"state\"\n" +
        "    ],\n" +
        "    \"excludes\" : [ ]\n" +
        "  }\n" +
        "}]" }, true);
    testBuilder().sqlQuery(sqlQueryLimit1).unOrdered()
        .baselineColumns("state", "city_analyzed", "review_count")
        .baselineValues("MA", "Cambridge", 11)
        .go();
  }

  @Test
  public final void runTestFilterLimit100() throws Exception {
    String sqlQueryLimit100 = "select state, city_analyzed, review_count from elasticsearch." + schema + "." + table + " where stars >= 4 limit 100";
    verifyJsonInPlanHelper(sqlQueryLimit100, new String[] {
        "[{\n" +
        "  \"from\" : 0,\n" +
        "  \"size\" : 100,\n" +
        "  \"query\" : {\n" +
        "    \"range\" : {\n" +
        "      \"stars\" : {\n" +
        "        \"from\" : 4,\n" +
        "        \"to\" : null,\n" +
        "        \"include_lower\" : true,\n" +
        "        \"include_upper\" : true,\n" +
        "        \"boost\" : 1.0\n" +
        "      }\n" +
        "    }\n" +
        "  },\n" +
        "  \"_source\" : {\n" +
        "    \"includes\" : [\n" +
        "      \"city_analyzed\",\n" +
        "      \"review_count\",\n" +
        "      \"stars\",\n" +
        "      \"state\"\n" +
        "    ],\n" +
        "    \"excludes\" : [ ]\n" +
        "  }\n" +
        "}]" },
      true);
    testBuilder().sqlQuery(sqlQueryLimit100).unOrdered()
        .baselineColumns("state", "city_analyzed", "review_count")
        .baselineValues("MA", "Cambridge", 11)
        .baselineValues("CA", "San Diego", 33)
        .baselineValues("MA", "Cambridge", 11)
        .go();
  }

  @Test
  public final void runTestFilterFetchOffset() throws Exception {
    String sqlQueryfetch = "select state, city_analyzed, review_count from elasticsearch." + schema + "." + table + " where stars >= 4 offset 1 fetch next 1 row only";
    verifyJsonInPlanHelper(sqlQueryfetch, new String[] {
        "[{\n" +
        "  \"from\" : 0,\n" +
        "  \"size\" : 2,\n" +
        "  \"query\" : {\n" +
        "    \"range\" : {\n" +
        "      \"stars\" : {\n" +
        "        \"from\" : 4,\n" +
        "        \"to\" : null,\n" +
        "        \"include_lower\" : true,\n" +
        "        \"include_upper\" : true,\n" +
        "        \"boost\" : 1.0\n" +
        "      }\n" +
        "    }\n" +
        "  },\n" +
        "  \"_source\" : {\n" +
        "    \"includes\" : [\n" +
        "      \"city_analyzed\",\n" +
        "      \"review_count\",\n" +
        "      \"stars\",\n" +
        "      \"state\"\n" +
        "    ],\n" +
        "    \"excludes\" : [ ]\n" +
        "  }\n" +
        "}]" },
      true);
    testBuilder()
        .sqlQuery(sqlQueryfetch)
        .unOrdered()
        .baselineColumns("state", "city_analyzed", "review_count")
        .baselineValues("CA", "San Diego", 33)
        .go();
  }

  @Test
  public final void runTestWithComplexArrayFetchOffset() throws Exception {
    // We cannot push down complex fields
    String sqlQuery = "select t.location_field[1] as location_1 from elasticsearch." + schema + "." + table + " t offset 2 fetch next 1 row only";
    verifyJsonInPlanHelper(sqlQuery, new String[] { ""
        + "[{\n" +
        "  \"from\" : 0,\n" +
        "  \"size\" : 3,\n" +
        "  \"query\" : {\n" +
        "    \"match_all\" : {\n" +
        "      \"boost\" : 1.0\n" +
        "    }\n" +
        "  },\n" +
        "  \"_source\" : {\n" +
        "    \"includes\" : [\n" +
        "      \"location_field\"\n" +
        "    ],\n" +
        "    \"excludes\" : [ ]\n" +
        "  }\n" +
        "}]" },
      true);
    testBuilder()
        .sqlQuery(sqlQuery)
        .unOrdered()
        .baselineColumns("location_1")
        .baselineValues(mapOf("lat", -33D, "lon", -33D))
        .go();
  }

  @Test
  public final void runTestWithComplexArrayLimit2() throws Exception {
    // We cannot push down complex fields
    String sqlQuery = "select t.location_field[1] as location_1 from elasticsearch." + schema + "." + table + " t limit 2";
    verifyJsonInPlanHelper(sqlQuery, new String[] {
        "[{\n" +
        "  \"from\" : 0,\n" +
        "  \"size\" : 2,\n" +
        "  \"query\" : {\n" +
        "    \"match_all\" : {\n" +
        "      \"boost\" : 1.0\n" +
        "    }\n" +
        "  },\n" +
        "  \"_source\" : {\n" +
        "    \"includes\" : [\n" +
        "      \"location_field\"\n" +
        "    ],\n" +
        "    \"excludes\" : [ ]\n" +
        "  }\n" +
        "}]" }, true);
    testBuilder()
        .sqlQuery(sqlQuery)
        .unOrdered()
        .baselineColumns("location_1")
        .baselineValues(mapOf("lat", -11D, "lon", -11D))
        .baselineValues(mapOf("lat", -22D, "lon", -22D))
        .go();
  }


  @Test
  public final void runTestWithComplexArrayLimit100() throws Exception {
    // We cannot push down complex fields
    String sqlQuery = "select t.location_field[1] as location_1 from elasticsearch." + schema + "." + table + " t limit 100";
    verifyJsonInPlanHelper(sqlQuery, new String[] {
        "[{\n" +
        "  \"from\" : 0,\n" +
        "  \"size\" : 100,\n" +
        "  \"query\" : {\n" +
        "    \"match_all\" : {\n" +
        "      \"boost\" : 1.0\n" +
        "    }\n" +
        "  },\n" +
        "  \"_source\" : {\n" +
        "    \"includes\" : [\n" +
        "      \"location_field\"\n" +
        "    ],\n" +
        "    \"excludes\" : [ ]\n" +
        "  }\n" +
        "}]" }, true);
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
  public final void runTestWithLimitBelowAggregation2() throws Exception {
    String sqlQueryLimit2 = "select city from (select city from elasticsearch." + schema + "." + table + " limit 2) t group by city";
    verifyJsonInPlanHelper(sqlQueryLimit2, new String[] {""
        + "[{\n" +
        "  \"from\" : 0,\n" +
        "  \"size\" : 2,\n" +
        "  \"query\" : {\n" +
        "    \"match_all\" : {\n" +
        "      \"boost\" : 1.0\n" +
        "    }\n" +
        "  },\n" +
        "  \"_source\" : {\n" +
        "    \"includes\" : [\n" +
        "      \"city\"\n" +
        "    ],\n" +
        "    \"excludes\" : [ ]\n" +
        "  }\n" +
        "}]"}, true);
    testBuilder().sqlQuery(sqlQueryLimit2).unOrdered()
        .baselineColumns("city")
        .baselineValues("Cambridge")
        .baselineValues("San Francisco")
        .go();
  }

  @Test
  public final void runTestWithLimitBelowAggregation100() throws Exception {
    String sqlQueryLimit100 = "select city from (select city from elasticsearch." + schema + "." + table + " limit 100) t group by city";
    verifyJsonInPlanHelper(sqlQueryLimit100, new String[] {
        "[{\n" +
        "  \"from\" : 0,\n" +
        "  \"size\" : 100,\n" +
        "  \"query\" : {\n" +
        "    \"match_all\" : {\n" +
        "      \"boost\" : 1.0\n" +
        "    }\n" +
        "  },\n" +
        "  \"_source\" : {\n" +
        "    \"includes\" : [\n" +
        "      \"city\"\n" +
        "    ],\n" +
        "    \"excludes\" : [ ]\n" +
        "  }\n" +
        "}]"}, true);
    testBuilder().sqlQuery(sqlQueryLimit100).unOrdered()
        .baselineColumns("city")
        .baselineValues("Cambridge")
        .baselineValues("San Francisco")
        .baselineValues("San Diego")
        .go();
  }

  @Test
  public final void runTestWithLeafLimitComplexArrayFetchOffset() throws Exception {
    try {
      test("set planner.leaf_limit_enable = true");
      // We cannot push down complex fields
      String sqlQuery = "select t.location_field[1] as location_1 from elasticsearch." + schema + "." + table + " t offset 2 fetch next 1 row only";
      testPlanMatchingPatterns(sqlQuery, new String[] { "Limit\\(offset=\\[2\\], fetch=\\[1\\]\\)", "Limit\\(offset=\\[0\\], fetch=\\[3\\]\\)"}, null);
      verifyJsonInPlanHelper(sqlQuery, new String[]{
          "[{\n" +
          "  \"from\" : 0,\n" +
          "  \"size\" : 3,\n" +
          "  \"query\" : {\n" +
          "    \"match_all\" : {\n" +
          "      \"boost\" : 1.0\n" +
          "    }\n" +
          "  },\n" +
          "  \"_source\" : {\n" +
          "    \"includes\" : [\n" +
          "      \"location_field\"\n" +
          "    ],\n" +
          "    \"excludes\" : [ ]\n" +
          "  }\n" +
          "}]"},
        true);
      testBuilder()
          .sqlQuery(sqlQuery)
          .unOrdered()
          .baselineColumns("location_1")
          .baselineValues(mapOf("lat", -33D, "lon", -33D))
          .go();
    } finally {
      test("set planner.leaf_limit_enable = false");
    }
  }


  @Test
  public final void runTestLeafLimitFilterLimit1() throws Exception {
    try {
      test("set planner.leaf_limit_enable = true");
      String sqlQueryLimit1 = "select state, city_analyzed, review_count from elasticsearch." + schema + "." + table + " where stars >= 4 limit 1";
      verifyJsonInPlanHelper(sqlQueryLimit1, new String[]{
          "[{\n" +
          "  \"from\" : 0,\n" +
          "  \"size\" : 1,\n" +
          "  \"query\" : {\n" +
          "    \"range\" : {\n" +
          "      \"stars\" : {\n" +
          "        \"from\" : 4,\n" +
          "        \"to\" : null,\n" +
          "        \"include_lower\" : true,\n" +
          "        \"include_upper\" : true,\n" +
          "        \"boost\" : 1.0\n" +
          "      }\n" +
          "    }\n" +
          "  },\n" +
          "  \"_source\" : {\n" +
          "    \"includes\" : [\n" +
          "      \"city_analyzed\",\n" +
          "      \"review_count\",\n" +
          "      \"stars\",\n" +
          "      \"state\"\n" +
          "    ],\n" +
          "    \"excludes\" : [ ]\n" +
          "  }\n" +
          "}]"}, true);
      testBuilder().sqlQuery(sqlQueryLimit1).unOrdered()
          .baselineColumns("state", "city_analyzed", "review_count")
          .baselineValues("MA", "Cambridge", 11)
          .go();
    } finally {
      test("set planner.leaf_limit_enable = false");
    }
  }

  @Test
  public final void runTestLeafLimitFilterLimit100000() throws Exception {
    try {
      test("set planner.leaf_limit_enable = true");
      String sqlQueryLimit100 = "select state, city_analyzed, review_count from elasticsearch." + schema + "." + table + " where stars >= 4 limit 100000";
      testPlanMatchingPatterns(sqlQueryLimit100, new String[] { "Limit\\(offset=\\[0\\], fetch=\\[1000\\]\\)"}, null);
      verifyJsonInPlanHelper(sqlQueryLimit100, new String[]{
          "[{\n" +
          "  \"from\" : 0,\n" +
          "  \"size\" : 1000,\n" +
          "  \"query\" : {\n" +
          "    \"range\" : {\n" +
          "      \"stars\" : {\n" +
          "        \"from\" : 4,\n" +
          "        \"to\" : null,\n" +
          "        \"include_lower\" : true,\n" +
          "        \"include_upper\" : true,\n" +
          "        \"boost\" : 1.0\n" +
          "      }\n" +
          "    }\n" +
          "  },\n" +
          "  \"_source\" : {\n" +
          "    \"includes\" : [\n" +
          "      \"city_analyzed\",\n" +
          "      \"review_count\",\n" +
          "      \"stars\",\n" +
          "      \"state\"\n" +
          "    ],\n" +
          "    \"excludes\" : [ ]\n" +
          "  }\n" +
          "}]"}, true);
      testBuilder().sqlQuery(sqlQueryLimit100).unOrdered()
          .baselineColumns("state", "city_analyzed", "review_count")
          .baselineValues("MA", "Cambridge", 11)
          .baselineValues("CA", "San Diego", 33)
          .baselineValues("MA", "Cambridge", 11)
          .go();
    } finally {
      test("set planner.leaf_limit_enable = false");
    }
  }

  @Test
  public void testLimitOne() throws Exception {
    String query = String.format("select * from elasticsearch.%s.%s limit 1", schema, table);
    LocalExecutionConfig config = LocalExecutionConfig.newBuilder()
      .setEnableLeafLimits(false)
      .setFailIfNonEmptySent(false)
      .setUsername(StandardSystemProperty.USER_NAME.value())
      .setSqlContext(Collections.<String>emptyList())
      .setInternalSingleThreaded(false)
      .setQueryResultsStorePath(format("%s.\"%s\"", TEMP_SCHEMA, "elasticLimitOne"))
      .setAllowPartitionPruning(true)
      .setExposeInternalSources(false)
      .setSubstitutionSettings(SubstitutionSettings.of())
      .build();

    RunQuery queryCmd = RunQuery
      .newBuilder()
      .setType(UserBitShared.QueryType.SQL)
      .setSource(SubmissionSource.LOCAL)
      .setPlan(query)
      .build();

    ProfileGrabber grabber = new ProfileGrabber();
    getLocalQueryExecutor().submitLocalQuery(ExternalIdHelper.generateExternalId(), grabber, queryCmd, false, config, false);
    QueryProfile profile = grabber.getProfile();

    Optional<OperatorProfile> scanProfile = profile.getFragmentProfile(0)
      .getMinorFragmentProfile(0).getOperatorProfileList().stream()
      .filter(operatorProfile -> operatorProfile.getOperatorType() == CoreOperatorType.ELASTICSEARCH_SUB_SCAN_VALUE)
      .findFirst();
    assertEquals(1L, scanProfile.get().getInputProfile(0).getRecords());
  }

  private static class ProfileGrabber implements QueryObserver {
    private CountDownLatch latch = new CountDownLatch(1);
    private QueryProfile profile;
    private DeferredException exception = new DeferredException();

    @Override
    public AttemptObserver newAttempt(AttemptId attemptId, AttemptReason reason) {
      return new AbstractAttemptObserver() {
        @Override
        public void execDataArrived(RpcOutcomeListener<Ack> outcomeListener, QueryWritableBatch result) {
          try {
            AutoCloseables.close(
              Arrays.stream(result.getBuffers())
                .map(CloseableByteBuf::new)
                .collect(ImmutableList.toImmutableList()));
          } catch (Exception e) {
            exception.addException(e);
          }
          outcomeListener.success(Acks.OK, null);
        }
      };
    }

    @Override
    public void execCompletion(UserResult result) {
      profile = result.getProfile();
      latch.countDown();
    }

    public QueryProfile getProfile() throws InterruptedException {
      await();
      return profile;
    }

    public void await() throws InterruptedException {
      latch.await();
    }
  }
}
