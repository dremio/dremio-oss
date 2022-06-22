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

import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;

import org.joda.time.LocalDateTime;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import com.dremio.common.util.TestTools;

public class ITTestDateTypeWithMultipleFormatterMix extends BaseTestDateTypeWithMultipleFormatter {

  @Rule
  public final TestRule timeoutRule = TestTools.getTimeoutRule(300, TimeUnit.SECONDS);

  @Test
  public final void runTestWithDefaultFormatter() throws Exception {
    populateDefaultFormatter();
    final String sql = "select field from elasticsearch." + schema + "." + table;
    verifyJsonInPlan(sql, new String[] {
      "[{\n" +
        "  \"from\" : 0,\n" +
        "  \"size\" : 4000,\n" +
        "  \"query\" : {\n" +
        "    \"match_all\" : {\n" +
        "      \"boost\" : 1.0\n" +
        "    }\n" +
        "  },\n" +
        "  \"_source\" : {\n" +
        "    \"includes\" : [\n" +
        "      \"field\"\n" +
        "    ],\n" +
        "    \"excludes\" : [ ]\n" +
        "  }\n" +
        "}]"});
    testBuilder()
      .sqlQuery(sql)
      .unOrdered()
      .baselineColumns("field")
      .baselineValues(DATE_TIME_RESULT)
      .baselineValues(DATE_TIME_RESULT)
      .baselineValues(DATE_TIME_RESULT)
      .go();
  }

  @Test
  public final void runTestWithDefaultFormatterExtract() throws Exception {
    populateDefaultFormatter();
    final String sql = "select extract(year from \"field\"), extract(month from \"field\")," +
      " extract(day from \"field\"), extract(hour from \"field\"), extract(minute from \"field\")," +
      " extract(second from \"field\") from elasticsearch." + schema + "." + table;
    verifyJsonInPlan(sql, new String[] {
      "[{\n" +
        "  \"from\" : 0,\n" +
        "  \"size\" : 4000,\n" +
        "  \"query\" : {\n" +
        "    \"match_all\" : {\n" +
        "      \"boost\" : 1.0\n" +
        "    }\n" +
        "  },\n" +
        "  \"_source\" : {\n" +
        "    \"includes\" : [\n" +
        "      \"field\"\n" +
        "    ],\n" +
        "    \"excludes\" : [ ]\n" +
        "  }\n" +
        "}]"});
    testBuilder()
      .sqlQuery(sql)
      .unOrdered()
      .baselineColumns("EXPR$0", "EXPR$1", "EXPR$2", "EXPR$3", "EXPR$4", "EXPR$5")
      .baselineValues(1970L, 1L, 23L, 20L, 23L, 51L)
      .baselineValues(1970L, 1L, 23L, 20L, 23L, 51L)
      .baselineValues(1970L, 1L, 23L, 20L, 23L, 51L)
      .go();
  }

  @Test
  public final void runTestWithDefaultFormatterConstant() throws Exception {
    populateDefaultFormatter();
    final String sql = "select date '2016-01-01' from elasticsearch." + schema + "." + table;
    verifyJsonInPlan(sql, new String[] {
      "[{\n" +
        "  \"from\" : 0,\n" +
        "  \"size\" : 4000,\n" +
        "  \"query\" : {\n" +
        "    \"match_all\" : {\n" +
        "      \"boost\" : 1.0\n" +
        "    }\n" +
        "  },\n" +
        "  \"_source\" : {\n" +
        "    \"includes\" : [ ],\n" +
        "    \"excludes\" : [ ]\n" +
        "  }\n" +
        "}]"});
    testBuilder()
      .sqlQuery(sql)
      .unOrdered()
      .baselineColumns("EXPR$0")
      .baselineValues(new LocalDateTime(Timestamp.valueOf("2016-01-01 00:00:00.000")))
      .baselineValues(new LocalDateTime(Timestamp.valueOf("2016-01-01 00:00:00.000")))
      .baselineValues(new LocalDateTime(Timestamp.valueOf("2016-01-01 00:00:00.000")))
      .go();
  }

  @Test
  public final void runTestWithDefaultFormatterConstantExtract() throws Exception {
    populateDefaultFormatter();
    final String sql = "select extract(year from date '2016-01-01'), extract(minute from date '2016-01-01') from elasticsearch." + schema + "." + table;
    verifyJsonInPlan(sql, new String[] {
      "[{\n" +
        "  \"from\" : 0,\n" +
        "  \"size\" : 4000,\n" +
        "  \"query\" : {\n" +
        "    \"match_all\" : {\n" +
        "      \"boost\" : 1.0\n" +
        "    }\n" +
        "  },\n" +
        "  \"_source\" : {\n" +
        "    \"includes\" : [ ],\n" +
        "    \"excludes\" : [ ]\n" +
        "  }\n" +
        "}]"});
    testBuilder()
      .sqlQuery(sql)
      .unOrdered()
      .baselineColumns("EXPR$0", "EXPR$1")
      .baselineValues(2016L, 0L)
      .baselineValues(2016L, 0L)
      .baselineValues(2016L, 0L)
      .go();
  }
}
