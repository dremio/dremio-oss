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

import static com.dremio.plugins.elastic.ElasticsearchType.DATE;
import static com.dremio.plugins.elastic.ElasticsearchType.KEYWORD;

import com.dremio.exec.expr.fn.impl.DateFunctionsUtils;
import com.google.common.collect.ImmutableMap;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ITTestDateTimePushdown extends ElasticBaseTestQuery {

  private static final Logger logger = LoggerFactory.getLogger(ITTestDateTimePushdown.class);

  private final DateTimeFormatter formatter =
      DateFunctionsUtils.getISOFormatterForFormatString("YYYY-MM-DD HH:MI:SS")
          .withZone(DateTimeZone.UTC);

  @Before
  public void setup() throws Exception {
    ElasticsearchCluster.ColumnData[] data =
        new ElasticsearchCluster.ColumnData[] {
          new ElasticsearchCluster.ColumnData(
              "datefield",
              DATE,
              ImmutableMap.of("format", "yyyyMMddHHmmss"),
              new Object[][] {
                {"20140210105042"},
                {null},
                {"20140212105042"},
                {"20140211105042"},
                {"20140210105042"},
                {"90140210105042"}
              }),
          new ElasticsearchCluster.ColumnData(
              "stringDate",
              KEYWORD,
              new Object[][] {
                {"2015-02-10 10:50:42"},
                {null},
                {"2015-02-10 10:50:42"},
                {"2014-02-11 10:50:42"},
                {"2014-02-10 10:50:42"},
                {"9013-02-10 10:50:42"}
              })
        };

    elastic.load(schema, table, data);
  }

  @Test
  public void testTimestamp() throws Exception {

    String sql =
        String.format(
            "select datefield from elasticsearch.%s.%s where datefield < timestamp '2014-02-12 00:00:00'",
            schema, table);
    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("datefield")
        .baselineValues(formatter.parseLocalDateTime("2014-02-10 10:50:42"))
        .baselineValues(formatter.parseLocalDateTime("2014-02-11 10:50:42"))
        .baselineValues(formatter.parseLocalDateTime("2014-02-10 10:50:42"))
        .go();
  }

  @Test // one of DX-10988 tests
  public void currentTimestampIsPushedDown() throws Exception {

    String sql =
        String.format(
            "SELECT datefield FROM elasticsearch.%s.%s"
                + " WHERE datefield > TIMESTAMPADD(DAY, -2, DATE '2018-06-03')",
            schema, table);
    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("datefield")
        .baselineValues(formatter.parseLocalDateTime("9014-02-10 10:50:42"))
        .go();

    verifyJsonInPlan(
        sql,
        new String[] {
          "[{\n"
              + "  \"from\" : 0,\n"
              + "  \"size\" : 4000,\n"
              + "  \"query\" : {\n"
              + "    \"range\" : {\n"
              + "      \"datefield\" : {\n"
              + "        \"from\" : \"2018-06-01T00:00:00.000Z\",\n"
              + "        \"to\" : null,\n"
              + "        \"include_lower\" : false,\n"
              + "        \"include_upper\" : true,\n"
              + "        \"format\" : \"date_time\",\n"
              + "        \"boost\" : 1.0\n"
              + "      }\n"
              + "    }\n"
              + "  },\n"
              + "  \"_source\" : {\n"
              + "    \"includes\" : [\n"
              + "      \"datefield\"\n"
              + "    ],\n"
              + "    \"excludes\" : [ ]\n"
              + "  }\n"
              + "}]"
        });
  }

  @Test
  public void testTimestampWithImplicitConversion() throws Exception {

    String sql =
        String.format(
            "select datefield from elasticsearch.%s.%s where datefield < '2014-02-12 00:00:00'",
            schema, table);
    verifyJsonInPlan(
        sql,
        new String[] {
          "[{\n"
              + "  \"from\" : 0,\n"
              + "  \"size\" : 4000,\n"
              + "  \"query\" : {\n"
              + "    \"range\" : {\n"
              + "      \"datefield\" : {\n"
              + "        \"from\" : null,\n"
              + "        \"to\" : \"2014-02-12T00:00:00.000Z\",\n"
              + "        \"include_lower\" : true,\n"
              + "        \"include_upper\" : false,\n"
              + "        \"format\" : \"date_time\",\n"
              + "        \"boost\" : 1.0\n"
              + "      }\n"
              + "    }\n"
              + "  },\n"
              + "  \"_source\" : {\n"
              + "    \"includes\" : [\n"
              + "      \"datefield\"\n"
              + "    ],\n"
              + "    \"excludes\" : [ ]\n"
              + "  }\n"
              + "}]"
        });
    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("datefield")
        .baselineValues(formatter.parseLocalDateTime("2014-02-10 10:50:42"))
        .baselineValues(formatter.parseLocalDateTime("2014-02-11 10:50:42"))
        .baselineValues(formatter.parseLocalDateTime("2014-02-10 10:50:42"))
        .go();
  }

  @Test
  public void testTimestampColumnWithImplicitConversion() throws Exception {

    String sql =
        String.format(
            "select datefield from elasticsearch.%s.%s where datefield < stringDate",
            schema, table);
    verifyJsonInPlan(
        sql,
        new String[] {
          "[{\n"
              + "  \"from\" : 0,\n"
              + "  \"size\" : 4000,\n"
              + "  \"query\" : {\n"
              + "    \"match_all\" : {\n"
              + "      \"boost\" : 1.0\n"
              + "    }\n"
              + "  },\n"
              + "  \"_source\" : {\n"
              + "    \"includes\" : [\n"
              + "      \"datefield\",\n"
              + "      \"stringDate\"\n"
              + "    ],\n"
              + "    \"excludes\" : [ ]\n"
              + "  }\n"
              + "}]"
        });
    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("datefield")
        .baselineValues(formatter.parseLocalDateTime("2014-02-10 10:50:42"))
        .baselineValues(formatter.parseLocalDateTime("2014-02-12 10:50:42"))
        .go();
  }
}
