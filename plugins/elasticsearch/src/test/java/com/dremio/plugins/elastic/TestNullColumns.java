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

import static com.dremio.plugins.elastic.ElasticsearchType.INTEGER;
import static com.dremio.plugins.elastic.ElasticsearchType.STRING;

import org.junit.Before;
import org.junit.Test;

import com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType;
import com.google.common.collect.ImmutableMap;

public class TestNullColumns extends ElasticBaseTestQuery {

  private static final String NULL_COLUMN_FILE = "/json/nullcolumntest.json";

  @Before
  public void setup() throws Exception {
    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
      new ElasticsearchCluster.ColumnData("state", STRING, ImmutableMap.of("index", "not_analyzed"), null),
      new ElasticsearchCluster.ColumnData("state_analyzed", STRING, null),
      new ElasticsearchCluster.ColumnData("nullint", INTEGER, null)
    };
    elastic.load(schema, table, data);
    elastic.dataFromFile(schema, table, NULL_COLUMN_FILE);
  }

  @Test
  public void testSelectAllColumns() throws Exception {
    // In this test, we are just running without checking the results since we have _uid here.
    // Also, note that in the pushdown query, we are not selecting any of null/unmapped fields!
    // So, when we actually do read the json from elasticsearch, we will have several columns without any column definition.
    final String query = "select * from elasticsearch." + schema + "." + table;
    verifyJsonInPlan(query, new String[] {
      "=[{\n" +
      "  \"from\" : 0,\n" +
      "  \"size\" : 4000,\n" +
      "  \"query\" : {\n" +
      "    \"match_all\" : { }\n" +
      "  },\n" +
      "  \"_source\" : {\n" +
      "    \"includes\" : [ \"_index\", \"_type\", \"_uid\", \"nullint\", \"state\", \"state_analyzed\" ],\n" +
      "    \"excludes\" : [ ]\n" +
      "  }\n" +
      "}]"} );
    test(query);
  }

  @Test
  public void testSelectNonNullColumn() throws Exception {
    final String query = "select state, state_analyzed from elasticsearch." + schema + "." + table;
    verifyJsonInPlan(query, new String[] {
      "=[{\n" +
      "  \"from\" : 0,\n" +
      "  \"size\" : 4000,\n" +
      "  \"query\" : {\n" +
      "    \"match_all\" : { }\n" +
      "  },\n" +
      "  \"_source\" : {\n" +
      "    \"includes\" : [ \"state\", \"state_analyzed\" ],\n" +
      "    \"excludes\" : [ ]\n" +
      "  }\n" +
      "}]"} );
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("state", "state_analyzed")
      .baselineValues("MA CA", "MA CA")
      .go();
  }

  @Test
  public void testSelectNullColumn() throws Exception {
    final String query = "select nullstring from elasticsearch." + schema + "." + table;
    errorMsgWithTypeTestHelper(query, ErrorType.VALIDATION, "Column 'nullstring' not found in any table");
  }

}
