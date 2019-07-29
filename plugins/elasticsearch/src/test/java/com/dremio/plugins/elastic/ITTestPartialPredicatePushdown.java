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

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.plugins.elastic.ElasticBaseTestQuery.ScriptsEnabled;

/**
 * Tests for validating that partial predicate pushdown works
 */
@ScriptsEnabled(enabled=false)
public class ITTestPartialPredicatePushdown extends ElasticPredicatePushdownBase {

  private static final Logger logger = LoggerFactory.getLogger(ITTestPartialPredicatePushdown.class);

  @Test
  public void partialPushdown() throws Exception {
    ElasticsearchCluster.ColumnData[] data = getBusinessData();

    elastic.load(schema, table, data);

    String sql = String.format("select * from elasticsearch.%s.%s where city = 'Cambridge' and business_id = full_address", schema, table);

    assertPushDownContains(sql,
      "{\n" +
      "  \"from\" : 0,\n" +
      "  \"size\" : 4000,\n" +
      "  \"query\" : {\n" +
      "    \"match\" : {\n" +
      "      \"city\" : {\n" +
      "        \"query\" : \"Cambridge\",\n" +
      "        \"operator\" : \"OR\",\n" +
      "        \"prefix_length\" : 0,\n" +
      "        \"max_expansions\" : 50000,\n" +
      "        \"fuzzy_transpositions\" : false,\n" +
      "        \"lenient\" : false,\n" +
      "        \"zero_terms_query\" : \"NONE\",\n" +
      "        \"boost\" : 1.0\n" +
      "      }\n" +
      "    }\n" +
      "  }\n" +
      "}"
    );
  }
}
