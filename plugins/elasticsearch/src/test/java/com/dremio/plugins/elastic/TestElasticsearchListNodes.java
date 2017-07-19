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

import static com.dremio.plugins.elastic.ElasticsearchType.STRING;

import org.junit.Before;
import org.junit.Test;

import com.dremio.plugins.elastic.ElasticBaseTestQuery.PublishHost;

// Enable http.publish_host so that we get localhost/127.0.0.1:9200 as http_address
@PublishHost(enabled=true)
public class TestElasticsearchListNodes extends ElasticBaseTestQuery {

  @Before
  public void loadTable() throws Exception {
    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
      new ElasticsearchCluster.ColumnData("location", STRING, new Object[][]{
        {"San Francisco"},
        {"Oakland"},
        {"San Jose"}
      })
    };

    elastic.load(schema, table, data);
  }

  @Test
  public void testGetHostList() throws Exception {
    String sql = String.format("select location from elasticsearch.%s.%s", schema, table);
    testBuilder().sqlQuery(sql).unOrdered()
      .baselineColumns("location")
      .baselineValues("San Francisco")
      .baselineValues("Oakland")
      .baselineValues("San Jose")
      .go();
  }
}
