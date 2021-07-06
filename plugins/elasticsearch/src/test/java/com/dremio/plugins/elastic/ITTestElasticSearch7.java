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

import static com.dremio.plugins.elastic.ElasticsearchType.TEXT;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.plugins.Version;
import com.dremio.plugins.elastic.ElasticBaseTestQuery.PublishHost;

// Enable http.publish_host so that we get localhost/127.0.0.1:9200 as http_address
@PublishHost(enabled=true)
public class ITTestElasticSearch7 extends ElasticBaseTestQuery {

  private static final Logger logger = LoggerFactory.getLogger(ITTestElasticSearch7.class);
  private static final Version ELASTICSEARCH_VERSION_7_0_X = new Version(7, 0, 0);

  @Test
  public void testSQLNotInQuery() throws Exception {
    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
      new ElasticsearchCluster.ColumnData("location", TEXT, new Object[][]{
        {"San Francisco"},
        {"Oakland"},
        {"San Jose"}
      })
    };

    elastic.load(schema, table, data);
    String sql = String.format("select location from elasticsearch.%s.%s where location NOT IN ('Oakland')", schema, table);
    testBuilder().sqlQuery(sql).unOrdered()
      .baselineColumns("location")
      .baselineValues("San Francisco")
      .baselineValues("San Jose")
      .go();
  }

  @Test
  public void testSQLNotEqualsQuery() throws Exception {
    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
      new ElasticsearchCluster.ColumnData("location", TEXT, new Object[][]{
        {"San Francisco"},
        {"Oakland"},
        {"San Jose"}
      })
    };

    elastic.load(schema, table, data);
    String sql = String.format("select location from elasticsearch.%s.%s where location != 'Oakland' ", schema, table);
    testBuilder().sqlQuery(sql).unOrdered()
      .baselineColumns("location")
      .baselineValues("San Francisco")
      .baselineValues("San Jose")
      .go();
  }

  @Test
  public void testGetESData() throws Exception {
    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
      new ElasticsearchCluster.ColumnData("location", TEXT, new Object[][]{
        {"San Francisco"},
        {"Oakland"},
        {"San Jose"}
      })
    };

    elastic.load(schema, table, data);
    String sql = String.format("select location from elasticsearch.%s.%s", schema, table);
    testBuilder().sqlQuery(sql).unOrdered()
      .baselineColumns("location")
      .baselineValues("San Francisco")
      .baselineValues("Oakland")
      .baselineValues("San Jose")
      .go();
  }

  @Test
  public void testGetESDataCount() throws Exception {
    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
      new ElasticsearchCluster.ColumnData("location", TEXT, new Object[][]{
        {"San Francisco"},
        {"Oakland"},
        {"San Jose"}
      })
    };

    elastic.load(schema, table, data);
    String sql = String.format("select count(*) from elasticsearch.%s.%s", schema, table);
    testBuilder().sqlQuery(sql).unOrdered()
      .baselineColumns("EXPR$0")
      .baselineValues(3L)
      .go();
  }
}
