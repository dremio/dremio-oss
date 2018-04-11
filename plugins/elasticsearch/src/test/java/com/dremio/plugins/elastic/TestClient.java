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

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import java.util.Random;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.plugins.elastic.ElasticActions.Search;
import com.dremio.plugins.elastic.ElasticConnectionPool.ElasticConnection;

/**
 * Tests for the shard-aware search client.
 */
@Ignore
public class TestClient extends ElasticBaseTestQuery {

  private static final Logger logger = LoggerFactory.getLogger(TestClient.class);

  private ElasticConnection connection;

  @BeforeClass
  public static void beforeStart() {
    Assume.assumeFalse(ElasticsearchCluster.USE_EXTERNAL_ES5);
  }

  @Before
  public void before() throws Exception {
    super.before();
    ElasticsearchStoragePlugin plugin = getSabotContext().getCatalogService().getSource("elasticsearch");
    connection = plugin.getRandomConnection();
  }

  @Test
  public void testSingleShardSearch() throws Exception {

    Random random = new Random();
    int docs = 999 + random.nextInt(10001 - 999);
    int batchSize = 1 + random.nextInt(5000 - 1);
    long totalHitsReceived = 0;

    elastic.populate(1, 0, schema, table, docs, ElasticsearchCluster.PRIMITIVE_TYPES);
    assertThat(elastic.search(schema, table).count, equalTo((long) docs));

    ClusterState state = state(schema);
    IndexRoutingTable irt = routing(state, schema);
    ShardsIterator iter = irt.randomAllActiveShardsIt();
    DiscoveryNodes nodes = state.nodes();

    assertThat(iter.size(), equalTo(1));
    ShardRouting routing = iter.nextOrNull();
    assertNotNull(routing);
    assertNull(iter.nextOrNull());

    DiscoveryNode dn = nodes.get(routing.currentNodeId());
    logger.info("--> executing search on: [{}] batch size: [{}]", routing, batchSize);

    Search search = new Search()
        .setQuery(String.format("{ \"query\": %s } ", QueryBuilders.matchAllQuery().toString()))
        .setResource(schema + "/" + table)
        .setParameter("scroll", "10s")
        .setParameter("size", Integer.toString(batchSize));

    byte[] result = connection.execute(search);
    totalHitsReceived = ElasticsearchCluster.asJsonObject(result).get(ElasticsearchConstants.HITS).getAsJsonObject().get(ElasticsearchConstants.TOTAL_HITS).getAsInt();
    assertThat(totalHitsReceived, equalTo((long) docs));
  }

  @Test
  public void testMultipleShardSearch() throws Exception {

    Random random = new Random();
    int docs = 3 + random.nextInt(10001 - 3);;
    int batchSize = 1 + random.nextInt(5000 - 1);
    int shards = 1 + random.nextInt(5 - 1);
    long totalHitsReceived = 0;

    elastic.populate(shards, 0, schema, table, docs, ElasticsearchCluster.PRIMITIVE_TYPES);
    assertThat(elastic.search(schema, table).count, equalTo((long) docs));

    ClusterState state = state(schema);
    IndexRoutingTable irt = routing(state, schema);
    ShardsIterator iter = irt.randomAllActiveShardsIt();
    DiscoveryNodes nodes = state.nodes();

    assertThat(iter.size(), equalTo(shards));

    ShardRouting routing;
    while ((routing = iter.nextOrNull()) != null) {

      DiscoveryNode dn = nodes.get(routing.currentNodeId());
      logger.info("--> executing search on: [{}] batch size: [{}]", routing, batchSize);

      Search search = new Search()
          .setQuery(String.format("{ \"query\": %s } ", QueryBuilders.matchAllQuery().toString()))
          .setResource(schema + "/" + table)
          .setParameter("scroll", "10s")
          .setParameter("preference", "_shards:" + routing.getId())
          .setParameter("size", Integer.toString(batchSize));
      byte[] result = connection.execute(search);

      totalHitsReceived += ElasticsearchCluster.asJsonObject(result).get("hits").getAsJsonObject().get("total").getAsInt();
    }

    logger.info("--> total hits received across [{}] shards: [{}]", iter.size(), totalHitsReceived);
    assertThat(totalHitsReceived, equalTo((long) docs));
  }

  private ClusterState state(final String index) {
    return elastic.getElasticInternalClient().admin().cluster().prepareState().setIndices(index).get().getState();
  }

  private IndexRoutingTable routing(final ClusterState state, final String index) {
    IndexRoutingTable routing = state.routingTable().index(index);
    logger.info("--> routing table for index: [{}]\n{}", index, routing.prettyPrint());
    return routing;
  }
}
