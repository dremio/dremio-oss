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

import static com.dremio.plugins.elastic.ElasticsearchType.KEYWORD;
import static com.dremio.plugins.elastic.ElasticsearchType.TEXT;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.plugins.elastic.ElasticBaseTestQuery.AllowPushdownNormalizedOrAnalyzedFields;
import com.google.common.collect.ImmutableMap;

@AllowPushdownNormalizedOrAnalyzedFields(enabled = true)
public class ITTestPushdownNormalized extends ElasticBaseTestQuery {

  private static final Logger logger = LoggerFactory.getLogger(ITTestPushdownNormalized.class);

  @Test
  public void testLikeOnAnalyzedText() throws Exception {
    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
        new ElasticsearchCluster.ColumnData("city_analyzed", TEXT,
            new Object[][]{
                {"cambridge"},
                {"San Francisco"},
                {"San Diego"},
                {"Cambridge"},
                {"San Francisco"}
            })
    };

    elastic.load(schema, table, data);

    String sql = String.format("select city_analyzed from elasticsearch.%s.%s where city_analyzed like '%%Cambridge%%' ", schema, table);
    testBuilder()
            .sqlQuery(sql)
            .unOrdered()
            .baselineColumns("city_analyzed")
            .expectsEmptyResultSet()
            .go();

    sql = String.format("select city_analyzed from elasticsearch.%s.%s where city_analyzed like '%%cambridge%%' ", schema, table);
    testBuilder()
            .sqlQuery(sql)
            .unOrdered()
            .baselineColumns("city_analyzed")
            .baselineValues("cambridge")
            .baselineValues("Cambridge")
            .go();
  }

  @Test
  public void testMatchOnAnalyzedText() throws Exception {
    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
        new ElasticsearchCluster.ColumnData("city_analyzed", TEXT,
            new Object[][]{
                {"cambridge"},
                {"San Francisco"},
                {"San Diego"},
                {"Cambridge"},
                {"San Francisco"}
            })
    };

    elastic.load(schema, table, data);

    String sql = String.format("select city_analyzed from elasticsearch.%s.%s where city_analyzed = 'Cambridge' ", schema, table);
    testBuilder()
            .sqlQuery(sql)
            .unOrdered()
            .baselineColumns("city_analyzed")
            .baselineValues("Cambridge")
            .baselineValues("Cambridge")
            .go();

    sql = String.format("select city_analyzed from elasticsearch.%s.%s where city_analyzed = 'cambridge' ", schema, table);
    testBuilder()
            .sqlQuery(sql)
            .unOrdered()
            .baselineColumns("city_analyzed")
            .baselineValues("cambridge")
            .baselineValues("cambridge")
            .go();
  }

  @Test
  public void testLikeOnNormalizedKeyword() throws Exception {
    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
        new ElasticsearchCluster.ColumnData("city_normalized", KEYWORD,
            ImmutableMap.of("normalizer", "lowercase_normalizer"),
            new Object[][]{
                {"cambridge"},
                {"San Francisco"},
                {"San Diego"},
                {"Cambridge"},
                {"San Francisco"}
            })
    };

    elastic.load(schema, table, data);

    String sql = String.format("select city_normalized from elasticsearch.%s.%s where city_normalized like '%%Cambridge%%' ", schema, table);
    testBuilder()
            .sqlQuery(sql)
            .unOrdered()
            .baselineColumns("city_normalized")
            .baselineValues("Cambridge")
            .baselineValues("cambridge")
            .go();

    sql = String.format("select city_normalized from elasticsearch.%s.%s where city_normalized like '%%cambridge%%' ", schema, table);
    testBuilder()
            .sqlQuery(sql)
            .unOrdered()
            .baselineColumns("city_normalized")
            .baselineValues("cambridge")
            .baselineValues("Cambridge")
            .go();
  }

  @Test
  public void testMatchOnNormalizedKeyword() throws Exception {
    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
        new ElasticsearchCluster.ColumnData("city_normalized", KEYWORD,
            ImmutableMap.of("normalizer", "lowercase_normalizer"),
            new Object[][]{
                {"cambridge"},
                {"San Francisco"},
                {"San Diego"},
                {"Cambridge"},
                {"San Francisco"}
            })
    };

    elastic.load(schema, table, data);

    String sql = String.format("select city_normalized from elasticsearch.%s.%s where city_normalized = 'Cambridge' ", schema, table);
    testBuilder()
            .sqlQuery(sql)
            .unOrdered()
            .baselineColumns("city_normalized")
            .baselineValues("Cambridge")
            .baselineValues("Cambridge")
            .go();

    sql = String.format("select city_normalized from elasticsearch.%s.%s where city_normalized = 'cambridge' ", schema, table);
    testBuilder()
            .sqlQuery(sql)
            .unOrdered()
            .baselineColumns("city_normalized")
            .baselineValues("cambridge")
            .baselineValues("cambridge")
            .go();
  }
}
