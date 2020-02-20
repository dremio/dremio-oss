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

import static com.dremio.plugins.elastic.ElasticsearchType.INTEGER;
import static com.dremio.plugins.elastic.ElasticsearchType.KEYWORD;
import static com.dremio.plugins.elastic.ElasticsearchType.TEXT;
import static org.junit.Assume.assumeFalse;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

public class ITTestColumnParameters extends ElasticBaseTestQuery {

  private static final Logger logger = LoggerFactory.getLogger(ITTestColumnParameters.class);

  @Test
  public void testNotIndexedInteger() throws Exception {

    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
        new ElasticsearchCluster.ColumnData("review_count", INTEGER, ImmutableMap.of("index", "false"),
            new Object[][]{
              {11},
              {22},
              {33},
              {11},
              {1}
      })
    };

    elastic.load(schema, table, data);

    String sql = String.format("select review_count from elasticsearch.%s.%s where review_count > 12", schema, table);
    testBuilder()
            .sqlQuery(sql)
            .unOrdered()
            .baselineColumns("review_count")
            .baselineValues(33)
            .baselineValues(22)
            .go();
  }

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
            .baselineValues("Cambridge")
            .go();

    sql = String.format("select city_analyzed from elasticsearch.%s.%s where city_analyzed like '%%cambridge%%' ", schema, table);
    testBuilder()
            .sqlQuery(sql)
            .unOrdered()
            .baselineColumns("city_analyzed")
            .baselineValues("cambridge")
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
            .go();

    sql = String.format("select city_analyzed from elasticsearch.%s.%s where city_analyzed = 'cambridge' ", schema, table);
    testBuilder()
            .sqlQuery(sql)
            .unOrdered()
            .baselineColumns("city_analyzed")
            .baselineValues("cambridge")
            .go();
  }

  @Test
  public void testLikeOnNormalizedKeyword() throws Exception {
    // DX-18338: failed to put mappings on indices in elasticsearch server
    if (elastic.getMinVersionInCluster().getMajor() == 5) {
      assumeFalse(elastic.getMinVersionInCluster().getMinor() <= 2);
    }

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
            .go();

    sql = String.format("select city_normalized from elasticsearch.%s.%s where city_normalized like '%%cambridge%%' ", schema, table);
    testBuilder()
            .sqlQuery(sql)
            .unOrdered()
            .baselineColumns("city_normalized")
            .baselineValues("cambridge")
            .go();
  }

  @Test
  public void testMatchOnNormalizedKeyword() throws Exception {
    // DX-18338: failed to put mappings on indices in elasticsearch server
    if (elastic.getMinVersionInCluster().getMajor() == 5) {
      assumeFalse(elastic.getMinVersionInCluster().getMinor() <= 2);
    }

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
            .go();

    sql = String.format("select city_normalized from elasticsearch.%s.%s where city_normalized = 'cambridge' ", schema, table);
    testBuilder()
            .sqlQuery(sql)
            .unOrdered()
            .baselineColumns("city_normalized")
            .baselineValues("cambridge")
            .go();
  }
}
