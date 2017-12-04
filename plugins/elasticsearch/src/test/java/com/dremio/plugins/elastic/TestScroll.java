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

import static com.dremio.plugins.elastic.ElasticsearchType.INTEGER;

import org.junit.Test;

import com.dremio.TestBuilder;
import com.dremio.plugins.elastic.ElasticBaseTestQuery.ElasticScrollSize;

@ElasticScrollSize(scrollSize=128)
public class TestScroll extends ElasticBaseTestQuery {

  @Test
  public void testScroll() throws Exception {
    final int rowCount = 300;
    Object[][] obj = new Object[rowCount][1];
    for (int i = 0; i < rowCount; i++) {
      obj[i][0] = i;
    }
    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
      new ElasticsearchCluster.ColumnData("val", INTEGER, obj)
    };

    elastic.load(schema, table, data);

    TestBuilder builder = testBuilder()
      .sqlQuery(String.format("select val from elasticsearch.%s.%s", schema, table))
      .unOrdered()
      .baselineColumns("val");

    for (int i = 0; i < rowCount; i++) {
      builder.baselineValues(i);
    }

    builder.go();
  }
}
