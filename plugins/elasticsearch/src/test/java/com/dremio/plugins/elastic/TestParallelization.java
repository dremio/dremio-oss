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

import org.junit.Test;

import static com.dremio.plugins.elastic.ElasticBaseTestQuery.TestNameGenerator.schemaName;
import static com.dremio.plugins.elastic.ElasticsearchType.STRING;

public class TestParallelization extends ElasticBaseTestQuery {

  @Test
  public void test() throws Exception {

    String schema = schemaName();
    elastic.schema(10, 0, schema);

    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
            new ElasticsearchCluster.ColumnData("column", STRING, new Object[][]{
                    {"value"},
                    {"value"},
                    {"value"},
                    {"value"},
                    {"value"},
                    {"value"},
                    {"value"},
                    {"value"},
                    {"value"},
                    {"value"},
                    {"value"}
            })
    };

    elastic.load(schema, table, data);
    testNoResult("set planner.width.max_per_node = 10");
    testNoResult("set planner.width.max_per_query = 10");
    testNoResult("set planner.slice_target = 1");

    String sql = String.format("select * from elasticsearch.%s.%s", schema, table);

    testPhysicalPlan(sql, "UnionExchange");
  }
}
