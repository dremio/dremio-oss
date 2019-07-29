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

import static com.dremio.plugins.elastic.ElasticsearchType.FLOAT;
import static com.dremio.plugins.elastic.ElasticsearchType.INTEGER;
import static com.dremio.plugins.elastic.ElasticsearchType.TEXT;

import org.junit.Before;
import org.junit.Test;

import com.dremio.exec.record.BatchSchema;
import com.google.common.collect.ImmutableMap;

public class ITTestEmptyIndexType extends ElasticBaseTestQuery {

  @Before
  public void setup() throws Exception {
    final ElasticsearchCluster.ColumnData[] justMapping = new ElasticsearchCluster.ColumnData[]{
        new ElasticsearchCluster.ColumnData("full_address", TEXT, null),
        new ElasticsearchCluster.ColumnData("city", TEXT, ImmutableMap.of("index", "false"), null),
        new ElasticsearchCluster.ColumnData("review_count", INTEGER, null),
        new ElasticsearchCluster.ColumnData("stars", FLOAT, null)
    };

    elastic.load(schema, table, justMapping);
  }

  @Test
  public void testEmptyIndex() throws Exception {

    final String query = "select * from elasticsearch." + schema + "." + table;

    testBuilder()
      .sqlQuery("describe elasticsearch." + schema + "." + table)
      .unOrdered()
      .baselineColumns("COLUMN_NAME", "DATA_TYPE", "IS_NULLABLE")
      .baselineValues("city", "CHARACTER VARYING", "YES")
      .baselineValues("full_address", "CHARACTER VARYING", "YES")
      .baselineValues("review_count", "INTEGER", "YES")
      .baselineValues("stars", "FLOAT", "YES")
      .go();
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns(BatchSchema.SCHEMA_UNKNOWN_NO_DATA_COLNAME)
      .expectsEmptyResultSet()
      .go();
  }

  @Test
  public void testIndexNotPresent() throws Exception {
    errorMsgTestHelper("SELECT * FROM elasticsearch.noIndex.noTable",
        "Table 'elasticsearch.noIndex.noTable' not found");

    final ElasticsearchCluster.ColumnData[] justMapping = new ElasticsearchCluster.ColumnData[]{
        new ElasticsearchCluster.ColumnData("full_address", TEXT, null),
        new ElasticsearchCluster.ColumnData("city", TEXT, ImmutableMap.of("index", "false"), null),
        new ElasticsearchCluster.ColumnData("review_count", INTEGER, null),
        new ElasticsearchCluster.ColumnData("stars", FLOAT, null)
    };

    elastic.load(schema, table, justMapping);
    errorMsgTestHelper(String.format("SELECT * FROM elasticsearch.%s.noTable", schema),
        String.format("Table 'elasticsearch.%s.noTable' not found", schema));
  }
}
