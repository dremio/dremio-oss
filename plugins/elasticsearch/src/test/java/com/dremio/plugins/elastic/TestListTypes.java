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

import static com.dremio.TestBuilder.listOf;
import static com.dremio.plugins.elastic.ElasticsearchType.STRING;

import org.junit.Test;
import java.util.Collections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.plugins.elastic.ElasticsearchCluster.ColumnData;
import com.dremio.plugins.elastic.ElasticsearchCluster.SearchResults;

/**
 * Tests variation in data types within the same field.
 */
public class TestListTypes extends ElasticBaseTestQuery {

  private static final Logger logger = LoggerFactory.getLogger(TestElasticsearchDataVariation.class);

  @Test
  public void testStringListCoercion() throws Exception {

    ColumnData[] data = new ColumnData[]{
            new ColumnData("field_a", STRING, new Object[][]{
                    {Collections.<String>emptyList()},             // empty array
                    {"test"}
            })
    };

    elastic.load(schema, table, data);

    SearchResults contents = elastic.search(schema, table);


    String sql = "select field_a from elasticsearch." + schema + "." + table;

    testBuilder().sqlQuery(sql).unOrdered()
        .baselineColumns("field_a")
        .baselineValues(Collections.<String>emptyList())
        .baselineValues(listOf("test"))
        .go();
  }
}
