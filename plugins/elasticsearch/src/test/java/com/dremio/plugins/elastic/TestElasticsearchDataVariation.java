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

import static com.dremio.plugins.elastic.ElasticsearchType.LONG;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.plugins.elastic.ElasticsearchCluster.ColumnData;
import com.dremio.plugins.elastic.ElasticsearchCluster.SearchResults;

/**
 * Tests variation in data types within the same field.
 */
public class TestElasticsearchDataVariation extends ElasticBaseTestQuery {

  private static final Logger logger = LoggerFactory.getLogger(TestElasticsearchDataVariation.class);

  @Test
  @org.junit.Ignore("Pending DX-290")
  public void testSelectVariableLongRepresentations() throws Exception {

    ColumnData[] data = new ColumnData[]{
            new ColumnData("field_a", LONG, new Object[][]{
                    {5L, 6L, 7L},             // array of simple longs
                    {123L},                   // single long value
                    {-6L},                    // negative
                    {1.5D},                   // single double value
                    {1.6D, 1.7D, -223.5566D}, // array of doubles
                    {43.3F},                  // single float value
                    {-555.4F, 435.01F},       // array of floats
                    {"5"},                    // single string value
                    {"7", 77},                // mixed type array
                    {77, "7"},                // mixed type array with leading long
                    {"7", 7.7, 77}            // mixed type array
            })
    };

    elastic.load(schema, table, data);

    SearchResults contents = elastic.search(schema, table);
    logger.info("--> index contents: {}", contents);

    String sql = "select field_a from elasticsearch." + schema + "." + table;

    testBuilder().sqlQuery(sql).unOrdered()
        .baselineColumns("field_a")
        .baselineValues("TBD")
        .go();
  }
}
