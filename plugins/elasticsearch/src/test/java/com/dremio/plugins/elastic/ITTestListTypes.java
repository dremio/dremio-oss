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

import static com.dremio.TestBuilder.listOf;
import static com.dremio.plugins.elastic.ElasticsearchType.TEXT;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.util.TestTools;
import com.dremio.plugins.elastic.ElasticsearchCluster.ColumnData;
import com.dremio.plugins.elastic.ElasticsearchCluster.SearchResults;

/**
 * Tests variation in data types within the same field.
 */
public class ITTestListTypes extends ElasticBaseTestQuery {

  private static final Logger logger = LoggerFactory.getLogger(ITTestElasticsearchDataVariation.class);

  @Rule
  public final TestRule TIMEOUT = TestTools.getTimeoutRule(300, TimeUnit.SECONDS);

  @Test
  public void testStringListCoercion() throws Exception {

    ColumnData[] data = new ColumnData[]{
            new ColumnData("field_a", TEXT, new Object[][]{
                    {Collections.<String>emptyList()},             // empty array
                    {"test"}
            })
    };

    loadWithRetry(schema, table, data);
    SearchResults contents = elastic.search(schema, table);


    String sql = "select field_a from elasticsearch." + schema + "." + table;

    testBuilder().sqlQuery(sql).unOrdered()
        .baselineColumns("field_a")
        .baselineValues(Collections.<String>emptyList())
        .baselineValues(listOf("test"))
        .go();
  }
}
