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
import static com.dremio.plugins.elastic.ElasticsearchType.TEXT;

import com.dremio.TestBuilder;
import com.dremio.common.util.TestTools;
import java.util.concurrent.TimeUnit;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

public class ITTestForceDoublePrecisionFalse extends ElasticBaseTestQuery {
  private static final ElasticsearchCluster.ColumnData[] DATA =
      new ElasticsearchCluster.ColumnData[] {
        new ElasticsearchCluster.ColumnData(
            "colFloatDoublePrecision", FLOAT, new Object[][] {{1234567891011.50}}),
        new ElasticsearchCluster.ColumnData("varCharArray", TEXT, new Object[][] {{"foo", "bar"}}),
      };

  @Rule public final TestRule timeoutRule = TestTools.getTimeoutRule(300, TimeUnit.SECONDS);

  @Test
  public void testForceDoublePrecisionFalse() throws Exception {
    elastic.load(schema, table, DATA);
    final String sql =
        "select cast ( colFloatDoublePrecision as varchar ) as colFloatDoublePrecision, varCharArray from elasticsearch."
            + schema
            + "."
            + table;
    testBuilder()
        .sqlQuery(sql)
        .ordered()
        .baselineColumns("colFloatDoublePrecision", "varCharArray")
        .baselineValues("1.234568E12", TestBuilder.listOf("foo", "bar"))
        .go();
  }
}
