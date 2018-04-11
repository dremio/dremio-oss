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

import static com.dremio.plugins.elastic.ElasticsearchType.DATE;

import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.exec.expr.fn.impl.DateFunctionsUtils;
import com.google.common.collect.ImmutableMap;

public class TestDateTimePushdown extends ElasticBaseTestQuery {

  private static final Logger logger = LoggerFactory.getLogger(TestDateTimePushdown.class);

  @Test
  public void testTimestamp() throws Exception {

    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
      new ElasticsearchCluster.ColumnData("datefield", DATE, ImmutableMap.of("format", "yyyyMMddHHmmss"), new Object[][]{
        {"20140210105042"},
        {null},
        {"20140212105042"},
        {"20140211105042"},
        {"20140210105042"}
      })
    };

    elastic.load(schema, table, data);

    final DateTimeFormatter formatter = DateFunctionsUtils.getFormatterForFormatString("YYYY-MM-DD HH:MI:SS").withZone(DateTimeZone.UTC);

    String sql = String.format("select datefield from elasticsearch.%s.%s where datefield < timestamp '2014-02-12 00:00:00'", schema, table);
    testBuilder()
      .sqlQuery(sql)
      .unOrdered()
      .baselineColumns("datefield")
      .baselineValues(formatter.parseLocalDateTime("2014-02-10 10:50:42"))
      .baselineValues(formatter.parseLocalDateTime("2014-02-11 10:50:42"))
      .baselineValues(formatter.parseLocalDateTime("2014-02-10 10:50:42"))
      .go();
  }

  @Test
  @Ignore("DX-7869")
  public void testTimestampWithImplicitConversion() throws Exception {

    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
      new ElasticsearchCluster.ColumnData("datefield", DATE, ImmutableMap.of("format", "yyyyMMddHHmmss"), new Object[][]{
        {"20140210105042"},
        {null},
        {"20140212105042"},
        {"20140211105042"},
        {"20140210105042"}
      })
    };

    elastic.load(schema, table, data);

    final DateTimeFormatter formatter = DateFunctionsUtils.getFormatterForFormatString("YYYY-MM-DD HH:MI:SS").withZone(DateTimeZone.UTC);

    String sql = String.format("select datefield from elasticsearch.%s.%s where datefield < '2014-02-12 00:00:00'", schema, table);
    testBuilder()
      .sqlQuery(sql)
      .unOrdered()
      .baselineColumns("datefield")
      .baselineValues(formatter.parseLocalDateTime("2014-02-10 10:50:42"))
      .baselineValues(formatter.parseLocalDateTime("2014-02-11 10:50:42"))
      .baselineValues(formatter.parseLocalDateTime("2014-02-10 10:50:42"))
      .go();
  }
}
