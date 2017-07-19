/*
 * Copyright 2016 Dremio Corporation
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
