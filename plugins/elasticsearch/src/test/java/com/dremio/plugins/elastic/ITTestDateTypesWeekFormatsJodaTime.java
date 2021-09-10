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

import static com.dremio.plugins.elastic.ElasticsearchType.DATE;
import static org.junit.Assume.assumeFalse;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.joda.time.LocalDateTime;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.util.TestTools;
import com.dremio.plugins.elastic.DateFormats.FormatterAndType;
import com.google.common.collect.ImmutableMap;

/**
 * Tests for week related date/datetime formats.
 */
@RunWith(Parameterized.class)
public class ITTestDateTypesWeekFormatsJodaTime extends ElasticBaseTestQuery {

  private static final Logger logger = LoggerFactory.getLogger(ITTestDateTypesWeekFormatsJodaTime.class);

  private final String format;
  private final FormatterAndType formatter;

  @Rule
  public final TestRule TIMEOUT = TestTools.getTimeoutRule(300, TimeUnit.SECONDS);

  public ITTestDateTypesWeekFormatsJodaTime(String format) {
    this.format = format;
    this.formatter = FormatterAndType.getFormatterAndType(format);
  }

  @Parameters
  public static Collection<Object[]> data() {
    final List<Object[]> data = new ArrayList();
    data.add(new Object[]{"basicWeekDate"});                       // xxxx’W'wwe
    data.add(new Object[]{"basic_week_date"});
    data.add(new Object[]{"basicWeekDateTime"});                   // xxxx’W'wwe’T'HHmmss.SSSZ
    data.add(new Object[]{"basic_week_date_time"});
    data.add(new Object[]{"basicWeekDateTimeNoMillis"});          // xxxx’W'wwe’T'HHmmssZ
    data.add(new Object[]{"basic_week_date_time_no_millis"});
    data.add(new Object[]{"weekyear"});
    data.add(new Object[]{"week_year"});
    data.add(new Object[]{"weekyear_week"});
    data.add(new Object[]{"weekyearWeek"});
    data.add(new Object[]{"weekyear_week_day"});
    data.add(new Object[]{"weekyearWeekDay"});
    return data;
  }

  @Test
  public void runTest() throws Exception {
    // These week related formats have been added to test cases dedicated for ES7 and bypassed here. - ES7 testcases -ITTestDateTypesWeekDateFormatsJavaTime/ITTestDateTypesWeekDateTimeFormatsJavaTime
    assumeFalse(elastic.getMinVersionInCluster().getMajor() == 7);
    LocalDateTime dt = new LocalDateTime(System.currentTimeMillis());
    final String value1 = formatter.dateFormatString(dt);
    logger.info(value1);
    dt = dt.plusYears(1);
    final String value2 = formatter.dateFormatString(dt);
    logger.info(value2);

    final ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
      new ElasticsearchCluster.ColumnData("field", DATE, ImmutableMap.of("format", format), new Object[][]{
        {value1},
        {value2}
      })
    };
    loadWithRetry(schema, table, data);
    final String sql = "select field from elasticsearch." + schema + "." + table;
    testBuilder()
      .sqlQuery(sql)
      .ordered()
      .baselineColumns("field")
      .baselineValues(formatter.parse(value1))
      .baselineValues(formatter.parse(value2))
      .go();
    }
}
