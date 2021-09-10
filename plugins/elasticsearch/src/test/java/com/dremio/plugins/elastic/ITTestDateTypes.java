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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.joda.time.LocalDateTime;
import org.junit.AfterClass;
import org.junit.BeforeClass;
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

import ch.qos.logback.classic.Level;

/**
 *
 */
@RunWith(Parameterized.class)
public class ITTestDateTypes extends ElasticBaseTestQuery {

  private static final Logger logger = LoggerFactory.getLogger(ITTestDateTypes.class);

  private final String format;
  private final FormatterAndType formatter;
  private final static ch.qos.logback.classic.Logger rootLogger = ((ch.qos.logback.classic.Logger)org.slf4j.LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME));
  private static Level originalLogLevel;

  @Rule
  public final TestRule TIMEOUT = TestTools.getTimeoutRule(300, TimeUnit.SECONDS);

  @BeforeClass
  public static void initLogLevel() {
    originalLogLevel = rootLogger.getLevel();
    rootLogger.setLevel(Level.DEBUG);
  }

  @AfterClass
  public static void restoreLogLevel() {
    rootLogger.setLevel(originalLogLevel);
  }

  public ITTestDateTypes(String format) {
    this.format = format;
    this.formatter = DateFormats.FormatterAndType.getFormatterAndType(format);
  }

  @Parameters
  public static Collection<Object[]> data() {
    List<Object[]> data = new ArrayList();
    data.add(new Object[]{"basicTime"});                           // HHmmss.SSSZ
    data.add(new Object[]{"basic_time"});
    data.add(new Object[]{"basicTimeNoMillis"});                   // HHmmssZ
    data.add(new Object[]{"basic_time_no_millis"});
    data.add(new Object[]{"basicTTime"});                          // 'T’HHmmss.SSSZ

    // Ignored, DX-11933: basic_t_Time vs basic_t_time between ES 5 and 6
    //data.add(new Object[]{"basic_t_time"});

    data.add(new Object[]{"basicTTimeNoMillis"});
    data.add(new Object[]{"basic_t_time_no_millis"});
    data.add(new Object[]{"time"});
    data.add(new Object[]{"time_no_millis"});
    data.add(new Object[]{"timeNoMillis"});
    data.add(new Object[]{"tTime"});
    data.add(new Object[]{"t_time"});
    data.add(new Object[]{"tTimeNoMillis"});
    data.add(new Object[]{"t_time_no_millis"});
    data.add(new Object[]{"basicDate"});                           // yyyyMMdd
    data.add(new Object[]{"basic_date"});
    data.add(new Object[]{"basicDateTime"});                       // yyyyMMdd’T'HHmmss.SSSZ
    data.add(new Object[]{"basic_date_time"});
    data.add(new Object[]{"basicDateTimeNoMillis"});               // yyyyMMdd’T'HHmmssZ
    data.add(new Object[]{"basic_date_time_no_millis"});
    data.add(new Object[]{"basicOrdinalDate"});                    // yyyyDDD
    data.add(new Object[]{"basic_ordinal_date"});
    data.add(new Object[]{"basicOrdinalDateTime"});                // yyyyDDD’T'HHmmss.SSSZ
    data.add(new Object[]{"basic_ordinal_date_time"});
    data.add(new Object[]{"basicOrdinalDateTimeNoMillis"});        // yyyyDDD’T'HHmmssZ
    data.add(new Object[]{"basic_ordinal_date_time_no_millis"});
    data.add(new Object[]{"date"});                                // yyyy-MM-dd
    data.add(new Object[]{"dateHour"});
    data.add(new Object[]{"date_hour"});
    data.add(new Object[]{"dateHourMinute"});                      // yyyy-MM-dd'T'HH:mm
    data.add(new Object[]{"date_hour_minute"});
    data.add(new Object[]{"dateHourMinuteSecond"});                // yyyy-MM-dd'T'HH:mm:ss
    data.add(new Object[]{"date_hour_minute_second"});
    data.add(new Object[]{"dateHourMinuteSecondFraction"});        // yyyy-MM-dd'T'HH:mm:ss.SSS
    data.add(new Object[]{"date_hour_minute_second_fraction"});
    data.add(new Object[]{"dateHourMinuteSecondMillis"});          // yyyy-MM-dd'T'HH:mm:ss.SSS
    data.add(new Object[]{"date_hour_minute_second_millis"});
    data.add(new Object[]{"dateOptionalTime"});
    data.add(new Object[]{"date_optional_time"});
    data.add(new Object[]{"dateTime"});
    data.add(new Object[]{"date_time"});
    data.add(new Object[]{"dateTimeNoMillis"});
    data.add(new Object[]{"date_time_no_millis"});
    data.add(new Object[]{"hour"});
    data.add(new Object[]{"hourMinute"});
    data.add(new Object[]{"hour_minute"});
    data.add(new Object[]{"hourMinuteSecond"});
    data.add(new Object[]{"hour_minute_second"});
    data.add(new Object[]{"hourMinuteSecondFraction"});
    data.add(new Object[]{"hour_minute_second_fraction"});
    data.add(new Object[]{"hourMinuteSecondMillis"});
    data.add(new Object[]{"hour_minute_second_millis"});
    data.add(new Object[]{"ordinalDate"});
    data.add(new Object[]{"ordinal_date"});
    data.add(new Object[]{"ordinalDateTime"});
    data.add(new Object[]{"ordinal_date_time"});
    data.add(new Object[]{"ordinalDateTimeNoMillis"});
    data.add(new Object[]{"ordinal_date_time_no_millis"});
    data.add(new Object[]{"weekDate"});
    data.add(new Object[]{"week_date"});
    data.add(new Object[]{"weekDateTime"});
    data.add(new Object[]{"week_date_time"});
    data.add(new Object[]{"week_date_time_no_millis"});
    data.add(new Object[]{"weekDateTimeNoMillis"});
    data.add(new Object[]{"year"});
    data.add(new Object[]{"yearMonth"});
    data.add(new Object[]{"year_month"});
    data.add(new Object[]{"yearMonthDay"});
    data.add(new Object[]{"year_month_day"});
    return data;
  }

  @Test
  public void runTest() throws Exception {
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
