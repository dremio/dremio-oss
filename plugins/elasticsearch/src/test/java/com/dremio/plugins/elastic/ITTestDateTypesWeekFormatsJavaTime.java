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

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.dremio.common.util.TestTools;
import com.google.common.collect.ImmutableMap;

/**
 * Tests for week related date/datetime formats using javatime.
 */
@RunWith(Parameterized.class)
public class ITTestDateTypesWeekFormatsJavaTime extends ElasticBaseTestQuery {

  private final String format;
  private final DateFormats.FormatterAndTypeJavaTime formatter;
  private static final String basicWeekDayFormat = "[[YYYY]'W'wwe]";
  private static final String weekYearDateFormat = "[[YYYY]-'W'ww-e]";
  private static final String basicWeekDatetimeFormat = "[[YYYY]'W'wwe['T']HHmmss.SSS[z]]";
  private static final String basicWeekDatetimeNoMillisFormat = "[[YYYY]'W'wwe['T']HHmmss[z]]";

  @Rule
  public final TestRule timeoutRule = TestTools.getTimeoutRule(300, TimeUnit.SECONDS);

  public ITTestDateTypesWeekFormatsJavaTime(String format) {
    this.format = format;
    this.formatter = DateFormats.FormatterAndTypeJavaTime.getFormatterAndType(format);
  }

  @Parameters
  public static Collection<Object[]> data() {
    final List<Object[]> data = new ArrayList();
    data.add(new Object[]{"basicWeekDate"});                       // xxxx’W'wwe
    data.add(new Object[]{"basic_week_date"});
    data.add(new Object[]{"weekyear_week_day"});
    data.add(new Object[]{"weekyearWeekDay"});
    data.add(new Object[]{"basicWeekDateTime"});                   // xxxx’W'wwe’T'HHmmss.SSSZ
    data.add(new Object[]{"basic_week_date_time"});
    data.add(new Object[]{"basicWeekDateTimeNoMillis"});          // xxxx’W'wwe’T'HHmmssZ
    data.add(new Object[]{"basic_week_date_time_no_millis"});
    return data;
  }

  @Test
  public void runTest() throws Exception {
    //These test cases are dedicated for ES7. Equivalent testcases for ES6 are covered in ITTestDateTypesWeekFormatsJodaTime
    assumeFalse(elastic.getMinVersionInCluster().getMajor() < 7);
    final DateTimeFormatter dateTimeFormatter;
    final String appendPattern;
    final String removePattern;
    switch(format){
      case "basicWeekDate" :                       // xxxx'W'wwe
      case "basic_week_date" :
        dateTimeFormatter = DateTimeFormatter.ofPattern(basicWeekDayFormat);
        appendPattern = "";
        removePattern = "T00:00";
        break;
      case "weekyear_week_day" :                  // xxxx-'W'ww-e
      case "weekyearWeekDay" :
        dateTimeFormatter = DateTimeFormatter.ofPattern(weekYearDateFormat);
        appendPattern = "";
        removePattern = "T00:00";
        break;
      case "basicWeekDateTime" :                   // xxxx’W'wwe’T'HHmmss.SSSZ
      case "basic_week_date_time" :
        dateTimeFormatter = DateTimeFormatter.ofPattern(basicWeekDatetimeFormat);
        appendPattern = "";
        removePattern = "";
        break;
      case "basicWeekDateTimeNoMillis" :          // xxxx’W'wwe’T'HHmmssZ
      case "basic_week_date_time_no_millis" :
        dateTimeFormatter = DateTimeFormatter.ofPattern(basicWeekDatetimeNoMillisFormat);
        appendPattern = ".000";
        removePattern = "";
        break;
      default:
        throw new IllegalStateException("Unexpected value: " + format);
    }
    final LocalDateTime dt1 = LocalDateTime.of(LocalDate.of(2021, 07, 21), LocalTime.of(13, 00, 05 , 1230000));
    final LocalDateTime dt2 = dt1.plusYears(1);
    final String value1 = dt1.atZone(ZoneOffset.UTC).format(dateTimeFormatter);
    final String value2 = dt2.atZone(ZoneOffset.UTC).format(dateTimeFormatter);
    final ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
      new ElasticsearchCluster.ColumnData("field", DATE, ImmutableMap.of("format", format), new Object[][]{
        {value1},
        {value2}
      })
    };
    loadWithRetry(schema, table, data);
    final String sql = "select CAST(field AS VARCHAR) as field from elasticsearch." + schema + "." + table;
    testBuilderVerification(sql, value1, value2 , dateTimeFormatter, removePattern, appendPattern);
  }

  public void testBuilderVerification(final String sql, final String value1, final String value2, final DateTimeFormatter dateTimeFormatter, final String removePattern, final String appendPattern) throws Exception {
    testBuilder()
      .sqlQuery(sql)
      .ordered()
      .baselineColumns("field")
      .baselineValues(formatter.parse(value1, dateTimeFormatter).toString().replace(removePattern, "").replace("T", " ") + appendPattern)
      .baselineValues(formatter.parse(value2, dateTimeFormatter).toString().replace(removePattern, "").replace("T", " ") + appendPattern)
      .go();
  }
}
