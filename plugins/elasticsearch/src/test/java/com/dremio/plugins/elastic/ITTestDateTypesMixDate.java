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

import com.dremio.common.util.TestTools;
import com.google.common.collect.ImmutableMap;

@RunWith(Parameterized.class)
public class ITTestDateTypesMixDate extends ElasticBaseTestQuery {

  private final String format;
  private final DateFormats.FormatterAndTypeMix formatter;
  private org.joda.time.format.DateTimeFormatter formatterToBaselineJD;
  private DateTimeFormatter formatterToBaselineJT;

  @Rule
  public final TestRule TIMEOUT = TestTools.getTimeoutRule(300, TimeUnit.SECONDS);

  public ITTestDateTypesMixDate(String format) {
    this.format = format;
    this.formatter = DateFormats.FormatterAndTypeMix.getFormatterAndType(format);
    this.formatterToBaselineJD = org.joda.time.format.DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS");
    this.formatterToBaselineJT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    List<Object[]> data = new ArrayList();
    data.add(new Object[]{"yyyyMMdd"});
    data.add(new Object[]{"yyyy-MM-dd"});
    data.add(new Object[]{"yyyy/MM/dd"});
    return data;
  }
  /*
  This test is specifically for ES6.8 in which, both joda time and java time are supported. If format is prefixed with 8 its considered to be java time other wise joda time.
   */
  @Test
  public void runTestDate() throws Exception {
    final LocalDateTime dt1 = LocalDateTime.of(LocalDate.now(), LocalTime.now(ZoneOffset.UTC));
    final LocalDateTime dt2 = dt1.plusYears(1);
    final String value1 = dt1.atZone(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern(getActualFormat(format)));
    final String value2 = dt2.atZone(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern(getActualFormat(format)));
    final ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
      new ElasticsearchCluster.ColumnData("field", DATE, ImmutableMap.of("format", format), new Object[][]{
        {value1},
        {value2}
      })
    };
    elastic.load(schema, table, data);
    String sql = "select CAST(field AS VARCHAR) as field from elasticsearch." + schema + "." + table;
    runTestBuilder(sql,value1,value2);
  }

  private void runTestBuilder(String sql, String value1, String value2) throws Exception {
    if(format.startsWith("8")) {
      testBuilder()
        .sqlQuery(sql)
        .ordered()
        .baselineColumns("field")
        .baselineValues(formatter.parse(value1 , formatterToBaselineJT).toString().replace("T", " ")+ ":00.000")
        .baselineValues(formatter.parse(value2 , formatterToBaselineJT).toString().replace("T", " ")+ ":00.000")
        .go();
    }
    else {
      testBuilder()
        .sqlQuery(sql)
        .ordered()
        .baselineColumns("field")
        .baselineValues(formatter.parse(value1 , formatterToBaselineJD).toString().replace("T", " "))
        .baselineValues(formatter.parse(value2 , formatterToBaselineJD).toString().replace("T", " "))
        .go();
    }
  }

  private String getActualFormat(String format) {
    final String actualFormat;
    if(format.startsWith("8")) {
      actualFormat = format.substring(1);
    }
    else {
      actualFormat = format;
    }
    return actualFormat;
  }
}
