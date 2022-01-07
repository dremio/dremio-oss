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

import com.dremio.common.util.TestTools;
import com.google.common.collect.ImmutableMap;

@RunWith(Parameterized.class)
public class ITTestDateTypesTimeJavaTime extends ElasticBaseTestQuery {

  private final String format;
  private final DateFormats.FormatterAndTypeJavaTime formatter;
  private final DateTimeFormatter dateTimeFormatter;

  @Rule
  public final TestRule TIMEOUT = TestTools.getTimeoutRule(300, TimeUnit.SECONDS);

  public ITTestDateTypesTimeJavaTime(String format) {
    this.format = format ;
    this.dateTimeFormatter = DateTimeFormatter.ofPattern(format);
    this.formatter = DateFormats.FormatterAndTypeJavaTime.getFormatterAndType(format);
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    List<Object[]> data = new ArrayList();
    data.add(new Object[]{"HH:mm:ss.SSSz"});
    data.add(new Object[]{"HHmmss.SSSz"});
    data.add(new Object[]{"HHmmss.SSS"});
    data.add(new Object[]{"HH:mm:ss.SSSzz"});
    data.add(new Object[]{"HH:mm:ss.SSSzzz"});
    return data;
  }

  @Test
  public void runTestDate() throws Exception {
    // Need to bypass this test case for elastic search version lower than ES7.
    assumeFalse(elastic.getMinVersionInCluster().getMajor() < 7);
    final LocalDateTime dt1 = LocalDateTime.of(LocalDate.now(), LocalTime.now(ZoneOffset.UTC));
    final LocalDateTime dt2 = dt1.plusYears(1);
    final String value1 = dt1.atZone(ZoneOffset.UTC).format(dateTimeFormatter);
    final String value2 = dt2.atZone(ZoneOffset.UTC).format(dateTimeFormatter);
    final ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
      new ElasticsearchCluster.ColumnData("field", DATE, ImmutableMap.of("format", format), new Object[][]{
        {value1},
        {value2}
      })
    };

    elastic.load(schema, table, data);
    final String sql = "select CAST(field AS VARCHAR) as field from elasticsearch." + schema + "." + table;
    testBuilder()
      .sqlQuery(sql)
      .ordered()
      .baselineColumns("field")
      .baselineValues(formatter.parse(value1, DateTimeFormatter.ofPattern("HH:mm:ss.SSS")).toString().replace("T", " "))
      .baselineValues(formatter.parse(value2, DateTimeFormatter.ofPattern("HH:mm:ss.SSS")).toString().replace("T", " "))
      .go();
  }
}
