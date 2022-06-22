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
public class ITTestDateTypesJavaTimeDate extends ElasticBaseTestQuery {

  private final String format;
  private final DateFormats.FormatterAndTypeJavaTime formatter;
  private final DateTimeFormatter dateTimeFormatter;

  @Rule
  public final TestRule timeoutRule = TestTools.getTimeoutRule(300, TimeUnit.SECONDS);

  public ITTestDateTypesJavaTimeDate(String format) {
    this.format = format ;
    this.dateTimeFormatter = DateTimeFormatter.ofPattern(getActualFormat(format));
    this.formatter = DateFormats.FormatterAndTypeJavaTime.getFormatterAndType(format);
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    List<Object[]> data = new ArrayList();
    data.add(new Object[]{"yyyyMMdd"});
    data.add(new Object[]{"yyyy-MM-dd"});
    data.add(new Object[]{"yyyy/MM/dd"});
    data.add(new Object[]{"8yyyy-MM-dd"});
    data.add(new Object[]{"uuuu-MM-dd"});
    data.add(new Object[]{"8uuuu-MM-dd"});
    data.add(new Object[]{"--MM-dd"});
    data.add(new Object[]{"--dd-MM"});
    data.add(new Object[]{"dd-MM--"});
    return data;
  }

  @Test
  public void runTestDate() throws Exception {
    // Format with prefix 8 is applicable for ES 6.8 and ES 7 only.
    assumeFalse(( format.startsWith("8")) && !(enable7vFeatures || enable68vFeatures));
    // Format with "u" as year is applicable for ES 7 only.
    assumeFalse(( format.contains("u") || format.contains("c")) && !(enable7vFeatures));
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
      .baselineValues(formatter.parse(value1, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm.SSS")).toString().replace("T", " ") + ":00.000")
      .baselineValues(formatter.parse(value2, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm.SSS")).toString().replace("T", " ") + ":00.000")
      .go();
  }
}
