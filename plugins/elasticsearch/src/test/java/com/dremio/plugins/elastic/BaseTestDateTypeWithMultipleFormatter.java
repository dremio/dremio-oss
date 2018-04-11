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

import java.io.IOException;
import java.sql.Timestamp;

import org.joda.time.LocalDateTime;

import com.google.common.collect.ImmutableMap;

/**
 * Utilities to preopulate various dates and times.
 */
public class BaseTestDateTypeWithMultipleFormatter extends ElasticBaseTestQuery {

  protected static final long DATE_TIME_LONG = 1974231927;
  protected static final String DATE_TIME_STRING = "1974231927";
  protected static final String DATE_TIME_STRING_2 = "1970-01-23T20:23:51.927";
  protected static final LocalDateTime DATE_TIME_RESULT = new LocalDateTime(Timestamp.valueOf("1970-01-23 20:23:51.927"));

  /**
   * Testing date formats:  "basic_date||year_month||year"
   */
  protected void populateDateFormatter() throws IOException {
    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
      new ElasticsearchCluster.ColumnData("field", DATE, ImmutableMap.of("format", "basic_date||year_month||year"), new Object[][]{
        {"20161209"},
        {"20171011"},
        {"2017-01"},
        {"2017"}
      })
    };
    elastic.load(schema, table, data);
  }

  /**
   * Testing default formats:  "strict_date_optional_time||epoch_millis"
   */
  protected void populateDefaultFormatter() throws IOException {
    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
      new ElasticsearchCluster.ColumnData("field", DATE, new Object[][]{
        {DATE_TIME_STRING},
        {DATE_TIME_STRING_2},
        {DATE_TIME_LONG}
      })
    };
    elastic.load(schema, table, data);
  }

  protected void populateCustomFormatter() throws IOException {
    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
      new ElasticsearchCluster.ColumnData("field", DATE, ImmutableMap.of("format", "MM-dd-yyyy||strict_date_optional_time"), new Object[][]{
        {DATE_TIME_STRING_2},
        {"10-12-2017"},
        {"01-23-1970"},
        {"10-12-2017"},
      })
    };
    elastic.load(schema, table, data);
  }

  /**
   * Testing formats with mix of default/date/timestamp:  "strict_date_optional_time||year||epoch_millis"
   */
  protected void populateComplexFormatter() throws IOException {
    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
      new ElasticsearchCluster.ColumnData("field", DATE, ImmutableMap.of("format", "strict_date_optional_time||year||epoch_millis"), new Object[][]{
        {DATE_TIME_STRING},
        {DATE_TIME_STRING_2},
        {DATE_TIME_LONG},
        {"2017"}
      })
    };
    elastic.load(schema, table, data);
  }

  /**
   * Testing time formats:  "basic_time||basic_t_Time||time_no_millis"
   */
  protected void populateTimeFormatter() throws IOException {
    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
      new ElasticsearchCluster.ColumnData("field", DATE, ImmutableMap.of("format", "basic_time||basic_t_Time||time_no_millis"), new Object[][]{
        {"010203.123Z"},
        {"T010203.345Z"},
        {"01:01:15Z"},
        {"010203.345Z"}
      })
    };
    elastic.load(schema, table, data);
  }
}
