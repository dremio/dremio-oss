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

import static com.dremio.plugins.elastic.ElasticsearchType.BOOLEAN;
import static com.dremio.plugins.elastic.ElasticsearchType.DATE;
import static com.dremio.plugins.elastic.ElasticsearchType.DOUBLE;
import static com.dremio.plugins.elastic.ElasticsearchType.FLOAT;
import static com.dremio.plugins.elastic.ElasticsearchType.GEO_POINT;
import static com.dremio.plugins.elastic.ElasticsearchType.INTEGER;
import static com.dremio.plugins.elastic.ElasticsearchType.LONG;
import static com.dremio.plugins.elastic.ElasticsearchType.TEXT;

import java.io.IOException;
import java.text.ParseException;

import org.joda.time.format.DateTimeFormatter;
import org.junit.Before;
import org.junit.Test;

import com.dremio.exec.expr.fn.impl.DateFunctionsUtils;
import com.google.common.collect.ImmutableMap;

public class ITTestAllElasticTypes extends ElasticBaseTestQuery {

  DateTimeFormatter formatter = DateFunctionsUtils.getISOFormatterForFormatString("YYYY-MM-DD HH:MI:SS");
  // set in @Before method
  private String ELASTIC_TABLE = null;

  @Before
  public void loadTable() throws IOException, ParseException {
    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
        new ElasticsearchCluster.ColumnData("business_id", TEXT, new Object[][]{
            {"12345"},
            {"abcde"},
            {"7890"},
            {"12345"},
            {"xyz"}
        }),
        new ElasticsearchCluster.ColumnData("full_address", TEXT, new Object[][]{
            {"12345 A Street, Cambridge, MA"},
            {"987 B Street, San Francisco, CA"},
            {"987 B Street, San Diego, CA"},
            {"12345 A Street, Cambridge, MA"},
            {"12345 C Avenue, San Francisco, CA"}
        }),
        new ElasticsearchCluster.ColumnData("city", TEXT, ImmutableMap.of("index", "false"),
            new Object[][]{
                {"Cambridge"},
                {"San Francisco"},
                {"San Diego"},
                {"Cambridge"},
                {"San Francisco"}
            }),
        new ElasticsearchCluster.ColumnData("city_analyzed", TEXT,
            new Object[][]{
                {"Cambridge"},
                {"San Francisco"},
                {"San Diego"},
                {"Cambridge"},
                {"San Francisco"}
            }),
        new ElasticsearchCluster.ColumnData("state", TEXT, ImmutableMap.of("index", "false"),
            new Object[][]{
                {"MA"},
                {"CA"},
                {"CA"},
                {"MA"},
                {"CA"}
            }),
        new ElasticsearchCluster.ColumnData("state_analyzed", TEXT, new Object[][]{
            {"MA"},
            {"CA"},
            {"CA"},
            {"MA"},
            {"CA"}
        }),
        new ElasticsearchCluster.ColumnData("review_count", INTEGER, new Object[][]{
            {11},
            {22},
            {33},
            {11},
            {1}
        }),
        new ElasticsearchCluster.ColumnData("review_count_bigint", LONG, new Object[][]{
            {11L},
            {22L},
            {33L},
            {11L},
            {1L}
        }),
        new ElasticsearchCluster.ColumnData("stars", FLOAT, new Object[][]{
            {4.5f},
            {3.5f},
            {5.0f},
            {4.5f},
            {1.0f}
        }),
        new ElasticsearchCluster.ColumnData("stars_double", DOUBLE, new Object[][]{
            {4.5D},
            {3.5D},
            {5.0D},
            {4.5D},
            {1.0D}
        }),
        new ElasticsearchCluster.ColumnData("location_field", GEO_POINT, new Object[][]{
            {ImmutableMap.of("lat", 11, "lon", 11), ImmutableMap.of("lat", -11, "lon", -11)},
            {ImmutableMap.of("lat", 22, "lon", 22), ImmutableMap.of("lat", -22, "lon", -22)},
            {ImmutableMap.of("lat", 33, "lon", 33), ImmutableMap.of("lat", -33, "lon", -33)},
            {ImmutableMap.of("lat", 44, "lon", 44), ImmutableMap.of("lat", -44, "lon", -44)},
            {ImmutableMap.of("lat", 55, "lon", 55), ImmutableMap.of("lat", -55, "lon", -55)}
        }),
        new ElasticsearchCluster.ColumnData("name", TEXT, new Object[][]{
            {"Store in Cambridge"},
            {"Store in San Francisco"},
            {"Store in San Diego"},
            {"Same store in Cambridge"},
            {"New store in San Francisco"},

        }),
        new ElasticsearchCluster.ColumnData("open", BOOLEAN, new Object[][]{
            {true},
            {true},
            {false},
            {true},
            {false}
        }),
        new ElasticsearchCluster.ColumnData("datefield", DATE, new Object[][]{
            {com.dremio.common.util.DateTimes.toDateTime(formatter.parseLocalDateTime("2014-02-10 10:50:42"))},
            {com.dremio.common.util.DateTimes.toDateTime(formatter.parseLocalDateTime("2014-02-11 10:50:42"))},
            {com.dremio.common.util.DateTimes.toDateTime(formatter.parseLocalDateTime("2014-02-12 10:50:42"))},
            {com.dremio.common.util.DateTimes.toDateTime(formatter.parseLocalDateTime("2014-02-11 10:50:42"))},
            {com.dremio.common.util.DateTimes.toDateTime(formatter.parseLocalDateTime("2014-02-10 10:50:42"))}
        })
    };

    elastic.load(schema, table, data);
    ELASTIC_TABLE = String.format("elasticsearch.%s.%s", schema, table);
  }

  // TODO - see if source data is what is getting to groovy
  // try inserting string into a field annotated as int
  // test pushdown of date expressions
  //    - simple case, does this work with the old method
  //    - we should probably disallow in scripts, see if we get the source string or a date class in groovy
  @Test
  public void testCastRoundTripsToVarchar() throws Exception {
    String INT_TYPE = "integer";
    String DOUBLE_TYPE = "double";
    String BIGINT_TYPE = "bigint";
    String FLOAT_TYPE = "float";
    String SHORT_VARCHAR = "varchar(2)";
    String LONG_VARCHAR = "varchar(65000)";
    String BOOLEAN_TYPE = "boolean";

    String[] allCastTypes = {INT_TYPE,       BIGINT_TYPE,           DOUBLE_TYPE,    FLOAT_TYPE, SHORT_VARCHAR,  LONG_VARCHAR, BOOLEAN_TYPE};
    String[] sourceFields = {"review_count", "review_count_bigint", "stars_double", "stars",    "state",        "city",       "\"open\"" };

    // Test round trips to varchar
    for (int i = 0; i < allCastTypes.length; i++) {
      String exprs = " cast(cast(%s as %s) as %s) = %s as outCol";
      testBuilder()
          .sqlQuery(String.format("select " + exprs + " from %s", sourceFields[i], LONG_VARCHAR, allCastTypes[i], sourceFields[i], ELASTIC_TABLE))
          .ordered()
          .baselineColumns("outCol")
          .baselineValues(true)
          .baselineValues(true)
          .baselineValues(true)
          .baselineValues(true)
          .baselineValues(true)
          .go();
    }
  }
}
