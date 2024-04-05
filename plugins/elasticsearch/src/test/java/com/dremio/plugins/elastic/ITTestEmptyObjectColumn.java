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

import static com.dremio.plugins.elastic.ElasticsearchType.INTEGER;
import static com.dremio.plugins.elastic.ElasticsearchType.OBJECT;
import static com.dremio.plugins.elastic.ElasticsearchType.TEXT;

import com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType;
import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Test;

public class ITTestEmptyObjectColumn extends ElasticBaseTestQuery {

  private static final String NULL_COLUMN_FILE = "/json/nullcolumntest.json";

  @Before
  public void setup() throws Exception {
    ElasticsearchCluster.ColumnData[] data =
        new ElasticsearchCluster.ColumnData[] {
          new ElasticsearchCluster.ColumnData(
              "state", TEXT, ImmutableMap.of("index", "false"), null),
          new ElasticsearchCluster.ColumnData("state_analyzed", TEXT, null),
          new ElasticsearchCluster.ColumnData("nullint", INTEGER, null),
          new ElasticsearchCluster.ColumnData("emptyStruct", OBJECT, null),
        };
    elastic.load(schema, table, data);
    elastic.dataFromFile(schema, table, NULL_COLUMN_FILE);
  }

  @Test
  public void testSelectEmptyStructColumn() throws Exception {
    final String query = "select emptyStruct from elasticsearch." + schema + "." + table;
    errorMsgWithTypeTestHelper(
        query, ErrorType.VALIDATION, "Column 'emptyStruct' not found in any table");
  }
}
