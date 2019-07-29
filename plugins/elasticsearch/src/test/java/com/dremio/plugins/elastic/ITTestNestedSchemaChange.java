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

import static com.dremio.TestBuilder.mapOf;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.TestBuilder;
import com.dremio.exec.ExecConstants;

/**
 * Tests for the nested data type.
 */
public class ITTestNestedSchemaChange extends ElasticBaseTestQuery {

  private static final Logger logger = LoggerFactory.getLogger(ITTestNestedSchemaChange.class);

  private static final String NEW_NESTED_COLUMN_1 = "/json/nested/new_nested_column/file1.json";
  private static final String NEW_NESTED_COLUMN_2 = "/json/nested/new_nested_column/file2.json";

  @BeforeClass
  public static void enableReattempts() throws Exception {
    test("ALTER SYSTEM SET \"" + ExecConstants.ENABLE_REATTEMPTS.getOptionName() + "\" = true");
  }

  @Test
  public void testNewNestedColumn() throws Exception {
    for (int i = 0; i < 100; i++) {
      elastic.dataFromFile(schema, table, NEW_NESTED_COLUMN_1);
    }
    elastic.dataFromFile(schema, table, NEW_NESTED_COLUMN_2);
    String query = String.format("select a from elasticsearch.%s.%s order by id", schema, table);
    TestBuilder testBuilder = testBuilder()
      .sqlQuery(query)
      .ordered()
      .baselineColumns("a");
    for (int i = 0; i < 100; i++) {
      testBuilder.baselineValues(mapOf("b", mapOf("c1", 1L)));
    }
    testBuilder.baselineValues(mapOf("b", mapOf("c2", 2L)));
    testBuilder.go();
  }
}
