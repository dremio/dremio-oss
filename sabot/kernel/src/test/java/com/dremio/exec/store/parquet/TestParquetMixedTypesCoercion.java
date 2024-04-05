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
package com.dremio.exec.store.parquet;

import com.dremio.BaseTestQuery;
import com.dremio.common.util.TestTools;
import com.dremio.exec.ExecConstants;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.arrow.vector.util.Text;
import org.junit.Test;

public class TestParquetMixedTypesCoercion extends BaseTestQuery {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Test
  public void testCtasOutputCoercion() throws Exception {
    try (AutoCloseable ac = withOption(ExecConstants.ENABLE_PARQUET_MIXED_TYPES_COERCION, true)) {
      final String testResPath = TestTools.getWorkingPath() + "/src/test/resources";
      testBuilder()
          .sqlQuery(
              "select A, B from dfs.\"%s/parquet/mixed_types_coercion/ctas_output\"", testResPath)
          .unOrdered()
          .baselineColumns("A", "B")
          .baselineValues(1, "X")
          .baselineValues(2, OBJECT_MAPPER.writeValueAsString(ImmutableMap.of("B", "X")))
          .baselineValues(
              3, OBJECT_MAPPER.writeValueAsString(ImmutableList.of("hello", "world", "there")))
          .build()
          .run();
    }
  }

  @Test
  public void testSingleFileCoercion() throws Exception {
    try (AutoCloseable ac = withOption(ExecConstants.ENABLE_PARQUET_MIXED_TYPES_COERCION, true)) {
      String nullValue = OBJECT_MAPPER.writeValueAsString(null);
      String val11 = OBJECT_MAPPER.writeValueAsString("abc");
      String val21 =
          OBJECT_MAPPER.writeValueAsString(ImmutableMap.of("mixed1", "def", "mixed2", "ghi"));
      String val31 = OBJECT_MAPPER.writeValueAsString("abc");
      String val32 = OBJECT_MAPPER.writeValueAsString("jkl");
      String val41 = OBJECT_MAPPER.writeValueAsString("abc");
      String val42 = OBJECT_MAPPER.writeValueAsString(ImmutableList.of(1.0, 2.0, 3.0));
      Map<String, Text> val53 =
          ImmutableMap.of(
              "mixed1", new Text(OBJECT_MAPPER.writeValueAsString("def")),
              "mixed2", new Text(OBJECT_MAPPER.writeValueAsString("ghi")));
      String val54 = OBJECT_MAPPER.writeValueAsString(true);
      Map<String, Text> val63 =
          ImmutableMap.of(
              "mixed1", new Text(OBJECT_MAPPER.writeValueAsString(5.0)),
              "mixed2", new Text(OBJECT_MAPPER.writeValueAsString(ImmutableList.of(6.0, 7.0))));
      String val64 = OBJECT_MAPPER.writeValueAsString(8.0);

      final String testResPath = TestTools.getWorkingPath() + "/src/test/resources";
      testBuilder()
          .sqlQuery(
              "select * from dfs.\"%s/parquet/mixed_types_coercion/single_file\"", testResPath)
          .unOrdered()
          .baselineColumns("_id", "mixedColumn", "mixedColumn_2", "mixedColumn_3", "mixedColumn_4")
          .baselineValues(1.0, val11, nullValue, null, nullValue)
          .baselineValues(2.0, val21, nullValue, null, nullValue)
          .baselineValues(3.0, val31, val32, null, nullValue)
          .baselineValues(4.0, val41, val42, null, nullValue)
          .baselineValues(5.0, nullValue, nullValue, val53, val54)
          .baselineValues(6.0, nullValue, nullValue, val63, val64)
          .build()
          .run();
    }
  }
}
