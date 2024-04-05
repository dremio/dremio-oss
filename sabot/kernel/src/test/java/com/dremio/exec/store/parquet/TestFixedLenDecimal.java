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
import com.dremio.exec.planner.physical.PlannerSettings;
import org.junit.Test;

/** DRILL-4704 */
public class TestFixedLenDecimal extends BaseTestQuery {

  private static final String DATAFILE = "cp.\"parquet/fixedlenDecimal.parquet\"";

  @Test
  public void testNullCount() throws Exception {
    String query =
        String.format("select count(*) as c from %s where department_id is null", DATAFILE);
    testBuilder().sqlQuery(query).unOrdered().baselineColumns("c").baselineValues(1L).build().run();
  }

  @Test
  public void testNotNullCount() throws Exception {
    String query =
        String.format("select count(*) as c from %s where department_id is not null", DATAFILE);
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("c")
        .baselineValues(106L)
        .build()
        .run();
  }

  @Test
  public void testSimpleQueryWithCast() throws Exception {
    String query =
        String.format(
            "select cast(department_id as bigint) as c from %s where cast(employee_id as decimal) = 170",
            DATAFILE);
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("c")
        .baselineValues(80L)
        .build()
        .run();
  }

  @Test
  public void testSimpleQueryDrill4704Fix() throws Exception {
    String query =
        String.format(
            "select cast(department_id as bigint) as c from %s where employee_id = 170", DATAFILE);
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("c")
        .baselineValues(80L)
        .build()
        .run();
  }

  @Test
  public void testHash32FunctionWithSeed() throws Exception {
    try (AutoCloseable option = withOption(PlannerSettings.ENABLE_REDUCE_PROJECT, false)) {
      String query =
          String.format(
              "select hash32(department_id, hash32(employee_id)) hash1, hash32(employee_id) hash2 from %s where department_id is null",
              DATAFILE);
      testBuilder()
          .sqlQuery(query)
          .unOrdered()
          .baselineColumns("hash1", "hash2")
          .baselineValues(1216062191, 1216062191)
          .go();
    }
  }

  @Test
  public void testHash64FunctionWithSeed() throws Exception {
    try (AutoCloseable option = withOption(PlannerSettings.ENABLE_REDUCE_PROJECT, false)) {
      String query =
          String.format(
              "select hash64(department_id, hash64(employee_id)) hash1, hash64(employee_id) hash2 from %s where department_id is null",
              DATAFILE);
      testBuilder()
          .sqlQuery(query)
          .unOrdered()
          .baselineColumns("hash1", "hash2")
          .baselineValues(6356026919477319845L, 6356026919477319845L)
          .go();
    }
  }
}
