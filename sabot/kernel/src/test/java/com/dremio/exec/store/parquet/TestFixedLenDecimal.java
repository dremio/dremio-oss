/*
 * Copyright (C) 2017 Dremio Corporation
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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.exec.planner.physical.PlannerSettings;

/**
 * DRILL-4704
 */
public class TestFixedLenDecimal extends BaseTestQuery {

  private static final String DATAFILE = "cp.`parquet/fixedlenDecimal.parquet`";

  // enable decimal data type
  @BeforeClass
  public static void enableDecimalDataType() throws Exception {
    runSQL(String.format("alter system set `%s` = true", PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY));
  }

  @AfterClass
  public static void disableDecimalDataType() throws Exception {
    runSQL(String.format("alter system set `%s` = false", PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY));
  }

  @Test
  public void testNullCount() throws Exception {
    String query = String.format("select count(*) as c from %s where department_id is null", DATAFILE);
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("c")
      .baselineValues(1L)
      .build()
      .run();
  }

  @Test
  public void testNotNullCount() throws Exception {
    String query = String.format("select count(*) as c from %s where department_id is not null", DATAFILE);
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
    String query = String.format("select cast(department_id as bigint) as c from %s where cast(employee_id as decimal) = 170", DATAFILE);
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
    String query = String.format("select cast(department_id as bigint) as c from %s where employee_id = 170", DATAFILE);
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("c")
      .baselineValues(80L)
      .build()
      .run();
  }
}
