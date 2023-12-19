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
package com.dremio.exec.planner.logical;

import org.junit.Test;

import com.dremio.PlanTestBase;

public class TestProjectPullUpConstTypeCast extends PlanTestBase {

  @Test
  public void testSimpleSelectExprList() throws Exception {
    String sql = "SELECT EXTRACT(hour from ts) FROM (\n" +
      "    SELECT ts FROM (VALUES (TIMESTAMP '2023-02-01 12:30:00'),(TIMESTAMP '2023-02-01 01:30:00')) as T3(ts)\n" +
      ") WHERE ts = '2023-02-01 12:30:00'";

    testPlanMatchingPatterns(
      sql,
      new String[] { "Project\\(EXPR\\$0=\\[12:BIGINT\\]\\) : rowType = RecordType\\(BIGINT EXPR\\$0\\)" });
  }
}
