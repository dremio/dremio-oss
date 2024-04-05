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
package com.dremio;

import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.physical.PlannerSettings;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TestCaseNormalQueriesJava extends TestAbstractCaseNormalQueries {
  @BeforeClass
  public static void setup() throws Exception {
    testNoResult(
        "ALTER SESSION SET \"%s\" = 1", PlannerSettings.CASE_EXPRESSIONS_THRESHOLD.getOptionName());
    testNoResult(
        "ALTER SESSION SET \"%s\" = 500",
        ExecConstants.CODE_GEN_CONSTANT_ARRAY_THRESHOLD.getOptionName());
    testNoResult(
        "ALTER SESSION SET \"%s\" = 'Java'", ExecConstants.QUERY_EXEC_OPTION.getOptionName());
  }

  @AfterClass
  public static void tearDown() throws Exception {
    testNoResult(
        "ALTER SESSION RESET \"%s\"", PlannerSettings.CASE_EXPRESSIONS_THRESHOLD.getOptionName());
    testNoResult(
        "ALTER SESSION RESET \"%s\"",
        ExecConstants.CODE_GEN_CONSTANT_ARRAY_THRESHOLD.getOptionName());
    testNoResult("ALTER SESSION RESET \"%s\"", ExecConstants.QUERY_EXEC_OPTION.getOptionName());
  }
}
