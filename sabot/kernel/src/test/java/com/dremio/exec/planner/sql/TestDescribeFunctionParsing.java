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

package com.dremio.exec.planner.sql;

import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.PlanTestBase;

public class TestDescribeFunctionParsing extends PlanTestBase {
  private static String createUdf1 = "CREATE FUNCTION\n" +
    "    protect_ssn (val VARCHAR)\n" +
    "    RETURNS VARCHAR\n" +
    "    RETURN\n" +
    "        SELECT\n" +
    "            CASE \n" +
    "                WHEN query_user() = 'dremio' \n" +
    "                    THEN val\n" +
    "                ELSE CONCAT('XXX-XX-',SUBSTR(val,8,4))\n" +
    "            END\n";

  private static String sourceUdf1 = "CREATE FUNCTION\n" +
    "    dfs_test.protect_ssn (val VARCHAR)\n" +
    "    RETURNS VARCHAR\n" +
    "    RETURN\n" +
    "        SELECT\n" +
    "            CASE \n" +
    "                WHEN query_user() = 'dremio' \n" +
    "                     THEN val\n" +
    "                ELSE CONCAT('XXX-XX-',SUBSTR(val,8,4))\n" +
    "            END\n";

  private static String describeQuery = "DESCRIBE FUNCTION %s";

  @BeforeClass
  public static void before() throws Exception {
    test(createUdf1);
    test(sourceUdf1);
  }

  @Test
  public void testDescribeFunction() throws Exception {
    test(String.format(describeQuery, "protect_ssn"));
    test(String.format(describeQuery, "dfs_test.protect_ssn"));
  }

}
