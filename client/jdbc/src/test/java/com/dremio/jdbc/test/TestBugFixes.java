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
package com.dremio.jdbc.test;

import org.junit.Test;

public class TestBugFixes extends JdbcTestQueryBase {

  @Test
  public void testDrill2503() throws Exception {
    final StringBuilder sb = new StringBuilder();
    sb.append(
        "SELECT "
        + "  CASE "
        + "    WHEN 'y' = 'x' THEN 0 "
        + "    WHEN 'y' = 'x' THEN 0 "
        + "    WHEN 'y' = 'x' THEN 0 "
        + "    WHEN 'y' = 'x' THEN 0 "
        + "    WHEN 'y' = 'x' THEN 0 "
        + "    WHEN 'y' = 'x' THEN 0 "
        + "    WHEN 'y' = 'x' THEN 0 "
        + "    WHEN 'y' = 'x' THEN 0 "
        + "    WHEN 'y' = 'x' THEN 0 "
        + "    WHEN 'y' = 'x' THEN 0 "
        + "    WHEN 'y' = 'x' THEN 0 "
        + "    WHEN 'y' = 'x' THEN 0 "
        + "    WHEN 'y' = 'x' THEN 0 "
        + "    WHEN 'y' = 'x' THEN 0 "
        + "    WHEN 'y' = 'x' THEN 0 "
        + "    WHEN 'y' = 'x' THEN 0 "
        + "    WHEN 'y' = 'x' THEN 0 "
        + "    WHEN 'y' = 'x' THEN 0 "
        + "    WHEN 'y' = 'x' THEN 0 "
        + "    WHEN 'y' = 'x' THEN 0 "
        + "    WHEN 'y' = 'x' THEN 0 "
        + "    WHEN 'y' = 'x' THEN 0 "
        + "    WHEN 'y' = 'x' THEN 0 "
        + "    WHEN 'y' = 'x' THEN 0 "
        + "    WHEN 'y' = 'x' THEN 0 "
        + "    WHEN 'y' = 'x' THEN 0 "
        + "    WHEN 'y' = 'x' THEN 0 "
        + "    WHEN 'y' = 'x' THEN 0 "
        + "    WHEN 'y' = 'x' THEN 0 "
        + "    WHEN 'y' = 'x' THEN 0 "
        + "    WHEN 'y' = 'x' THEN 0 "
        + "    WHEN 'y' = 'x' THEN 0 "
        + "    WHEN 'y' = 'x' THEN 0 "
        + "    WHEN 'y' = 'x' THEN 0 "
        + "    WHEN 'y' = 'x' THEN 0 "
        + "    WHEN 'y' = 'x' THEN 0 "
        + "    WHEN 'y' = 'x' THEN 0 "
        + "    WHEN 'y' = 'x' THEN 0 "
        + "    WHEN 'y' = 'x' THEN 0 "
        + "    WHEN 'y' = 'x' THEN 0 "
        + "    WHEN 'y' = 'x' THEN 0 "
        + "    WHEN 'y' = 'x' THEN 0 "
        + "    WHEN 'y' = 'x' THEN 0 "
        + "    WHEN 'y' = 'x' THEN 0 "
        + "    WHEN 'y' = 'x' THEN 0 "
        + "    WHEN 'y' = 'x' THEN 0 "
        + "    WHEN 'y' = 'x' THEN 0 "
        + "  END  "
        + "  FROM INFORMATION_SCHEMA.CATALOGS "
    );

    testQuery(sb.toString());
  }
}
