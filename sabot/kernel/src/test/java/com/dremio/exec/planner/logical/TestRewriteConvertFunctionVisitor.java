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

public class TestRewriteConvertFunctionVisitor extends PlanTestBase {
  @Test
  public void testConvertFromRewrite() throws Exception {
    final String convertFromQuery = String.format(
      "SELECT account_id, convert_from(setting,'json') as setting1 \n" +
        "FROM %s",
      "cp.\"car-ownership.parquet\"");

    testPlanMatchingPatterns(
      convertFromQuery,
      new String[]{"ConvertFromJson\\(account_id=\\[\\$0], setting=\\[\\$1], " +
        "CONVERT_FROM_JSON_0=\\[CONVERT\\(CONVERT_FROM_JSON_0\\)], " +
        "conversions=\\[\\[originField='setting', inputField='CONVERT_FROM_JSON_0']]\\)"}
    );
  }

  @Test
  public void testConvertToRewrite() throws Exception {
    final String convertToQuery = String.format(
      "SELECT CONVERT_TO('{\"name\":\"John\", \"age\":30, \"car\":null}','json')"
    );
    testPlanMatchingPatterns(
      convertToQuery,
      new String[]{"Project\\(EXPR\\$0\\=\\[CONVERT_TOjson\\('\\{\"name\":\"John\", \"age\":30, \"car\":null\\}'" +
        ":VARCHAR\\(37\\)\\)\\]\\)"}
    );
  }
}
