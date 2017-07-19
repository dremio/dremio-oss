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
package com.dremio.exec.fn.impl;

import org.junit.Test;

import com.dremio.BaseTestQuery;

public class TestTrigFunctions extends BaseTestQuery {

  @Test
  public void testCot() throws Exception {
    final String query = "select cot(radians(30)) as cot_result from (values(1))";
    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("cot_result")
        .baselineValues(1.7320508075688774)
        .go();
  }
}
