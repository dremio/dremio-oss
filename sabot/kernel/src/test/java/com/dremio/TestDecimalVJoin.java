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

import org.junit.Test;

public class TestDecimalVJoin extends BaseTestQuery {

  @Test
  public void testVJoinDecimal() throws Exception {
    final String query =
        "select count(*) from "
            + "(select EXPR$0 as d1 from cp.\"join/decimal_join/decimals-500.parquet\")t1"
            + " join "
            + "(select EXPR$0 as d2 from cp.\"join/decimal_join/decimals-a.parquet\")t2"
            + " on t1.d1 = t2.d2";
    testBuilder().sqlQuery(query).unOrdered().baselineColumns("EXPR$0").baselineValues(500L).go();
  }
}
