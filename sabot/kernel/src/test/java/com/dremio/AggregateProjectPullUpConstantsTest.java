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

public class AggregateProjectPullUpConstantsTest extends BaseTestQuery {

  /* Tests:
   * DX-39851: AssertionError: Cannot add expression of different type to set
   * and
   * CALCITE-2946: RelBuilder wrongly skips creation of Aggregate that prunes columns if input produces one row at most
   */
  @Test
  public void testPruneConstColWithOnlyOneInputRow() throws Exception {
    String query =
        ""
            + "select a from"
            + "(select 'a' as a, c from"
            + "(select count(*) as c from "
            + "(values (1)) as t(v) group by (v)))"
            + "group by a,c";

    testBuilder().sqlQuery(query).unOrdered().baselineColumns("a").baselineValues("a").go();
  }
}
