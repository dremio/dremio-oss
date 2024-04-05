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

public class TestFunctionsQueryPlan extends PlanTestBase {
  @Test
  public void testDremioProjectJoinTransposeRuleForNullableOperands() throws Exception {

    final String table1 = "cp.\"tpch/customer.parquet\"";
    final String table2 = "cp.\"tpch/orders.parquet\"";

    String query =
        String.format(
            "SELECT nvl(y.o_orderstatus, 'None') AS Lid FROM %s x LEFT JOIN ("
                + "SELECT \"a\".\"o_orderkey\", \"b\".\"o_orderstatus\"\n"
                + "FROM (SELECT \"o_orderkey\" FROM %s) \"a\" LEFT JOIN"
                + "(SELECT \"o_orderkey\", \"o_orderstatus\" FROM %s) \"b\"\n"
                + "ON \"a\".\"o_orderkey\" = \"b\".\"o_orderkey\"\n"
                + ") y ON \"x\".\"c_custkey\" = \"y\".\"o_orderkey\"",
            table1, table2, table2);

    // test plan to assert that operands are nullable
    testPhysicalPlan(query, "Lid=[CASE(IS NOT NULL($1), $1, 'None':VARCHAR(65536))]");

    // test execution
    test(query);
  }
}
