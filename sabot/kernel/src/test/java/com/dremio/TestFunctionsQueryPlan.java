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

  @Test
  public void testDremioProjectJoinTransposeRuleSameRowTypes() throws Exception {
    final String query =
        " SELECT  \n"
            + "      CASE WHEN NVL (Table2.col3, 0) = 1 THEN Table3.col1 ELSE Table1.col1 END AS col3\n"
            + "FROM (select 1 as col1, 2 as col2) Table1\n"
            + "      LEFT OUTER JOIN  \n"
            + "      (select 1 as col2, 2 as col3) as Table2\n"
            + "         ON Table1.col2 = Table2.col2\n"
            + "      LEFT OUTER JOIN (select 1 as col2, 2 as col1)  Table3\n"
            + "         ON Table3.col2 = Table1.col2";
    // test execution
    // Without the fix, this will fail with
    // (java.lang.AssertionError) Cannot add expression of different type to set:
    // set type is RecordType(BOOLEAN NOT NULL EXPR$0) NOT NULL
    // expression type is RecordType(BOOLEAN EXPR$0) NOT NULL
    test(query);
  }
}
