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

import org.junit.Test;

import com.dremio.PlanTestBase;
import com.dremio.exec.planner.physical.PlannerSettings;


public class TestAliasExpansion extends PlanTestBase {

  @Test
  public void testSimpleAliasExpansionForWhereClause() throws Exception {
    try (AutoCloseable c = withOption(PlannerSettings.EXTENDED_ALIAS, true)) {
      String query = "SELECT c_custkey AS c FROM cp.tpch.\"customer.parquet\" WHERE c = 3 ";
      testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("c")
        .baselineValues(3)
        .go();
    }
  }

  @Test
  public void testSimpleAliasExpansionForSelect() throws Exception {
    try (AutoCloseable c = withOption(PlannerSettings.EXTENDED_ALIAS, true)) {
      String query = "SELECT c_name AS n, upper(n)  FROM cp.tpch.\"customer.parquet\" ORDER BY n LIMIT 3";
      testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("n", "EXPR$1")
        .baselineValues("Customer#000000001", "CUSTOMER#000000001")
        .baselineValues("Customer#000000002", "CUSTOMER#000000002")
        .baselineValues("Customer#000000003", "CUSTOMER#000000003")
        .go();
    }
  }

  @Test
  public void testSimpleAliasExpansionForOn() throws Exception {
    try (AutoCloseable c = withOption(PlannerSettings.EXTENDED_ALIAS, true)) {
      String query = "SELECT c_custkey AS c, o_orderkey o  FROM cp.tpch.\"customer.parquet\" " +
        "JOIN cp.tpch.\"orders.parquet\" " +
        " ON c = o" +
        " ORDER BY c LIMIT 3";

      testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("c", "o")
        .baselineValues(1, 1)
        .baselineValues(2, 2)
        .baselineValues(3, 3)
        .go();
    }
  }


  @Test
  public void testAliasPreferenceForInnerSubQuery() throws Exception {
    try (AutoCloseable c = withOption(PlannerSettings.EXTENDED_ALIAS, true)) {
      String query = "SELECT c_custkey AS c, lower(c) FROM ( SELECT c_custkey, c_mktsegment AS c " +
        "FROM cp.tpch.\"customer.parquet\") " +
        "ORDER BY c LIMIT 3";

      testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("c", "EXPR$1")
        .baselineValues(1, "building")
        .baselineValues(2, "automobile")
        .baselineValues(3, "automobile")
        .go();
    }
  }

  @Test
  public void testAliasPreferenceFilterAppliedOnInnerSubQuery() throws Exception {
    try (AutoCloseable c = withOption(PlannerSettings.EXTENDED_ALIAS, true)) {
      String query = "select  c_name as n, n from (SELECT c_mktsegment as n, c_name " +
        "FROM cp.tpch.\"customer.parquet\") WHERE n = 'BUILDING' " +
        "ORDER BY c_name LIMIT 3";

      testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("n", "n1")
        .baselineValues("Customer#000000001", "BUILDING")
        .baselineValues("Customer#000000008", "BUILDING")
        .baselineValues("Customer#000000011", "BUILDING")
        .go();
    }
  }

  @Test
  public void testCircularAliasReference() throws Exception {
    try (AutoCloseable c = withOption(PlannerSettings.EXTENDED_ALIAS, true)) {
      String query = "SELECT c_custkey + 1 AS c_custkey FROM cp.tpch.\"customer.parquet\"" +
        " WHERE c_custkey = 1   ORDER BY c_custkey" +
        " lIMIT 3";

      testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("c_custkey")
        .baselineValues(2)
        .go();
    }
  }

  @Test
  public void testOuterQueryReferenceInnerAlias() throws Exception {
    try (AutoCloseable ignore = withOption(PlannerSettings.EXTENDED_ALIAS, true)) {
      String query = "select c_mktsegment from (SELECT c_mktsegment, c_name as aliasName " +
        "FROM cp.tpch.\"customer.parquet\") " +
        "WHERE aliasName = 'Customer#000000001' " +
        "ORDER BY aliasName " +
        "LIMIT 3";

      testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("c_mktsegment")
        .baselineValues("BUILDING")
        .go();
    }
  }

  @Test
  public void testAliasWithSubqueryJoinOn() throws Exception {
    try (AutoCloseable ignore = withOption(PlannerSettings.EXTENDED_ALIAS, true)) {
      String query = "select aliasName from " +
        "(SELECT c_name as aliasName, c_custkey " +
        "FROM cp.tpch.\"customer.parquet\") " +
        "JOIN cp.tpch.\"customer.parquet\" " +
        "ON aliasName = c_name " +
        "ORDER BY aliasName " +
        "LIMIT 3";

      testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("aliasName")
        .baselineValues("Customer#000000001")
        .baselineValues("Customer#000000002")
        .baselineValues("Customer#000000003")
        .go();
    }
  }
}
