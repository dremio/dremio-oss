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
package com.dremio.exec.client;

import static com.dremio.TestBuilder.listOf;
import static com.dremio.TestBuilder.mapOf;

import java.util.Properties;

import org.junit.After;
import org.junit.Test;

import com.dremio.BaseTestQuery;

/**
 * Tests that pass custom properties to server to modify the behavior of how the queries are treated.
 */
public class TestDremioClientWithOptions extends BaseTestQuery {
  @Test // DX-5338
  public void supportFullyQualifiedProjects() throws Exception {
    updateClientToSupportFullyQualifiedProject();

    final String query = "SELECT " +
        "cp.`tpch/lineitem.parquet`.l_orderkey, " +
        "cp.`tpch/lineitem.parquet`.l_extendedprice " +
        "FROM cp.`tpch/lineitem.parquet` " +
        "ORDER BY cp.`tpch/lineitem.parquet`.l_orderkey " +
        "LIMIT 2";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("`l_orderkey`", "`l_extendedprice`")
        .baselineValues(1, 56688.12d)
        .baselineValues(1, 24710.35d)
        .go();
  }

  @Test // DX-5338
  public void supportFullyQualifiedProjectsQueryView() throws Exception {
    // View here contains a projecting components of complex data type columns. Simulating a scenario where you can
    // create a dataset with complex field component projections and querying the dataset from clients such as tableau.
    test("CREATE VIEW dfs_test.complexView AS " +
        "SELECT t.a.arrayval as c1, t.a.arrayval[0].val1[0] as c2 " +
        "FROM cp.`nested/nested_3.json` as t LIMIT 1");

    updateClientToSupportFullyQualifiedProject();
    final String query = "SELECT dfs_test.complexView.c1, dfs_test.complexView.c2 FROM dfs_test.complexView";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("c1", "c2")
        .baselineValues(listOf(
            mapOf("val1", listOf("a1")),
            mapOf("val2", "[a2]"),
            mapOf("val3", "[a3]")
        ), "a1")
        .go();
  }

  private static void updateClientToSupportFullyQualifiedProject() throws Exception {
    final Properties properties = new Properties();
    properties.put("supportFullyQualifiedProjects", "true");
    updateClient(properties);
  }

  @After
  public void resetClient() throws Exception {
    updateClient((Properties)null);
  }
}
