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
package com.dremio.exec.store.sys;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.proto.UserBitShared;

public class TestSystemTable extends BaseTestQuery {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestSystemTable.class);

  @BeforeClass
  public static void setupMultiNodeCluster() throws Exception {
    updateTestCluster(3, null);
  }

  @Test
  public void alterSessionOption() throws Exception {

    newTest() //
      .sqlQuery("select bool_val as bool from sys.options where name = '%s' order by type desc", ExecConstants.JSON_ALL_TEXT_MODE)
      .baselineColumns("bool")
      .ordered()
      .baselineValues(false)
      .go();

    test("alter session set `%s` = true", ExecConstants.JSON_ALL_TEXT_MODE);

    newTest() //
      .sqlQuery("select bool_val as bool from sys.options where name = '%s' order by type desc ", ExecConstants.JSON_ALL_TEXT_MODE)
      .baselineColumns("bool")
      .ordered()
      .baselineValues(false)
      .baselineValues(true)
      .go();
  }

  // DRILL-2670
  @Test
  public void optionsOrderBy() throws Exception {
    test("select * from sys.options order by name");
  }

  @Test
  public void differentCase() throws Exception {
    test("select * from SYS.OPTIONS");
  }

  @Test
  public void threadsTable() throws Exception {
    test("select * from sys.threads");
  }

  @Test
  public void memoryTable() throws Exception {
    test("select * from sys.memory");
  }

  @Test
  public void fragmentsTable() throws Exception {
    test("select * from sys.fragments");
  }

  /**
   * @return the configured max width per node, at the SYSTEM level
   */
  private long getConfiguredMaxWidthPerNode() throws Exception {
    final String fetchMaxWidthQuery = String.format(
      "SELECT num_val FROM sys.options WHERE name='%s' AND type='SYSTEM'",
      ExecConstants.MAX_WIDTH_PER_NODE_KEY);
    String maxWidthString = getResultString(
      testRunAndReturn(UserBitShared.QueryType.SQL, fetchMaxWidthQuery), "", false);

    return Long.parseLong(maxWidthString.substring(0, maxWidthString.indexOf('\n')));
  }

  @Test
  @Ignore
  public void nodesTable() throws Exception {
    long configuredMaxWidth = getConfiguredMaxWidthPerNode();

    // this test exposes an interesting "fact" about how cluster load is being computed
    // WorkManager computes the cluster load using the system max_width_per_node
    // but when we query `sys.nodes` it returns the value, for 'configured_max_width'
    // that has been set at the session level

    final String query = "SELECT CAST((cluster_load*100) AS INTEGER) AS cluster_load, " +
      "configured_max_width, actual_max_width FROM sys.nodes";

    // the query will run one fragment per node
    int expectedClusterLoad = (int) Math.round(100.0 / configuredMaxWidth);

    testBuilder()
      .sqlQuery(query)
      .ordered()
      .baselineColumns("cluster_load", "configured_max_width", "actual_max_width")
      .baselineValues(expectedClusterLoad, MAX_WIDTH_PER_NODE, MAX_WIDTH_PER_NODE)
      .baselineValues(expectedClusterLoad, MAX_WIDTH_PER_NODE, MAX_WIDTH_PER_NODE)
      .baselineValues(expectedClusterLoad, MAX_WIDTH_PER_NODE, MAX_WIDTH_PER_NODE)
      .go();
  }
}
