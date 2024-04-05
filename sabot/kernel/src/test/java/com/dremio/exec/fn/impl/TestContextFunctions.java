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
package com.dremio.exec.fn.impl;

import static com.dremio.exec.rpc.user.security.testing.UserServiceTestImpl.TEST_USER_1;
import static com.dremio.exec.rpc.user.security.testing.UserServiceTestImpl.TEST_USER_1_PASSWORD;
import static com.dremio.exec.rpc.user.security.testing.UserServiceTestImpl.TEST_USER_2;
import static com.dremio.exec.rpc.user.security.testing.UserServiceTestImpl.TEST_USER_2_PASSWORD;

import com.dremio.PlanTestBase;
import com.dremio.QueryTestUtil;
import com.dremio.TestResult;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.exec.client.DremioClient;
import com.dremio.sabot.rpc.user.UserSession;
import java.util.Properties;
import org.junit.Test;

public class TestContextFunctions extends PlanTestBase {

  @Test
  public void userUDFForAnonymousConnection() throws Exception {
    final String user = "anonymous";
    updateClient(user);
    testBuilder()
        .sqlQuery(
            "select user, session_user, system_user, query_user() as query_user from cp.\"employee.json\" limit 1")
        .unOrdered()
        .baselineColumns("user", "session_user", "system_user", "query_user")
        .baselineValues(user, user, user, user)
        .go();
  }

  @Test
  public void userUDFForNamedConnection() throws Exception {
    updateClient(TEST_USER_1, TEST_USER_1_PASSWORD);
    testBuilder()
        .sqlQuery(
            "select user, session_user, system_user, query_user() as query_user from cp.\"employee.json\" limit 1")
        .unOrdered()
        .baselineColumns("user", "session_user", "system_user", "query_user")
        .baselineValues(TEST_USER_1, TEST_USER_1, TEST_USER_1, TEST_USER_1)
        .go();
  }

  @Test
  public void userUDFInFilterCondition() throws Exception {
    updateClient(TEST_USER_2, TEST_USER_2_PASSWORD);
    final String query =
        String.format(
            "select employee_id from cp.\"employee.json\" where '%s' = user order by employee_id limit 1",
            TEST_USER_2);
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("employee_id")
        .baselineValues(1L)
        .go();
  }

  @Test
  public void queryUserConstantReduction() throws Exception {
    final String caSalesUser = TEST_USER_1;
    final String waSalesUser = TEST_USER_2;

    final String query =
        String.format(
            "SELECT sales_city "
                + "FROM cp.\"region.json\" t WHERE "
                + "(query_user() = '%s' and t.sales_state_province = 'CA') OR "
                + "(query_user() = '%s' and t.sales_state_province = 'WA')"
                + "ORDER BY sales_city LIMIT 2",
            caSalesUser, waSalesUser);

    updateClient(caSalesUser, TEST_USER_1_PASSWORD);
    testPhysicalPlan(query, "Filter(condition=[=($1, 'CA')])");

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("sales_city")
        .baselineValues("Altadena")
        .baselineValues("Arcadia")
        .go();

    updateClient(waSalesUser, TEST_USER_2_PASSWORD);
    testPhysicalPlan(query, "Filter(condition=[=($1, 'WA')])");

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("sales_city")
        .baselineValues("Anacortes")
        .baselineValues("Ballard")
        .go();
  }

  @Test
  public void currentSchemaUDFWhenDefaultSchemaNotSet() throws Exception {
    testBuilder()
        .sqlQuery("select current_schema from cp.\"employee.json\" limit 1")
        .unOrdered()
        .baselineColumns("current_schema")
        .baselineValues("")
        .go();
  }

  @Test
  public void currentSchemaUDFWithSingleLevelDefaultSchema() throws Exception {
    testBuilder()
        .optionSettingQueriesForTestQuery("USE dfs_test")
        .sqlQuery("select current_schema from cp.\"employee.json\" limit 1")
        .unOrdered()
        .baselineColumns("current_schema")
        .baselineValues("dfs_test")
        .go();
  }

  /**
   * We want to return the old client to the class state when we are done running tests with a
   * different client.
   *
   * @param newClient
   * @return default client
   */
  private static DremioClient swapClient(DremioClient newClient) {
    DremioClient old = client;
    client = newClient;
    return old;
  }

  /**
   * We need a client with no prior queries performed. However, this means we are foregoing defaults
   * like maximum execution width.
   *
   * @return a client with no prior queries run.
   * @throws Exception
   */
  private static DremioClient createCleanClientHelper() throws Exception {
    final Properties props = new Properties();
    props.setProperty(UserSession.USER, TEST_USER_1);
    props.setProperty(UserSession.PASSWORD, TEST_USER_1_PASSWORD);
    return QueryTestUtil.createCleanClient(config, clusterCoordinator, props);
  }

  /**
   * We need to put back the original client so the defaults are the same from test to test.
   *
   * @param cleanClient - client we used for the test
   * @param oldClient - client we swapped out
   */
  private static void teardownCleanClientTest(DremioClient cleanClient, DremioClient oldClient) {
    cleanClient.close();
    client = oldClient;
  }

  @Test
  public void lastQueryIdUDFWithNoPriorQueries() throws Exception {
    DremioClient cleanClient = createCleanClientHelper();
    DremioClient oldClient = swapClient(cleanClient);

    testBuilder()
        .sqlQuery("select last_query_id from cp.\"employee.json\" limit 1")
        .unOrdered()
        .baselineColumns("last_query_id")
        .baselineValues(null)
        .go();

    teardownCleanClientTest(cleanClient, oldClient);
  }

  @Test
  public void lastQueryIdUDFWithPriorQuery() throws Exception {
    DremioClient cleanClient = createCleanClientHelper();
    DremioClient oldClient = swapClient(cleanClient);

    final TestResult res =
        testBuilder()
            .sqlQuery("select last_query_id from cp.\"employee.json\" limit 1")
            .unOrdered()
            .baselineColumns("last_query_id")
            .baselineValues(null)
            .go();

    testBuilder()
        .sqlQuery("select last_query_id from cp.\"employee.json\" limit 1")
        .unOrdered()
        .baselineColumns("last_query_id")
        .baselineValues(QueryIdHelper.getQueryId(res.getQueryId()))
        .go();

    teardownCleanClientTest(cleanClient, oldClient);
  }
}
