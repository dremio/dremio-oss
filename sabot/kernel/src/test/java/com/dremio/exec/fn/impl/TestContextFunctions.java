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

import com.dremio.PlanTestBase;

public class TestContextFunctions extends PlanTestBase {

  @Test
  public void userUDFForAnonymousConnection() throws Exception {
    updateClient("");
    testBuilder()
        .sqlQuery("select user, session_user, system_user, query_user() as query_user from cp.`employee.json` limit 1")
        .unOrdered()
        .baselineColumns("user", "session_user", "system_user", "query_user")
        .baselineValues("anonymous", "anonymous", "anonymous", "anonymous")
        .go();
  }

  @Test
  public void userUDFForNamedConnection() throws Exception {
    final String testUserName = "testUser1";
    updateClient(testUserName);
    testBuilder()
        .sqlQuery("select user, session_user, system_user, query_user() as query_user from cp.`employee.json` limit 1")
        .unOrdered()
        .baselineColumns("user", "session_user", "system_user", "query_user")
        .baselineValues(testUserName, testUserName, testUserName, testUserName)
        .go();
  }

  @Test
  public void userUDFInFilterCondition() throws Exception {
    final String testUserName = "testUser2";
    updateClient(testUserName);
    final String query = String.format(
        "select employee_id from cp.`employee.json` where '%s' = user order by employee_id limit 1", testUserName);
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("employee_id")
        .baselineValues(1L)
        .go();
  }

  @Test
  public void queryUserConstantReduction() throws Exception {
    final String caSalesUser = "testUser1";
    final String waSalesUser = "testUser2";

    final String query = String.format("SELECT sales_city " +
            "FROM cp.`region.json` t WHERE " +
                "(query_user() = '%s' and t.sales_state_province = 'CA') OR " +
                "(query_user() = '%s' and t.sales_state_province = 'WA')" +
            "ORDER BY sales_city LIMIT 2", caSalesUser, waSalesUser);

    updateClient(caSalesUser);
    testPhysicalPlan(query, "Filter(condition=[=($1, 'CA')])");

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("sales_city")
        .baselineValues("Altadena")
        .baselineValues("Arcadia")
        .go();

    updateClient(waSalesUser);
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
        .sqlQuery("select current_schema from cp.`employee.json` limit 1")
        .unOrdered()
        .baselineColumns("current_schema")
        .baselineValues("")
        .go();
  }

  @Test
  public void currentSchemaUDFWithSingleLevelDefaultSchema() throws Exception {
    testBuilder()
        .optionSettingQueriesForTestQuery("USE dfs_test")
        .sqlQuery("select current_schema from cp.`employee.json` limit 1")
        .unOrdered()
        .baselineColumns("current_schema")
        .baselineValues("dfs_test")
        .go();
  }
}
