package com.dremio.exec.planner;

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

import com.dremio.PlanTestBase;
import com.dremio.common.exceptions.UserRemoteException;
import org.junit.Assert;
import org.junit.Test;

public class TestApproxPercentile extends PlanTestBase {
  /*
  We have several different testing classes like PlanTestBase and JdbcTestQueryBase.
  The former supports validating exceptions but the existing exception thrown is for customers (UserRemoteException) and
  it does not support validating output.

  The latter supports validating output but does not support validating exception.

  Ideally we want to find a test solution submitting not through query client and only load modules necessary to
  test query execution and query planning only
   */

  @Test
  public void testApproxPercentile() throws Exception {
    final String sql = "SELECT approx_percentile(employee_id, 0.6) from cp.\"employee.json\" ";
    testBuilder().unOrdered().sqlQuery(sql).baselineColumns("EXPR$0").baselineValues(694.5).go();
  }

  @Test
  public void testApproxPercentileWithFilter() throws Exception {
    final String sql =
        "SELECT approx_percentile(employee_id, 0.6) from cp.\"employees.json\" where isFTE = true";
    testBuilder().unOrdered().sqlQuery(sql).baselineColumns("EXPR$0").baselineValues(1107.3).go();
  }

  @Test
  public void testApproxPercentileEmptyResult() throws Exception {
    final String sql =
        "SELECT approx_percentile(employee_id, 0.6) from cp.\"employees.json\" where false";
    testBuilder().unOrdered().sqlQuery(sql).baselineColumns("EXPR$0").baselineValues(null).go();
  }

  @Test
  public void testApproxPercentileMultipleProjections() throws Exception {
    final String sql = "SELECT approx_percentile(employee_id, 0.6), 111 from cp.\"employee.json\"";
    testBuilder()
        .unOrdered()
        .sqlQuery(sql)
        .baselineColumns("EXPR$0", "EXPR$1")
        .baselineValues(694.5, 111)
        .go();
  }

  @Test
  public void testApproxPercentileMultipleProjections2() throws Exception {
    final String sql =
        "SELECT approx_percentile(employee_id, 0.6), approx_percentile(employee_id, 0.6)"
            + " from cp.\"employee.json\"";
    testBuilder()
        .unOrdered()
        .sqlQuery(sql)
        .baselineColumns("EXPR$0", "EXPR$1")
        .baselineValues(694.5, 694.5)
        .go();
  }

  @Test
  public void testApproxPercentileCombiningColumns() throws Exception {
    final String sql =
        "SELECT approx_percentile(employee_id + employee_id, 0.6) from cp.\"employee.json\" ";
    testBuilder().unOrdered().sqlQuery(sql).baselineColumns("EXPR$0").baselineValues(1389.0).go();
  }

  @Test
  public void testApproxPercentileOutOfRange1() {
    final String sql = "SELECT approx_percentile(employee_id, 1.6) from cp.\"employee.json\" ";
    Assert.assertThrows(UserRemoteException.class, () -> test(sql));
  }

  @Test
  public void testApproxPercentileOutOfRange2() throws Exception {
    final String sql = "SELECT approx_percentile(employee_id, -1.6) from cp.\"employee.json\" ";
    Assert.assertThrows(UserRemoteException.class, () -> test(sql));
  }

  @Test
  public void testApproxPercentileInvalidInput() {
    final String sql = "SELECT approx_percentile(full_name, 0.6) from cp.\"employee.json\" ";
    Assert.assertThrows(UserRemoteException.class, () -> test(sql));
    /*
    The inner exception is the following but how can it be catched or verified
    when performing query execution through QueryClient?
    ```
    SYSTEM ERROR: IllegalArgumentException: q should be in [0,1], got 1.6
    ```
    */
  }
}
