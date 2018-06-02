/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import java.util.regex.Pattern;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.dremio.exec.planner.physical.PlannerSettings;

public class TestTransitiveJoin extends PlanTestBase {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestTransitiveJoin.class);

  @Before
  public void setupOptions() throws Exception {
    setSessionOption(PlannerSettings.TRANSITIVE_JOIN, "true");
  }

  @After
  public void resetOptions() throws Exception {
    resetSessionOption(PlannerSettings.TRANSITIVE_JOIN);
  }

  @Test
  public void testTransitiveJoin() throws Exception {
    testPlanOneExpectedPattern(""
        + "select l.l_orderkey as x, c.c_custkey as y \n" +
        "  from cp.`tpch/lineitem.parquet` l " +
        "  join cp.`tpch/customer.parquet` c " +
        "  on l.l_orderkey = c.c_custkey "
        + "where l_orderkey = 1 limit 1" +
        "", Pattern.quote("Filter(condition=[=($0, 1)]) : rowType = RecordType(INTEGER c_custkey)"));
  }
}
