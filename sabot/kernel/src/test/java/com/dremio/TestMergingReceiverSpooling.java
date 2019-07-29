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

import java.util.concurrent.TimeUnit;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import com.dremio.common.config.SabotConfig;
import com.dremio.common.util.TestTools;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.proto.UserBitShared;

/**
 * Forces a query to spill in the merging receiver and confirms it doesn't get stuck
 */
public class TestMergingReceiverSpooling extends BaseTestQuery {
  @Rule
  public final TestRule TIMEOUT = TestTools.getTimeoutRule(120, TimeUnit.SECONDS); // Longer timeout than usual.

  @Test
  public void tpch18() throws Exception{
    updateTestCluster(2, SabotConfig.create("dremio-spooling-test.conf"));

    String query = "select l_orderkey from cp.\"tpch/lineitem.parquet\" group by l_orderkey having sum(l_quantity) > 300";

    setSessionOption(ExecConstants.SLICE_TARGET, "10");
    setSessionOption(PlannerSettings.HASHAGG, "false");

    testRunAndPrint(UserBitShared.QueryType.SQL, query);
  }

}
