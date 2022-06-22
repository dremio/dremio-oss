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
package com.dremio.exec.physical.impl;

import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.PlanTestBase;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.physical.PlannerSettings;

public class TestNewMuxPlanning extends PlanTestBase {
  private static final String sql = "select * from lineitem join orders on l_orderkey = o_orderkey order by l_orderkey desc";
  private static final String[] NONE = new String[] {};

  @BeforeClass
  public static void setup() throws Exception {
    try (AutoCloseable a = withOption(ExecConstants.SLICE_TARGET_OPTION, 1);
         AutoCloseable b = withOption(PlannerSettings.BROADCAST, false);
         AutoCloseable c = withOption(PlannerSettings.MUX_BUFFER_THRESHOLD, 1)) {
      testNoResult("use dfs_test");
      testNoResult("create table lineitem distribute by (l_orderkey) as select * from cp.tpch.\"lineitem.parquet\"");
      testNoResult("create table orders distribute by (o_orderkey) as select * from cp.tpch.\"orders.parquet\"");
    }
  }

  @Test
  public void expectMux() throws Exception {
    run(1, toArray("UnorderedMuxExchange"), NONE);
  }

  @Test
  public void noMux() throws Exception {
    run(PlannerSettings.MUX_BUFFER_THRESHOLD.getDefault().getNumVal(), NONE, toArray("UnorderedMuxExchange"));
  }

  private static String[] toArray(String... items) {
    return items;
  }

  private void run(long threshold, String[] include, String[] exclude) throws Exception {
    try (AutoCloseable a = withOption(ExecConstants.SLICE_TARGET_OPTION, 1);
         AutoCloseable b = withOption(PlannerSettings.BROADCAST, false);
         AutoCloseable c = withOption(PlannerSettings.MUX_BUFFER_THRESHOLD, threshold)) {
      testPlanMatchingPatterns(sql, include, exclude);
    }
  }
}
