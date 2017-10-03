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
package com.dremio;

import java.util.concurrent.TimeUnit;

import com.dremio.common.util.TestTools;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.physical.PlannerSettings;

import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

public class TestVectorizedPartitionSender extends BaseTestQuery {

  // As we are setting the max batch size to 128, we end up with lot of small batches from scan which causes the tests
  // to timeout. Increase the default timeouts.
  @ClassRule
  public static final TestRule CLASS_TIMEOUT = TestTools.getTimeoutRule(2000, TimeUnit.SECONDS);

  @Rule
  public final TestRule TIMEOUT = TestTools.getTimeoutRule(200, TimeUnit.SECONDS);

  private static void testDistributed(final String fileName) throws Exception {
    final String query = getFile(fileName).replace(";", " ");
    try (AutoCloseable op1 = withOption(ExecConstants.SLICE_TARGET_OPTION, 10L);
         AutoCloseable op2 = withOption(ExecConstants.TARGET_BATCH_RECORDS_MAX, 128L)
    ) {
      testBuilder()
          .unOrdered()
          .optionSettingQueriesForBaseline("SET `exec.operator.partitioner.vectorize` = false")
          .sqlBaselineQuery(query)
          .optionSettingQueriesForTestQuery("SET `exec.operator.partitioner.vectorize` = true")
          .sqlQuery(query)
          .go();

      try (AutoCloseable ac = withOption(ExecConstants.PARTITION_SENDER_BATCH_ADAPTIVE, true)) {
        testBuilder()
            .unOrdered()
            .optionSettingQueriesForBaseline("SET `exec.operator.partitioner.vectorize` = false")
            .sqlBaselineQuery(query)
            .optionSettingQueriesForTestQuery("SET `exec.operator.partitioner.vectorize` = true")
            .sqlQuery(query)
            .go();
      }
    }
  }

  @Test
  public void tpch01() throws Exception{
    testDistributed("queries/tpch/01.sql");
  }

  @Test
  @Ignore("cartesian")
  public void tpch02() throws Exception{
    testDistributed("queries/tpch/02.sql");
  }

  @Test
  public void tpch03() throws Exception{
    testDistributed("queries/tpch/03.sql");
  }

  @Test
  public void tpch04() throws Exception{
    testDistributed("queries/tpch/04.sql");
  }

  @Test
  public void tpch05() throws Exception{
    testDistributed("queries/tpch/05.sql");
  }

  @Test
  public void tpch07() throws Exception{
    testDistributed("queries/tpch/07.sql");
  }

  @Test
  public void tpch08() throws Exception{
    testDistributed("queries/tpch/08.sql");
  }

  @Test
  public void tpch09() throws Exception{
    testDistributed("queries/tpch/09.sql");
  }

  @Test
  public void tpch10() throws Exception{
    testDistributed("queries/tpch/10.sql");
  }

  @Test
  public void tpch11() throws Exception{
    testDistributed("queries/tpch/11.sql");
  }

  @Test
  public void tpch12() throws Exception{
    testDistributed("queries/tpch/12.sql");
  }

  @Test
  public void tpch13() throws Exception{
    testDistributed("queries/tpch/13.sql");
  }

  @Test
  public void tpch16() throws Exception{
    testDistributed("queries/tpch/16.sql");
  }

  @Test
  public void tpch17() throws Exception{
    testDistributed("queries/tpch/17.sql");
  }

  @Test
  public void tpch18() throws Exception{
    testDistributed("queries/tpch/18.sql");
  }

  @Test
  @Ignore("cartesian")
  public void tpch19() throws Exception{
    testDistributed("queries/tpch/19.sql");
  }

  @Test
  public void tpch20() throws Exception{
    testDistributed("queries/tpch/20.sql");
  }

  @Test
  @Ignore("cartesian")
  public void tpch21() throws Exception{
    testDistributed("queries/tpch/21.sql");
  }

  @Test
  public void tpch22() throws Exception{
    testDistributed("queries/tpch/22.sql");
  }

  @Test
  public void testDisableMux() throws Exception {
    try (AutoCloseable o = withOption(PlannerSettings.MUX_EXCHANGE, false)) {
      testDistributed("queries/tpch/17.sql");
    }
  }

  @Test
  public void testHashToMergeExchange() throws Exception {
    String query = "select `integer` from cp.`/jsoninput/input2.json` where 2 in (select flatten(t.l) from cp.`/jsoninput/input2.json` t)";
    try (AutoCloseable o = withOption(ExecConstants.SLICE_TARGET_OPTION, 1)) {
      testBuilder()
        .unOrdered()
        .sqlQuery(query).baselineColumns("integer")
        .baselineValues(2010L)
        .baselineValues(-2002L)
        .baselineValues(2001L)
        .baselineValues(6005L)
        .go();
    }
  }
}
