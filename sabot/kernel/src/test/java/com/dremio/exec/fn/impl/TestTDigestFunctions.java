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

import java.util.HashMap;
import java.util.Map;

import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Test;

import com.dremio.DremioTestWrapper;
import com.dremio.PlanTestBase;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.expr.fn.impl.DateFunctionsUtils;

public class TestTDigestFunctions extends PlanTestBase {
  private static final double MEDIAN = 34245.12;
  private static final double Q90 = 66633.84;
  private static final double Q99 = 86085.65;
  private static final double MEDIAN_TAX = 0.04;
  private static final double Q90_TAX = 0.08;
  private static final double MEDIAN_N_O = 34329.33;
  private static final double Q90_N_O = 66654.52;
  private static final double MEDIAN_N_F = 33298.92;
  private static final double Q90_N_F = 67175.52;
  private static final double MEDIAN_A_F = 34129.44;
  private static final double Q90_A_F = 66935.95;
  private static final double MEDIAN_R_F = 34245.12;
  private static final double Q90_R_F = 66172.05;

  @Test
  public void testTDigest() throws Exception {
    runTestExpected(true);
    runTestExpected(false);

  }

  private void runTestExpected(boolean twoPhase) throws Exception {
    final String sql = "select tdigest(distinct l_orderkey) as td from cp.\"tpch/lineitem.parquet\"";
    final String twoPhase1 = "StreamAgg(group=[{}], td=[TDIGEST_MERGE($0)])";
    final String twoPhase2 = "StreamAgg(group=[{}], td=[TDIGEST($0)])";
    if (twoPhase) {
      test("set planner.slice_target = 1");
      testPlanSubstrPatterns(sql, new String[]{twoPhase1, twoPhase2}, null);
    } else {
      test("set planner.slice_target = 100000");
      testPlanSubstrPatterns(sql, new String[]{twoPhase2}, new String[]{twoPhase1});
    }
  }

  @Test
  public void q90() throws Exception {
    String query = "select tdigest(l_extendedprice) q90 from cp.\"tpch/lineitem.parquet\"";
    Map<String, DremioTestWrapper.BaselineValuesForTDigest> tolerances = new HashMap<>();
    tolerances.put("`q90`", new DremioTestWrapper.BaselineValuesForTDigest(1e-3, 0.9));
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("q90")
      .baselineValues(Q90)
      .baselineTolerancesForTDigest(tolerances)
      .go();
  }

  @Test
  public void q99() throws Exception {
    String query = "select tdigest(l_extendedprice) q99 from cp.\"tpch/lineitem.parquet\"";
    Map<String, DremioTestWrapper.BaselineValuesForTDigest> tolerances = new HashMap<>();
    tolerances.put("`q99`", new DremioTestWrapper.BaselineValuesForTDigest(1e-3, 0.99));
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("q99")
      .baselineValues(Q99)
      .baselineTolerancesForTDigest(tolerances)
      .go();
  }

  @Test
  public void median() throws Exception {
    String query = "select tdigest(l_extendedprice) med from cp.\"tpch/lineitem.parquet\"";
    Map<String, DremioTestWrapper.BaselineValuesForTDigest> tolerances = new HashMap<>();
    tolerances.put("`med`", new DremioTestWrapper.BaselineValuesForTDigest(1e-3, 0.5));
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("med")
      .baselineValues(MEDIAN)
      .baselineTolerancesForTDigest(tolerances)
      .go();
  }

  @Test
  public void twoPhaseQ90() throws Exception {
    test("set planner.slice_target = 1");
    String query = "select tdigest(l_extendedprice) q90 from cp.\"tpch/lineitem.parquet\"";
    Map<String, DremioTestWrapper.BaselineValuesForTDigest> tolerances = new HashMap<>();
    tolerances.put("`q90`", new DremioTestWrapper.BaselineValuesForTDigest(1e-3, 0.9));
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("q90")
      .baselineValues(Q90)
      .baselineTolerancesForTDigest(tolerances)
      .go();
  }

  @Test
  public void twoPhaseQ99() throws Exception {
    test("set planner.slice_target = 1");
    String query = "select tdigest(l_extendedprice) q99 from cp.\"tpch/lineitem.parquet\"";
    Map<String, DremioTestWrapper.BaselineValuesForTDigest> tolerances = new HashMap<>();
    tolerances.put("`q99`", new DremioTestWrapper.BaselineValuesForTDigest(1e-3, 0.99));
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("q99")
      .baselineValues(Q99)
      .baselineTolerancesForTDigest(tolerances)
      .go();
  }

  @Test
  public void twoPhaseMedian() throws Exception {
    test("set planner.slice_target = 1");
    String query = "select tdigest(l_extendedprice) med from cp.\"tpch/lineitem.parquet\"";
    Map<String, DremioTestWrapper.BaselineValuesForTDigest> tolerances = new HashMap<>();
    tolerances.put("`med`", new DremioTestWrapper.BaselineValuesForTDigest(1e-3, 0.5));
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("med")
      .baselineValues(MEDIAN)
      .baselineTolerancesForTDigest(tolerances)
      .go();
  }

  @Test
  public void tDigestDateQ0() throws Exception {
    try (AutoCloseable ac = withSystemOption(ExecConstants.PARQUET_AUTO_CORRECT_DATES_VALIDATOR, true)) {
      test("set planner.slice_target = 1");
      String query = "select tdigest(l_shipdate) ship from cp.\"tpch/lineitem.parquet\"";
      Map<String, DremioTestWrapper.BaselineValuesForTDigest> tolerances = new HashMap<>();
      tolerances.put("`ship`", new DremioTestWrapper.BaselineValuesForTDigest(0.6, 0.01));

      org.joda.time.format.DateTimeFormatter formatter = DateFunctionsUtils.getISOFormatterForFormatString("YYYY-MM-DD");
      LocalDateTime date = formatter.parseLocalDateTime("1992-04-27");
      Long unixTimeStamp = com.dremio.common.util.DateTimes.toMillis(date);
      testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("ship")
        .baselineValues(unixTimeStamp.doubleValue())
        .baselineTolerancesForTDigest(tolerances)
        .go();
    }
  }

  @Test
  public void tDigestTimeQ0() throws Exception {
    test("set planner.slice_target = 1");
    String query = "select tdigest(time_col) t FROM cp.parquet.\"all_scalar_types.parquet\"";
    Map<String, DremioTestWrapper.BaselineValuesForTDigest> tolerances = new HashMap<>();
    tolerances.put("`t`", new DremioTestWrapper.BaselineValuesForTDigest(0, 0.01));

    DateTimeFormatter timeFormatter = DateFunctionsUtils.getISOFormatterForFormatString("HH24:MI:SS").withZone(DateTimeZone.UTC);
    LocalDateTime date = timeFormatter.parseLocalDateTime("10:20:30");
    Long unixTimeStamp = com.dremio.common.util.DateTimes.toMillis(date);
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("t")
      .baselineValues(unixTimeStamp.doubleValue())
      .baselineTolerancesForTDigest(tolerances)
      .go();
  }


  @Test
  public void tDigestTimeStampQ0() throws Exception {
    test("set planner.slice_target = 1");
    String query = "select tdigest(timestamp_col) t FROM cp.parquet.\"all_scalar_types.parquet\"";
    Map<String, DremioTestWrapper.BaselineValuesForTDigest> tolerances = new HashMap<>();
    tolerances.put("`t`", new DremioTestWrapper.BaselineValuesForTDigest(0.1, 0.01));

    DateTimeFormatter timeFormatter = DateFunctionsUtils.getISOFormatterForFormatString("YYYY-MM-DD HH24:MI:SS.FFF");
    LocalDateTime date = timeFormatter.parseLocalDateTime("2008-02-23 10:20:30.000");
    Long unixTimeStamp = com.dremio.common.util.DateTimes.toMillis(date);
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("t")
      .baselineValues(unixTimeStamp.doubleValue())
      .baselineTolerancesForTDigest(tolerances)
      .go();
  }

  @Test
  public void testTDigestNumericDataTypes() throws Exception {
    String sql = "SELECT \n" +
      "    tdigest(bool_col) as a, \n" +
      "    tdigest(int_col) as b,\n" +
      "    tdigest(bigint_col) as c, \n" +
      "    tdigest(float4_col) as d, \n" +
      "    tdigest(float8_col) as e \n" +
      "FROM cp.parquet.\"all_scalar_types.parquet\"";
    Map<String, DremioTestWrapper.BaselineValuesForTDigest> tolerances = new HashMap<>();
    tolerances.put("`a`", new DremioTestWrapper.BaselineValuesForTDigest(0, 0));
    tolerances.put("`b`", new DremioTestWrapper.BaselineValuesForTDigest(0, 0));
    tolerances.put("`c`", new DremioTestWrapper.BaselineValuesForTDigest(0, 0));
    tolerances.put("`d`", new DremioTestWrapper.BaselineValuesForTDigest(0, 0));
    tolerances.put("`e`", new DremioTestWrapper.BaselineValuesForTDigest(0, 0));

    testBuilder()
      .sqlQuery(sql)
      .ordered()
      .baselineColumns("a", "b", "c", "d", "e")
      .baselineValues(1.0D, 1.0D, 1.0D, 0.5D, 0.5D)
      .baselineTolerancesForTDigest(tolerances)
      .go();
  }


}
