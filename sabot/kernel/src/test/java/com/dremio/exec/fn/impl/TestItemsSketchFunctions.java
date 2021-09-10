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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import com.dremio.DremioTestWrapper;
import com.dremio.PlanTestBase;
import com.google.common.collect.ImmutableList;
import com.yahoo.sketches.ArrayOfBooleansSerDe;
import com.yahoo.sketches.ArrayOfDoublesSerDe;
import com.yahoo.sketches.ArrayOfLongsSerDe;
import com.yahoo.sketches.ArrayOfNumbersSerDe;
import com.yahoo.sketches.ArrayOfStringsSerDe;

/**
 * TestCountMinSketchFunctions
 */
public class TestItemsSketchFunctions extends PlanTestBase {

  @Test
  public void testPlanWithItemsSketch() throws Exception {
    runTestExpected(true);
    runTestExpected(false);
  }

  private void runTestExpected(boolean twoPhase) throws Exception {
    final String sql = "select items_sketch(full_name) as td from cp.\"employees.json\"";
    final String twoPhase1 = "StreamAgg(group=[{}], td=[items_sketch_merge_varchar($0)])";
    final String twoPhase2 = "StreamAgg(group=[{}], td=[ITEMS_SKETCH($0)])";
    if (twoPhase) {
      test("set planner.slice_target = 1");
      testPlanSubstrPatterns(sql, new String[]{twoPhase1, twoPhase2}, null);
    } else {
      test("set planner.slice_target = 100000");
      testPlanSubstrPatterns(sql, new String[]{twoPhase2}, new String[]{twoPhase1});
    }
  }

  @Test
  public void testVarchar() throws Exception {
    test("set planner.slice_target = 1");
    String query = "select items_sketch(full_name) varchar_cms from cp.\"employees.json\"";
    Map<String, DremioTestWrapper.BaselineValuesForItemsSketch> sketchMap = new HashMap<>();
    List<Pair<Object, Long>> expected = new ArrayList<>();
    expected.add(Pair.of("Steve Eurich", 1L));
    sketchMap.put("`varchar_cms`", new DremioTestWrapper.BaselineValuesForItemsSketch(expected, null, new ArrayOfStringsSerDe()));
    test(query, "varchar_cms", sketchMap, true);
    test(query, "varchar_cms", sketchMap, false);
  }

  public void test(String query, String col, Map<String, DremioTestWrapper.BaselineValuesForItemsSketch> sketchMap, boolean twoPhase) throws Exception {
    if (twoPhase) {
      test("set planner.slice_target = 1");
    } else {
      test("set planner.slice_target = 100000");
    }
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns(col)
      .baselineValues("dummy")
      .baselineTolerancesForItemsSketch(sketchMap)
      .go();
  }

  @Test
  public void testItemsSketchAllTypes() throws Exception {
    test("set planner.slice_target = 1");
    verifyAllTypes();
    test("set planner.slice_target = 100000");
    verifyAllTypes();
  }

  private void verifyAllTypes() throws Exception{
    String sql = "SELECT \n" +
      "    items_sketch(bool_col) as a, \n" +
      "    items_sketch(int_col) as b,\n" +
      "    items_sketch(bigint_col) as c, \n" +
      "    items_sketch(float4_col) as d, \n" +
      "    items_sketch(float8_col) as e, \n" +
      "    items_sketch(date_col) as f, \n" +
      "    items_sketch(timestamp_col) as g, \n" +
      "    items_sketch(time_col) as h, \n" +
      "    items_sketch(varchar_col) as i, \n" +
      "    items_sketch(INTERVAL '4 5:12:10' DAY TO SECOND) as j, \n" +
      "    items_sketch(INTERVAL '2-6' YEAR TO MONTH) as k\n" +
      "FROM cp.parquet.\"all_scalar_types_multiple_rows.parquet\"";

    final Map<String, DremioTestWrapper.BaselineValuesForItemsSketch> sketchMap = new HashMap<>();
    List<Pair<Object, Long>> a = ImmutableList.of(Pair.of(true, 2L));
    List<Pair<Object, Long>> b = ImmutableList.of(Pair.of(1, 2L));
    List<Pair<Object, Long>> c = ImmutableList.of(Pair.of(1L, 2L));
    List<Pair<Object, Long>> d = ImmutableList.of(Pair.of(0.5f, 2L));
    List<Pair<Object, Long>> e = ImmutableList.of(Pair.of(0.5d, 2L));
    List<Pair<Object, Long>> f = ImmutableList.of(Pair.of(1203724800000L, 2L));
    List<Pair<Object, Long>> g = ImmutableList.of(Pair.of(1203762030000L, 2L));
    List<Pair<Object, Long>> h = ImmutableList.of(Pair.of(37230000, 2L));
    List<Pair<Object, Long>> i = ImmutableList.of(Pair.of("random", 2L));
    List<Pair<Object, Long>> j = ImmutableList.of(Pair.of(18730000, 2L));
    List<Pair<Object, Long>> k = ImmutableList.of(Pair.of(30, 2L));

    sketchMap.put("`a`", new DremioTestWrapper.BaselineValuesForItemsSketch(a, null, new ArrayOfBooleansSerDe()));
    sketchMap.put("`b`", new DremioTestWrapper.BaselineValuesForItemsSketch(b, null, new ArrayOfNumbersSerDe()));
    sketchMap.put("`c`", new DremioTestWrapper.BaselineValuesForItemsSketch(c, null, new ArrayOfLongsSerDe()));
    sketchMap.put("`d`", new DremioTestWrapper.BaselineValuesForItemsSketch(d, null, new ArrayOfNumbersSerDe()));
    sketchMap.put("`e`", new DremioTestWrapper.BaselineValuesForItemsSketch(e, null, new ArrayOfDoublesSerDe()));
    sketchMap.put("`f`", new DremioTestWrapper.BaselineValuesForItemsSketch(f, null, new ArrayOfLongsSerDe()));
    sketchMap.put("`g`", new DremioTestWrapper.BaselineValuesForItemsSketch(g, null, new ArrayOfLongsSerDe()));
    sketchMap.put("`h`", new DremioTestWrapper.BaselineValuesForItemsSketch(h, null, new ArrayOfNumbersSerDe()));
    sketchMap.put("`i`", new DremioTestWrapper.BaselineValuesForItemsSketch(i, null,  new ArrayOfStringsSerDe()));
    sketchMap.put("`j`", new DremioTestWrapper.BaselineValuesForItemsSketch(j, null, new ArrayOfNumbersSerDe()));
    sketchMap.put("`k`", new DremioTestWrapper.BaselineValuesForItemsSketch(k, null, new ArrayOfNumbersSerDe()));

    testBuilder()
      .sqlQuery(sql)
      .ordered()
      .baselineColumns("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k")
      .baselineValues("dummy", "dummy", "dummy", "dummy", "dummy", "dummy", "dummy", "dummy", "dummy", "dummy", "dummy")
      .baselineTolerancesForItemsSketch(sketchMap)
      .go();
  }

  @Test
  public void testHeavyHitters() throws Exception {
    String sql = "SELECT \n" +
      "    items_sketch(val) as a \n" +
      "FROM cp.json.\"heavy_hitters.json\"";

    final Map<String, DremioTestWrapper.BaselineValuesForItemsSketch> sketchMap = new HashMap<>();
    List<Object> heavyHitters = ImmutableList.of("a", "b", "c", "d", "e");

    sketchMap.put("`a`", new DremioTestWrapper.BaselineValuesForItemsSketch(null, heavyHitters, new ArrayOfStringsSerDe()));

    testBuilder()
      .sqlQuery(sql)
      .ordered()
      .baselineColumns("a")
      .baselineValues("dummy")
      .baselineTolerancesForItemsSketch(sketchMap)
      .go();
  }
}
