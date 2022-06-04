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
package com.dremio.exec.physical.impl.sort;

import org.apache.arrow.vector.util.JsonStringArrayList;
import org.apache.arrow.vector.util.JsonStringHashMap;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.exec.ExecConstants;
import com.dremio.resource.GroupResourceInformation;

/**
 * Placeholder for all sort related test. Can be used as we move
 * more tests to use the new test framework
 */
public class TestSort extends BaseTestQuery {

  private static final JsonStringHashMap<String, Object> x = new JsonStringHashMap<>();
  private static final JsonStringArrayList<JsonStringHashMap<String, Object>> repeated_map = new JsonStringArrayList<>();

  static {
    x.put("c", 1L);
    repeated_map.add(0, x);
  }

  @Test
  public void testSortWithComplexInput() throws Exception {
    testBuilder()
        .sqlQuery("select (t.a) as col from cp.\"jsoninput/repeatedmap_sort_bug.json\" t order by t.b")
        .ordered()
        .baselineColumns("col")
        .baselineValues(repeated_map)
        .go();
  }

  @Test
  public void testSortWithRepeatedMapWithExchanges() throws Exception {
    testBuilder()
        .sqlQuery("select (t.a) as col from cp.\"jsoninput/repeatedmap_sort_bug.json\" t order by t.b")
        .optionSettingQueriesForTestQuery("alter session set \"planner.slice_target\" = 1")
        .ordered()
        .baselineColumns("col")
        .baselineValues(repeated_map)
        .go();

    // reset the planner.slice_target
    test("alter session set \"planner.slice_target\" = " + ExecConstants.SLICE_TARGET_DEFAULT);
  }

  @Test
  public void testSortSpill() throws Exception {
    setSessionOption(GroupResourceInformation.MAX_WIDTH_PER_NODE_KEY, "6");
    try {
      test("CREATE TABLE dfs_test.test_sort PARTITION BY (l_modline, l_moddate) AS " +
        "SELECT l.*, l_shipdate - ((EXTRACT(DAY FROM l_shipdate) - 1) * INTERVAL '1' DAY) l_moddate, " +
        "MOD(l_linenumber,3) l_modline " +
        "FROM cp.\"tpch/lineitem.parquet\" l ORDER BY l_moddate"
      );
    } finally {
      resetSessionOption(GroupResourceInformation.MAX_WIDTH_PER_NODE_KEY);
    }
  }
}
