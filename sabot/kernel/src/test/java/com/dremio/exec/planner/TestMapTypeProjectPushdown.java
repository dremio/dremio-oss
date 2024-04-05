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
package com.dremio.exec.planner;

import com.dremio.PlanTestBase;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.physical.PlannerSettings;
import org.junit.Test;

// Tests to validate that the map_entry function is pushed down as much as possible
public class TestMapTypeProjectPushdown extends PlanTestBase {

  final String path1 = "cp.parquet.map_data_types";

  @Test
  public void testSimpleProjectPushDownForMapDataType() throws Exception {
    try (AutoCloseable ignored = withOption(ExecConstants.ENABLE_MAP_DATA_TYPE, true);
        AutoCloseable ignored1 =
            withOption(PlannerSettings.SPLIT_COMPLEX_FILTER_EXPRESSION, true)) {
      String query =
          String.format(
              "SELECT counters['stats.put.failure'] FROM  %s.\"map_type.parquet\"", path1);
      testPlanMatchingPatterns(
          query,
          new String[] {
            ".*Project.*ITEM.*value" + "(?s).*Project.*EXPR.*MAP_ENTRY.*'stats.put.failure'",
          });
    }
  }

  @Test
  public void testProjectPastJoinPushDown() throws Exception {
    try (AutoCloseable ignored = withOption(ExecConstants.ENABLE_MAP_DATA_TYPE, true);
        AutoCloseable ignored1 =
            withOption(PlannerSettings.SPLIT_COMPLEX_FILTER_EXPRESSION, true)) {
      String query =
          String.format(
              "select attributes['channel_name'] , counters['suspend_because'] "
                  + "from %s.\"map_type.parquet\" t0 JOIN %s.\"map_float_type.parquet\" t1 "
                  + "ON object_name = retailer_id",
              path1, path1);
      testPlanMatchingPatterns(
          query,
          new String[] {
            ".*HashJoin.*0.*2"
                + "(?s).*Project.*ITEM.*'value'"
                + "(?s).*Project.*EXPR.*MAP_ENTRY.*'channel_name'"
                + "(?s).*TableFunction.*map_float_type"
                + "(?s).*Project.*ITEM.*'value'"
                + "(?s).*Project.*EXPR.*MAP_ENTRY.*'suspend_because'"
                + "(?s).*TableFunction.*map_type",
          });
    }
  }

  @Test
  public void testProjectWithFilterInJoinConditionPushDown() throws Exception {
    try (AutoCloseable ignored = withOption(ExecConstants.ENABLE_MAP_DATA_TYPE, true);
        AutoCloseable ignored1 =
            withOption(PlannerSettings.SPLIT_COMPLEX_FILTER_EXPRESSION, true)) {
      String query =
          String.format(
              "select \"date\", cur_time_stamp "
                  + "from %s.\"map_type.parquet\" t0 JOIN %s.\"map_float_type.parquet\" t1 "
                  + "ON object_name = retailer_id WHERE attributes['channel_name'] = counters['suspend_because']",
              path1, path1);
      testPlanMatchingPatterns(
          query,
          new String[] {
            ".*HashJoin.*AND.*inner"
                + "(?s).*Project.*ITEM.*'value'"
                + "(?s).*Project.*EXPR.*MAP_ENTRY.*'channel_name'"
                + "(?s).*TableFunction.*map_float_type"
                + "(?s).*Project.*ITEM.*'value'"
                + "(?s).*Project.*EXPR.*MAP_ENTRY.*'suspend_because'"
                + "(?s).*TableFunction.*map_type",
          });
    }
  }

  @Test
  public void testProjectInGroupByPastJoinPushdown() throws Exception {
    try (AutoCloseable ignored = withOption(ExecConstants.ENABLE_MAP_DATA_TYPE, true);
        AutoCloseable ignored1 =
            withOption(PlannerSettings.SPLIT_COMPLEX_FILTER_EXPRESSION, true)) {
      String query =
          String.format(
              "WITH t3 as (select * from %s.\"map_type.parquet\" t0 JOIN %s.\"map_float_type.parquet\"  ON object_name = retailer_id)\n"
                  + "select sum(amount) from t3 group by attributes['channel_name'], counters['suspend_because']\n"
                  + "\n",
              path1, path1);
      testPlanMatchingPatterns(
          query,
          new String[] {
            ".*HashAgg.*SUM"
                + "(?s).*HashJoin"
                + "(?s).*Project.*ITEM.*'value'"
                + "(?s).*Project.*EXPR.*MAP_ENTRY.*'channel_name'"
                + "(?s).*TableFunction.*map_float_type"
                + "(?s).*Project.*ITEM.*'value'"
                + "(?s).*Project.*EXPR.*MAP_ENTRY.*'suspend_because'"
                + "(?s).*TableFunction.*map_type",
          });
    }
  }

  @Test
  public void testProjectInOrderByPastJoinPushdown() throws Exception {
    try (AutoCloseable ignored = withOption(ExecConstants.ENABLE_MAP_DATA_TYPE, true);
        AutoCloseable ignored1 =
            withOption(PlannerSettings.SPLIT_COMPLEX_FILTER_EXPRESSION, true)) {
      String query =
          String.format(
              "WITH t3 as (select * from %s.\"map_type.parquet\" t0 JOIN %s.\"map_float_type.parquet\"  ON object_name = retailer_id)\n"
                  + "select sum(amount) from t3 GROUP By attributes['channel_name'], counters['suspend_because'] "
                  + "ORDER BY attributes['channel_name']"
                  + "\n",
              path1, path1);
      testPlanMatchingPatterns(
          query,
          new String[] {
            ".*Sort.*ASC"
                + "(?s).*HashAgg"
                + "(?s).*HashJoin.*inner"
                + "(?s).*Project.*ITEM.*'value'"
                + "(?s).*Project.*EXPR.*MAP_ENTRY.*'channel_name'"
                + "(?s).*TableFunction.*map_float_type"
                + "(?s).*Project.*ITEM.*'value'"
                + "(?s).*Project.*EXPR.*MAP_ENTRY.*'suspend_because'"
                + "(?s).*TableFunction.*map_type",
          });
    }
  }

  @Test
  public void testProjectAndProjectInFilterPastJoinPushdown() throws Exception {
    try (AutoCloseable ignored = withOption(ExecConstants.ENABLE_MAP_DATA_TYPE, true);
        AutoCloseable ignored1 =
            withOption(PlannerSettings.SPLIT_COMPLEX_FILTER_EXPRESSION, true)) {
      String query =
          String.format(
              "WITH t3 as (select * from %s.\"map_type.parquet\" t0 JOIN  %s.\"map_float_type.parquet\"  ON object_name = retailer_id)\n"
                  + "select measures['current_stock'] as CS , attributes['channel_name'] AS CN from t3 WHERE \n"
                  + "(attributes['product_name'] = 'prod' OR counters['suspend_because'] = 'FAIL') "
                  + "AND currency_code = 'PHP'\n"
                  + "\n",
              path1, path1);
      testPlanMatchingPatterns(
          query,
          new String[] {
            ".*SelectionVectorRemover"
                + "(?s).*Filter"
                + "(?s).*HashJoin.*inner"
                + "(?s).*Project.*ITEM.*'value'.*ITEM.*'value'.*ITEM.*'value'"
                + "(?s).*Project.*EXPR.*MAP_ENTRY.*'product_name'"
                + "(?s).*Project.*EXPR.*MAP_ENTRY.*'channel_name'"
                + "(?s).*Project.*EXPR.*MAP_ENTRY.*'current_stock'"
                + "(?s).*SelectionVectorRemover"
                + "(?s).*TableFunction.*map_float_type"
                + "(?s).*Project.*ITEM.*'value'"
                + "(?s).*Project.*EXPR.*MAP_ENTRY.*'suspend_because'"
                + "(?s).*TableFunction.*map_type",
          });
    }
  }
}
