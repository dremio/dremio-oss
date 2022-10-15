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

import org.junit.Test;

import com.dremio.PlanTestBase;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.physical.PlannerSettings;

/**
 * Tests to check if col['literal'] syntax works well
 */
public class TestMapTypeSyntax extends PlanTestBase {

  final String path1 = "cp.parquet.map_data_types";

  @Test
  public void testSimpleProjectOnMapWithKeyStringAndValueString() throws Exception {
    try (AutoCloseable ignored = withOption(ExecConstants.ENABLE_MAP_DATA_TYPE, true);
      AutoCloseable ignored1 = withOption(PlannerSettings.SPLIT_COMPLEX_FILTER_EXPRESSION, true)
    ) {
      String query = String.format("SELECT counters['stats.put.failure'] FROM  %s.\"map_type.parquet\"" , path1);
      testPlanMatchingPatterns(query,
        new String[]{
          ".*Project.*ITEM.*value" +
            "(?s).*Project.*EXPR.*map_entry.*'stats.put.failure'",
        });
    }
  }

  @Test
  public void testSimpleFilterOnMapWithKeyStringAndValueString() throws Exception {
    try (AutoCloseable ignored = withOption(ExecConstants.ENABLE_MAP_DATA_TYPE, true);
      AutoCloseable ignored1 = withOption(PlannerSettings.SPLIT_COMPLEX_FILTER_EXPRESSION, true)) {
      String query = String.format("SELECT counters FROM  %s.\"map_type.parquet\" WHERE  counters['stats.put.failure'] = 'test'", path1);
      testPlanMatchingPatterns(query, new String[]{".*Project"
        + "(?s).*SelectionVectorRemover"
        + "(?s).*Filter.*'test'"
        + "(?s).*Project.*ITEM.*'value'"
        + "(?s).*Project.*EXPR.*map_entry.*'stats.put.failure'",});
    }
  }

  @Test
  public void testSimpleGroupByValueOnMapWithKeyStringAndValueString() throws Exception {
    try (AutoCloseable ignored = withOption(ExecConstants.ENABLE_MAP_DATA_TYPE, true);
      AutoCloseable ignored1 = withOption(PlannerSettings.SPLIT_COMPLEX_FILTER_EXPRESSION, true)) {
      String query = String.format("SELECT counters['stats.put.failure'] FROM  %s.\"map_type.parquet\" GROUP BY  counters['stats.put.failure']", path1);
      testPlanMatchingPatterns(query, new String[]{
        ".*HashAgg"
          + "(?s).*Project.*ITEM.*'value'"
          + "(?s).*Project.*EXPR.*map_entry.*'stats.put.failure'",});
    }
  }

  @Test
  public void testSimpleProjectOnMapWithKeyStringAndValueFloat() throws Exception {
    try (AutoCloseable ignored = withOption(ExecConstants.ENABLE_MAP_DATA_TYPE, true);
      AutoCloseable ignored1 = withOption(PlannerSettings.SPLIT_COMPLEX_FILTER_EXPRESSION, true)
    ) {
      String query = String.format("SELECT measures['current_stock'] FROM  %s.\"map_float_type.parquet\"" , path1);
      testPlanMatchingPatterns(query,
        new String[]{
          ".*Project.*ITEM.*value" +
            "(?s).*Project.*EXPR.*map_entry.*'current_stock'",
        });
    }
  }

  @Test
  public void testSimpleFilterOnMapWithKeyStringAndValueFloat() throws Exception {
    try (AutoCloseable ignored = withOption(ExecConstants.ENABLE_MAP_DATA_TYPE, true);
      AutoCloseable ignored1 = withOption(PlannerSettings.SPLIT_COMPLEX_FILTER_EXPRESSION, true)) {
      String query = String.format("SELECT measures FROM  %s.\"map_float_type.parquet\" WHERE  measures['current_stock'] = 44", path1);
      testPlanMatchingPatterns(query, new String[]{".*Project"
        + "(?s).*SelectionVectorRemover"
        + "(?s).*Filter.*44"
        + "(?s).*Project.*ITEM.*'value'"
        + "(?s).*Project.*EXPR.*map_entry.*'current_stock'",});
    }
  }

  @Test
  public void testSimpleGroupByValueOnMapWithKeyStringAndValueFloat() throws Exception {
    try (AutoCloseable ignored = withOption(ExecConstants.ENABLE_MAP_DATA_TYPE, true);
      AutoCloseable ignored1 = withOption(PlannerSettings.SPLIT_COMPLEX_FILTER_EXPRESSION, true)) {
      String query = String.format("SELECT measures['current_stock'] FROM  %s.\"map_float_type.parquet\" GROUP BY  measures['current_stock']", path1);
      testPlanMatchingPatterns(query, new String[]{
        ".*HashAgg"
          + "(?s).*Project.*ITEM.*'value'"
          + "(?s).*Project.*EXPR.*map_entry.*'current_stock'",});
    }
  }


  @Test
  public void testSimpleOrderByWithKeyStringAndValueFloat() throws Exception {
    try (AutoCloseable ignored = withOption(ExecConstants.ENABLE_MAP_DATA_TYPE, true);
      AutoCloseable ignored1 = withOption(PlannerSettings.SPLIT_COMPLEX_FILTER_EXPRESSION, true)) {
      String query = String.format("SELECT measures['current_stock'] FROM  %s.\"map_float_type.parquet\" ORDER BY  measures['current_stock']", path1);
      testPlanMatchingPatterns(query, new String[]{
        ".*Sort"
          + "(?s).*Project.*ITEM.*'value'"
          + "(?s).*Project.*EXPR.*map_entry.*'current_stock'",});
    }
  }

  @Test
  public void testSimpleExprInSelect() throws Exception {
    try (AutoCloseable ignored = withOption(ExecConstants.ENABLE_MAP_DATA_TYPE, true);
      AutoCloseable ignored1 = withOption(PlannerSettings.SPLIT_COMPLEX_FILTER_EXPRESSION, true)) {
      String query = String.format(
        "SELECT retailer_id, \n" + " 100 + measures['test'] , activity_type FROM %s.\"map_float_type.parquet\"", path1);
      testPlanMatchingPatterns(query, new String[]{
        ".*Project.*EXPR.*100.*ITEM.*'value'"
          + "(?s).*Project.*EXPR.*map_entry.*'test'",});
    }
  }

  @Test
  public void testSimpleExprInWhere() throws Exception {
    try (AutoCloseable ignored = withOption(ExecConstants.ENABLE_MAP_DATA_TYPE, true);
      AutoCloseable ignored1 = withOption(PlannerSettings.SPLIT_COMPLEX_FILTER_EXPRESSION, true)) {
      String query = String.format(
        "SELECT retailer_id, activity_type " +
          "FROM %s.\"map_float_type.parquet\"" +
          "WHERE measures['test'] + 3 < 5" , path1);
      testPlanMatchingPatterns(query, new String[]{
        ".*SelectionVectorRemover"
          + "(?s).*Filter.*<.*ITEM.*'value'.*5"
          + "(?s).*Project.*EXPR.*map_entry.*'test'",});
    }
  }

  @Test
  public void testSimpleJoin() throws Exception {
    try (AutoCloseable ignored = withOption(ExecConstants.ENABLE_MAP_DATA_TYPE, true);
      AutoCloseable ignored1 = withOption(PlannerSettings.SPLIT_COMPLEX_FILTER_EXPRESSION, true)) {
      String query = String.format(
        "SELECT  measures['current_stock']  FROM %s.\"map_float_type.parquet\" JOIN %s.\"map_type.parquet\" " +
          "ON counters['stats.put.failure']  = retailer_id\n"
           , path1, path1);
      testPlanMatchingPatterns(query, new String[]{
        ".*HashJoin.*0.*2"
          + "(?s).*Project.*ITEM.*'value'"
          + "(?s).*Project.*EXPR.*map_entry.*'current_stock'"
          + "(?s).*TableFunction"
          + "(?s).*Project.*ITEM.*'value'"
          + "(?s).*Project.*EXPR.*map_entry.*'stats.put.failure'"
      });
    }
  }

  @Test
  public void testSyntaxWithITEMOperator() throws Exception {
    try (AutoCloseable ignored = withOption(ExecConstants.ENABLE_MAP_DATA_TYPE, true);
      AutoCloseable ignored1 = withOption(PlannerSettings.SPLIT_COMPLEX_FILTER_EXPRESSION, true)) {
      String query = String.format(
        "SELECT t.measures['current_stock'] FROM %s.\"map_float_type.parquet\" t "
        , path1);
      testPlanMatchingPatterns(query, new String[]{
        ".*Project.*ITEM.*'value'"
          + "(?s).*Project.*EXPR.*map_entry.*'current_stock'"
      });
    }
  }


}
