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
 * Tests to check if filter are applied correctly for map_entry function which returns a struct type
 */
public class TestFilterSplitOnMapEntryFunction extends PlanTestBase {

  final String path = "cp.parquet.map_data_types";

  @Test
  public void testPlanForSimpleFilterOnMapEntryFunction() throws Exception {
    try (AutoCloseable ignored = withOption(ExecConstants.ENABLE_MAP_DATA_TYPE, true);
         AutoCloseable ignored1 = withOption(PlannerSettings.SPLIT_COMPLEX_FILTER_EXPRESSION, true)
    ) {
      String query = String.format("SELECT * FROM  %s.\"map_type.parquet\" " +
        "WHERE last_matching_map_entry_for_key(counters, 'stats.put.failure')['value'] = 'test'", path);
      testPlanMatchingPatterns(query,
        new String[]{
          ".*Project"+
          "(?s).*SelectionVectorRemover.*EXPR.*7" +
          "(?s).*Filter.*'test'" +
          "(?s).*Project.*last_matching_map_entry_for_key.*'stats.put.failure'"
      });
    }
  }

}
