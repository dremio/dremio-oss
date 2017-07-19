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
package com.dremio.exec.store.parquet;

import java.util.List;

import com.dremio.exec.planner.common.ScanRelBase;

/**
 * Utility class to understand a collection of filter conditions
 */
public final class FilterConditions {

  public static final double SORTED_ADJUSTMENT = 0.1d;
  private static final int PRIMARY_SORT_INDEX = 0;

  private FilterConditions(){}

  /**
   * Our goal here is to look at whether the provided filter conditions benefit
   * from the sortedness of the data.
   *
   * @param conditions
   *          The conditions to consider
   * @return Whether the conditions benefit from sortedness. Currently can only
   *         return true if a single condition is present as we don't yet
   *         support or benefit from pushdowns of multiple conditions.
   */
  public static boolean isSortedByFilterConditions(List<FilterCondition> conditions){
    return conditions != null

        // only support a single condition for now.
        && conditions.size() == 1

        // we only are interested in a filter on a primary sort field
        && conditions.get(0).getSort() == PRIMARY_SORT_INDEX;
  }

  /**
   * Get the cost adjustment associated with a set of conditions.
   * @param conditions
   * @return
   */
  public static double getCostAdjustment(List<FilterCondition> conditions){
    if(isSortedByFilterConditions(conditions)){
      return SORTED_ADJUSTMENT;
    } else {
      return ScanRelBase.DEFAULT_COST_ADJUSTMENT;
    }
  }
}
