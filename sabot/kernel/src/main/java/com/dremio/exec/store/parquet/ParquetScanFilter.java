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
package com.dremio.exec.store.parquet;

import java.util.List;
import java.util.stream.Collectors;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.store.ScanFilter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * Implementation of {@link ScanFilter} for parquet scan.
 */
@JsonTypeName("ParquetScanFilter")
public class ParquetScanFilter implements ScanFilter {
  private static final double SORTED_ADJUSTMENT = 0.1d;
  private static final int PRIMARY_SORT_INDEX = 0;

  private final List<ParquetFilterCondition> conditions;

  /**
   * Currently Parquet scan supports only one condition.
   * @param conditions
   */
  @JsonCreator
  public ParquetScanFilter(@JsonProperty("conditions") List<ParquetFilterCondition> conditions) {
    Preconditions.checkArgument(conditions != null && !conditions.isEmpty(), "need a non-null, non-empty condition");
    this.conditions = ImmutableList.copyOf(conditions);
  }

  /**
   * Get the underlying conditions
   * @return
   */
  public List<ParquetFilterCondition> getConditions() {
    return conditions;
  }

  @Override
  public double getCostAdjustment(){
    if(isSortedByFilterConditions(conditions.get(0))){
      return SORTED_ADJUSTMENT;
    } else {
      return ScanRelBase.DEFAULT_COST_ADJUSTMENT;
    }
  }

  /**
   * Our goal here is to look at whether the provided filter conditions benefit
   * from the sortedness of the data.
   *
   * @param condition The condition to consider
   * @return Whether the conditions benefit from sortedness. Currently can only
   *         return true if a single condition is present as we don't yet
   *         support or benefit from pushdowns of multiple conditions.
   */
  private static boolean isSortedByFilterConditions(ParquetFilterCondition condition){
    return condition != null
        // we only are interested in a filter on a primary sort field
        && condition.getSort() == PRIMARY_SORT_INDEX;
  }

  @Override
  public String toString() {
    return conditions.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ParquetScanFilter that = (ParquetScanFilter) o;
    return Objects.equal(conditions, that.conditions);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(conditions);
  }

  @Override
  public List<SchemaPath> getPaths() {
    return conditions.stream().map(ParquetFilterCondition::getPath).collect(Collectors.toList());
  }
}
