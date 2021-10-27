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
package com.dremio.reflection.hints.features;

import java.util.Objects;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;


/**
 * user filter is A OR B
 * materialization filter A
 */
public class FilterDisjointFeature implements HintFeature {
  private final RexNode userFilter;
  private final RexNode materializationFilter;
  private final RelDataType datasetRowType;

  public FilterDisjointFeature(RexNode userFilter, RexNode materializationFilter, RelDataType datasetRowType) {
    this.userFilter = userFilter;
    this.materializationFilter = materializationFilter;
    this.datasetRowType = datasetRowType;
  }

  public RexNode getUserFilter() {
    return userFilter;
  }

  public RexNode getMaterializationFilter() {
    return materializationFilter;
  }

  public RelDataType getDatasetRowType() {
    return datasetRowType;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FilterDisjointFeature that = (FilterDisjointFeature) o;
    return RexUtil.eq(userFilter, that.userFilter) &&
        RexUtil.eq(materializationFilter, that.materializationFilter);
  }

  @Override
  public int hashCode() {
    return Objects.hash(userFilter.toString(), materializationFilter.toString());
  }

  @Override
  public String toString() {
    return "FilterDisjointFeature{" +
        "userFilter=" + userFilter +
        ", materializationFilter=" + materializationFilter +
        '}';
  }
}
