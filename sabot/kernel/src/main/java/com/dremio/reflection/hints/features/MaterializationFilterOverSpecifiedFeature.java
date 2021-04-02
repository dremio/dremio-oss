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

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;

/**
 * Example 1:
 * User Query Filter: A AND B
 * Materialization Filter: A AND C
 * <p>
 * Example 2:
 * User Query Filter: A
 * Materialization Filter: B
 */
public class MaterializationFilterOverSpecifiedFeature implements HintFeature {
  private RexNode materializationFilter;

  public MaterializationFilterOverSpecifiedFeature(RexNode materializationFilter) {
    this.materializationFilter = materializationFilter;
  }

  public RexNode getMaterializationFilter() {
    return materializationFilter;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MaterializationFilterOverSpecifiedFeature that = (MaterializationFilterOverSpecifiedFeature) o;
    return RexUtil.eq(materializationFilter, that.materializationFilter);
  }

  @Override
  public int hashCode() {
    return Objects.hash(materializationFilter.toString());
  }

  @Override
  public String toString() {
    return "MaterializationFilterOverSpecified{" +
        "materializationFilter=" + materializationFilter +
        '}';
  }
}
