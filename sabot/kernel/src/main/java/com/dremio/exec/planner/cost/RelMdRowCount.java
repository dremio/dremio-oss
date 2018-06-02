/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.exec.planner.cost;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;

import com.dremio.exec.planner.common.FlattenRelBase;
import com.dremio.exec.planner.common.JdbcRelBase;
import com.dremio.exec.planner.common.LimitRelBase;
import com.dremio.exec.planner.physical.FlattenPrel;

public class RelMdRowCount extends org.apache.calcite.rel.metadata.RelMdRowCount {
  private static final RelMdRowCount INSTANCE = new RelMdRowCount();

  public static final RelMetadataProvider SOURCE = ReflectiveRelMetadataProvider.reflectiveSource(BuiltInMethod.ROW_COUNT.method, INSTANCE);

  @Override
  public Double getRowCount(Aggregate rel, RelMetadataQuery mq) {
    ImmutableBitSet groupKey = ImmutableBitSet.range(rel.getGroupCount());

    if (groupKey.isEmpty()) {
      return 1.0;
    }

    return rel.estimateRowCount(mq);
  }

  // DX-3859:  Need to make sure that join row count is calculated in a reasonable manner.  Calcite's default
  // implementation is leftRowCount * rightRowCount * discountBySelectivity, which is too large (cartesian join).
  // Since we do not support cartesian join, we should just take the maximum of the two join input row counts.
  @Override
  public Double getRowCount(Join rel, RelMetadataQuery mq) {
    if (rel.getCondition().isAlwaysTrue()) {
      return super.getRowCount(rel, mq);
    } else {
      return Math.max(mq.getRowCount(rel.getLeft()), mq.getRowCount(rel.getRight()));
    }
  }

  public Double getRowCount(MultiJoin rel, RelMetadataQuery mq) {
    if (rel.getJoinFilter().isAlwaysTrue()) {
      double rowCount = 1;
      for (RelNode input : rel.getInputs()) {
        rowCount *= mq.getRowCount(input);
      }
      return rowCount;
    } else {
      double max = 1;
      for (RelNode input : rel.getInputs()) {
        max = Math.max(max, mq.getRowCount(input));
      }
      return max;
    }
  }

  public Double getRowCount(FlattenRelBase flatten, RelMetadataQuery mq) {
    return flatten.estimateRowCount(mq);
  }

  public Double getRowCount(FlattenPrel flatten, RelMetadataQuery mq) {
    return flatten.estimateRowCount(mq);
  }

  public Double getRowCount(LimitRelBase limit, RelMetadataQuery mq) {
    return limit.estimateRowCount(mq);
  }

  public Double getRowCount(JdbcRelBase jdbc, RelMetadataQuery mq) {
    return jdbc.getSubTree().estimateRowCount(mq);
  }

  @Override
  public Double getRowCount(Filter rel, RelMetadataQuery mq) {
    return rel.estimateRowCount(mq);
  }
}
