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
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;

import com.dremio.exec.planner.common.ScanRelBase;

public class RelMdDistinctRowCount extends org.apache.calcite.rel.metadata.RelMdDistinctRowCount {
  private static final RelMdDistinctRowCount INSTANCE =
      new RelMdDistinctRowCount();

  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltInMethod.DISTINCT_ROW_COUNT.method, INSTANCE);

  public Double getDistinctRowCount(ScanRelBase scan, RelMetadataQuery mq, ImmutableBitSet groupKey, RexNode predicate) {
    return getDistinctRowCountFromEstimateRowCount(scan, mq, groupKey, predicate);
  }

  public Double getDistinctRowCount(Aggregate rel, RelMetadataQuery mq,
                                    ImmutableBitSet groupKey, RexNode predicate) {
    if (predicate == null || predicate.isAlwaysTrue()) {
      if (groupKey.isEmpty()) {
        return 1D;
      }
    }

    final ImmutableBitSet allGroupSet = rel.getGroupSet().union(groupKey);
    return getDistinctRowCountFromEstimateRowCount(rel.getInput(), mq, allGroupSet, predicate);
  }

  public Double getDistinctRowCount(Join rel, RelMetadataQuery mq,
                                    ImmutableBitSet groupKey, RexNode predicate) {
    if (predicate == null || predicate.isAlwaysTrue()) {
      if (groupKey.isEmpty()) {
        return 1D;
      }
    }
    return getDistinctRowCountFromEstimateRowCount(rel, mq, groupKey, predicate);
  }

  // Relnode's distinct row count given the grouping key and predicate should depend on a few things.
  // 1.  proportional to the number of records in the table
  // 2.  inversely proportional to the number of grouping keys (group by A should be fewer rows than group by A, B, C)
  // 3.  proportional the filter/predicate selectivity
  private Double getDistinctRowCountFromEstimateRowCount(RelNode rel, RelMetadataQuery mq, ImmutableBitSet groupKey, RexNode predicate) {
    final int groupKeySize = groupKey.cardinality();
    return rel.estimateRowCount(mq) * (1.0 - Math.pow(0.9, groupKeySize)) * RelMdUtil.guessSelectivity(predicate);
  }
}
