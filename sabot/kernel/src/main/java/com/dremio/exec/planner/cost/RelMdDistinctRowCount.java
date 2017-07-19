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
package com.dremio.exec.planner.cost;

import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;

import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.planner.logical.ScanRel;

public class RelMdDistinctRowCount extends org.apache.calcite.rel.metadata.RelMdDistinctRowCount {
  private static final RelMdDistinctRowCount INSTANCE =
      new RelMdDistinctRowCount();

  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltInMethod.DISTINCT_ROW_COUNT.method, INSTANCE);

  public Double getDistinctRowCount(ScanRel scan, RelMetadataQuery mq, ImmutableBitSet groupKey, RexNode predicate) {
    // Consistent with the estimation of Aggregate row count in RelMdRowCount : distinctRowCount = rowCount * 10%.
    return scan.estimateRowCount(DefaultRelMetadataProvider.INSTANCE.getRelMetadataQuery()) * 0.1;
  }

  public Double getDistinctRowCount(ScanRelBase scan, RelMetadataQuery mq, ImmutableBitSet groupKey, RexNode predicate) {
    // Consistent with the estimation of Aggregate row count in RelMdRowCount : distinctRowCount = rowCount * 10%.
    return scan.estimateRowCount(DefaultRelMetadataProvider.INSTANCE.getRelMetadataQuery()) * 0.1;
  }
}
