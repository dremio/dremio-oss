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
package com.dremio.exec.planner.sql.handlers;

import org.apache.calcite.rel.RelNode;

import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.exec.planner.physical.HashJoinPrel;
import com.dremio.exec.planner.physical.IncrementalRefreshByPartitionPlaceholderPrel;

/**
 * A class that replaces IncrementalRefreshByPartitionPlaceholderPrel with an actual plan stored in
 * placeholderPrel.getSnapshotDiffContext().getDeleteFilesFilter()
 * The plan is not available originally, so we insert a IncrementalRefreshByPartitionPlaceholderPrel in place of actual plan
 * Later once the plan is available we use IncrementalRefreshByPartitionFinalizeShuttle
 * to replace the IncrementalRefreshByPartitionPlaceholderPrel with the actual plan
 */
class IncrementalRefreshByPartitionFinalizeShuttle extends StatelessRelShuttleImpl {


  public IncrementalRefreshByPartitionFinalizeShuttle() {
    super();
  }

  @Override
  public RelNode visit(final RelNode other) {
    if (other instanceof HashJoinPrel) {
      final HashJoinPrel hashJoinPrel = (HashJoinPrel) other;

      //if we find a IncrementalRefreshByPartitionPlaceholderPrel, that is the right child of a hashJoinPrel,
      //replace the right argument of the hash join with the pre-generated delete plan
      if(hashJoinPrel.getRight() instanceof IncrementalRefreshByPartitionPlaceholderPrel)
      {
        final IncrementalRefreshByPartitionPlaceholderPrel placeholderPrel = (IncrementalRefreshByPartitionPlaceholderPrel)hashJoinPrel.getRight();
        if(placeholderPrel.getSnapshotDiffContext().getDeleteFilesFilter() != null){
          return hashJoinPrel.copy(hashJoinPrel.getTraitSet(),
            hashJoinPrel.getCondition(),
            hashJoinPrel.getLeft(),
            placeholderPrel.getSnapshotDiffContext().getDeleteFilesFilter(),
            hashJoinPrel.getJoinType(),
            hashJoinPrel.isSemiJoinDone());
        }
      }

    }
    return super.visit(other);
  }
}
