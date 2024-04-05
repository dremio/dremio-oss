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
package com.dremio.exec.planner.logical;

import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.type.RelDataTypeField;

/**
 * When a user writes a query that has NO-OP sort operations, then we can remove them to increase
 * performance.
 *
 * <p>For example a user can write a query:
 *
 * <p>SELECT * FROM ( SELECT ENAME, SAL FROM EMP ORDER BY ENAME) ORDER BY SAL
 *
 * <p>Which will have the following plan:
 *
 * <p>LogicalSort(sort0=[$1], dir0=[ASC]) LogicalProject(ENAME=[$0], SAL=[$1])
 * LogicalSort(sort0=[$0], dir0=[ASC]) LogicalProject(ENAME=[$1], SAL=[$5])
 * LogicalTableScan(table=[[EMP]])
 *
 * <p>Notice that the above plan has a SORT on ENAME followed by a SORT on SAL. It is redundant to
 * sort on ENAME and then to just sort on SAL at the end. An optimization is to just remove the sort
 * on ENAME.
 *
 * <p>The general rule is to remove sorts that have parent operations that make them redundant
 * (negate them). There are a few exceptions like if the SORT operation has an OFFSET or a FETCH,
 * since those operations change the result set and rely on the ordering of the input.
 */
public final class RedundantSortEliminator {
  private RedundantSortEliminator() {}

  public static RelNode apply(RelNode relNode) {
    return relNode.accept(RedundantSortEliminatorRelShuttle.INSTANCE);
  }

  private static final class RedundantSortEliminatorRelShuttle extends StatelessRelShuttleImpl {
    private static final RedundantSortEliminatorRelShuttle HAS_NEGATING_PARENT_TRUE =
        new RedundantSortEliminatorRelShuttle(true);
    private static final RedundantSortEliminatorRelShuttle HAS_NEGATING_PARENT_FALSE =
        new RedundantSortEliminatorRelShuttle(false);
    public static final RedundantSortEliminatorRelShuttle INSTANCE = HAS_NEGATING_PARENT_FALSE;

    private final boolean hasNegatingParent;

    private RedundantSortEliminatorRelShuttle(boolean hasNegatingParent) {
      this.hasNegatingParent = hasNegatingParent;
    }

    @Override
    public RelNode visit(LogicalSort sort) {
      boolean isPureSortOperations = (sort.fetch == null) && (sort.offset == null);
      if (isPureSortOperations && hasNegatingParent) {
        // Note that even if are under an aggregate or join, but there is a fetch / offset
        // inbetween,
        // then we no longer have a redundant parent.
        return sort.getInput().accept(this);
      }

      // Calcite represents LIMIT rels as LogicalSorts.
      // The quirk is that we are matching the LIMIT operation in this sort logic,
      // but we don't want to treat it like a sort (since there is no sorting going on).
      boolean isNotRealSortOperation = !sort.collation.getFieldCollations().isEmpty();
      RedundantSortEliminatorRelShuttle shuttle =
          isNotRealSortOperation ? HAS_NEGATING_PARENT_TRUE : HAS_NEGATING_PARENT_FALSE;
      RelNode prunedInput = sort.getInput().accept(shuttle);
      boolean inputWasPruned = prunedInput != sort.getInput();
      if (!inputWasPruned) {
        return sort;
      }

      return LogicalSort.create(prunedInput, sort.collation, sort.offset, sort.fetch);
    }

    @Override
    public RelNode visit(LogicalAggregate logicalAggregate) {
      // Any sorts that come below this node should be removed.
      RelNode prunedInput = logicalAggregate.getInput().accept(HAS_NEGATING_PARENT_TRUE);
      boolean inputWasPruned = prunedInput != logicalAggregate.getInput();
      if (!inputWasPruned) {
        return logicalAggregate;
      }

      return LogicalAggregate.create(
          prunedInput,
          logicalAggregate.getHints(),
          logicalAggregate.getGroupSet(),
          logicalAggregate.getGroupSets(),
          logicalAggregate.getAggCallList());
    }

    @Override
    public RelNode visit(LogicalJoin logicalJoin) {
      // Any sorts that come below this node should be removed.
      RelNode leftPrunedInput = logicalJoin.getLeft().accept(HAS_NEGATING_PARENT_TRUE);
      RelNode rightPrunedInput = logicalJoin.getRight().accept(HAS_NEGATING_PARENT_TRUE);

      boolean leftInputWasPruned = leftPrunedInput != logicalJoin.getLeft();
      boolean rightInputWasPruned = rightPrunedInput != logicalJoin.getRight();
      if (!leftInputWasPruned && !rightInputWasPruned) {
        return logicalJoin;
      }

      return LogicalJoin.create(
          leftPrunedInput,
          rightPrunedInput,
          logicalJoin.getHints(),
          logicalJoin.getCondition(),
          logicalJoin.getVariablesSet(),
          logicalJoin.getJoinType(),
          logicalJoin.isSemiJoinDone(),
          (ImmutableList<RelDataTypeField>) logicalJoin.getSystemFieldList());
    }
  }
}
