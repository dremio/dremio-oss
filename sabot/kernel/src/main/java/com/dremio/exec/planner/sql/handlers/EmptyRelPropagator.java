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

import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.logical.ValuesRel;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;

/**
 * Removes a node and propagates the empty RelNode up if the node under this is an empty RelNode.
 */
public class EmptyRelPropagator extends StatelessRelShuttleImpl {
  public static final EmptyRelPropagator INSTANCE = new EmptyRelPropagator();

  private EmptyRelPropagator() {}

  public static RelNode propagateEmptyRel(RelNode relNode) {
    return relNode.accept(EmptyRelPropagator.INSTANCE);
  }

  @Override
  public RelNode visit(LogicalFilter filter) {
    RelNode visitedFilter = super.visit(filter);
    RelNode visitedChild = visitedFilter.getInput(0);
    if (isEmpty(visitedChild)) {
      return createEmpty(filter);
    }

    return visitedFilter;
  }

  @Override
  public RelNode visit(LogicalCorrelate correlate) {
    RelNode visitedJoin =
        visit(
            LogicalJoin.create(
                correlate.getLeft(),
                correlate.getRight(),
                ImmutableList.of(),
                correlate.getCluster().getRexBuilder().makeLiteral(true),
                ImmutableSet.of(correlate.getCorrelationId()),
                correlate.getJoinType()));

    if (isEmpty(visitedJoin)) {
      return createEmpty(correlate);
    }

    return super.visit(correlate);
  }

  @Override
  public RelNode visit(LogicalProject project) {
    RelNode visitedProject = super.visit(project);
    RelNode visitedChild = visitedProject.getInput(0);
    if (isEmpty(visitedChild)) {
      return createEmpty(project);
    }

    return visitedProject;
  }

  @Override
  public RelNode visit(LogicalJoin join) {
    LogicalJoin visitedJoin = (LogicalJoin) super.visit(join);
    RelNode visitedLeft = visitedJoin.getLeft();
    RelNode visitedRight = visitedJoin.getRight();

    boolean leftEmpty = isEmpty(visitedLeft);
    boolean rightEmpty = isEmpty(visitedRight);

    boolean bothNodesAreEmpty = leftEmpty && rightEmpty;
    boolean innerJoinAndEitherEmpty =
        (join.getJoinType() == JoinRelType.INNER) && (leftEmpty || rightEmpty);
    boolean leftJoinAndLeftEmpty = (join.getJoinType() == JoinRelType.LEFT) && leftEmpty;
    boolean rightJoinAndRightEmpty = (join.getJoinType() == JoinRelType.RIGHT) && rightEmpty;

    boolean joinIsEmpty =
        bothNodesAreEmpty
            || innerJoinAndEitherEmpty
            || leftJoinAndLeftEmpty
            || rightJoinAndRightEmpty;
    if (joinIsEmpty) {
      return createEmpty(join);
    }

    /* DX-85931
    We can do some optimization here. I.e., Left join with right side as an Empty rel can be rewritten into the
    left node with a project on top with null columns.
    E.g.,
      Before: Join(Left)
              -->A
              -->Empty

      After: Project(A.*, Null for Empty.*)
             -->A
     */

    return visitedJoin;
  }

  @Override
  public RelNode visit(LogicalUnion union) {
    // Go through all the children and keep the non-empty ones
    List<RelNode> nonEmptyChildren = new ArrayList<>();
    for (RelNode child : union.getInputs()) {
      RelNode visitedChild = child.accept(this);
      if (!isEmpty(visitedChild)) {
        nonEmptyChildren.add(visitedChild);
      }
    }

    RelNode rewrittenUnion;
    if (nonEmptyChildren.isEmpty()) {
      rewrittenUnion = createEmpty(union);
    } else if (nonEmptyChildren.size() == 1) {
      // We still need to cast the only rel node the way the union would
      RelNode onlyChild = nonEmptyChildren.get(0);
      rewrittenUnion = MoreRelOptUtil.createCastRel(onlyChild, union.getRowType());
    } else {
      rewrittenUnion = LogicalUnion.create(nonEmptyChildren, union.all);
    }

    return rewrittenUnion;
  }

  private static boolean isEmpty(RelNode relNode) {
    if (!(relNode instanceof LogicalValues)) {
      return false;
    }

    LogicalValues values = (LogicalValues) relNode;
    return ValuesRel.isEmpty(values);
  }

  private static RelNode createEmpty(RelNode relNode) {
    return LogicalValues.createEmpty(relNode.getCluster(), relNode.getRowType());
  }
}
