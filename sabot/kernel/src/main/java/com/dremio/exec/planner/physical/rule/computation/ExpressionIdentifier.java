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
package com.dremio.exec.planner.physical.rule.computation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * Detects the computations to be pushed down.
 */
class ExpressionIdentifier {

  private ExpressionIdentifier(){

  }

  /**
   * Detects the computation to pushed out of a join condition, providing the shifted computations
   * for the new projects under join as well as the mapping of RexNode.toString to the index of
   * those computations.
   */
  static LeftRightAndComputationIndex identifyNodesToPush(JoinSideDetector joinSideDetector,
      RexNode rexNode) {
    LeftAndRightNodesToPush leftAndRightNodesToPush =
      Preconditions.checkNotNull(rexNode.accept(new Visistor(joinSideDetector)));
    ComputationIndexAndComputations left = dedupeAndCreateComputationIndex(leftAndRightNodesToPush.left,
      joinSideDetector.leftCount, //offset for left computation index to start at
      0);  //left pushed down computation is not shifted
    ComputationIndexAndComputations right = dedupeAndCreateComputationIndex(leftAndRightNodesToPush.right,
      joinSideDetector.leftCount + left.computations.size() + joinSideDetector.rightCount, //offset for right computations to start at
      joinSideDetector.leftCount); //right pushed down offset is shifted back by the right offset
    return new LeftRightAndComputationIndex(
      ImmutableMap.<String, Integer>builder()
        .putAll(left.rexNodeToComputationIndex)
        .putAll(right.rexNodeToComputationIndex)
        .build(),
      left.computations,
      right.computations);
  }

  private static ComputationIndexAndComputations dedupeAndCreateComputationIndex(
      List<RexNode> rexNodes, int replacementOffset, int projectOffset) {
    Map<String, Integer> map = new HashMap<>();
    List<RexNode> computations = new ArrayList<>();
    for (RexNode rexNode : rexNodes) {
      String nodeId = rexNode.toString();
      if(!map.containsKey(nodeId)) {
        map.put(nodeId, map.size() + replacementOffset);
        computations.add(RexUtil.shift(rexNode, -projectOffset));
      }
    }
    return new ComputationIndexAndComputations(map, computations);
  }

  private static class Visistor extends RexVisitorImpl<LeftAndRightNodesToPush> {
    private final JoinSideDetector sideDetector;

    public Visistor(
      JoinSideDetector sideDetector) {
      super(true);
      this.sideDetector = sideDetector;
    }

    @Override public LeftAndRightNodesToPush visitCall(RexCall call) {
      JoinSide joinSide = sideDetector.detectSide(call);
      switch (joinSide) {
        case BOTH:
          List<RexNode> left = new ArrayList<>();
          List<RexNode> right = new ArrayList<>();
          for (RexNode sub : call.getOperands()) {
            LeftAndRightNodesToPush leftAndRightNodesToPush = sub.accept(this);
            left.addAll(leftAndRightNodesToPush.left);
            right.addAll(leftAndRightNodesToPush.right);
          }
          return new LeftAndRightNodesToPush(left, right);
        case LEFT:
          return left(call);
        case RIGHT:
          return right(call);
        case EMPTY:
          return LeftAndRightNodesToPush.EMPTY;
        default:
          throw new IllegalArgumentException(joinSide.name());
      }
    }

    @Override public LeftAndRightNodesToPush visitInputRef(RexInputRef inputRef) {
      return LeftAndRightNodesToPush.EMPTY;
    }

    @Override public LeftAndRightNodesToPush visitLiteral(RexLiteral literal) {
      return LeftAndRightNodesToPush.EMPTY;
    }

    private LeftAndRightNodesToPush right(RexNode rexNode) {
      return new LeftAndRightNodesToPush(ImmutableList.of(), ImmutableList.of(rexNode));
    }

    private LeftAndRightNodesToPush left(RexNode rexNode) {
      return new LeftAndRightNodesToPush(ImmutableList.of(rexNode), ImmutableList.of());
    }
  }

  private static class ComputationIndexAndComputations {
    Map<String, Integer> rexNodeToComputationIndex;
    List<RexNode> computations;

    public ComputationIndexAndComputations(Map<String, Integer> rexNodeToComputationIndex,
      List<RexNode> computations) {
      assert computations.size() == rexNodeToComputationIndex.size();
      this.rexNodeToComputationIndex = rexNodeToComputationIndex;
      this.computations = computations;
    }
  }

  static class LeftRightAndComputationIndex {
    final Map<String, Integer> rexNodeToComputationIndex;
    final List<RexNode> leftComputations;
    final List<RexNode> rightComputations;

    public LeftRightAndComputationIndex(Map<String, Integer> rexNodeToComputationIndex,
      List<RexNode> leftComputations,
      List<RexNode> rightComputations) {
      this.rexNodeToComputationIndex = rexNodeToComputationIndex;
      this.leftComputations = leftComputations;
      this.rightComputations = rightComputations;
    }

    @Override public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      LeftRightAndComputationIndex that = (LeftRightAndComputationIndex) o;
      return rexNodeToComputationIndex.equals(that.rexNodeToComputationIndex)
        && leftComputations.equals(that.leftComputations)
        && rightComputations.equals(that.rightComputations);
    }

    @Override public int hashCode() {
      return Objects.hash(rexNodeToComputationIndex, leftComputations, rightComputations);
    }

    @Override public String toString() {
      return "LeftAndRightComputationIndex{" +
        "rexNodeToComputationIndex=" + rexNodeToComputationIndex +
        ", leftComputations=" + leftComputations +
        ", rightComputations=" + rightComputations +
        '}';
    }
  }

  private static class LeftAndRightNodesToPush {
    static final LeftAndRightNodesToPush EMPTY =
      new LeftAndRightNodesToPush(ImmutableList.of(), ImmutableList.of());
    List<RexNode> left;
    List<RexNode> right;
    public LeftAndRightNodesToPush(List<RexNode> left,
      List<RexNode> right) {
      this.left = left;
      this.right = right;
    }
  }
}
