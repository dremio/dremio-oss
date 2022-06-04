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

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;

import com.google.common.base.Preconditions;

class JoinSideDetector  {
  private final RexVisitor<JoinSide> visitor = new Visitor();

  final int leftCount;
  final int rightCount;

  JoinSideDetector(int leftCount, int rightCount) {
    this.leftCount = leftCount;
    this.rightCount = rightCount;
  }

  JoinSide detectSide(RexNode rexNode) {
    return Preconditions.checkNotNull(rexNode.accept(visitor));
  }

  private class Visitor extends RexVisitorImpl<JoinSide> {
    private Visitor() {
      super(true);
    }

    @Override
    public JoinSide visitInputRef(RexInputRef ref) {
      assert ref.getIndex() < rightCount + leftCount;
      return ref.getIndex() < leftCount
        ? JoinSide.LEFT
        : JoinSide.RIGHT;
    }

    @Override
    public JoinSide visitLiteral(RexLiteral lit) {
      return JoinSide.EMPTY;
    }

    @Override
    public JoinSide visitCall(RexCall call) {
      return call.getOperands().stream()
        .map(child -> child.accept(this))
        .reduce(JoinSide.EMPTY, JoinSide::merge);
    }

    @Override public JoinSide visitOver(RexOver over) {
      throw new IllegalArgumentException();
    }

    @Override public JoinSide visitLocalRef(RexLocalRef localRef) {
      throw new IllegalArgumentException();
    }

    @Override public JoinSide visitCorrelVariable(RexCorrelVariable correlVariable) {
      throw new IllegalArgumentException();
    }

    @Override public JoinSide visitDynamicParam(RexDynamicParam dynamicParam) {
      throw new IllegalArgumentException();
    }

    @Override public JoinSide visitRangeRef(RexRangeRef rangeRef) {
      throw new IllegalArgumentException();
    }

    @Override public JoinSide visitFieldAccess(RexFieldAccess fieldAccess) {
      throw new IllegalArgumentException();
    }

    @Override public JoinSide visitSubQuery(RexSubQuery subQuery) {
      throw new IllegalArgumentException();
    }

    @Override public JoinSide visitTableInputRef(RexTableInputRef ref) {
      throw new IllegalArgumentException();
    }

    @Override public JoinSide visitPatternFieldRef(RexPatternFieldRef fieldRef) {
      throw new IllegalArgumentException();
    }
  }

}
