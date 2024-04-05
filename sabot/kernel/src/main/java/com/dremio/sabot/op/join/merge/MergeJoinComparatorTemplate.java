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
package com.dremio.sabot.op.join.merge;

import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.sabot.exec.context.FunctionContext;
import com.dremio.sabot.op.join.merge.MergeJoinOperator.InternalState;
import com.google.common.base.Preconditions;
import javax.inject.Named;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.commons.lang3.tuple.Pair;

/**
 * The comparator handles different join types, and specific merge join looping logic
 *
 * <p>Notable issue TODO: DX-12621 Improve performance by projecting records in the same column at a
 * time
 */
public abstract class MergeJoinComparatorTemplate implements MergeJoinComparator {
  private MarkedAsyncIterator leftIterator;
  private MarkedAsyncIterator rightIterator;
  private VectorContainer outgoing;

  // keep track of last batch passed to doSetup, in case we move across batches
  private VectorAccessible lastLeft;
  private VectorAccessible lastRight;

  private JoinRelType joinType;
  private int targetRecordsPerBatch;

  // keep track of how many output records have we produced
  private int outputRecordsCounter;

  private InternalState state = InternalState.NEEDS_SETUP;

  private FunctionContext context;

  @Override
  public void setupMergeJoin(
      FunctionContext functionContext,
      JoinRelType joinType,
      MarkedAsyncIterator leftIterator,
      MarkedAsyncIterator rightIterator,
      VectorContainer outgoing,
      int targetRecordsPerBatch) {
    Preconditions.checkArgument(state == InternalState.NEEDS_SETUP);
    state = InternalState.OUT_OF_LOOPS;

    this.leftIterator = leftIterator;
    this.rightIterator = rightIterator;
    this.joinType = joinType;
    this.targetRecordsPerBatch = targetRecordsPerBatch;
    this.outputRecordsCounter = 0;
    this.outgoing = outgoing;
    this.context = functionContext;

    lastLeft = null;
    lastRight = null;
  }

  @Override
  public InternalState getInternalState() {
    return state;
  }

  @Override
  public boolean finishNonMatching() {
    Preconditions.checkArgument(!leftIterator.hasNext() || !rightIterator.hasNext());

    // allocate new buffer for new batch
    if (outputRecordsCounter == 0) {
      outgoing.allocateNew();
    }

    if (joinType == JoinRelType.RIGHT) {
      return projectRightNonMatching();
    } else if (joinType == JoinRelType.LEFT) {
      return projectLeftNonMatching();
    } else if (joinType == JoinRelType.FULL) {
      if (leftIterator.hasNext()) {
        return projectLeftNonMatching();
      } else {
        return projectRightNonMatching();
      }
    } else {
      throw new IllegalStateException("Reach unexpected state");
    }
  }

  // helper function to continue left outer join after all comparisons
  private boolean projectLeftNonMatching() {
    while (leftIterator.hasNext() && outputRecordsCounter < targetRecordsPerBatch) {
      Pair<VectorAccessible, Integer> next = leftIterator.next();
      yieldLeft(next.getLeft(), next.getRight());
    }

    return !leftIterator.hasNext();
  }

  // helper function to continue right outer join after all comparisons
  private boolean projectRightNonMatching() {
    while (rightIterator.hasNext() && outputRecordsCounter < targetRecordsPerBatch) {
      Pair<VectorAccessible, Integer> next = rightIterator.next();
      yieldRight(next.getLeft(), next.getRight());
    }

    return !rightIterator.hasNext();
  }

  @Override
  public void continueJoin() {
    // allocate new buffer for new batch
    if (outputRecordsCounter == 0) {
      outgoing.allocateNew();
    }

    // Special logic for reaching end of right table in inner loop
    if (!rightIterator.hasNext() && state == InternalState.IN_INNER_LOOP) {
      state = InternalState.IN_OUTER_LOOP;
      if (leftIterator.hasNext()) {
        leftIterator.next();
        if (leftIterator.hasNext()) {
          continueFromInOuterLoop();
        } else {
          return;
        }
      } else {
        // reaching end of comparison
        rightIterator.clearMark();
        return;
      }
    }

    // comparison continues if:
    // out of loops / inner loop --> have data from both table
    // outer loop --> have data from left, because will resume from marked position from right
    while (outputRecordsCounter < targetRecordsPerBatch
        && leftIterator.hasNext()
        && (rightIterator.hasNext() || state == InternalState.IN_OUTER_LOOP)) {
      switch (state) {
        case OUT_OF_LOOPS:
          continueFromOutOfLoops();
          break;
        case IN_OUTER_LOOP:
          continueFromInOuterLoop();
          break;
        case IN_INNER_LOOP:
          continueFromInInnerLoop();
          break;
        default:
          throw new IllegalStateException("Reach illegal state");
      }
    }
  }

  @Override
  public int getOutputSize() {
    return this.outputRecordsCounter;
  }

  @Override
  public void resetOutputCounter() {
    this.outputRecordsCounter = 0;
  }

  /**
   * while (r < s) { advance r, yield <r, null> if left or full join } while (r > s) { advance s,
   * yield <null, s> if right or full join }
   *
   * <p>if (r == s) { mark s yield <r, s> advance s
   *
   * <p>goto inner_loop }
   */
  private void continueFromOutOfLoops() {
    while (outputRecordsCounter < targetRecordsPerBatch) {
      if (!leftIterator.hasNext() || !rightIterator.hasNext()) {
        // need more data
        return;
      }

      final Pair<VectorAccessible, Integer> left = leftIterator.peek();
      final Pair<VectorAccessible, Integer> right = rightIterator.peek();

      final int compareOutput =
          _doCompare(left.getLeft(), right.getLeft(), left.getRight(), right.getRight());

      if (compareOutput < 0) {
        // left < right
        if (joinType == JoinRelType.LEFT || joinType == JoinRelType.FULL) {
          yieldLeft(left.getLeft(), left.getRight());
        }
        leftIterator.next();
      } else if (compareOutput > 0) {
        // left > right
        if (joinType == JoinRelType.RIGHT || joinType == JoinRelType.FULL) {
          yieldRight(right.getLeft(), right.getRight());
        }
        rightIterator.next();
      } else {
        // equal
        yield(left.getLeft(), right.getLeft(), left.getRight(), right.getRight());

        rightIterator.mark();
        rightIterator.next();

        state = InternalState.IN_INNER_LOOP;
        return;
      }
    }
  }

  /**
   * if (r == (s at marked position)) { reset s to mark yield <r, s> advance s
   *
   * <p>goto inner_loop } else { goto out_of_loops }
   */
  private void continueFromInOuterLoop() {
    final Pair<VectorAccessible, Integer> left = leftIterator.peek();

    // check for end of cartesian join
    final Pair<VectorAccessible, Integer> rightMarked = rightIterator.peekMark();
    final int compareMarked =
        _doCompare(left.getLeft(), rightMarked.getLeft(), left.getRight(), rightMarked.getRight());
    if (compareMarked != 0) {
      // end of cartesian join, return to out of loops
      state = InternalState.OUT_OF_LOOPS;
      rightIterator.clearMark();
    } else {
      rightIterator.resetToMark();

      final Pair<VectorAccessible, Integer> right = rightIterator.peek();

      yield(left.getLeft(), right.getLeft(), left.getRight(), right.getRight());
      rightIterator.next();

      state = InternalState.IN_INNER_LOOP;
    }
  }

  /**
   * while (r == s) { yield <r, s> advance s }
   *
   * <p>advance r goto outer_loop
   */
  private void continueFromInInnerLoop() {
    while (outputRecordsCounter < targetRecordsPerBatch) {
      if (!leftIterator.hasNext() || !rightIterator.hasNext()) {
        // need more data
        return;
      }

      final Pair<VectorAccessible, Integer> left = leftIterator.peek();
      final Pair<VectorAccessible, Integer> right = rightIterator.peek();

      final int compareOutput =
          _doCompare(left.getLeft(), right.getLeft(), left.getRight(), right.getRight());

      if (compareOutput != 0) {
        // left != right
        state = InternalState.IN_OUTER_LOOP;
        leftIterator.next();
        return;
      } else {
        yield(left.getLeft(), right.getLeft(), left.getRight(), right.getRight());
        rightIterator.next();
        continue;
      }
    }
  }

  // helper function for comparing two records
  private int _doCompare(
      VectorAccessible leftBatch, VectorAccessible rightBatch, int leftIndex, int rightIndex) {
    if (this.lastLeft != leftBatch || this.lastRight != rightBatch) {
      doSetup(this.context, leftBatch, rightBatch, outgoing);
      lastLeft = leftBatch;
      lastRight = rightBatch;
    }
    return doCompare(leftIndex, rightIndex);
  }

  // helper function for yielding records
  private void yield(
      VectorAccessible leftBatch, VectorAccessible rightBatch, int leftIndex, int rightIndex) {
    if (this.lastLeft != leftBatch || this.lastRight != rightBatch) {
      doSetup(this.context, leftBatch, rightBatch, outgoing);
      lastLeft = leftBatch;
      lastRight = rightBatch;
    }

    doProject(leftIndex, rightIndex, this.outputRecordsCounter);
    this.outputRecordsCounter++;
  }

  private void yieldLeft(VectorAccessible leftBatch, int leftIndex) {
    if (this.lastLeft != leftBatch) {
      doSetup(this.context, leftBatch, rightIterator.getDummyBatch(), outgoing);
      lastLeft = leftBatch;
    }

    doProject(leftIndex, -1, this.outputRecordsCounter);
    this.outputRecordsCounter++;
  }

  private void yieldRight(VectorAccessible rightBatch, int rightIndex) {
    if (this.lastRight != rightBatch) {
      doSetup(this.context, leftIterator.getDummyBatch(), rightBatch, outgoing);
      lastRight = rightBatch;
    }

    doProject(-1, rightIndex, this.outputRecordsCounter);
    this.outputRecordsCounter++;
  }

  /** below are template methods that actually do the join */
  protected abstract void doSetup(
      @Named("context") FunctionContext context,
      @Named("leftBatch") VectorAccessible leftBatch,
      @Named("rightBatch") VectorAccessible rightBatch,
      @Named("outgoing") VectorAccessible outgoing);

  // return a negative integer, zero, or a positive integer as the first argument is less than,
  // equal to, or greater than the second
  protected abstract int doCompare(
      @Named("leftIndex") int leftIndex, @Named("rightIndex") int rightIndex);

  // project, set index of null side to -1 to skip
  protected abstract void doProject(
      @Named("leftIndex") int leftIndex,
      @Named("rightIndex") int rightIndex,
      @Named("outIndex") int outIndex);
}
