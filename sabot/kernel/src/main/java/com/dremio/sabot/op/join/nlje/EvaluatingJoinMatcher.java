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
package com.dremio.sabot.op.join.nlje;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.calcite.rel.core.JoinRelType;

import com.dremio.common.AutoCloseables;
import com.dremio.common.AutoCloseables.RollbackCloseable;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.exec.expr.ClassProducer;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.copier.FieldBufferCopier;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;

import io.netty.buffer.ArrowBuf;

/**
 * Manages the matching and outputting process of NLJ. Will output both match records and unmatched probe records in the left/full cases.
 */
class EvaluatingJoinMatcher implements AutoCloseable, JoinMatcher {

  private enum State {INIT, JOINING, NON_MATCHES, BATCH_COMPLETE}

  private final OperatorContext context;
  private final ImmutableList<FieldBufferCopier> buildCopiers;
  private final ImmutableList<FieldBufferCopier> probeCopiers;

  private DualRange inputRange;
  private VectorRange outputRange;
  private State state = State.INIT;
  private MatchGenerator matchGenerator;
  private final BufferAllocator allocator;
  private final VectorAccessible probe;
  private final JoinRelType joinType;
  private long totalProbedCount;
  private final int targetGenerateAtOnce;
  private final Stopwatch matchWatch = Stopwatch.createUnstarted();
  private final Stopwatch copyWatch = Stopwatch.createUnstarted();

  public EvaluatingJoinMatcher(
      OperatorContext context,
      VectorAccessible probe,
      VectorAccessible build,
      int targetGenerateAtOnce,
      DualRange initialMatchState,
      ImmutableList<FieldBufferCopier> probeCopiers,
      ImmutableList<FieldBufferCopier> buildCopiers, JoinRelType joinType) {
    this.inputRange = initialMatchState;
    this.context = context;
    this.joinType = joinType;
    this.probe = probe;
    this.targetGenerateAtOnce = targetGenerateAtOnce;
    this.allocator = context.getAllocator();
    this.buildCopiers = buildCopiers;
    this.probeCopiers = probeCopiers;
  }

  @Override
  public int output() {
    Preconditions.checkState(!needNextInput());
    if(state == State.JOINING) {
      return outputJoin();
    } else {
      return outputNonMatches();
    }
  }

  private int outputJoin() {
    Preconditions.checkArgument(state == State.JOINING);
    final IntRange range = outputRange.getCurrentOutputRange();
    final int count = outputRange.getCurrentOutputCount();

    copyWatch.start();
    {
      final long probe = outputRange.getProbeOffsets2() + range.start * VectorRange.PROBE_OUTPUT_SIZE;
      for(FieldBufferCopier fb : probeCopiers) {
        fb.allocate(count);
        fb.copy(probe, count);
      }
    }

    {
      final long build = outputRange.getBuildOffsets4() + range.start * VectorRange.BUILD_OUTPUT_SIZE;
      for(FieldBufferCopier fb : buildCopiers) {
        fb.allocate(count);
        fb.copy(build, count);
      }
    }

    copyWatch.stop();

    // now set things up for next time.
    outputRange = outputRange.nextOutput();
    if(outputRange.isEmpty()) {
      // we don't have any records for current match review.
      inputRange = inputRange.nextOutput();
      if(inputRange.isEmpty()) {
        // we depleted all of the data in this batch for matches. Either finish batch or output non matches.
        if(joinType == JoinRelType.INNER) {
          state = State.BATCH_COMPLETE;
        } else {
          state = State.NON_MATCHES;
        }
        return count;
      }

      matchWatch.start();
      outputRange = matchGenerator.tryMatch(inputRange, outputRange);
      matchWatch.stop();
    }

    return count;
  }

  /**
   * All non-matching probe outputs are returned in a single batch that is no larger than the input batch.
   * @return
   */
  private int outputNonMatches() {
    Preconditions.checkState(state == State.NON_MATCHES, "Expected state NON_MATCHES, got %s.", state);
    final MatchedVector probeMatchVector = matchGenerator.getProbeMatchVector();
    try (ArrowBuf offsetCopier = allocator.buffer(probeMatchVector.count() * VectorRange.PROBE_OUTPUT_SIZE)) {
      int output = 0;
      final int end = probe.getRecordCount();
      for (int i = 0; i < end; i++) {
        if (probeMatchVector.isSet(i)) {
          offsetCopier.setShort(output * VectorRange.PROBE_OUTPUT_SIZE, i);
          output++;
        }
      }

      copyWatch.start();
      for (FieldBufferCopier fb : probeCopiers) {
        fb.allocate(output);
        fb.copy(offsetCopier.memoryAddress(), output);
      }
      copyWatch.stop();

      state = State.BATCH_COMPLETE;
      return output;
    }
  }

  public long getCopyNanos() {
    return copyWatch.elapsed(TimeUnit.NANOSECONDS);
  }

  public long getMatchNanos() {
    return matchWatch.elapsed(TimeUnit.NANOSECONDS);
  }

  @Override
  public void setup(LogicalExpression expr, ClassProducer classProducer, VectorAccessible probe, VectorAccessible build) throws Exception {
    Preconditions.checkState(state == State.INIT);

    matchGenerator = MatchGenerator.generate(expr, classProducer, probe, build);

    try(RollbackCloseable rbc = new RollbackCloseable()) {
      final boolean maintainMatches = joinType != JoinRelType.INNER;
      final Optional<MatchedVector> matched;
      if(maintainMatches) {
        matched = Optional.of(rbc.add(new MatchedVector(allocator)));
      } else {
        matched = Optional.empty();
      }
      outputRange = rbc.add(new VectorRange(targetGenerateAtOnce, context.getTargetBatchSize()));
      outputRange.allocate(allocator);
      matchGenerator.setup(matched, context.getFunctionContext(), probe, build, targetGenerateAtOnce);
      rbc.commit();
    }

  }

  @Override
  public void startNextProbe(int records) {
    Preconditions.checkState(state == State.BATCH_COMPLETE || state == State.INIT);
    matchGenerator.clearProbeValidity(records);
    matchWatch.start();
    inputRange = inputRange.startNextProbe(records).nextOutput();
    outputRange = matchGenerator.tryMatch(inputRange, outputRange).nextOutput();
    matchWatch.stop();
    state = State.JOINING;
  }

  @Override
  public boolean needNextInput() {
    return !outputRange.hasNext() && state == State.BATCH_COMPLETE;
  }

  @Override
  public long getProbeCount() {
    return totalProbedCount;
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(matchGenerator, inputRange, outputRange);
  }

}
