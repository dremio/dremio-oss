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

import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.ValueVector;
import org.apache.calcite.rel.core.JoinRelType;

import com.dremio.common.AutoCloseables;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.exec.expr.ClassProducer;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.copier.FieldBufferCopier;
import com.google.common.base.MoreObjects;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;

import io.netty.buffer.ArrowBuf;

/**
 * Manages the matching and outputting process of NLJ. Will output both match records and unmatched probe records in the left/full cases.
 */
class EvaluatingJoinMatcher implements AutoCloseable, JoinMatcher {

  private enum State {INIT, JOINING, NON_MATCHES, BATCH_COMPLETE}

  private final OperatorContext context;
  // defines how many matches we do at once.
  private final int probeBatchSize;
  private final int buildBatchCount;
  private final int[] buildRecordsPerBatch;
  private final ImmutableList<FieldBufferCopier> buildCopiers;
  private final ImmutableList<FieldBufferCopier> probeCopiers;

  private MatchState matchState = new MatchState(0,0,0,0,0);
  private State state = State.INIT;
  private MatchGenerator matchGenerator;
  private final BufferAllocator allocator;
  private final int targetGenerateAtOnce;
  private final VectorAccessible probe;
  private final JoinRelType joinType;
  private long totalProbedCount;
  private final Stopwatch matchWatch = Stopwatch.createUnstarted();
  private final Stopwatch copyWatch = Stopwatch.createUnstarted();

  public EvaluatingJoinMatcher(OperatorContext context, VectorAccessible probe, VectorAccessible build, final int targetGenerateAtOnce,
      ImmutableList<FieldBufferCopier> probeCopiers, ImmutableList<FieldBufferCopier> buildCopiers, JoinRelType joinType) {
    this.context = context;
    VectorWrapper<?> wrapper = build.iterator().next();
    ValueVector[] vectors = wrapper.getValueVectors();
    int[] counts = new int[vectors.length];
    int maxBuildCount = 0;
    for(int i = 0; i < vectors.length; i++) {
      counts[i] = vectors[i].getValueCount();
      maxBuildCount = Math.max(maxBuildCount, counts[i]);
    }
    this.joinType = joinType;
    this.probe = probe;
    this.targetGenerateAtOnce = targetGenerateAtOnce;
    this.allocator = context.getAllocator();
    this.probeBatchSize = (int) (1.0d * targetGenerateAtOnce)/maxBuildCount;
    this.buildRecordsPerBatch = counts;
    this.buildBatchCount = counts.length;
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
    final int count = matchState.getOutputEnd() - matchState.getOutputStart();

    copyWatch.start();
    {
      final long probe = matchGenerator.getProbeOutputAddress() + matchState.getOutputStart() * MatchGenerator.PROBE_OUTPUT_SIZE;
      for(FieldBufferCopier fb : probeCopiers) {
        fb.allocate(count);
        fb.copy(probe, count);
      }
    }

    {
      final long build = matchGenerator.getBuildOutputAddress() + matchState.getOutputStart() * MatchGenerator.BUILD_OUTPUT_SIZE;
      for(FieldBufferCopier fb : buildCopiers) {
        fb.allocate(count);
        fb.copy(build, count);
      }
    }

    copyWatch.stop();

    int probeStart = matchState.probeStart;
    int buildBatch = matchState.buildBatchIndex;
    matchState = matchState.nextOutput();

    if(buildBatch != matchState.buildBatchIndex || probeStart != matchState.probeStart) {
      matchWatch.start();
      matchState = matchGenerator.tryMatch(matchState, buildRecordsPerBatch[matchState.buildBatchIndex]);
      matchWatch.stop();
    }

    if (!matchState.depleted()) {
      return count;
    }

    // we finished all of the data for probe matches, now output probe non-matches.
    if(joinType == JoinRelType.INNER) {
      state = State.BATCH_COMPLETE;
    } else {
      state = State.NON_MATCHES;
    }
    return count;
  }

  /**
   * All non-matching probe outputs are returned in a single batch that is no larger than the input batch.
   * @return
   */
  private int outputNonMatches() {
    Preconditions.checkState(state == State.NON_MATCHES);
    final ArrowBuf probeMatchVector = matchGenerator.getProbeMatchVector();
    int nonMatchCount = BitVectorHelper.getNullCount(probeMatchVector, probe.getRecordCount());
    try (ArrowBuf offsetCopier = allocator.buffer(nonMatchCount * MatchGenerator.PROBE_OUTPUT_SIZE)) {
      int output = 0;
      final int end = probe.getRecordCount();
      for (int i = 0; i < end; i++) {
        if (BitVectorHelper.get(probeMatchVector, i) == 0) {
          offsetCopier.setShort(output * MatchGenerator.PROBE_OUTPUT_SIZE, i);
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
    matchGenerator.setup(allocator, context.getFunctionContext(), probe, build, targetGenerateAtOnce, joinType != JoinRelType.INNER);
  }

  @Override
  public void startNextProbe(int records) {
    Preconditions.checkState(state == State.BATCH_COMPLETE || state == State.INIT);
    matchGenerator.clearProbeValidity();
    matchWatch.start();
    matchState = matchGenerator.tryMatch(matchState.reset(records), buildRecordsPerBatch[matchState.buildBatchIndex]);
    matchWatch.stop();
    state = State.JOINING;
  }

  @Override
  public boolean needNextInput() {
    return matchState.depleted() && state == State.BATCH_COMPLETE;
  }

  @Override
  public long getProbeCount() {
    return totalProbedCount;
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(matchGenerator);
  }

  /**
   * Encapsulates the state of output for a particular probe batch.
   */
  public class MatchState {
    private final int probeStart;
    private final int probeCount;
    private final int outputStart;
    private final int outputCount;
    private final int buildBatchIndex;

    private MatchState(int probeStart, int probeCount, int outputStart, int outputCount, int buildBatchIndex) {
      super();
      this.probeStart = probeStart;
      this.probeCount = probeCount;
      this.outputStart = outputStart;
      this.outputCount = outputCount;
      this.buildBatchIndex = buildBatchIndex;
      // System.out.print(this);
    }

    public boolean depleted() {
      return probeStart == probeCount && outputStart == outputCount;
    }

    public MatchState nextOutput() {
      if(getOutputEnd() < outputCount) {
        // next output
        return new MatchState(probeStart, probeCount, getOutputEnd(), outputCount, buildBatchIndex);
      }

      if(getProbeEnd() < probeCount) {
        // next probe range.
        totalProbedCount += getProbeEnd() - probeStart;
        return new MatchState(getProbeEnd(), probeCount, 0, 0, buildBatchIndex);
      }

      if(buildBatchIndex + 1 < buildBatchCount) {
        // next build batch.
        return new MatchState(0, probeCount, 0, 0, buildBatchIndex + 1);
      }

      return reset(0);
    }

    public int getProbeEnd() {
      return Math.min(probeStart + probeBatchSize, probeCount);
    }

    public int getOutputEnd() {
      return Math.min(outputStart + context.getTargetBatchSize(), outputCount);
    }

    public MatchState withOutputCount(int outputCount) {
      return new MatchState(probeStart, probeCount, 0, outputCount, buildBatchIndex);
    }

    public int getProbeStart() {
      return probeStart;
    }

    public int getOutputStart() {
      return outputStart;
    }

    public int getBuildBatchIndex() {
      return buildBatchIndex;
    }

    public MatchState reset(int records) {
      return new MatchState(0, records, 0, 0, 0);
    }

    public String toString() {
      return MoreObjects
          .toStringHelper(this)
          .add("probeStart", probeStart)
          .add("probeCount", probeCount)
          .add("outputStart", outputStart)
          .add("outputCount", outputCount)
          .add("buildBatchIndex", buildBatchIndex)
          .add("buildBatchCount", buildBatchCount)
          .toString();
    }
  }

}
