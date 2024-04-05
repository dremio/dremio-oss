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
package com.dremio.sabot.op.receiver.unordered;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.physical.config.UnorderedReceiver;
import com.dremio.exec.record.ArrowRecordBatchLoader;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.sabot.exec.context.MetricDef;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.receiver.RawFragmentBatch;
import com.dremio.sabot.op.receiver.RawFragmentBatchProvider;
import com.dremio.sabot.op.receiver.ReceiverLatencyTracker;
import com.dremio.sabot.op.spi.BatchStreamProvider;
import com.dremio.sabot.op.spi.ProducerOperator;
import com.google.common.base.Preconditions;

public class UnorderedReceiverOperator implements ProducerOperator {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(UnorderedReceiverOperator.class);

  private State state = State.NEEDS_SETUP;
  private final ArrowRecordBatchLoader batchLoader;
  private final RawFragmentBatchProvider fragProvider;
  private final OperatorStats stats;
  private final UnorderedReceiver config;
  private final OperatorContext context;
  private final VectorContainer outgoing;
  private final BatchStreamProvider streams;
  private final ReceiverLatencyTracker latencyTracker = new ReceiverLatencyTracker();

  public enum Metric implements MetricDef {
    BYTES_RECEIVED,
    BATCHES_RECEIVED,
    NUM_SENDERS,
    SUM_TX_MILLIS,
    MAX_TX_MILLIS,
    SUM_QUEUE_MILLIS,
    MAX_QUEUE_MILLIS;

    @Override
    public int metricId() {
      return ordinal();
    }
  }

  public UnorderedReceiverOperator(
      final BatchStreamProvider streams,
      final OperatorContext context,
      final UnorderedReceiver config) {
    if (config.getNumSenders() > 0) {
      final RawFragmentBatchProvider[] buffers =
          streams.getBuffers(config.getSenderMajorFragmentId());
      Preconditions.checkArgument(buffers.length == 1);
      this.fragProvider = buffers[0];
    } else {
      this.fragProvider = null;
    }
    this.streams = streams;
    this.context = context;
    this.config = config;
    this.stats = context.getStats();
    this.stats.setLongStat(Metric.NUM_SENDERS, config.getNumSenders());
    this.outgoing = context.createOutputVectorContainer(config.getSchema());

    // In normal case, batchLoader does not require an allocator. However, in case of
    // splitAndTransfer of a value vector,
    // we may need an allocator for the new offset vector. Therefore, here we pass the context's
    // allocator to batchLoader.
    this.batchLoader = new ArrowRecordBatchLoader(outgoing);
  }

  @Override
  public State getState() {
    return state == State.BLOCKED && !streams.isPotentiallyBlocked() ? State.CAN_PRODUCE : state;
  }

  @Override
  public VectorAccessible setup() throws Exception {
    state.is(State.NEEDS_SETUP);
    if (config.getNumSenders() == 0) {
      state = State.DONE;
    } else {
      state = State.CAN_PRODUCE;
    }
    return outgoing;
  }

  @Override
  public int outputData() throws Exception {
    // use getState here so we can transition out of blocked.
    getState().is(State.CAN_PRODUCE);

    batchLoader.resetRecordCount();

    try (final RawFragmentBatch batch = fragProvider.getNext()) {

      if (batch == null) {
        if (fragProvider.isStreamDone()) {
          state = State.DONE;
        } else {
          state = State.BLOCKED;
        }
        return 0;
      }
      latencyTracker.updateLatencyFromBatch(batch.getHeader());

      int size = batchLoader.load(batch);
      stats.addLongStat(Metric.BYTES_RECEIVED, size);
      stats.addLongStat(Metric.BATCHES_RECEIVED, 1);

      final int count = batchLoader.getRecordCount();
      stats.batchReceived(0, count, size);
      return outgoing.setAllCount(count);
    }
  }

  @Override
  public <OUT, IN, EXCEP extends Throwable> OUT accept(
      OperatorVisitor<OUT, IN, EXCEP> visitor, IN value) throws EXCEP {
    return visitor.visitProducer(this, value);
  }

  @Override
  public void close() throws Exception {
    stats.setLongStat(Metric.SUM_TX_MILLIS, latencyTracker.getSumTxMillis());
    stats.setLongStat(Metric.MAX_TX_MILLIS, latencyTracker.getMaxTxMillis());
    stats.setLongStat(Metric.SUM_QUEUE_MILLIS, latencyTracker.getSumQueueMillis());
    stats.setLongStat(Metric.MAX_QUEUE_MILLIS, latencyTracker.getMaxQueueMillis());
    AutoCloseables.close((AutoCloseable) batchLoader, outgoing);
  }

  public static class Creator implements ReceiverCreator<UnorderedReceiver> {

    @Override
    public ProducerOperator create(
        BatchStreamProvider streams, OperatorContext context, UnorderedReceiver config)
        throws ExecutionSetupException {

      return new UnorderedReceiverOperator(streams, context, config);
    }
  }
}
