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
package com.dremio.sabot.op.receiver;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.physical.config.BridgeFileWriterSender;
import com.dremio.exec.proto.ExecProtos;
import com.dremio.exec.record.ArrowRecordBatchLoader;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.spi.BatchStreamProvider;
import com.dremio.sabot.op.spi.ProducerOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class AbstractBridgeReaderOperator implements ProducerOperator {
  private static final Logger logger = LoggerFactory.getLogger(AbstractBridgeReaderOperator.class);
  private final OperatorContext context;
  private final String uniqueId;
  private final OperatorStats stats;
  private final ArrowRecordBatchLoader batchLoader;
  private final VectorContainer outgoing;
  private final BatchStreamProvider batchStreamProvider;
  private final RawFragmentBatchProvider batchProvider;

  private State state = State.NEEDS_SETUP;

  public AbstractBridgeReaderOperator(
      BatchStreamProvider streams,
      OperatorContext context,
      BatchSchema batchSchema,
      String bridgeSetId) {
    this.context = context;

    ExecProtos.FragmentHandle handle = context.getFragmentHandle();
    this.uniqueId =
        BridgeFileWriterSender.computeUniqueId(
            handle.getQueryId(), bridgeSetId, handle.getMinorFragmentId());
    this.stats = context.getStats();
    this.outgoing = context.createOutputVectorContainer(batchSchema);
    this.batchLoader = new ArrowRecordBatchLoader(outgoing);
    this.batchStreamProvider = streams;
    this.batchProvider = streams.getBuffersFromFiles(uniqueId, handle.getMajorFragmentId());
    logger.debug("uniqueId {}", uniqueId);
  }

  @Override
  public <OUT, IN, EXCEP extends Throwable> OUT accept(
      OperatorVisitor<OUT, IN, EXCEP> visitor, IN value) throws EXCEP {
    return visitor.visitProducer(this, value);
  }

  @Override
  public State getState() {
    return state == State.BLOCKED && !batchStreamProvider.isPotentiallyBlocked()
        ? State.CAN_PRODUCE
        : state;
  }

  @Override
  public VectorAccessible setup() throws Exception {
    state.is(State.NEEDS_SETUP);
    state = State.CAN_PRODUCE;
    return outgoing;
  }

  OperatorStats getStats() {
    return stats;
  }

  @Override
  public int outputData() throws Exception {
    // use getState here so we can transition out of blocked.
    getState().is(State.CAN_PRODUCE);

    batchLoader.resetRecordCount();
    try (final RawFragmentBatch batch = getNextBatch()) {
      if (batch == null) {
        if (batchProvider.isStreamDone()) {
          state = State.DONE;
          logger.debug("switching to DONE state since the stream is done");
        } else {
          state = State.BLOCKED;
          // logger.debug("switching to BLOCKED state since the stream returned null");
        }
        return 0;
      }

      int size = batchLoader.load(batch);
      updateMetrics(batch.getByteCount());

      final int count = batchLoader.getRecordCount();
      // logger.debug("read batch {} records", count);

      stats.batchReceived(0, count, size);
      return outgoing.setAllCount(count);
    }
  }

  private RawFragmentBatch getNextBatch() throws Exception {
    try (AutoCloseable ac = OperatorStats.getWaitRecorder(stats)) {
      return batchProvider.getNext();
    }
  }

  abstract void updateMetrics(long bytesRead);

  @Override
  public void close() throws Exception {
    AutoCloseables.close((AutoCloseable) batchLoader, batchProvider, outgoing);
  }
}
