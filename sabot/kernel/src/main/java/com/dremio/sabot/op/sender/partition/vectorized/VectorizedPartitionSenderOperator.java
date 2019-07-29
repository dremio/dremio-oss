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
package com.dremio.sabot.op.sender.partition.vectorized;

import static com.dremio.sabot.op.sender.partition.PartitionSenderOperator.Metric.N_RECEIVERS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID;
import org.apache.arrow.vector.types.pojo.Field;

import com.carrotsearch.hppc.IntArrayList;
import com.dremio.common.AutoCloseables;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.util.Numbers;
import com.dremio.exec.physical.config.HashPartitionSender;
import com.dremio.exec.physical.config.MinorFragmentEndpoint;
import com.dremio.exec.proto.ExecProtos;
import com.dremio.exec.proto.ExecRPC;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.TypedFieldId;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.exec.rpc.AccountingExecTunnel;
import com.dremio.sabot.exec.rpc.TunnelProvider;
import com.dremio.sabot.op.sender.BaseSender;
import com.dremio.sabot.op.sender.partition.PartitionSenderOperator.Metric;
import com.dremio.sabot.op.sender.partition.vectorized.MultiDestCopier.CopyWatches;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;

import io.netty.util.internal.PlatformDependent;

/**
 * Implementation of hash partition sender that relies on vectorized copy of the data.<br>
 * Each incoming batch may be processed in multiple passes, each time copying up to numRecordsBeforeFlush rows.
 */
public class VectorizedPartitionSenderOperator extends BaseSender {
  @VisibleForTesting
  public static final int PARTITION_MULTIPLE = 8;

  /** used to ensure outgoing batches creation and */
  private final Object batchCreationLock = new Object();

  private final OperatorContext context;
  private final HashPartitionSender config;
  private final TunnelProvider tunnelProvider;
  private final int numReceivers;
  private final AtomicIntegerArray remainingReceivers;
  private IntArrayList terminations = new IntArrayList();
  private final AtomicInteger remaingReceiverCount;

  private State state = State.NEEDS_SETUP;
  private IntVector partitionIndices;

  private final OperatorStats stats;
  private final CopyWatches copyWatches = new CopyWatches();
  private final Stopwatch preCopyWatch = Stopwatch.createUnstarted();
  private final Stopwatch flushWatch = Stopwatch.createUnstarted();

  /**
   * number of records before we flush any outgoing batch.<br>
   * used to be potentially different for each destination, but not anymore as we use this value to decide
   * how many rows we should copy in a single pass
   * */
  private int numRecordsBeforeFlush;

  private List<MultiDestCopier> copiers = Lists.newArrayList();

  /**
   * two outgoing batches per receiver, so we can delay flushing the batches after we copy the incoming
   * batch.
   */
  private final OutgoingBatch[] batches;

  /**
   * modLookup[p] = outgoing batch that should receive the next row for partition p.<br>
   * Sized to a power-of-two to ensure we can use bitwise operation instead of mod when computing
   * the target partition for a given hash
   * Batches are assigned round-robin to the partition lookup:
   *   modLookup[0] = batches[0]
   *   modLookup[1] = batches[1]
   *   ...
   *   modLookup[#receivers-1] = batches[#receivers-1]
   *   modLookup[#receivers] = batches[0]
   *   modLookup[#receivers+1] = batches[1]
   *   ...
   *   modLookup(2*#receivers-1] = batches[#receivers-1]
   *   ...
   *   # Pattern repeats PARTITION_MULTIPLE times
   * The relationship between a partition# and a batch# is always:
   *   batchNum % numReceivers = partitionNum % numReceivers
   * Please note that this relationship holds even though there are twice as many batches as there are receivers
   */
  private final OutgoingBatch[] modLookup;

  /** number of partitions. Set to the next power of 2 of the number of receivers */
  private final int modSize;

  /**
   * holds the (batchIdx, rowIdx) as a compound value for each row that will be copied in the current pass
   */
  private IntVector copyIndices;

  /**
   * true if all receivers finished.
   */
  private volatile boolean nobodyListening = false;

  public VectorizedPartitionSenderOperator(final OperatorContext context,
                                           final TunnelProvider tunnelProvider,
                                           final HashPartitionSender config) {
    super(config);
    this.context = context;
    this.config = config;
    this.tunnelProvider = tunnelProvider;
    this.stats = context.getStats();
    this.numReceivers = config.getDestinations().size();

    remainingReceivers = new AtomicIntegerArray(numReceivers);
    remaingReceiverCount = new AtomicInteger(numReceivers);

    stats.setLongStat(N_RECEIVERS, numReceivers);

    modSize = PARTITION_MULTIPLE * Numbers.nextPowerOfTwo(numReceivers);
    modLookup = new OutgoingBatch[modSize];
    batches = new OutgoingBatch[2 * numReceivers];
  }

  @Override
  public State getState() {
    return state;
  }

  @Override
  public void setup(VectorAccessible incoming) throws Exception {
    state.is(State.NEEDS_SETUP);
    Preconditions.checkState(incoming.getSchema().getSelectionVectorMode() == BatchSchema.SelectionVectorMode.NONE,
      "Vectorized Partition Sender doesn't support SV " + incoming.getSchema().getSelectionVectorMode());

    checkSchema(incoming.getSchema());

    // how many records we can keep in memory before we are forced to flush the outgoing batch
    numRecordsBeforeFlush = config.getProps().getTargetBatchSize();
    stats.setLongStat(Metric.BUCKET_SIZE, numRecordsBeforeFlush);

    //
    final BufferAllocator allocator = context.getAllocator();
    // we need to synchronize this to ensure no receiver termination message is lost
    synchronized (batchCreationLock) {
      initBatchesAndLookup(incoming);

      // some receivers may have finished already, make sure to terminate corresponding outgoing batches
      for (int index = 0; index < terminations.size(); index++) {
        final int p = terminations.buffer[index];
        batches[p].terminate();
        batches[p + numReceivers].terminate();
      }
      terminations.clear();
    }

    copiers = MultiDestCopier.getCopiers(VectorContainer.getFieldVectors(incoming), batches, copyWatches);

    copyIndices = new IntVector("copy-compound-indices", allocator);
    copyIndices.allocateNew(numRecordsBeforeFlush);

    initHashVector(incoming);

    state = State.CAN_CONSUME;
  }

  private void initHashVector(VectorAccessible incoming) {
    Preconditions.checkArgument(config.getExpr() instanceof SchemaPath,
      "hash expression expected to be a SchemaPath but was : " + config.getExpr().getClass().getName());

    final SchemaPath expr = (SchemaPath) config.getExpr();
    final TypedFieldId typedFieldId = incoming.getSchema().getFieldId(expr);
    final Field field = incoming.getSchema().getColumn(typedFieldId.getFieldIds()[0]);
    Preconditions.checkArgument(field.getType().getTypeID() == ArrowTypeID.Int);
    partitionIndices = incoming.getValueAccessorById(IntVector.class, typedFieldId.getFieldIds()[0]).getValueVector();
  }

  /**
   * setup all outgoing batches and modLookup
   */
  private void initBatchesAndLookup(VectorAccessible incoming) {
    final BufferAllocator allocator = context.getAllocator();
    final List<MinorFragmentEndpoint> destinations = config.getDestinations(context.getEndpointsIndex());
    for (int p = 0; p < numReceivers; p++) {
      final int batchB = numReceivers + p;

      final MinorFragmentEndpoint destination = destinations.get(p);
      final AccountingExecTunnel tunnel = tunnelProvider.getExecTunnel(destination.getEndpoint());

      batches[p] = new OutgoingBatch(p, batchB, numRecordsBeforeFlush, incoming, allocator, tunnel, config, context, destination.getMinorFragmentId(), stats);
      batches[batchB] = new OutgoingBatch(batchB, p, numRecordsBeforeFlush, incoming, allocator, tunnel, config, context, destination.getMinorFragmentId(), stats);

      // Only allocate the primary batch. Backup batch is allocated when it is needed.
      batches[p].allocateNew();
    }
    for (int p = 0; p < modSize; p++) {
      modLookup[p] = batches[p % numReceivers];
    }
  }

  @Override
  public void consumeData(int records) throws Exception {
    state.is(State.CAN_CONSUME);

    if (nobodyListening) {
      state = State.DONE;
      return;
    }

    int start = 0;
    int numPasses = 0;

    final OutgoingBatch[] batches = this.batches;
    final int numBatches = batches.length;
    final List<MultiDestCopier> copiers = this.copiers;

    while (start < records) {
      preCopyWatch.start();
      // copy at most numRecordsBeforeFlush, this way we'll need at most 2 batches per destination
      // to allow delaying the flushing until after all rows for this pass have been copied
      int numRowsToCopy = Math.min(records - start, numRecordsBeforeFlush);
      generateCopyIndices(start, numRowsToCopy);
      preCopyWatch.stop();

      // copy
      final long addr = copyIndices.getDataBufferAddress();
      for (MultiDestCopier copier : copiers) {
        copier.copy(addr, start, numRowsToCopy);
      }

      // flush
      flushWatch.start();
      for (int b = 0; b < numBatches; b++) {
        final OutgoingBatch batch = batches[b];
        if (batch.isFull()) {
          batch.flush();
        }
      }
      flushWatch.stop();

      start += numRowsToCopy;
      numPasses++;
    }
    stats.addLongStat(Metric.NUM_COPIES, numPasses);
    stats.setLongStat(Metric.PRECOPY_NS, preCopyWatch.elapsed(NANOSECONDS));
    stats.setLongStat(Metric.FLUSH_NS, flushWatch.elapsed(NANOSECONDS));
    copyWatches.updateStats(stats);
  }

  @Override
  public void noMoreToConsume() throws Exception {
    state.is(State.CAN_CONSUME);

    if (nobodyListening) {
      state = State.DONE;
      return;
    }

    // flush everything
    flushWatch.start();
    for (OutgoingBatch batch : batches) {
      batch.flush();
    }
    flushWatch.stop();

    sendTermination();

    state = State.DONE;

    stats.setLongStat(Metric.FLUSH_NS, flushWatch.elapsed(NANOSECONDS));
  }

  @Override
  public void receivingFragmentFinished(ExecProtos.FragmentHandle handle) throws Exception {
    final int id = handle.getMinorFragmentId();
    if (remainingReceivers.compareAndSet(id, 0, 1)) {
      synchronized (batchCreationLock) {
        if (batches[id] == null) {
          terminations.add(id);
        } else {
          batches[id].terminate();
          batches[id + numReceivers].terminate();
        }
      }

      int remaining = remaingReceiverCount.decrementAndGet();
      if (remaining == 0) {
        nobodyListening = true;
      }
    }
  }

  private void sendTermination() {
    final ExecProtos.FragmentHandle handle = context.getFragmentHandle();

    stats.startWait();
    for (MinorFragmentEndpoint destination : config.getDestinations(context.getEndpointsIndex())) {
      // don't send termination message if the receiver fragment is already terminated.
      if (remainingReceivers.get(destination.getMinorFragmentId()) == 0) {
        ExecRPC.FragmentStreamComplete completion = ExecRPC.FragmentStreamComplete.newBuilder()
            .setQueryId(handle.getQueryId())
            .setSendingMajorFragmentId(handle.getMajorFragmentId())
            .setSendingMinorFragmentId(handle.getMinorFragmentId())
            .setReceivingMajorFragmentId(config.getReceiverMajorFragmentId())
            .addReceivingMinorFragmentId(destination.getMinorFragmentId())
            .build();
        tunnelProvider.getExecTunnel(destination.getEndpoint()).sendStreamComplete(completion);
      }
    }
    stats.stopWait();
  }

  private void generateCopyIndices(final int start, final int numRowsToCopy) {
    long srcAddr = partitionIndices.getDataBufferAddress() + start*4;
    long dstAddr = copyIndices.getDataBufferAddress();

    final int mod = modSize - 1;
    final OutgoingBatch[] modLookup = this.modLookup;
    final OutgoingBatch[] batches = this.batches;

    //populate using the destination (batchIdx, rowIdx) for each incoming row
    final long max = srcAddr + numRowsToCopy*4;
    for (; srcAddr < max; srcAddr+=4, dstAddr+=4) {
      final int partition = (PlatformDependent.getInt(srcAddr) & 0x7FFFFFFF) & mod; // abs(hash) % modSize
      final OutgoingBatch batch = modLookup[partition];
      final int compound = batch.preCopyRow();
      PlatformDependent.putInt(dstAddr, compound);

      if (batch.isFull()) {
        // if current batch is full, we will copy to a different batch from now on
        final int nextBatchIdx = batch.getNextBatchIdx();
        final OutgoingBatch nextBatch = batches[nextBatchIdx];
        nextBatch.allocateNew();
        for (MultiDestCopier copier : copiers) {
          copier.updateTargets(nextBatchIdx, nextBatch.getFieldVector(copier.getFieldId()));
        }
        // Paired batches must be located at very specific places within modLookup. In particular, the batch pair
        // repeats every #receivers (see the comment above the modLookup definition).
        assert (batch.getBatchIdx() % numReceivers) == (nextBatchIdx % numReceivers) :
          String.format("Batch pairs must be aligned to #receivers. Instead: curr batch: %d, next batch: %d, #receivers: %d",
            batch.getBatchIdx(), nextBatchIdx, numReceivers);
        for (int b = (nextBatchIdx % numReceivers); b < modSize; b += numReceivers) {
          modLookup[b] = nextBatch;
        }
      }
    }
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(Arrays.asList(batches), Arrays.asList(copyIndices, partitionIndices));
  }

  @Override
  public <OUT, IN, EXCEP extends Throwable> OUT accept(OperatorVisitor<OUT, IN, EXCEP> visitor, IN value) throws EXCEP {
    return visitor.visitTerminalOperator(this, value);
  }

  @VisibleForTesting
  public OperatorContext getOperatorContext() {
    return context;
  }
}
