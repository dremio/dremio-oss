/*
 * Copyright (C) 2017 Dremio Corporation
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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.NullableIntVector;
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID;
import org.apache.arrow.vector.types.pojo.Field;

import com.carrotsearch.hppc.IntArrayList;
import com.dremio.common.AutoCloseables;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.util.Numbers;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.physical.MinorFragmentEndpoint;
import com.dremio.exec.physical.config.HashPartitionSender;
import com.dremio.exec.proto.ExecProtos;
import com.dremio.exec.proto.ExecRPC;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.TypedFieldId;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.server.options.OptionManager;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.exec.rpc.AccountingExecTunnel;
import com.dremio.sabot.exec.rpc.TunnelProvider;
import com.dremio.sabot.op.sender.BaseSender;
import com.dremio.sabot.op.sender.partition.PartitionSenderOperator.Metric;
import com.dremio.sabot.op.sender.partition.vectorized.MultiDestCopier.CopyWatches;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;

import io.netty.util.internal.PlatformDependent;

/**
 * Implementation of hash partition sender that relies on vectorized copy of the data.<br>
 * Each incoming batch may be processed in multiple passes, each time copying up to numRecordsBeforeFlush rows.
 */
public class VectorizedPartitionSenderOperator extends BaseSender {

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
  private NullableIntVector partitionIndices;

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
   * Twice the number of receivers to ensure we can use bitwise operation instead of mod when computing
   * the target partition for a given hash AND we can update both associated batches with a simple access
   */
  private final OutgoingBatch[] modLookup;

  /** next power of 2 of number of receivers */
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

    modSize = Numbers.nextPowerOfTwo(numReceivers);
    modLookup = new OutgoingBatch[numReceivers * 2];
    batches = new OutgoingBatch[numReceivers * 2];
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
    numRecordsBeforeFlush = calculateBucketSize(incoming);
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
    partitionIndices = incoming.getValueAccessorById(NullableIntVector.class, typedFieldId.getFieldIds()[0]).getValueVector();
  }

  /**
   * setup all outgoing batches and modLookup
   */
  private void initBatchesAndLookup(VectorAccessible incoming) {
    final BufferAllocator allocator = context.getAllocator();
    final List<MinorFragmentEndpoint> destinations = config.getDestinations();
    for (int p = 0; p < numReceivers; p++) {
      final int batchB = numReceivers + p;

      final MinorFragmentEndpoint destination = destinations.get(p);
      final AccountingExecTunnel tunnel = tunnelProvider.getExecTunnel(destination.getEndpoint());

      batches[p] = new OutgoingBatch(p, batchB, numRecordsBeforeFlush, incoming, allocator, tunnel, config, context, destination.getId(), stats);
      batches[batchB] = new OutgoingBatch(batchB, p, numRecordsBeforeFlush, incoming, allocator, tunnel, config, context, destination.getId(), stats);

      // Only allocate the primary batch. Backup batch is allocated when it is needed.
      batches[p].allocateNew();

      modLookup[p] = batches[p];
      modLookup[batchB] = batches[p];
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
      final long addr = copyIndices.getBuffer().memoryAddress();
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
        if (batches == null) {
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
    for (MinorFragmentEndpoint destination : config.getDestinations()) {
      // don't send termination message if the receiver fragment is already terminated.
      if (remainingReceivers.get(destination.getId()) == 0) {
        ExecRPC.FragmentStreamComplete completion = ExecRPC.FragmentStreamComplete.newBuilder()
            .setQueryId(handle.getQueryId())
            .setSendingMajorFragmentId(handle.getMajorFragmentId())
            .setSendingMinorFragmentId(handle.getMinorFragmentId())
            .setReceivingMajorFragmentId(config.getOppositeMajorFragmentId())
            .addReceivingMinorFragmentId(destination.getId())
            .build();
        tunnelProvider.getExecTunnel(destination.getEndpoint()).sendStreamComplete(completion);
      }
    }
    stats.stopWait();
  }

  private void generateCopyIndices(final int start, final int numRowsToCopy) {
    long srcAddr = partitionIndices.getBuffer().memoryAddress() + start*4;
    long dstAddr = copyIndices.getBuffer().memoryAddress();

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
        modLookup[batch.getBatchIdx()] = nextBatch;
        modLookup[nextBatchIdx] = nextBatch;
      }
    }
  }

  /**
   * Calculate the target bucket size. If option <i>exec.partitioner.batch.adaptive</i> is set we take a fixed value set
   * in option (<i>exec.partitioner.batch.records</i>). Otherwise we calculate the batch size based on record size.
   * @param incoming
   * @return
   */
  private int calculateBucketSize(VectorAccessible incoming) {
    final OptionManager options = context.getOptions();
    final int configuredTargetRecordCount = (int)context.getOptions().getOption(ExecConstants.TARGET_BATCH_RECORDS_MAX);
    if (!options.getOption(ExecConstants.PARTITION_SENDER_BATCH_ADAPTIVE)) {
      return configuredTargetRecordCount;
    }

    final int listSizeEstimate = (int) options.getOption(ExecConstants.BATCH_LIST_SIZE_ESTIMATE);
    final int varFieldSizeEstimate = (int) options.getOption(ExecConstants.BATCH_VARIABLE_FIELD_SIZE_ESTIMATE);
    final int estimatedRecordSize = incoming.getSchema().estimateRecordSize(listSizeEstimate, varFieldSizeEstimate);

    final int minTargetBucketRecordCount = (int) options.getOption(ExecConstants.TARGET_BATCH_RECORDS_MIN);
    final int targetOutgoingBatchSize = (int) Math.min(
        options.getOption(ExecConstants.PARTITION_SENDER_MAX_BATCH_SIZE),
        options.getOption(ExecConstants.PARTITION_SENDER_MAX_MEM) / numReceivers);

    final int newBucketSize = Math.min(configuredTargetRecordCount,
        Math.max(targetOutgoingBatchSize/estimatedRecordSize, minTargetBucketRecordCount));

    return Math.max(1, Numbers.nextPowerOfTwo(newBucketSize) - 1);
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(Arrays.asList(batches), Collections.singletonList(copyIndices));
  }

  @Override
  public <OUT, IN, EXCEP extends Throwable> OUT accept(OperatorVisitor<OUT, IN, EXCEP> visitor, IN value) throws EXCEP {
    return visitor.visitTerminalOperator(this, value);
  }

}
