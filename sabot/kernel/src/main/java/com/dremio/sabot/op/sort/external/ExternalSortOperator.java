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
package com.dremio.sabot.op.sort.external;

import static com.dremio.exec.ExecConstants.ENABLE_SPILLABLE_OPERATORS;

import com.dremio.exec.physical.config.ExternalSort;
import com.dremio.exec.proto.CoordExecRPC;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.ExecProtos.ExtSortSpillNotificationMessage;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators.BooleanValidator;
import com.dremio.options.TypeValidators.DoubleValidator;
import com.dremio.options.TypeValidators.RangeDoubleValidator;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.exec.fragment.OutOfBandMessage;
import com.dremio.sabot.op.sort.external.VectorSorter.SortState;
import com.dremio.sabot.op.spi.Operator.ShrinkableOperator;
import com.dremio.sabot.op.spi.SingleInputOperator;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;

/**
 * Primary algorithm:
 *
 * <p>Multiple allocators: - incoming data - one allocator per batch - simpleIntVector or treeVector
 * - outputCopier (always holds reservation at least as much as x) -
 *
 * <p>Each record batch group that comes in is: - tracked in a QuickSorter and the whole set is
 * sorted at the time a final list is requested (default mode) - each batch is locally sorter, then
 * added to a SplayTreeSorter of sv4 values (sv4), the SplayTree is traversed when the final list is
 * requested (if SplaySort is enabled) - (in either sort method case, the data-buffers used to track
 * the row-indices in the batches are resized as new batches come in.)
 *
 * <p>We ensure that we always have a reservation of at least maxBatchSize. This guarantees that we
 * can spill.
 *
 * <p>We spill to disk if any of the following conditions occurs: - OutOfMemory or Can't add max
 * batch size reservation or Hit 64k batches
 *
 * <p>When we spill, we do batch copies (guaranteed successful since we previously reserved memory
 * to do the spills) to generate a new spill run. A DiskRun is written to disk.
 *
 * <p>Once spilled to disk, we keep track of each spilled run (including the maximum batch size of
 * that run).
 *
 * <p>Once we complete a number of runs, we determine a merge plan to complete the data merges. (For
 * now, the merge plan is always a simple priority queue n-way merge where n is the final number of
 * disk runs.)
 */
@Options
public class ExternalSortOperator implements SingleInputOperator, ShrinkableOperator {
  public static final BooleanValidator OOB_SORT_TRIGGER_ENABLED =
      new BooleanValidator("exec.operator.sort.oob_trigger_enabled", true);
  public static final DoubleValidator OOB_SORT_SPILL_TRIGGER_FACTOR =
      new RangeDoubleValidator("exec.operator.sort.oob_trigger_factor", 0.0d, 10.0d, .75d);
  public static final DoubleValidator OOB_SORT_SPILL_TRIGGER_HEADROOM_FACTOR =
      new RangeDoubleValidator("exec.operator.sort.oob_trigger_headroom_factor", 0.0d, 10.0d, .2d);

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(ExternalSortOperator.class);

  private final OperatorContext context;
  private final ExternalSort config;
  private VectorSorter vectorSorter;

  private BufferAllocator allocator;
  public static boolean oobSpillNotificationsEnabled;

  public int oobSends;
  public int oobReceives;
  public int oobDropLocal;
  public int oobDropWrongState;
  public int oobDropUnderThreshold;
  public int oobSpills;
  private final Stopwatch produceDataWatch = Stopwatch.createUnstarted();
  private final Stopwatch consumeDataWatch = Stopwatch.createUnstarted();
  private final Stopwatch noMoreToConsumeWatch = Stopwatch.createUnstarted();

  @Override
  public State getState() {
    return vectorSorter.getState();
  }

  public ExternalSortOperator(OperatorContext context, ExternalSort popConfig)
      throws OutOfMemoryException {
    this.context = context;
    this.config = popConfig;
    this.vectorSorter =
        new VectorSorter(
            context,
            this::notifyOthersOfSpill,
            config.getOrderings(),
            config.getProps().getLocalOperatorId());
    this.allocator = context.getAllocator();
    // not sending oob spill notifications to sibling minor fragments when MemoryArbiter is ON
    oobSpillNotificationsEnabled =
        !(context.getOptions().getOption(ENABLE_SPILLABLE_OPERATORS))
            && context.getOptions().getOption(OOB_SORT_TRIGGER_ENABLED);
  }

  @Override
  public VectorAccessible setup(VectorAccessible incoming) {
    return vectorSorter.setup(incoming, true);
  }

  @Override
  public void close() throws Exception {
    /**
     * 'diskRuns' holds a ref to a VectorContainer, which is created by 'memoryRun'. Thus,'diskRuns'
     * must be closed before 'memoryRun' so that all the buffers referred in the VectorContainer
     * etc. are released first. Otherwise 'memoryRun' close would fail reporting memory leak.
     */
    vectorSorter.close();
    addDisplayStatsWithZeroValue(context, EnumSet.allOf(ExternalSortStats.Metric.class));
    updateStats(true);
  }

  @VisibleForTesting
  public void setStateToCanConsume() {
    this.vectorSorter.setStateToCanConsume();
  }

  @VisibleForTesting
  public void setStates(State masterState, SortState sortState) {
    this.vectorSorter.setStates(masterState, sortState);
  }

  @Override
  public void consumeData(int records) throws Exception {
    consumeDataWatch.start();
    vectorSorter.consumeData(records);
    consumeDataWatch.stop();
    updateStats(false);
  }

  @Override
  public void noMoreToConsume() throws Exception {
    noMoreToConsumeWatch.start();
    vectorSorter.noMoreToConsume();
    noMoreToConsumeWatch.stop();
    updateStats(false);
  }

  @Override
  public int getOperatorId() {
    return this.config.getProps().getLocalOperatorId();
  }

  @Override
  public long shrinkableMemory() {
    return vectorSorter.shrinkableMemory();
  }

  @Override
  public boolean shrinkMemory(long size) throws Exception {
    return vectorSorter.shrinkMemory(size);
  }

  /**
   * Printing operator state for debug logs
   *
   * @return
   */
  @Override
  public String getOperatorStateToPrint() {
    return vectorSorter.getOperatorStateToPrint();
  }

  @Override
  public int outputData() throws Exception {
    produceDataWatch.start();
    vectorSorter.getState().is(State.CAN_PRODUCE);

    if (vectorSorter.handleIfSpillInProgress()) {
      produceDataWatch.stop();
      return 0;
    }

    if (vectorSorter.handleIfCannotCopy()) {
      produceDataWatch.stop();
      updateStats(false);
      return 0;
    }

    int noOfOutputRecords = vectorSorter.outputData();
    produceDataWatch.stop();
    return noOfOutputRecords;
  }

  /** When this operator starts spilling, notify others if the triggering is enabled. */
  private void notifyOthersOfSpill() {
    if (!oobSpillNotificationsEnabled) {
      return;
    }

    try {
      OutOfBandMessage.Payload payload =
          new OutOfBandMessage.Payload(
              ExtSortSpillNotificationMessage.newBuilder()
                  .setMemoryUse(allocator.getAllocatedMemory())
                  .build());
      for (CoordExecRPC.FragmentAssignment a : context.getAssignments()) {
        OutOfBandMessage message =
            new OutOfBandMessage(
                context.getFragmentHandle().getQueryId(),
                context.getFragmentHandle().getMajorFragmentId(),
                a.getMinorFragmentIdList(),
                config.getProps().getOperatorId(),
                context.getFragmentHandle().getMinorFragmentId(),
                payload,
                true);

        NodeEndpoint endpoint = context.getEndpointsIndex().getNodeEndpoint(a.getAssignmentIndex());
        context.getTunnelProvider().getExecTunnel(endpoint).sendOOBMessage(message);
      }
      oobSends++;
      updateStats(false);
      if (logger.isDebugEnabled()) {
        logger.debug(
            "notifyOthersOfSpill allocated memory {}. headroom {} oobsends {} oobreceives {}",
            allocator.getAllocatedMemory(),
            allocator.getHeadroom(),
            oobSends,
            oobReceives);
      }
    } catch (Exception ex) {
      logger.warn("Failure while attempting to notify others of spilling.", ex);
    }
  }

  /**
   * When a out of band message arrives, spill if we're within a factor of the other operator that
   * is spilling.
   */
  @Override
  public void workOnOOB(OutOfBandMessage message) {
    ++oobReceives;

    // ignore self notification.
    if (message.getSendingMinorFragmentId() == context.getFragmentHandle().getMinorFragmentId()) {
      oobDropLocal++;
      return;
    }

    if (vectorSorter.getState() != State.CAN_CONSUME) {
      oobDropWrongState++;
      return;
    }

    if (logger.isDebugEnabled()) {
      logger.debug(
          "workOnOOB allocated memory {}. headroom {} oobsends {} oobreceives {}",
          allocator.getAllocatedMemory(),
          allocator.getHeadroom(),
          oobSends,
          oobReceives);
    }

    // check to see if we're at the point where we want to spill.
    final ExtSortSpillNotificationMessage spill =
        message.getPayload(ExtSortSpillNotificationMessage.PARSER);
    final long allocatedMemoryBeforeSpilling = allocator.getAllocatedMemory();
    final double triggerFactor = context.getOptions().getOption(OOB_SORT_SPILL_TRIGGER_FACTOR);
    final double headroomRemaining =
        allocator.getHeadroom() * 1.0d / (allocator.getHeadroom() + allocator.getAllocatedMemory());
    if (allocatedMemoryBeforeSpilling < (spill.getMemoryUse() * triggerFactor)
        && headroomRemaining
            > context.getOptions().getOption(OOB_SORT_SPILL_TRIGGER_HEADROOM_FACTOR)) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "Skipping OOB spill trigger, current allocation is {}, which is not within the current factor of the spilling operator ({}) which has memory use of {}. Headroom is at {} which is greater than trigger headroom of {}",
            allocatedMemoryBeforeSpilling,
            triggerFactor,
            spill.getMemoryUse(),
            headroomRemaining,
            context.getOptions().getOption(OOB_SORT_SPILL_TRIGGER_HEADROOM_FACTOR));
      }
      oobDropUnderThreshold++;
      return;
    }

    ++oobSpills;
    updateStats(false);

    if (vectorSorter.isMicroSpillEnabled()) {
      vectorSorter.startMicroSpilling();
    } else {
      vectorSorter.rotateRuns();
    }
  }

  public void updateStats(boolean closed) {
    OperatorStats stats = context.getStats();
    vectorSorter.updateStats(closed);

    Map<String, Long> vectorSorterStats = vectorSorter.getStats(closed);

    if (!closed) {
      if (vectorSorter.memoryStatsAvailable()) {
        stats.setLongStat(
            ExternalSortStats.Metric.PEAK_BATCHES_IN_MEMORY,
            vectorSorterStats.get(ExternalSortStats.Metric.PEAK_BATCHES_IN_MEMORY.name()));
        stats.setLongStat(ExternalSortStats.Metric.OOB_SENDS, oobSends);
        stats.setLongStat(ExternalSortStats.Metric.OOB_RECEIVES, oobReceives);
        stats.setLongStat(ExternalSortStats.Metric.OOB_DROP_LOCAL, oobDropLocal);
        stats.setLongStat(ExternalSortStats.Metric.OOB_DROP_WRONG_STATE, oobDropWrongState);
        stats.setLongStat(ExternalSortStats.Metric.OOB_DROP_UNDER_THRESHOLD, oobDropUnderThreshold);
        stats.setLongStat(ExternalSortStats.Metric.OOB_SPILL, oobSpills);
      }
    }

    if (vectorSorter.diskStatsAvailable()) {
      stats.setLongStat(
          ExternalSortStats.Metric.SPILL_COUNT,
          vectorSorterStats.get(ExternalSortStats.Metric.SPILL_COUNT.name()));
      stats.setLongStat(
          ExternalSortStats.Metric.MERGE_COUNT,
          vectorSorterStats.get(ExternalSortStats.Metric.MERGE_COUNT.name()));
      stats.setLongStat(
          ExternalSortStats.Metric.MAX_BATCH_SIZE,
          vectorSorterStats.get(ExternalSortStats.Metric.MAX_BATCH_SIZE.name()));
      stats.setLongStat(
          ExternalSortStats.Metric.AVG_BATCH_SIZE,
          vectorSorterStats.get(ExternalSortStats.Metric.AVG_BATCH_SIZE.name()));
      stats.setLongStat(
          ExternalSortStats.Metric.SPILL_TIME_NANOS,
          vectorSorterStats.get(ExternalSortStats.Metric.SPILL_TIME_NANOS.name()));
      stats.setLongStat(
          ExternalSortStats.Metric.MERGE_TIME_NANOS,
          vectorSorterStats.get(ExternalSortStats.Metric.MERGE_TIME_NANOS.name()));
      stats.setLongStat(
          ExternalSortStats.Metric.BATCHES_SPILLED,
          vectorSorterStats.get(ExternalSortStats.Metric.BATCHES_SPILLED.name()));
      stats.setLongStat(
          ExternalSortStats.Metric.UNCOMPRESSED_BYTES_READ,
          vectorSorterStats.get(ExternalSortStats.Metric.UNCOMPRESSED_BYTES_READ.name()));
      stats.setLongStat(
          ExternalSortStats.Metric.UNCOMPRESSED_BYTES_WRITTEN,
          vectorSorterStats.get(ExternalSortStats.Metric.UNCOMPRESSED_BYTES_WRITTEN.name()));
      stats.setLongStat(
          ExternalSortStats.Metric.IO_BYTES_READ,
          vectorSorterStats.get(ExternalSortStats.Metric.IO_BYTES_READ.name()));
      stats.setLongStat(
          ExternalSortStats.Metric.TOTAL_SPILLED_DATA_SIZE,
          vectorSorterStats.get(ExternalSortStats.Metric.TOTAL_SPILLED_DATA_SIZE.name()));
      // if we use the old encoding path, we don't get the io bytes so we'll behave similar to
      // legacy, reporting pre-compressed size.
      stats.setLongStat(
          ExternalSortStats.Metric.IO_BYTES_WRITTEN,
          vectorSorterStats.get(ExternalSortStats.Metric.IO_BYTES_WRITTEN.name()));
      stats.setLongStat(
          ExternalSortStats.Metric.COMPRESSION_NANOS,
          vectorSorterStats.get(ExternalSortStats.Metric.COMPRESSION_NANOS.name()));
      stats.setLongStat(
          ExternalSortStats.Metric.DECOMPRESSION_NANOS,
          vectorSorterStats.get(ExternalSortStats.Metric.DECOMPRESSION_NANOS.name()));
      stats.setLongStat(
          ExternalSortStats.Metric.IO_READ_WAIT_NANOS,
          vectorSorterStats.get(ExternalSortStats.Metric.IO_READ_WAIT_NANOS.name()));
      stats.setLongStat(
          ExternalSortStats.Metric.IO_WRITE_WAIT_NANOS,
          vectorSorterStats.get(ExternalSortStats.Metric.IO_WRITE_WAIT_NANOS.name()));
      stats.setLongStat(
          ExternalSortStats.Metric.OOM_ALLOCATE_COUNT,
          vectorSorterStats.get(ExternalSortStats.Metric.OOM_ALLOCATE_COUNT.name()));
      stats.setLongStat(
          ExternalSortStats.Metric.OOM_COPY_COUNT,
          vectorSorterStats.get(ExternalSortStats.Metric.OOM_COPY_COUNT.name()));
      stats.setLongStat(
          ExternalSortStats.Metric.SPILL_COPY_NANOS,
          vectorSorterStats.get(ExternalSortStats.Metric.SPILL_COPY_NANOS.name()));
    }
    stats.setLongStat(
        ExternalSortStats.Metric.CAN_PRODUCE_MILLIS,
        produceDataWatch.elapsed(TimeUnit.MILLISECONDS));
    stats.setLongStat(
        ExternalSortStats.Metric.CAN_CONSUME_MILLIS,
        consumeDataWatch.elapsed(TimeUnit.MILLISECONDS));
    stats.setLongStat(
        ExternalSortStats.Metric.SETUP_MILLIS,
        vectorSorterStats.get(ExternalSortStats.Metric.SETUP_MILLIS.name()));
    stats.setLongStat(
        ExternalSortStats.Metric.NO_MORE_TO_CONSUME_MILLIS,
        noMoreToConsumeWatch.elapsed(TimeUnit.MILLISECONDS));
  }

  @Override
  public <OUT, IN, EXCEP extends Throwable> OUT accept(
      OperatorVisitor<OUT, IN, EXCEP> visitor, IN value) throws EXCEP {
    return visitor.visitSingleInput(this, value);
  }
}
